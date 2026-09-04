'''
Tests for monitoring.tracing -- the config surface over the span-suppression
decisions that used to be welded into the image.

The point of this block is that flipping a toggle changes what the process
emits WITHOUT an image build, so the tests that matter are the ones asserting a
toggle changes behaviour in both positions. A test that only asserts the default
would pass just as happily against a key nothing reads: pydantic ignores unknown
keys, so an unwired option is accepted, inert, and looks configured in the
deployed ConfigMap. Those behavioural tests live next to the four call sites;
this module covers the model and the two resolvers.
'''
import pytest

from discord_bot.utils.common import (
    GeneralConfig, MonitoringTracingConfig, resolve_tracing_config,
    tracing_config_from_settings,
)


# Every field, with the value that reproduces the behaviour shipped before the
# block existed. Driven as a table so a new toggle cannot be added without
# making a deliberate statement about its default here.
SHIPPED_DEFAULTS = {
    'suppress_db_probe_auto_instrumentation': True,
    'suppress_egress_probe_auto_instrumentation': True,
    'suppress_download_readiness_auto_instrumentation': True,
    'trace_queue_worker_status_poll': False,
}


def test_defaults_reproduce_the_behaviour_that_shipped():
    '''Adding the block changes nothing on its own -- that is the whole contract.'''
    config = MonitoringTracingConfig()
    for field, expected in SHIPPED_DEFAULTS.items():
        assert getattr(config, field) is expected, field


def test_defaults_table_covers_every_field():
    '''
    Guard on the table above rather than on the model.

    Without this a new toggle could be added with a default that silently
    changes what a pod emits, and every other test here would still pass.
    '''
    assert set(MonitoringTracingConfig.model_fields) == set(SHIPPED_DEFAULTS)


@pytest.mark.parametrize('field', sorted(SHIPPED_DEFAULTS))
def test_every_toggle_accepts_the_non_default_position(field):
    '''Each field parses in the position that is not its default.'''
    flipped = not SHIPPED_DEFAULTS[field]
    config = MonitoringTracingConfig(**{field: flipped})
    assert getattr(config, field) is flipped


def test_block_is_optional_on_monitoring():
    '''A config with no tracing block loads; monitoring.tracing is then None.'''
    config = GeneralConfig(discord_token='abctoken',
                           monitoring={'otlp': {'enabled': False}})
    assert config.monitoring.tracing is None


def test_block_parses_off_a_real_general_config():
    '''The block is reachable through the real model, not just constructed directly.'''
    config = GeneralConfig(discord_token='abctoken', monitoring={
        'otlp': {'enabled': False},
        'tracing': {'suppress_db_probe_auto_instrumentation': False},
    })
    assert config.monitoring.tracing.suppress_db_probe_auto_instrumentation is False
    # Unset siblings still hold their shipped defaults.
    assert config.monitoring.tracing.trace_queue_worker_status_poll is False


@pytest.mark.parametrize('general_config', [
    None,
    GeneralConfig(discord_token='abctoken', monitoring={'otlp': {'enabled': False}}),
], ids=['no-general-config', 'no-tracing-block'])
def test_resolver_returns_defaults_when_the_block_is_absent(general_config):
    '''
    Absent config resolves to a real defaults object, never None.

    Returning None would push the same two-step None-dance onto every call site
    and invite one of them to read "not configured" as "disabled" -- which for
    the three suppress_* toggles is the opposite of the shipped behaviour.
    '''
    assert resolve_tracing_config(general_config) == MonitoringTracingConfig()


def test_resolver_returns_the_configured_block():
    '''A configured block is passed through rather than re-defaulted.'''
    general_config = GeneralConfig(discord_token='abctoken', monitoring={
        'otlp': {'enabled': False},
        'tracing': {'trace_queue_worker_status_poll': True},
    })
    assert resolve_tracing_config(general_config).trace_queue_worker_status_poll is True


@pytest.mark.parametrize('settings', [
    {},
    {'general': {}},
    {'general': {'monitoring': {}}},
    {'general': {'monitoring': {'tracing': None}}},
], ids=['empty', 'no-monitoring', 'no-tracing', 'null-tracing'])
def test_settings_resolver_defaults_on_every_absent_shape(settings):
    '''The cogs read the raw settings dict, where any level may be missing.'''
    assert tracing_config_from_settings(settings) == MonitoringTracingConfig()


def test_settings_resolver_reads_the_block():
    '''The cog path reaches the same values the cli path does.'''
    settings = {'general': {'monitoring': {'tracing': {
        'trace_queue_worker_status_poll': True,
    }}}}
    assert tracing_config_from_settings(settings).trace_queue_worker_status_poll is True


def test_settings_resolver_agrees_with_the_general_config_resolver():
    '''
    Both resolvers answer the same question and must not drift.

    The cli parses settings into a GeneralConfig and the cogs read the dict
    directly; a toggle that resolved differently across those two paths would
    give the bot pod's poller a different answer from its health server.
    '''
    block = {'suppress_egress_probe_auto_instrumentation': False,
             'trace_queue_worker_status_poll': True}
    settings = {'general': {'discord_token': 'abctoken',
                            'monitoring': {'otlp': {'enabled': False}, 'tracing': block}}}
    general_config = GeneralConfig(**settings['general'])

    assert tracing_config_from_settings(settings) == resolve_tracing_config(general_config)
