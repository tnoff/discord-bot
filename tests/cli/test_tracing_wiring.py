'''
Does each monitoring.tracing toggle actually REACH the object that reads it?

Separate from tests/utils/test_tracing_config.py, which covers the model, and
from the four behavioural tests next to the call sites, which prove each toggle
changes what is emitted. Neither of those catches the failure this module exists
for: a value that parses correctly, changes behaviour correctly when passed by
hand, and never gets passed, because an entrypoint or an intermediate
constructor drops it and a base-class default silently takes its place. That
failure has shipped here before -- it reaches prod looking configured, with the
key present in the ConfigMap and no error anywhere.

So every test below builds a REAL GeneralConfig from a config dict, calls the
real entrypoint, and asserts on the object that entrypoint constructed. The
non-default position is used throughout on purpose: asserting the default would
pass just as well against a toggle that is never read.
'''

import pytest

from discord_bot.cli import database as database_cli
from discord_bot.cli import downloader as downloader_cli
from discord_bot.cli._lib import worker_pod
from discord_bot.cogs.music import Music
from discord_bot.utils.common import GeneralConfig

from tests.helpers import fake_context, fake_engine, fake_stores  # pylint: disable=unused-import


def _general_config(tracing: dict, *, health_server=True) -> GeneralConfig:
    '''A real GeneralConfig carrying the given tracing block.'''
    monitoring = {'otlp': {'enabled': False}, 'tracing': tracing}
    if health_server:
        monitoring['health_server'] = {
            'enabled': True, 'port': 18080, 'bind_address': '127.0.0.1',
        }
    # The worker pods refuse to start without redis; harmless on the other paths.
    return GeneralConfig(discord_token='abctoken', monitoring=monitoring,
                         redis_url='redis://localhost:6379')


def _patch_db_collaborators(mocker, engine):
    '''Patch only what would talk to the outside world; leave construction real.

    Mirrors tests/cli/test_database.py's helper: managed_db yields the engine, and
    everything the pod would dial out to is stubbed, so the health server under
    assertion is the real one cli.database built.
    '''
    mocker.patch.object(database_cli, 'setup_observability')
    mocker.patch.object(database_cli, 'instrument_sqlalchemy')
    managed = mocker.patch.object(database_cli, 'managed_db')
    managed.return_value.__enter__.return_value = engine
    return mocker.patch.object(database_cli, 'run_database')


# --------------------------------------------------------------------------- #
# db probe -> DatabasePingHealthServer (the db pod only)
#
# The bot used to run this same probe on a 30s period against its own engine, so
# the toggle had to reach two construction paths. It has no engine since the
# persistence cutover -- its readiness check is a TCP probe of the db pod -- so
# the db pod is the only place a db probe span can now originate, and the only
# place the toggle is read.
# --------------------------------------------------------------------------- #

def test_db_pod_entrypoint_forwards_the_db_probe_toggle(mocker, fake_engine):  # pylint: disable=redefined-outer-name
    '''cli.database builds the health server with the configured value, not the default.

    This is the toggle the project was opened for: the probe was 100% of this
    pod's trace volume, so suppressing it left the docker-apps alert on
    database.ready_check with nothing to drill into during an incident.
    '''
    run_database = _patch_db_collaborators(mocker, fake_engine)

    general_config = _general_config({'suppress_db_probe_auto_instrumentation': False})
    database_cli.run({'general': {'database_server': {}},
                      'music': {'download': {'cache': {}}}}, general_config)

    health_server = run_database.call_args.args[1]
    assert health_server._suppress_db_probe_auto_instrumentation is False  # pylint: disable=protected-access


def test_db_pod_entrypoint_defaults_to_suppressed(mocker, fake_engine):  # pylint: disable=redefined-outer-name
    '''With no tracing block the pod keeps the behaviour that shipped.'''
    run_database = _patch_db_collaborators(mocker, fake_engine)

    general_config = GeneralConfig(discord_token='abctoken', monitoring={
        'otlp': {'enabled': False},
        'health_server': {'enabled': True, 'port': 18080, 'bind_address': '127.0.0.1'},
    })
    database_cli.run({'general': {'database_server': {}},
                      'music': {'download': {'cache': {}}}}, general_config)

    health_server = run_database.call_args.args[1]
    assert health_server._suppress_db_probe_auto_instrumentation is True  # pylint: disable=protected-access


# --------------------------------------------------------------------------- #
# downloader -> DownloadWorkerBase (egress probe + readiness peek)
# --------------------------------------------------------------------------- #

def _run_downloader(mocker, general_config):
    '''Run cli.downloader with the outside world patched; return the worker kwargs.'''
    mocker.patch.object(downloader_cli, 'setup_observability')
    mocker.patch.object(worker_pod, 'RedisManager')
    mocker.patch.object(worker_pod, 'RedisPingHealthServer')
    mocker.patch.object(downloader_cli, 'HttpBrokerClient')
    mocker.patch.object(downloader_cli, 'DownloadHttpServer')
    mocker.patch.object(downloader_cli, 'DownloadMetrics')
    mocker.patch.object(downloader_cli, 'build_exit_probe')
    mocker.patch.object(downloader_cli, 'run_downloader')
    worker_cls = mocker.patch.object(downloader_cli, 'RedisDownloadWorker')

    settings = {
        'general': {},
        'music': {
            'broker_client': {'url': 'http://broker:8081'},
            'download': {'download_dir': '/tmp'},
        },
    }
    downloader_cli.run(settings, general_config)
    return worker_cls.call_args.kwargs


def test_downloader_entrypoint_forwards_both_download_toggles(mocker):
    '''Both downloader-side toggles reach the worker constructor.

    They are keyword arguments with defaults on a base class two levels below the
    entrypoint (RedisDownloadWorker forwards **kwargs to DownloadWorkerBase), which
    is exactly the arrangement in which a dropped argument is invisible.
    '''
    general_config = _general_config({
        'suppress_egress_probe_auto_instrumentation': False,
        'suppress_download_readiness_auto_instrumentation': False,
    }, health_server=False)

    kwargs = _run_downloader(mocker, general_config)

    assert kwargs['suppress_egress_probe_auto_instrumentation'] is False
    assert kwargs['suppress_download_readiness_auto_instrumentation'] is False


def test_downloader_entrypoint_defaults_to_suppressed(mocker):
    '''No tracing block leaves both downloader toggles at the shipped behaviour.'''
    general_config = GeneralConfig(discord_token='abctoken',
                                   monitoring={'otlp': {'enabled': False}},
                                   redis_url='redis://localhost:6379')

    kwargs = _run_downloader(mocker, general_config)

    assert kwargs['suppress_egress_probe_auto_instrumentation'] is True
    assert kwargs['suppress_download_readiness_auto_instrumentation'] is True


def test_downloader_toggles_are_independent(mocker):
    '''Flipping one does not carry the other with it.

    Cheap, but it is what catches a copy-paste that wires both call sites to the
    same field -- which would read as working in every single-toggle test.
    '''
    general_config = _general_config({
        'suppress_egress_probe_auto_instrumentation': False,
    }, health_server=False)

    kwargs = _run_downloader(mocker, general_config)

    assert kwargs['suppress_egress_probe_auto_instrumentation'] is False
    assert kwargs['suppress_download_readiness_auto_instrumentation'] is True


# --------------------------------------------------------------------------- #
# poller -> the two HttpQueueWorkerClient subclasses the music cog builds
# --------------------------------------------------------------------------- #

def _music_settings(tracing: dict | None) -> dict:
    '''Music-cog settings, optionally carrying a tracing block.'''
    general: dict = {'include': {'music': True}}
    if tracing is not None:
        general['monitoring'] = {'otlp': {'enabled': False}, 'tracing': tracing}
    return {
        'general': general,
        'music': {
            'broker_client': {'url': 'http://broker-host:8081'},
            'download_client': {'url': 'http://downloader-host:8083'},
            'youtube_music_search_client': {'url': 'http://search-host:8084'},
            'media_search_client': {'url': 'http://search-host:8084'},
        },
    }


@pytest.mark.parametrize('tracing,expected', [
    ({'trace_queue_worker_status_poll': True}, True),
    ({'trace_queue_worker_status_poll': False}, False),
    (None, False),
], ids=['on', 'off', 'no-tracing-block'])
def test_music_cog_forwards_the_poller_toggle(fake_context, tracing, expected, fake_stores):  # pylint: disable=redefined-outer-name
    '''
    Both status pollers the cog owns get the configured value.

    The cog is the construction site for these two clients, so this is where the
    toggle either reaches the pod or is quietly replaced by the default -- the
    failure this fleet has actually shipped. Asserted on both clients because
    they are separate constructor calls: wiring one and forgetting the other
    leaves half the bot's poll spans unswitchable.
    '''
    cog = Music(fake_context['bot'], _music_settings(tracing),
                fake_context['dispatcher'], fake_stores)

    assert cog.download_client._trace_status_poll is expected  # pylint: disable=protected-access
    assert cog.youtube_music_search_client._trace_status_poll is expected  # pylint: disable=protected-access
