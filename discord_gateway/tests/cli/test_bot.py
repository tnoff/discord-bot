'''Tests for the gateway's CLI entrypoint (discord_gateway.cli.bot).'''
import logging as stdlib_logging
import asyncio
import signal
from tempfile import NamedTemporaryFile
from unittest.mock import MagicMock, AsyncMock

from click.testing import CliRunner
import pytest
from yaml import dump


from discord_gateway.cli.bot import main, main_loop

from tests.helpers import fake_bot_yielder, FakeGuild
from tests.pod_cli_support import invoke_main_with_health_server, invoke_with_config


def test_run_with_no_args():
    '''
    Throw error with no config options
    '''
    runner = CliRunner()
    result = runner.invoke(main, [])
    assert "Error: Missing argument 'CONFIG_FILE'" in result.output


def test_run_no_file():
    '''
    Test with no config file
    '''
    with NamedTemporaryFile() as temp_config:
        runner = CliRunner()
        result = runner.invoke(main, [temp_config.name])
        assert 'General config section required' in str(result.exception)


def test_run_config_but_no_data():
    '''
    An empty general config is now schema-valid (discord_token is optional so
    gateway-less broker/downloader don't need it), but the bot CLI still refuses to
    run without its required HA wiring / token.
    '''
    with NamedTemporaryFile(suffix='.yml') as temp_config:
        config_data = {
            'general': {},
        }
        with open(temp_config.name, 'w', encoding='utf-8') as writer:
            dump(config_data, writer)
        runner = CliRunner()
        result = runner.invoke(main, [temp_config.name])
        assert result.exit_code == 1
        assert 'required' in str(result.exception)


@pytest.mark.asyncio
async def test_run_config_only_token(mocker):
    '''
    Run with only token
    '''
    with NamedTemporaryFile(suffix='.yml') as temp_config:
        config_data = {
            'general': {
                'discord_token': 'foo',
                'dispatch_http_url': 'http://localhost:8082',
                'commands_http_url': 'http://localhost:8086',
                'database_http_url': 'http://localhost:8085',
            },
        }
        with open(temp_config.name, 'w', encoding='utf-8') as writer:
            dump(config_data, writer)
        mocker.patch('discord_core.cli._lib.gateway.Bot', side_effect=fake_bot_yielder(guilds=[]))
        runner = CliRunner()
        result = runner.invoke(main, [temp_config.name])
        await asyncio.sleep(.01)
        assert result.exception is None


@pytest.mark.asyncio
async def test_run_config_reject_list(mocker):
    '''
    Leave server within rejectlist
    '''
    with NamedTemporaryFile(suffix='.yml') as temp_config:
        fake_guild = FakeGuild()
        guilds = [fake_guild]
        config_data = {
            'general': {
                'discord_token': 'foo',
                'dispatch_http_url': 'http://localhost:8082',
                'commands_http_url': 'http://localhost:8086',
                'database_http_url': 'http://localhost:8085',
                'rejectlist_guilds': [
                    fake_guild.id,
                ],
            }
        }
        with open(temp_config.name, 'w', encoding='utf-8') as writer:
            dump(config_data, writer)

        mocker.patch('discord_core.cli._lib.gateway.Bot', side_effect=fake_bot_yielder(guilds=guilds))
        runner = CliRunner()
        runner.invoke(main, [temp_config.name])
        await asyncio.sleep(.01)
        assert guilds[0].left_guild is True


@pytest.mark.asyncio
async def test_run_config_no_reject_list(mocker):
    '''
    Run config with no checklist
    '''
    with NamedTemporaryFile(suffix='.yml') as temp_config:
        config_data = {
            'general': {
                'discord_token': 'foo',
                'dispatch_http_url': 'http://localhost:8082',
                'commands_http_url': 'http://localhost:8086',
                'database_http_url': 'http://localhost:8085',
            }
        }
        with open(temp_config.name, 'w', encoding='utf-8') as writer:
            dump(config_data, writer)
        guilds = [FakeGuild()]
        mocker.patch('discord_core.cli._lib.gateway.Bot', side_effect=fake_bot_yielder(guilds=guilds))
        runner = CliRunner()
        runner.invoke(main, [temp_config.name])
        await asyncio.sleep(.01)
        assert guilds[0].left_guild is False


@pytest.mark.asyncio
async def test_run_config_with_intents(mocker):
    '''
    Run config with intents
    '''
    with NamedTemporaryFile(suffix='.yml') as temp_config:
        config_data = {
            'general': {
                'discord_token': 'foo',
                'dispatch_http_url': 'http://localhost:8082',
                'commands_http_url': 'http://localhost:8086',
                'database_http_url': 'http://localhost:8085',
                'intents': [
                    'members',
                ]
            },
        }
        with open(temp_config.name, 'w', encoding='utf-8') as writer:
            dump(config_data, writer)
        mocker.patch('discord_core.cli._lib.gateway.Bot', side_effect=fake_bot_yielder(guilds=[]))
        runner = CliRunner()
        result = runner.invoke(main, [temp_config.name])
        await asyncio.sleep(.01)
        assert result.exception is None


@pytest.mark.asyncio(loop_scope="session")
async def test_keyboard_interrupt_calls_cog_unload():
    '''
    Test that KeyboardInterrupt triggers cog_unload on all cogs
    '''
    # Create a fake cog that tracks if cog_unload was called
    class FakeCog:
        def __init__(self):
            self.cog_unload_called = False
            self.cog_unload_call_count = 0

        async def cog_unload(self):
            self.cog_unload_called = True
            self.cog_unload_call_count += 1

    # Create a fake bot that raises KeyboardInterrupt when start is called
    class FakeBotWithInterrupt:
        def __init__(self, *_args, **_kwargs):
            self.startup_functions = []
            self.bot_closed = False
            self.close_called = False

        def event(self, func):
            self.startup_functions.append(func)

        def is_closed(self):
            return self.bot_closed

        async def start(self, token): #pylint:disable=unused-argument
            # Call startup functions first
            for func in self.startup_functions:
                await func()
            # Then raise KeyboardInterrupt
            raise KeyboardInterrupt('Simulated Ctrl+C')

        async def __aenter__(self):
            return self

        async def __aexit__(self, *args):
            pass

        async def add_cog(self, cog):
            pass

        async def close(self):
            self.close_called = True
            self.bot_closed = True

    # Create fake cogs
    cog1 = FakeCog()
    cog2 = FakeCog()
    cog_list = [cog1, cog2]

    # Create fake bot
    bot = FakeBotWithInterrupt()

    # Run main_loop which should catch KeyboardInterrupt and call cog_unload
    await main_loop(bot, cog_list, 'fake-token')

    # Verify cog_unload was called on all cogs
    assert cog1.cog_unload_called is True
    assert cog1.cog_unload_call_count == 1
    assert cog2.cog_unload_called is True
    assert cog2.cog_unload_call_count == 1

    # Verify bot.close() was called
    assert bot.close_called is True


@pytest.mark.asyncio(loop_scope="session")
async def test_sigterm_calls_cog_unload():
    '''
    Test that SIGTERM (Docker stop) triggers cog_unload on all cogs
    '''
    # Create a fake cog that tracks if cog_unload was called
    class FakeCog:
        def __init__(self):
            self.cog_unload_called = False
            self.cog_unload_call_count = 0

        async def cog_unload(self):
            self.cog_unload_called = True
            self.cog_unload_call_count += 1

    # Create a fake bot that will receive SIGTERM
    class FakeBotWithSignal:
        def __init__(self, *_args, **_kwargs):
            self.startup_functions = []
            self.bot_closed = False
            self.close_called = False
            self.started = False

        def event(self, func):
            self.startup_functions.append(func)

        def is_closed(self):
            return self.bot_closed

        async def start(self, token): #pylint:disable=unused-argument
            # Call startup functions first
            for func in self.startup_functions:
                await func()
            self.started = True
            # Simulate bot running, then receiving SIGTERM
            await asyncio.sleep(0.01)  # Give signal handler time to register
            # Send SIGTERM to ourselves
            signal.raise_signal(signal.SIGTERM)
            # Wait a bit for signal to be processed
            await asyncio.sleep(0.1)

        async def __aenter__(self):
            return self

        async def __aexit__(self, *args):
            pass

        async def add_cog(self, cog):
            pass

        async def close(self):
            self.close_called = True
            self.bot_closed = True

    # Create fake cogs
    cog1 = FakeCog()
    cog2 = FakeCog()
    cog_list = [cog1, cog2]

    # Create fake bot
    bot = FakeBotWithSignal()

    # Run main_loop which should catch SIGTERM and call cog_unload
    await main_loop(bot, cog_list, 'fake-token')

    # Verify bot was started
    assert bot.started is True

    # Verify cog_unload was called on all cogs
    assert cog1.cog_unload_called is True
    assert cog1.cog_unload_call_count == 1
    assert cog2.cog_unload_called is True
    assert cog2.cog_unload_call_count == 1

    # Verify bot.close() was called
    assert bot.close_called is True


@pytest.mark.asyncio(loop_scope="session")
async def test_main_loop_generic_exception(mocker):
    '''main_loop returns early (line 145) when bot.start() raises a non-KeyboardInterrupt exception'''
    class _FakeBotGenericError:
        def event(self, _func):
            pass
        def is_closed(self):
            return False
        async def start(self, _token):
            raise RuntimeError('unexpected error')
        async def __aenter__(self):
            return self
        async def __aexit__(self, *_args):
            pass
        async def add_cog(self, _cog):
            pass
        async def close(self):
            pass

    # The bad format string at cli.py:144 (logger.debug('...', str(e))) would trigger
    # handleError which can surface as an exception in pytest's capture context.
    # Patch 'main' logger to avoid the pre-existing production-code formatting bug.
    mocker.patch.object(stdlib_logging.getLogger('main'), 'debug')
    # Should return without propagating the exception
    await main_loop(_FakeBotGenericError(), [], 'token')


@pytest.mark.asyncio(loop_scope="session")
async def test_main_loop_cog_unload_exception():
    '''main_loop logs exception (lines 154-155) when cog_unload raises during shutdown'''
    class _FakeCogWithRaise:
        async def cog_unload(self):
            raise ValueError('unload error')

    class _FakeBotInterrupt:
        def event(self, _func):
            pass
        def is_closed(self):
            return False
        async def start(self, _token):
            raise KeyboardInterrupt()
        async def __aenter__(self):
            return self
        async def __aexit__(self, *_args):
            pass
        async def add_cog(self, _cog):
            pass
        async def close(self):
            pass

    # Should complete without propagating the exception from cog_unload
    await main_loop(_FakeBotInterrupt(), [_FakeCogWithRaise()], 'token')


@pytest.mark.asyncio(loop_scope="session")
async def test_main_loop_with_health_server():
    '''main_loop creates a task for health_server.serve() when health_server is not None (line 138)'''
    mock_health_server = MagicMock()
    mock_health_server.serve = AsyncMock()

    class _FakeBotQuick:
        def event(self, _func):
            pass
        def is_closed(self):
            return True
        async def start(self, _token):
            pass
        async def __aenter__(self):
            return self
        async def __aexit__(self, *_args):
            pass
        async def add_cog(self, _cog):
            pass
        async def close(self):
            pass

    await main_loop(_FakeBotQuick(), [], 'token', health_server=mock_health_server)
    mock_health_server.serve.assert_called_once()


@pytest.mark.asyncio(loop_scope="session")
async def test_main_loop_second_signal_noop():
    '''Second signal while shutdown is already triggered hits the early return (line 121)'''
    class _FakeBotDoubleSignal:
        def __init__(self):
            self.close_called = 0
            self.bot_closed = False

        def event(self, _func):
            pass
        def is_closed(self):
            return self.bot_closed

        async def start(self, _token):
            await asyncio.sleep(0.01)
            signal.raise_signal(signal.SIGTERM)  # first — sets shutdown_triggered
            await asyncio.sleep(0.01)
            signal.raise_signal(signal.SIGTERM)  # second — hits line 121 (early return)
            await asyncio.sleep(0.05)

        async def __aenter__(self):
            return self
        async def __aexit__(self, *_args):
            pass
        async def add_cog(self, _cog):
            pass

        async def close(self):
            self.close_called += 1
            self.bot_closed = True

    bot = _FakeBotDoubleSignal()
    await main_loop(bot, [], 'token')
    # close() called exactly once; second signal was a no-op
    assert bot.close_called == 1


def test_main_runner_no_event_loop(mocker):
    '''main_runner falls through to asyncio.run (lines 318-319, 325-326) when no loop is running'''
    with NamedTemporaryFile(suffix='.yml') as temp_config:
        config_data = {'general': {'discord_token': 'foo', 'dispatch_http_url': 'http://localhost:8082',
                                   'commands_http_url': 'http://localhost:8086',
                                      'database_http_url': 'http://localhost:8085'}}
        with open(temp_config.name, 'w', encoding='utf-8') as writer:
            dump(config_data, writer)
        mocker.patch('discord_core.cli._lib.gateway.Bot', side_effect=fake_bot_yielder(guilds=[]))
        runner = CliRunner()
        result = runner.invoke(main, [temp_config.name])
        assert result.exception is None


def _patch_otlp(mocker):
    '''Patch all OpenTelemetry symbols to avoid real network connections'''
    for name in [
        'TracerProvider', 'RequestsInstrumentor', 'RedisInstrumentor',
        'OTLPSpanExporter', 'BatchSpanProcessor', 'get_aggregated_resources',
        'OTELResourceDetector', 'OTLPMetricExporter', 'PeriodicExportingMetricReader',
        'MeterProvider', 'set_meter_provider', 'LoggerProvider', 'set_logger_provider',
        'OTLPLogExporter', 'BatchLogRecordProcessor',
    ]:
        mocker.patch(f'discord_core.cli._lib.common.{name}')
    mocker.patch('discord_core.cli._lib.common.trace')
    # LoggingHandler mock is added to the root logger; .level must be an int or
    # callHandlers() raises TypeError on "record.levelno >= hdlr.level"
    mock_handler = MagicMock(spec=stdlib_logging.Handler)
    mock_handler.level = stdlib_logging.NOTSET
    mocker.patch('discord_core.cli._lib.common.LoggingHandler', return_value=mock_handler)


@pytest.mark.asyncio
async def test_main_with_otlp_enabled(mocker):
    '''setup_otlp wires the exporters and attaches the batch span processor'''
    with NamedTemporaryFile(suffix='.yml') as temp_config:
        config_data = {
            'general': {
                'discord_token': 'foo',
                'dispatch_http_url': 'http://localhost:8082',
                'commands_http_url': 'http://localhost:8086',
                'database_http_url': 'http://localhost:8085',
                'monitoring': {'otlp': {'enabled': True}},
            }
        }
        with open(temp_config.name, 'w', encoding='utf-8') as writer:
            dump(config_data, writer)
        _patch_otlp(mocker)
        mocker.patch('discord_core.cli._lib.gateway.Bot', side_effect=fake_bot_yielder(guilds=[]))
        runner = CliRunner()
        result = runner.invoke(main, [temp_config.name])
        await asyncio.sleep(.01)
        assert result.exception is None


@pytest.mark.asyncio
async def test_main_with_otlp_retired_filter_keys_ignored(mocker):
    '''
    A ConfigMap still carrying the retired filter_high_volume_spans /
    high_volume_span_patterns keys must start clean. The collector-side filter
    deploys before those keys are removed from docker-apps, so this is the
    state every pod runs in during that window.
    '''
    with NamedTemporaryFile(suffix='.yml') as temp_config:
        config_data = {
            'general': {
                'discord_token': 'foo',
                'dispatch_http_url': 'http://localhost:8082',
                'commands_http_url': 'http://localhost:8086',
                'database_http_url': 'http://localhost:8085',
                'monitoring': {
                    'otlp': {
                        'enabled': True,
                        'filter_high_volume_spans': True,
                        'high_volume_span_patterns': [r'^utils\.retry_command_async$'],
                    },
                },
            }
        }
        with open(temp_config.name, 'w', encoding='utf-8') as writer:
            dump(config_data, writer)
        _patch_otlp(mocker)
        mocker.patch('discord_core.cli._lib.gateway.Bot', side_effect=fake_bot_yielder(guilds=[]))
        runner = CliRunner()
        result = runner.invoke(main, [temp_config.name])
        await asyncio.sleep(.01)
        assert result.exception is None


@pytest.mark.asyncio
async def test_main_with_tracing_block(mocker):
    '''
    A ConfigMap carrying the monitoring.tracing block starts clean.

    The counterpart to the retired-keys test above, over the same yaml -> click ->
    pydantic path the deployed ConfigMap actually takes. tests/checks/test_tracing_wiring.py
    proves the values reach the objects that read them; this proves the block
    parses where it is really written, with every toggle in its non-default
    position so a typo in any field name would fail here.
    '''
    with NamedTemporaryFile(suffix='.yml') as temp_config:
        config_data = {
            'general': {
                'discord_token': 'foo',
                'dispatch_http_url': 'http://localhost:8082',
                'commands_http_url': 'http://localhost:8086',
                'database_http_url': 'http://localhost:8085',
                'monitoring': {
                    'otlp': {'enabled': True},
                    'tracing': {
                        'suppress_db_probe_auto_instrumentation': False,
                        'suppress_egress_probe_auto_instrumentation': False,
                        'suppress_download_readiness_auto_instrumentation': False,
                        'trace_queue_worker_status_poll': True,
                    },
                },
            }
        }
        with open(temp_config.name, 'w', encoding='utf-8') as writer:
            dump(config_data, writer)
        _patch_otlp(mocker)
        mocker.patch('discord_core.cli._lib.gateway.Bot', side_effect=fake_bot_yielder(guilds=[]))
        runner = CliRunner()
        result = runner.invoke(main, [temp_config.name])
        await asyncio.sleep(.01)
        assert result.exception is None


@pytest.mark.asyncio
async def test_main_with_memory_profiling(mocker):
    '''Memory profiling block executes when enabled (lines 240-245)'''
    with NamedTemporaryFile(suffix='.yml') as temp_config:
        config_data = {
            'general': {
                'discord_token': 'foo',
                'dispatch_http_url': 'http://localhost:8082',
                'commands_http_url': 'http://localhost:8086',
                'database_http_url': 'http://localhost:8085',
                'monitoring': {
                    'otlp': {'enabled': False},
                    'memory_profiling': {'enabled': True},
                },
            }
        }
        with open(temp_config.name, 'w', encoding='utf-8') as writer:
            dump(config_data, writer)
        mocker.patch('discord_core.cli._lib.common.MemoryProfiler')
        mocker.patch('discord_core.cli._lib.gateway.Bot', side_effect=fake_bot_yielder(guilds=[]))
        runner = CliRunner()
        result = runner.invoke(main, [temp_config.name])
        await asyncio.sleep(.01)
        assert result.exception is None


@pytest.mark.asyncio
async def test_main_with_process_metrics(mocker):
    '''Process metrics block executes when enabled (lines 249-253)'''
    with NamedTemporaryFile(suffix='.yml') as temp_config:
        config_data = {
            'general': {
                'discord_token': 'foo',
                'dispatch_http_url': 'http://localhost:8082',
                'commands_http_url': 'http://localhost:8086',
                'database_http_url': 'http://localhost:8085',
                'monitoring': {
                    'otlp': {'enabled': False},
                    'process_metrics': {'enabled': True},
                },
            }
        }
        with open(temp_config.name, 'w', encoding='utf-8') as writer:
            dump(config_data, writer)
        mocker.patch('discord_core.cli._lib.common.ProcessMetricsProfiler')
        mocker.patch('discord_core.cli._lib.gateway.Bot', side_effect=fake_bot_yielder(guilds=[]))
        runner = CliRunner()
        result = runner.invoke(main, [temp_config.name])
        await asyncio.sleep(.01)
        assert result.exception is None


@pytest.mark.asyncio
async def test_main_with_health_server_monitoring(mocker):
    '''Health server is created and passed to main_loop when monitoring.health_server.enabled=True
    (lines 138, 296-297)'''
    with NamedTemporaryFile(suffix='.yml') as temp_config:
        config_data = {
            'general': {
                'discord_token': 'foo',
                'dispatch_http_url': 'http://localhost:8082',
                'commands_http_url': 'http://localhost:8086',
                'database_http_url': 'http://localhost:8085',
                'monitoring': {
                    'otlp': {'enabled': False},
                    'health_server': {'enabled': True},
                },
            }
        }
        with open(temp_config.name, 'w', encoding='utf-8') as writer:
            dump(config_data, writer)
        result = await invoke_main_with_health_server(
            mocker, main, temp_config.name, 'discord_gateway.cli.health.HealthServer')
        assert result.exception is None


def test_bot_run_raises_when_dispatch_http_url_missing():
    '''cli/bot.py run() raises DiscordBotException when dispatch_http_url is absent.'''
    result = invoke_with_config(main, {'general': {'discord_token': 'foo'}})
    assert 'dispatch_http_url required' in str(result.exception)


def test_bot_run_needs_no_database_url(mocker):
    '''cli/bot.py run() starts without database_http_url.

    The gateway holds no database client: the history playlist is the broker's to create when it
    opens a guild, and every other playlist, markov and analytics call is the commands pod's.
    '''
    with NamedTemporaryFile(suffix='.yml') as temp_config:
        config_data = {'general': {'discord_token': 'foo',
                                   'dispatch_http_url': 'http://localhost:8082',
                                   'commands_http_url': 'http://localhost:8086'}}
        with open(temp_config.name, 'w', encoding='utf-8') as writer:
            dump(config_data, writer)
        mocker.patch('discord_core.cli._lib.gateway.Bot', side_effect=fake_bot_yielder(guilds=[]))
        load_cogs = mocker.patch('discord_gateway.cli.bot.load_cogs', return_value=[])
        runner = CliRunner()
        result = runner.invoke(main, [temp_config.name])
        assert result.exception is None
        load_cogs.assert_called_once()


def test_bot_run_raises_when_commands_http_url_missing():
    '''cli/bot.py run() refuses to start without the commands pod's URL.

    The command stubs only forward, so a bot with nowhere to forward to would come up and answer
    every command with "unavailable"; better to fail at startup.
    '''
    result = invoke_with_config(main, {'general': {'discord_token': 'foo',
                                                   'dispatch_http_url': 'http://localhost:8082'}})
    assert 'commands_http_url required' in str(result.exception)
