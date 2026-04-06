import asyncio
import logging as stdlib_logging
import signal
from tempfile import NamedTemporaryFile
from unittest.mock import MagicMock, AsyncMock

from click.testing import CliRunner
import pytest
from yaml import dump

from discord_bot.cli import main, main_loop, FilterOKRetrySpans, read_config

from tests.helpers import fake_bot_yielder, FakeGuild

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
    Test with empty config
    '''
    with NamedTemporaryFile(suffix='.yml') as temp_config:
        config_data = {
            'general': {},
        }
        with open(temp_config.name, 'w', encoding='utf-8') as writer:
            dump(config_data, writer)
        runner = CliRunner()
        result = runner.invoke(main, [temp_config.name])
        assert 'Invalid general config' in str(result.exception)

@pytest.mark.asyncio
async def test_run_config_only_token(mocker):
    '''
    Run with only token
    '''
    with NamedTemporaryFile(suffix='.yml') as temp_config:
        config_data = {
            'general': {
                'discord_token': 'foo'
            },
        }
        with open(temp_config.name, 'w', encoding='utf-8') as writer:
            dump(config_data, writer)
        mocker.patch('discord_bot.cli.Bot', side_effect=fake_bot_yielder(guilds=[]))
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
                'rejectlist_guilds': [
                    fake_guild.id,
                ],
            }
        }
        with open(temp_config.name, 'w', encoding='utf-8') as writer:
            dump(config_data, writer)

        mocker.patch('discord_bot.cli.Bot', side_effect=fake_bot_yielder(guilds=guilds))
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
            }
        }
        with open(temp_config.name, 'w', encoding='utf-8') as writer:
            dump(config_data, writer)
        guilds = [FakeGuild()]
        mocker.patch('discord_bot.cli.Bot', side_effect=fake_bot_yielder(guilds=guilds))
        runner = CliRunner()
        runner.invoke(main, [temp_config.name])
        await asyncio.sleep(.01)
        assert guilds[0].left_guild is False

@pytest.mark.asyncio
async def test_run_config_with_db(mocker):
    '''
    Run config with sqlite db
    '''
    with NamedTemporaryFile(suffix='.yml') as temp_config:
        with NamedTemporaryFile(suffix='.sql') as temp_db:
            config_data = {
                'general': {
                    'discord_token': 'foo',
                    'sql_connection_statement': f'sqlite:///{temp_db.name}',
                    'rejectlist_guilds': [
                        1234,
                    ],
                }
            }
            with open(temp_config.name, 'w', encoding='utf-8') as writer:
                dump(config_data, writer)
            mocker.patch('discord_bot.cli.Bot', side_effect=fake_bot_yielder(guilds=[]))
            runner = CliRunner()
            result = runner.invoke(main, [temp_config.name])
            await asyncio.sleep(.01)
            assert result.exception is None

@pytest.mark.asyncio
async def test_run_config_with_intents(mocker):
    '''
    Run config with intents
    '''
    with NamedTemporaryFile(suffix='.yml') as temp_config:
        with NamedTemporaryFile(suffix='.sql') as temp_db:
            config_data = {
                'general': {
                    'discord_token': 'foo',
                    'sql_connection_statement': f'sqlite:///{temp_db.name}',
                    'intents': [
                        'members',
                    ]
                },
            }
            with open(temp_config.name, 'w', encoding='utf-8') as writer:
                dump(config_data, writer)
            mocker.patch('discord_bot.cli.Bot', side_effect=fake_bot_yielder(guilds=[]))
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


# FilterOKRetrySpans tests
class TestFilterOKRetrySpans:
    '''Tests for the FilterOKRetrySpans span processor'''

    def test_ok_span_matching_pattern_is_filtered(self, mocker):
        '''Test that OK spans matching a pattern are not forwarded'''
        mock_processor = mocker.MagicMock()
        patterns = [r'^utils\.retry_command_async$']

        filter_proc = FilterOKRetrySpans(mock_processor, patterns)

        span = mocker.MagicMock()
        span.name = 'utils.retry_command_async'
        span.status.is_ok = True

        filter_proc.on_end(span)

        mock_processor.on_end.assert_not_called()

    def test_error_span_matching_pattern_is_forwarded(self, mocker):
        '''Test that error spans matching a pattern are still forwarded'''
        mock_processor = mocker.MagicMock()
        patterns = [r'^utils\.retry_command_async$']

        filter_proc = FilterOKRetrySpans(mock_processor, patterns)

        span = mocker.MagicMock()
        span.name = 'utils.retry_command_async'
        span.status.is_ok = False

        filter_proc.on_end(span)

        mock_processor.on_end.assert_called_once_with(span)

    def test_ok_span_not_matching_pattern_is_forwarded(self, mocker):
        '''Test that OK spans not matching any pattern are forwarded'''
        mock_processor = mocker.MagicMock()
        patterns = [r'^utils\.retry_command_async$']

        filter_proc = FilterOKRetrySpans(mock_processor, patterns)

        span = mocker.MagicMock()
        span.name = 'some.other.span'
        span.status.is_ok = True

        filter_proc.on_end(span)

        mock_processor.on_end.assert_called_once_with(span)

    def test_regex_pattern_with_wildcard(self, mocker):
        '''Test that regex patterns with wildcards work correctly'''
        mock_processor = mocker.MagicMock()
        patterns = [r'^utils\..*']  # Matches any span starting with 'utils.'

        filter_proc = FilterOKRetrySpans(mock_processor, patterns)

        # Test various spans starting with 'utils.'
        for span_name in ['utils.foo', 'utils.bar.baz', 'utils.']:
            mock_processor.reset_mock()
            span = mocker.MagicMock()
            span.name = span_name
            span.status.is_ok = True
            filter_proc.on_end(span)
            mock_processor.on_end.assert_not_called()

        # Test span not starting with 'utils.'
        mock_processor.reset_mock()
        span = mocker.MagicMock()
        span.name = 'other.utils.span'
        span.status.is_ok = True
        filter_proc.on_end(span)
        mock_processor.on_end.assert_called_once()

    def test_multiple_patterns(self, mocker):
        '''Test that multiple patterns are checked'''
        mock_processor = mocker.MagicMock()
        patterns = [
            r'^sql_retry\.retry_db_command$',
            r'^utils\.message_send_async$',
            r'.*heartbeat.*',
        ]

        filter_proc = FilterOKRetrySpans(mock_processor, patterns)

        # Each of these should be filtered
        filtered_spans = [
            'sql_retry.retry_db_command',
            'utils.message_send_async',
            'system.heartbeat.check',
            'heartbeat',
        ]

        for span_name in filtered_spans:
            mock_processor.reset_mock()
            span = mocker.MagicMock()
            span.name = span_name
            span.status.is_ok = True
            filter_proc.on_end(span)
            assert not mock_processor.on_end.called, f'Span {span_name} should have been filtered'

    def test_empty_patterns_forwards_all(self, mocker):
        '''Test that empty patterns list forwards all spans'''
        mock_processor = mocker.MagicMock()
        patterns = []

        filter_proc = FilterOKRetrySpans(mock_processor, patterns)

        span = mocker.MagicMock()
        span.name = 'any.span.name'
        span.status.is_ok = True

        filter_proc.on_end(span)

        mock_processor.on_end.assert_called_once_with(span)

    def test_on_start_forwards_to_next_processor(self, mocker):
        '''Test that on_start is forwarded to the next processor'''
        mock_processor = mocker.MagicMock()
        patterns = [r'^test$']

        filter_proc = FilterOKRetrySpans(mock_processor, patterns)

        span = mocker.MagicMock()
        parent_context = mocker.MagicMock()

        filter_proc.on_start(span, parent_context)

        mock_processor.on_start.assert_called_once_with(span, parent_context)

    def test_shutdown_forwards_to_next_processor(self, mocker):
        '''Test that shutdown is forwarded to the next processor'''
        mock_processor = mocker.MagicMock()
        patterns = []

        filter_proc = FilterOKRetrySpans(mock_processor, patterns)
        filter_proc.shutdown()

        mock_processor.shutdown.assert_called_once()

    def test_force_flush_forwards_to_next_processor(self, mocker):
        '''Test that force_flush is forwarded to the next processor'''
        mock_processor = mocker.MagicMock()
        mock_processor.force_flush.return_value = True
        patterns = []

        filter_proc = FilterOKRetrySpans(mock_processor, patterns)
        result = filter_proc.force_flush(timeout_millis=5000)

        mock_processor.force_flush.assert_called_once_with(5000)
        assert result is True


# ---------------------------------------------------------------------------
# read_config
# ---------------------------------------------------------------------------

def test_read_config_none():
    '''read_config returns empty dict when config_file is None (line 99)'''
    assert read_config(None) == {}


# ---------------------------------------------------------------------------
# main_loop edge cases
# ---------------------------------------------------------------------------

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


# ---------------------------------------------------------------------------
# main_runner — no running event loop path
# ---------------------------------------------------------------------------

def test_main_runner_no_event_loop(mocker):
    '''main_runner falls through to asyncio.run (lines 318-319, 325-326) when no loop is running'''
    with NamedTemporaryFile(suffix='.yml') as temp_config:
        config_data = {'general': {'discord_token': 'foo'}}
        with open(temp_config.name, 'w', encoding='utf-8') as writer:
            dump(config_data, writer)
        mocker.patch('discord_bot.cli.Bot', side_effect=fake_bot_yielder(guilds=[]))
        runner = CliRunner()
        result = runner.invoke(main, [temp_config.name])
        assert result.exception is None


# ---------------------------------------------------------------------------
# monitoring config paths
# ---------------------------------------------------------------------------

def _patch_otlp(mocker):
    '''Patch all OpenTelemetry symbols to avoid real network connections'''
    for name in [
        'TracerProvider', 'RequestsInstrumentor', 'SQLAlchemyInstrumentor',
        'OTLPSpanExporter', 'BatchSpanProcessor', 'get_aggregated_resources',
        'OTELResourceDetector', 'OTLPMetricExporter', 'PeriodicExportingMetricReader',
        'MeterProvider', 'set_meter_provider', 'LoggerProvider', 'set_logger_provider',
        'OTLPLogExporter', 'BatchLogRecordProcessor',
    ]:
        mocker.patch(f'discord_bot.cli.{name}')
    mocker.patch('discord_bot.cli.trace')
    # LoggingHandler mock is added to the root logger; .level must be an int or
    # callHandlers() raises TypeError on "record.levelno >= hdlr.level"
    mock_handler = MagicMock(spec=stdlib_logging.Handler)
    mock_handler.level = stdlib_logging.NOTSET
    mocker.patch('discord_bot.cli.LoggingHandler', return_value=mock_handler)


@pytest.mark.asyncio
async def test_main_with_otlp_filter_enabled(mocker):
    '''OTLP monitoring block executes with filter_high_volume_spans=True (lines 189-202, 205-218)'''
    with NamedTemporaryFile(suffix='.yml') as temp_config:
        config_data = {
            'general': {
                'discord_token': 'foo',
                'monitoring': {'otlp': {'enabled': True}},
            }
        }
        with open(temp_config.name, 'w', encoding='utf-8') as writer:
            dump(config_data, writer)
        _patch_otlp(mocker)
        mocker.patch('discord_bot.cli.Bot', side_effect=fake_bot_yielder(guilds=[]))
        runner = CliRunner()
        result = runner.invoke(main, [temp_config.name])
        await asyncio.sleep(.01)
        assert result.exception is None


@pytest.mark.asyncio
async def test_main_with_otlp_filter_disabled(mocker):
    '''OTLP monitoring block executes with filter_high_volume_spans=False (line 204)'''
    with NamedTemporaryFile(suffix='.yml') as temp_config:
        config_data = {
            'general': {
                'discord_token': 'foo',
                'monitoring': {'otlp': {'enabled': True, 'filter_high_volume_spans': False}},
            }
        }
        with open(temp_config.name, 'w', encoding='utf-8') as writer:
            dump(config_data, writer)
        _patch_otlp(mocker)
        mocker.patch('discord_bot.cli.Bot', side_effect=fake_bot_yielder(guilds=[]))
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
                'monitoring': {
                    'otlp': {'enabled': False},
                    'memory_profiling': {'enabled': True},
                },
            }
        }
        with open(temp_config.name, 'w', encoding='utf-8') as writer:
            dump(config_data, writer)
        mocker.patch('discord_bot.cli.MemoryProfiler')
        mocker.patch('discord_bot.cli.Bot', side_effect=fake_bot_yielder(guilds=[]))
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
                'monitoring': {
                    'otlp': {'enabled': False},
                    'process_metrics': {'enabled': True},
                },
            }
        }
        with open(temp_config.name, 'w', encoding='utf-8') as writer:
            dump(config_data, writer)
        mocker.patch('discord_bot.cli.ProcessMetricsProfiler')
        mocker.patch('discord_bot.cli.Bot', side_effect=fake_bot_yielder(guilds=[]))
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
                'monitoring': {
                    'otlp': {'enabled': False},
                    'health_server': {'enabled': True},
                },
            }
        }
        with open(temp_config.name, 'w', encoding='utf-8') as writer:
            dump(config_data, writer)
        mock_hs = MagicMock()
        mock_hs.serve = AsyncMock()
        mocker.patch('discord_bot.cli.HealthServer', return_value=mock_hs)
        mocker.patch('discord_bot.cli.Bot', side_effect=fake_bot_yielder(guilds=[]))
        runner = CliRunner()
        result = runner.invoke(main, [temp_config.name])
        await asyncio.sleep(.01)
        assert result.exception is None


def test_run_config_with_postgresql_db(mocker):
    '''postgresql URL is rewritten to postgresql+asyncpg and engine is disposed synchronously.

    Covers cli.py lines 186 (url rewrite), 270-271 (RuntimeError → loop=None), 275 (asyncio.run).
    This test is intentionally synchronous so there is no running event loop in the finally block,
    exercising the asyncio.run(db_engine.dispose()) path.
    '''
    mock_sync_engine = MagicMock()
    mock_async_engine = AsyncMock()
    # Prevent actual DB connection
    mocker.patch('discord_bot.cli.create_engine', return_value=mock_sync_engine)
    mocker.patch('discord_bot.cli.BASE.metadata.create_all')
    create_async_engine_mock = mocker.patch('discord_bot.cli.create_async_engine', return_value=mock_async_engine)
    mocker.patch('discord_bot.cli.main_runner')

    with NamedTemporaryFile(suffix='.yml') as temp_config:
        config_data = {
            'general': {
                'discord_token': 'foo',
                'sql_connection_statement': 'postgresql://user:pass@localhost/testdb',
            }
        }
        with open(temp_config.name, 'w', encoding='utf-8') as writer:
            dump(config_data, writer)
        runner = CliRunner()
        result = runner.invoke(main, [temp_config.name])

    assert result.exception is None
    # Verify the URL was rewritten to use the asyncpg driver
    called_url = create_async_engine_mock.call_args[0][0]
    assert 'asyncpg' in str(called_url)
    # asyncio.run disposed the async engine (no running loop in a sync test)
    mock_async_engine.dispose.assert_awaited_once()
