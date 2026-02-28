import asyncio
import signal
from tempfile import NamedTemporaryFile

from click.testing import CliRunner
import pytest
from yaml import dump

from discord_bot.cli import main, main_loop, FilterOKRetrySpans

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
        mocker.patch('discord_bot.cli.Bot', side_effect=fake_bot_yielder())
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
            mocker.patch('discord_bot.cli.Bot', side_effect=fake_bot_yielder())
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
            mocker.patch('discord_bot.cli.Bot', side_effect=fake_bot_yielder())
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
