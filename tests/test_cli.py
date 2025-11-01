import asyncio
import signal
from tempfile import NamedTemporaryFile

from click.testing import CliRunner
import pytest
from yaml import dump

from discord_bot.cli import main, main_loop

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
        assert 'Unable to run bot without token' in str(result.exception)

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
                        'fake-guild-1234',
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
async def test_keyboard_interrupt_calls_cog_unload(mocker):
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

    # Create fake logger
    fake_logger = mocker.MagicMock()

    # Create fake cogs
    cog1 = FakeCog()
    cog2 = FakeCog()
    cog_list = [cog1, cog2]

    # Create fake bot
    bot = FakeBotWithInterrupt()

    # Run main_loop which should catch KeyboardInterrupt and call cog_unload
    await main_loop(bot, cog_list, 'fake-token', fake_logger)

    # Verify cog_unload was called on all cogs
    assert cog1.cog_unload_called is True
    assert cog1.cog_unload_call_count == 1
    assert cog2.cog_unload_called is True
    assert cog2.cog_unload_call_count == 1

    # Verify bot.close() was called
    assert bot.close_called is True

@pytest.mark.asyncio(loop_scope="session")
async def test_sigterm_calls_cog_unload(mocker):
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

    # Create fake logger
    fake_logger = mocker.MagicMock()

    # Create fake cogs
    cog1 = FakeCog()
    cog2 = FakeCog()
    cog_list = [cog1, cog2]

    # Create fake bot
    bot = FakeBotWithSignal()

    # Run main_loop which should catch SIGTERM and call cog_unload
    await main_loop(bot, cog_list, 'fake-token', fake_logger)

    # Verify bot was started
    assert bot.started is True

    # Verify cog_unload was called on all cogs
    assert cog1.cog_unload_called is True
    assert cog1.cog_unload_call_count == 1
    assert cog2.cog_unload_called is True
    assert cog2.cog_unload_call_count == 1

    # Verify bot.close() was called
    assert bot.close_called is True
