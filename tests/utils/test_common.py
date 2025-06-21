from functools import partial
import logging
from pathlib import Path
from tempfile import NamedTemporaryFile, TemporaryDirectory

from jsonschema.exceptions import ValidationError
from discord.errors import DiscordServerError, RateLimited
import pytest

from discord_bot.exceptions import ExitEarlyException
from discord_bot.utils.common import GENERAL_SECTION_SCHEMA
from discord_bot.utils.common import validate_config
from discord_bot.utils.common import get_logger
from discord_bot.utils.common import retry_command
from discord_bot.utils.common import retry_discord_message_command
from discord_bot.utils.common import async_retry_command
from discord_bot.utils.common import async_retry_discord_message_command
from discord_bot.utils.common import rm_tree
from discord_bot.utils.common import return_loop_runner

from tests.helpers import fake_bot_yielder

class TestException(Exception):
    pass

def test_validate_minimal_config():
    minimal_input = {
        'discord_token': 'abctoken'
    }
    validate_config(minimal_input, GENERAL_SECTION_SCHEMA)

def test_sql_statement_config():
    sql_input = {
        'discord_token': 'abctoken',
        'sql_connection_statement': 'sqlite:///foo.sql'
    }
    validate_config(sql_input, GENERAL_SECTION_SCHEMA)

def test_logging_config():
    logging_input = {
        'discord_token': 'abctoken',
        'logging': {},
    }
    with pytest.raises(ValidationError) as exc:
        validate_config(logging_input, GENERAL_SECTION_SCHEMA)
    assert "'log_dir' is a required property" in str(exc.value)

    logging_input = {
        'discord_token': 'abctoken',
        'logging': {
            'log_dir': '/var/foo',
            'log_file_count': 1,
            'log_file_max_bytes': 10 * 1024,
            'log_level': 30,
        }
    }
    validate_config(logging_input, GENERAL_SECTION_SCHEMA)

    logging_input = {
        'discord_token': 'abctoken',
        'logging': {
            'log_dir': '/var/foo/',
            'log_file_count': 1,
            'log_file_max_bytes': 10 * 1024,
            'log_level': 123,
        }
    }
    with pytest.raises(ValidationError) as exc:
        validate_config(logging_input, GENERAL_SECTION_SCHEMA)
    assert '123 is not one of [0, 10, 20, 30, 40, 50]' in str(exc.value)

def test_includes_config():
    include_input = {
        'discord_token': 'abctoken',
        'include': {
            'default': False,
        }
    }
    validate_config(include_input, GENERAL_SECTION_SCHEMA)

def test_intents_config():
    intents_input = {
        'discord_token': 'abctoken',
        'intents': [
            'message'
        ]
    }
    validate_config(intents_input, GENERAL_SECTION_SCHEMA)

def test_rejectlist_config():
    reject_input = {
        'discord_token': 'abctoken',
        'rejectlist_guilds': [
            '12345'
        ]
    }
    validate_config(reject_input, GENERAL_SECTION_SCHEMA)

def test_otlp_config_bad():
    reject_input = {
        'discord_token': 'abctoken',
        'otlp': {},
    }
    with pytest.raises(ValidationError) as e:
        validate_config(reject_input, GENERAL_SECTION_SCHEMA)
    assert 'log_endpoint' in str(e.value)

def test_otlp_config_minimal():
    reject_input = {
        'discord_token': 'abctoken',
        'otlp': {
            'trace_endpoint': 'foo',
            'log_endpoint': 'foo',
            'metric_endpoint': 'foo',
        },
    }
    validate_config(reject_input, GENERAL_SECTION_SCHEMA)

def test_get_logger():
    # Test default options
    logger = get_logger('foo', {})
    assert logger.getEffectiveLevel() == 10
    assert logger.hasHandlers() is True


    with TemporaryDirectory() as tmp_dir:
        # Test some more specific options
        log_args =  {
            'log_dir': tmp_dir,
            'log_file_count': 1,
            'log_file_max_bytes': 10 * 1024,
            'log_level': 30,
        }
        logger = get_logger('foo', log_args)
        assert logger.getEffectiveLevel() == 30
        assert logger.hasHandlers() is True

def test_retry_command(mocker):
    # Test very basic call command
    def test_command(x, y: 2):
        return x + y
    val = retry_command(partial(test_command, 3, y=1))
    assert val == 4

    # Pass a funcion that should fail everytime, make sure it retries
    mock_time = mocker.patch('discord_bot.utils.common.sleep', return_value=False)
    def test_command_raises(x, y):
        raise TestException('Test Exception')
    with pytest.raises(TestException):
        retry_command(partial(test_command_raises, 2, 3), accepted_exceptions=TestException)
    assert mock_time.call_count == 3
    # Same test but set max retries this time
    mock_time = mocker.patch('discord_bot.utils.common.sleep', return_value=False)
    with pytest.raises(TestException):
        retry_command(partial(test_command_raises, 2, 3), max_retries=1, accepted_exceptions=TestException)
    assert mock_time.call_count == 1

    # Lets try with a specific exception to pass in
    def test_command_raises_again(x):
        raise TestException('foo')
    mock_time = mocker.patch('discord_bot.utils.common.sleep', return_value=False)
    with pytest.raises(TestException):
        retry_command(partial(test_command_raises_again, 2), accepted_exceptions=TestException)
    assert mock_time.call_count == 3

    # Check does not retry when another exception type passed
    mock_time = mocker.patch('discord_bot.utils.common.sleep', return_value=False)
    with pytest.raises(TestException):
        retry_command(partial(test_command_raises_again, 2), accepted_exceptions=TypeError)
    assert mock_time.call_count == 0

    # Test post exceptions
    stub = mocker.stub(name='test_post')
    mock_time = mocker.patch('discord_bot.utils.common.sleep', return_value=False)
    with pytest.raises(TestException):
        retry_command(partial(test_command_raises_again, 2), accepted_exceptions=(TestException), post_exception_functions=[stub])
    assert mock_time.call_count == 3
    assert stub.called is True

    # Try a post exception that throws another error
    def test_throw(*_args, **_kwargs):
        raise TestException('Test')
    mock_time = mocker.patch('discord_bot.utils.common.sleep', return_value=False)
    with pytest.raises(TestException):
        retry_command(partial(test_throw, 2), accepted_exceptions=(TestException), post_exception_functions=[test_throw])
    assert mock_time.call_count == 0

def test_retry_discord(mocker):
    class FakeResponse():
        def __init__(self):
            self.status = 500
            self.reason = 'Cat unplugged the machines'
    def test_send_message():
        raise DiscordServerError(FakeResponse(), 'bar')
    mock_time = mocker.patch('discord_bot.utils.common.sleep', return_value=False)
    with pytest.raises(DiscordServerError):
        retry_discord_message_command(partial(test_send_message))
    assert mock_time.call_count == 3

@pytest.mark.asyncio
async def test_retry_command_async(mocker):
    class FakeResponse():
        def __init__(self):
            self.status = 500
            self.reason = 'Cat unplugged the machines'
    async def test_send_message():
        raise DiscordServerError(FakeResponse(), 'bar')
    mock_time = mocker.patch('discord_bot.utils.common.async_sleep', return_value=False)
    with pytest.raises(DiscordServerError):
        await async_retry_command(partial(test_send_message), accepted_exceptions=DiscordServerError)
    assert mock_time.call_count == 3

@pytest.mark.asyncio
async def test_retry_command_async_with_post(mocker):
    class FakeResponse():
        def __init__(self):
            self.status = 500
            self.reason = 'Cat unplugged the machines'
    async def test_send_message():
        raise DiscordServerError(FakeResponse(), 'bar')
    async def test_post(_ex, _is_last):
        return 'foo'
    mock_time = mocker.patch('discord_bot.utils.common.async_sleep', return_value=False)
    with pytest.raises(DiscordServerError):
        await async_retry_command(partial(test_send_message), accepted_exceptions=DiscordServerError, post_exception_functions=[test_post])
    assert mock_time.call_count == 3

def test_retry_discord_rate_limited(mocker):
    def test_send_message():
        raise RateLimited(2)
    mock_time = mocker.patch('discord_bot.utils.common.sleep', return_value=False)
    with pytest.raises(RateLimited):
        retry_discord_message_command(partial(test_send_message))
    assert mock_time.call_count == 3

@pytest.mark.asyncio
async def test_retry_command_async_429(mocker):
    async def test_send_message():
        raise RateLimited(2)
    mock_time = mocker.patch('discord_bot.utils.common.async_sleep', return_value=False)
    with pytest.raises(RateLimited):
        await async_retry_discord_message_command(partial(test_send_message))
    assert mock_time.call_count == 3

def test_rm_tree():
    with TemporaryDirectory(ignore_cleanup_errors=True) as tmp_dir:
        with TemporaryDirectory(ignore_cleanup_errors=True, dir=tmp_dir) as tmp_dir2:
            with NamedTemporaryFile(dir=tmp_dir2, delete=False) as tmp_file:
                path = Path(tmp_file.name)
                path.write_text('tmp-file', encoding='utf-8')

                rm_tree(Path(tmp_dir))
                assert not path.exists()
                assert not Path(tmp_dir).exists()

@pytest.mark.asyncio(scope="session")
async def test_return_loop_runner():
    def fake_func():
        raise ExitEarlyException('exiting')
    fake_bot = fake_bot_yielder()()
    with NamedTemporaryFile() as tmpfile:
        path = Path(tmpfile.name)
        runner = return_loop_runner(fake_func, fake_bot, logging, path)
        assert await runner() is False
        assert path.read_text(encoding='utf-8') == '0'

@pytest.mark.asyncio(scope="session")
async def test_return_loop_runner_standard_exception():
    def fake_func():
        raise Exception('exiting') #pylint:disable=broad-exception-raised
    fake_bot = fake_bot_yielder()()
    with NamedTemporaryFile() as tmpfile:
        path = Path(tmpfile.name)
        runner = return_loop_runner(fake_func, fake_bot, logging, path)
        assert await runner() is False
        assert path.read_text(encoding='utf-8') == '0'

@pytest.mark.asyncio(scope="session")
async def test_return_loop_runner_continue_exception():

    fake_bot = fake_bot_yielder()()
    class FakeException(Exception):
        pass
    def fake_func():
        fake_bot.bot_closed = True
        raise FakeException('foo')
    with NamedTemporaryFile() as tmpfile:
        path = Path(tmpfile.name)
        runner = return_loop_runner(fake_func, fake_bot, logging, path, continue_exceptions=FakeException)
        runner()
        assert not fake_bot.is_closed()
        assert path.read_text(encoding='utf-8') == '1'
