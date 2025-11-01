from functools import partial
import logging
from pathlib import Path
from tempfile import NamedTemporaryFile, TemporaryDirectory

from jsonschema.exceptions import ValidationError
from discord.errors import DiscordServerError, RateLimited, NotFound
import pytest

from discord_bot.exceptions import ExitEarlyException
from discord_bot.utils.common import GENERAL_SECTION_SCHEMA
from discord_bot.utils.common import validate_config
from discord_bot.utils.common import get_logger
from discord_bot.utils.common import async_retry_command
from discord_bot.utils.common import async_retry_discord_message_command
from discord_bot.utils.common import discord_format_string_embed
from discord_bot.utils.common import rm_tree
from discord_bot.utils.common import return_loop_runner

from tests.helpers import fake_bot_yielder

class CommonException(Exception):
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
    assert "'enabled' is a required property" in str(e.value)

def test_otlp_config_minimal():
    reject_input = {
        'discord_token': 'abctoken',
        'otlp': {
            'enabled': True,
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
        await async_retry_command(partial(test_send_message), retry_exceptions=DiscordServerError)
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
        await async_retry_command(partial(test_send_message), retry_exceptions=DiscordServerError, post_exception_functions=[test_post])
    assert mock_time.call_count == 3

@pytest.mark.asyncio
async def test_retry_command_async_429(mocker):
    async def test_send_message():
        raise RateLimited(2)
    mock_time = mocker.patch('discord_bot.utils.common.async_sleep', return_value=False)
    with pytest.raises(RateLimited):
        await async_retry_discord_message_command(partial(test_send_message))
    assert mock_time.call_count == 3

@pytest.mark.asyncio
async def test_retry_command_async_404(mocker):
    class FakeResponse():
        def __init__(self):
            self.status = 404
            self.reason = 'Cat ate the message'
    async def test_send_message():
        raise NotFound(FakeResponse(), 'bar')
    mock_time = mocker.patch('discord_bot.utils.common.async_sleep', return_value=False)
    await async_retry_discord_message_command(partial(test_send_message), allow_404=True)
    assert mock_time.call_count == 0

def test_rm_tree():
    with TemporaryDirectory(ignore_cleanup_errors=True) as tmp_dir:
        with TemporaryDirectory(ignore_cleanup_errors=True, dir=tmp_dir) as tmp_dir2:
            with NamedTemporaryFile(dir=tmp_dir2, delete=False) as tmp_file:
                path = Path(tmp_file.name)
                path.write_text('tmp-file', encoding='utf-8')

                rm_tree(Path(tmp_dir))
                assert not path.exists()
                assert not Path(tmp_dir).exists()

@pytest.mark.asyncio(loop_scope="session")
async def test_return_loop_runner():
    def fake_func():
        raise ExitEarlyException('exiting')
    fake_bot = fake_bot_yielder()()
    runner = return_loop_runner(fake_func, fake_bot, logging)
    assert await runner() is False

@pytest.mark.asyncio(loop_scope="session")
async def test_return_loop_runner_standard_exception():
    def fake_func():
        raise Exception('exiting') #pylint:disable=broad-exception-raised
    fake_bot = fake_bot_yielder()()
    runner = return_loop_runner(fake_func, fake_bot, logging)
    assert await runner() is False

@pytest.mark.asyncio(loop_scope="session")
async def test_return_loop_runner_continue_exception():

    fake_bot = fake_bot_yielder()()
    class FakeException(Exception):
        pass
    call_count = 0
    async def fake_func():
        nonlocal call_count
        call_count += 1
        if call_count == 1:
            raise FakeException('foo')  # First call raises continue exception
        fake_bot.bot_closed = True  # Second call closes bot to exit loop
    runner = return_loop_runner(fake_func, fake_bot, logging, continue_exceptions=FakeException)
    await runner()
    assert fake_bot.is_closed()  # Bot should be closed after loop exits
    assert call_count == 2  # Function should be called twice (continue exception, then close)

def test_discord_format_string_embed_no_url():
    """Test discord_format_string_embed with string containing no URLs"""
    input_string = "This is just a plain text string"
    result = discord_format_string_embed(input_string)
    assert result == "This is just a plain text string"


def test_discord_format_string_embed_single_url():
    """Test discord_format_string_embed with string containing single URL"""
    input_string = "Check out this link: https://example.com/path"
    result = discord_format_string_embed(input_string)
    assert result == "Check out this link: <https://example.com/path>"


def test_discord_format_string_embed_multiple_urls():
    """Test discord_format_string_embed with string containing multiple URLs"""
    input_string = "Visit https://google.com and https://github.com for more info"
    result = discord_format_string_embed(input_string)
    assert result == "Visit <https://google.com> and <https://github.com> for more info"


def test_discord_format_string_embed_url_with_parameters():
    """Test discord_format_string_embed with URL containing query parameters"""
    input_string = "Search: https://example.com/search?q=test&page=1"
    result = discord_format_string_embed(input_string)
    assert result == "Search: <https://example.com/search?q=test&page=1>"


def test_discord_format_string_embed_url_with_fragment():
    """Test discord_format_string_embed with URL containing fragment"""
    input_string = "Docs: https://docs.python.org/3/library/re.html#match-objects"
    result = discord_format_string_embed(input_string)
    assert result == "Docs: <https://docs.python.org/3/library/re.html#match-objects>"


def test_discord_format_string_embed_url_at_start():
    """Test discord_format_string_embed with URL at start of string"""
    input_string = "https://example.com is a great site"
    result = discord_format_string_embed(input_string)
    assert result == "<https://example.com> is a great site"


def test_discord_format_string_embed_url_at_end():
    """Test discord_format_string_embed with URL at end of string"""
    input_string = "Visit my website at https://example.com"
    result = discord_format_string_embed(input_string)
    assert result == "Visit my website at <https://example.com>"


def test_discord_format_string_embed_url_only():
    """Test discord_format_string_embed with string that is only a URL"""
    input_string = "https://example.com"
    result = discord_format_string_embed(input_string)
    assert result == "<https://example.com>"


def test_discord_format_string_embed_mixed_content():
    """Test discord_format_string_embed with URLs mixed with other content"""
    input_string = "Go to https://github.com/user/repo for code, or email user@example.com"
    result = discord_format_string_embed(input_string)
    # Only HTTPS URLs should be wrapped, not email addresses
    assert result == "Go to <https://github.com/user/repo> for code, or email user@example.com"


def test_discord_format_string_embed_already_formatted():
    """Test discord_format_string_embed with already formatted URL"""
    input_string = "Check out <https://example.com>"
    result = discord_format_string_embed(input_string)
    # Should wrap the URL inside the brackets, resulting in double brackets
    assert result == "Check out <<https://example.com>>"


def test_discord_format_string_embed_https_variations():
    """Test discord_format_string_embed with different HTTPS URL variations"""
    test_cases = [
        ("https://example.com", "<https://example.com>"),
        ("https://www.example.com", "<https://www.example.com>"),
        ("https://subdomain.example.com", "<https://subdomain.example.com>"),
        ("https://example.com:8080", "<https://example.com:8080>"),
        ("https://192.168.1.1", "<https://192.168.1.1>"),
    ]

    for input_url, expected in test_cases:
        result = discord_format_string_embed(input_url)
        assert result == expected, f"Failed for {input_url}"
