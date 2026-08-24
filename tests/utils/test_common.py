from functools import partial
import logging
from pathlib import Path
from tempfile import NamedTemporaryFile, TemporaryDirectory
from unittest.mock import AsyncMock, MagicMock

from pydantic import ValidationError as PydanticValidationError
from aiohttp import ClientResponseError
from discord.errors import DiscordServerError, HTTPException, RateLimited, NotFound
from opentelemetry.trace.status import StatusCode
import pytest

from discord_bot.exceptions import ExitEarlyException
from discord_bot.utils.common import GeneralConfig, LoggingConfig, RedisSentinelConfig
from discord_bot.utils.common import get_logger
from discord_bot.utils.discord_retry import async_retry_command
from discord_bot.utils.discord_retry import async_retry_discord_message_command
from discord_bot.utils.discord_utils import discord_format_string_embed
from discord_bot.utils.common import rm_tree
from discord_bot.utils.common import return_loop_runner, _LOOP_ERROR_BACKOFF_MAX_SECONDS
from discord_bot.utils.loop_health import LoopHealth

from tests.helpers import fake_bot_yielder

class CommonException(Exception):
    pass

# Pydantic validation tests
def test_pydantic_validate_minimal_config():
    minimal_input = {
        'discord_token': 'abctoken'
    }
    config = GeneralConfig(**minimal_input)
    assert config.discord_token == 'abctoken'
    assert config.include.default is True  # Default value

def test_pydantic_config_allows_missing_discord_token():
    '''discord_token is optional — gateway-less processes (broker/downloader) omit it.'''
    config = GeneralConfig(redis_url='redis://localhost:6379/0')
    assert config.discord_token is None

def test_pydantic_sql_statement_config():
    sql_input = {
        'discord_token': 'abctoken',
        'sql_connection_statement': 'postgresql://user@localhost/discord_bot'
    }
    config = GeneralConfig(**sql_input)
    assert config.sql_connection_statement == 'postgresql://user@localhost/discord_bot'

def test_pydantic_logging_config_missing_required():
    logging_input = {
        'discord_token': 'abctoken',
        'logging': {},
    }
    with pytest.raises(PydanticValidationError) as exc:
        GeneralConfig(**logging_input)
    assert 'log_level' in str(exc.value)

def test_pydantic_logging_config_valid():
    logging_input = {
        'discord_token': 'abctoken',
        'logging': {
            'log_dir': '/var/foo',
            'log_file_count': 1,
            'log_file_max_bytes': 10 * 1024,
            'log_level': 30,
        }
    }
    config = GeneralConfig(**logging_input)
    assert config.logging.log_level == 30

def test_pydantic_logging_config_invalid_log_level():
    logging_input = {
        'discord_token': 'abctoken',
        'logging': {
            'log_dir': '/var/foo/',
            'log_file_count': 1,
            'log_file_max_bytes': 10 * 1024,
            'log_level': 123,
        }
    }
    with pytest.raises(PydanticValidationError) as exc:
        GeneralConfig(**logging_input)
    assert 'log_level' in str(exc.value)

def test_pydantic_includes_config():
    include_input = {
        'discord_token': 'abctoken',
        'include': {
            'default': False,
            'markov': True,
        }
    }
    config = GeneralConfig(**include_input)
    assert config.include.default is False
    assert config.include.markov is True  # pylint: disable=no-member

def test_pydantic_intents_config():
    intents_input = {
        'discord_token': 'abctoken',
        'intents': [
            'message'
        ]
    }
    config = GeneralConfig(**intents_input)
    assert config.intents == ['message']

def test_pydantic_rejectlist_config():
    reject_input = {
        'discord_token': 'abctoken',
        'rejectlist_guilds': [
            12345
        ]
    }
    config = GeneralConfig(**reject_input)
    assert config.rejectlist_guilds == [12345]

def test_pydantic_otlp_config_bad():
    reject_input = {
        'discord_token': 'abctoken',
        'monitoring': {
            'otlp': {},
        },
    }
    with pytest.raises(PydanticValidationError) as exc:
        GeneralConfig(**reject_input)
    assert 'enabled' in str(exc.value)

def test_pydantic_otlp_config_minimal():
    reject_input = {
        'discord_token': 'abctoken',
        'monitoring': {
            'otlp': {
                'enabled': True,
            },
        },
    }
    config = GeneralConfig(**reject_input)
    assert config.monitoring.otlp.enabled is True

def test_pydantic_otlp_config_ignores_retired_span_filter_keys():
    '''
    Span filtering moved to the otel-collector, so both keys are gone from the
    model. A ConfigMap still carrying them must stay loadable and simply ignore
    them — that is what lets the collector-side filter deploy before the config
    is cleaned up, rather than needing the two changes to land together.
    '''
    config_input = {
        'discord_token': 'abctoken',
        'monitoring': {
            'otlp': {
                'enabled': True,
                'filter_high_volume_spans': False,
                'high_volume_span_patterns': [r'^custom\.span$'],
            },
        },
    }
    config = GeneralConfig(**config_input)
    assert config.monitoring.otlp.enabled is True
    assert not hasattr(config.monitoring.otlp, 'filter_high_volume_spans')
    assert not hasattr(config.monitoring.otlp, 'high_volume_span_patterns')

def test_get_logger():
    # Test default options
    logger = get_logger('foo', None)
    assert logger.getEffectiveLevel() == 10
    assert logger.hasHandlers() is True


    with TemporaryDirectory() as tmp_dir:
        # Test some more specific options
        logging_config = LoggingConfig(
            log_dir=tmp_dir,
            log_file_count=1,
            log_file_max_bytes=10 * 1024,
            log_level=30,
        )
        logger = get_logger('foo', logging_config)
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
    mock_time = mocker.patch('discord_bot.utils.retry.async_sleep', return_value=False)
    with pytest.raises(DiscordServerError):
        await async_retry_command(partial(test_send_message), retry_exceptions=DiscordServerError)
    assert mock_time.call_count == 3


@pytest.mark.asyncio
async def test_retry_discord_message_command_server_error(mocker):
    '''DiscordServerError (5xx) should be retried with exponential backoff'''
    class FakeResponse():
        def __init__(self):
            self.status = 503
            self.reason = 'Service Unavailable'
    async def test_send_message():
        raise DiscordServerError(FakeResponse(), 'bar')
    mock_time = mocker.patch('discord_bot.utils.retry.async_sleep', return_value=False)
    with pytest.raises(DiscordServerError):
        await async_retry_discord_message_command(partial(test_send_message))
    assert mock_time.call_count == 3

@pytest.mark.asyncio
async def test_retry_command_async_429(mocker):
    async def test_send_message():
        raise RateLimited(2)
    mock_time = mocker.patch('discord_bot.utils.retry.async_sleep', return_value=False)
    with pytest.raises(RateLimited):
        await async_retry_discord_message_command(partial(test_send_message))
    assert mock_time.call_count == 3

@pytest.mark.asyncio
async def test_retry_command_async_http_429(mocker):
    '''HTTPException with status 429 (e.g. error code 40062) should be retried with backoff'''
    class FakeResponse():
        def __init__(self):
            self.status = 429
            self.reason = 'Service resource is being rate limited'
    async def test_send_message():
        raise HTTPException(FakeResponse(), 'bar')
    mock_time = mocker.patch('discord_bot.utils.retry.async_sleep', return_value=False)
    with pytest.raises(HTTPException):
        await async_retry_discord_message_command(partial(test_send_message))
    assert mock_time.call_count == 3

@pytest.mark.asyncio
async def test_retry_command_async_http_non_429(mocker):
    '''HTTPException with a non-429 status should propagate immediately without retrying'''
    class FakeResponse():
        def __init__(self):
            self.status = 403
            self.reason = 'Missing Permissions'
    async def test_send_message():
        raise HTTPException(FakeResponse(), 'bar')
    mock_time = mocker.patch('discord_bot.utils.retry.async_sleep', return_value=False)
    with pytest.raises(HTTPException):
        await async_retry_discord_message_command(partial(test_send_message))
    assert mock_time.call_count == 0

@pytest.mark.asyncio
async def test_retry_command_async_404(mocker):
    class FakeResponse():
        def __init__(self):
            self.status = 404
            self.reason = 'Cat ate the message'
    async def test_send_message():
        raise NotFound(FakeResponse(), 'bar')
    mock_time = mocker.patch('discord_bot.utils.retry.async_sleep', return_value=False)
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
async def test_return_loop_runner_standard_exception_backs_off_and_continues(mocker):
    # An unexpected error must not kill the loop task (health-green zombie); it
    # should back off and re-run the loop body until the bot is actually closed.
    mock_sleep = mocker.patch('discord_bot.utils.common.sleep', new_callable=AsyncMock)
    fake_bot = fake_bot_yielder()()
    call_count = 0
    async def fake_func():
        nonlocal call_count
        call_count += 1
        if call_count == 1:
            raise Exception('transient error') #pylint:disable=broad-exception-raised
        fake_bot.bot_closed = True  # Second call closes bot to exit loop
    runner = return_loop_runner(fake_func, fake_bot, logging)
    assert await runner() is None  # Loop exits via bot close, not via the error
    assert call_count == 2  # Survived the error and ran again
    mock_sleep.assert_awaited_once()  # Backed off before continuing

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

@pytest.mark.asyncio(loop_scope="session")
async def test_return_loop_runner_continue_exception_records_error_on_health():
    # A tolerated exception is still not progress: it counts against health, so a
    # loop that only ever hits its continue_exceptions eventually reports stalled.
    health = LoopHealth('test_loop', stale_after_seconds=60)
    fake_bot = fake_bot_yielder()()
    class FakeException(Exception):
        pass
    call_count = 0
    async def fake_func():
        nonlocal call_count
        call_count += 1
        if call_count <= 2:
            raise FakeException('foo')
        fake_bot.bot_closed = True
    runner = return_loop_runner(fake_func, fake_bot, logging, continue_exceptions=FakeException,
                               health=health)
    await runner()
    assert call_count == 3
    assert health.is_healthy  # the success at call 3 cleared it

@pytest.mark.asyncio(loop_scope="session")
async def test_return_loop_runner_exit_exception_sets_span_ok(mocker):
    """Test that exit exceptions set the OpenTelemetry span status to OK"""
    def fake_func():
        raise ExitEarlyException('exiting')

    # Mock the span
    mock_span = mocker.MagicMock()
    mocker.patch('discord_bot.utils.common.get_current_span', return_value=mock_span)

    fake_bot = fake_bot_yielder()()
    runner = return_loop_runner(fake_func, fake_bot, logging)
    result = await runner()

    # Verify the function returns False
    assert result is False

    # Verify that set_status was called with StatusCode.OK
    mock_span.set_status.assert_called_once_with(StatusCode.OK)

@pytest.mark.asyncio(loop_scope="session")
async def test_return_loop_runner_standard_exception_does_not_set_span_ok(mocker):
    """Test that standard exceptions do NOT set the span status to OK"""
    mocker.patch('discord_bot.utils.common.sleep', new_callable=AsyncMock)
    call_count = 0
    async def fake_func():
        nonlocal call_count
        call_count += 1
        if call_count == 1:
            raise Exception('unexpected error') #pylint:disable=broad-exception-raised
        fake_bot.bot_closed = True  # Second call closes bot to exit loop

    # Mock the span
    mock_span = mocker.MagicMock()
    mocker.patch('discord_bot.utils.common.get_current_span', return_value=mock_span)

    fake_bot = fake_bot_yielder()()
    runner = return_loop_runner(fake_func, fake_bot, logging)
    result = await runner()

    # Loop exits via bot close (returns None), not via the error
    assert result is None

    # Verify that set_status was NOT called (standard exceptions shouldn't set OK status)
    mock_span.set_status.assert_not_called()

@pytest.mark.asyncio(loop_scope="session")
async def test_return_loop_runner_survives_broker_500(mocker):
    """A broker HTTP 500 (Redis blip) must not wedge the result loop task."""
    mock_sleep = mocker.patch('discord_bot.utils.common.sleep', new_callable=AsyncMock)
    fake_bot = fake_bot_yielder()()
    call_count = 0
    async def fake_func():
        nonlocal call_count
        call_count += 1
        if call_count == 1:
            raise ClientResponseError(
                request_info=MagicMock(), history=(),
                status=500, message='Internal Server Error',
            )
        fake_bot.bot_closed = True  # Second call closes bot to exit loop
    runner = return_loop_runner(fake_func, fake_bot, logging)
    assert await runner() is None  # Loop survived the 500 and exited cleanly on close
    assert call_count == 2
    mock_sleep.assert_awaited_once()

@pytest.mark.asyncio(loop_scope="session")
async def test_return_loop_runner_never_gives_up_and_reports_unhealthy(mocker):
    # The incident shape: a loop erroring against a peer that hasn't upgraded yet.
    # It must NOT exit — exiting is what turned a ~20s deploy skew into a dead
    # consumer for the life of the pod (docs findings/2026-07-31). It keeps
    # retrying, and LoopHealth is what tells the alert/probe it's unhealthy.
    mock_sleep = mocker.patch('discord_bot.utils.common.sleep', new_callable=AsyncMock)
    fake_bot = fake_bot_yielder()()
    health = LoopHealth('test_loop', stale_after_seconds=60)
    call_count = 0
    async def fake_func():
        nonlocal call_count
        call_count += 1
        if call_count >= 20:  # far past the old 5-error give-up threshold
            fake_bot.bot_closed = True
        raise Exception('always broken') #pylint:disable=broad-exception-raised
    runner = return_loop_runner(fake_func, fake_bot, logging, health=health)
    await runner()
    assert call_count == 20  # kept retrying rather than exiting at 5
    assert health.consecutive_errors == 20
    assert mock_sleep.await_count == 20

@pytest.mark.asyncio(loop_scope="session")
async def test_return_loop_runner_backoff_grows_and_is_capped(mocker):
    # Retrying forever must not hot-spin at 1s for the length of a peer outage.
    mock_sleep = mocker.patch('discord_bot.utils.common.sleep', new_callable=AsyncMock)
    fake_bot = fake_bot_yielder()()
    call_count = 0
    async def fake_func():
        nonlocal call_count
        call_count += 1
        if call_count >= 8:
            fake_bot.bot_closed = True
        raise Exception('always broken') #pylint:disable=broad-exception-raised
    runner = return_loop_runner(fake_func, fake_bot, logging)
    await runner()
    delays = [call.args[0] for call in mock_sleep.await_args_list]
    assert delays[:6] == [1.0, 2.0, 4.0, 8.0, 16.0, 30.0]  # doubles, then caps
    assert max(delays) == _LOOP_ERROR_BACKOFF_MAX_SECONDS

@pytest.mark.asyncio(loop_scope="session")
async def test_return_loop_runner_backoff_resets_after_success(mocker):
    # A recovered loop shouldn't stay stuck at the capped backoff.
    mock_sleep = mocker.patch('discord_bot.utils.common.sleep', new_callable=AsyncMock)
    fake_bot = fake_bot_yielder()()
    call_count = 0
    async def fake_func():
        nonlocal call_count
        call_count += 1
        if call_count in (1, 2, 3):
            raise Exception('transient') #pylint:disable=broad-exception-raised
        if call_count == 5:
            raise Exception('later error') #pylint:disable=broad-exception-raised
        if call_count == 6:
            fake_bot.bot_closed = True
    runner = return_loop_runner(fake_func, fake_bot, logging)
    await runner()
    delays = [call.args[0] for call in mock_sleep.await_args_list]
    assert delays == [1.0, 2.0, 4.0, 1.0]  # back to 1.0 after the success at call 4

@pytest.mark.asyncio(loop_scope="session")
async def test_return_loop_runner_health_tracks_success_and_errors(mocker):
    # Health, not liveness: successes re-arm the window, errors count up.
    mocker.patch('discord_bot.utils.common.sleep', new_callable=AsyncMock)
    fake_bot = fake_bot_yielder()()
    health = LoopHealth('test_loop', stale_after_seconds=60)
    call_count = 0
    async def fake_func():
        nonlocal call_count
        call_count += 1
        if call_count in (1, 2):
            raise Exception('intermittent') #pylint:disable=broad-exception-raised
        if call_count == 4:
            fake_bot.bot_closed = True
    runner = return_loop_runner(fake_func, fake_bot, logging, health=health)
    await runner()
    assert health.consecutive_errors == 0  # reset by the success at call 3
    assert health.is_healthy

@pytest.mark.asyncio(loop_scope="session")
async def test_return_loop_runner_marks_stopped_on_exit_exception():
    # A deliberate exit is not a wedge: the loop reports stopped so a draining
    # pod doesn't fail its own liveness probe on the way out.
    health = LoopHealth('test_loop', stale_after_seconds=60)
    def fake_func():
        raise ExitEarlyException('exiting')
    fake_bot = fake_bot_yielder()()
    runner = return_loop_runner(fake_func, fake_bot, logging, health=health)
    assert await runner() is False
    assert health.status == 'stopped'
    assert health.is_healthy

@pytest.mark.asyncio(loop_scope="session")
async def test_return_loop_runner_marks_stopped_on_bot_close():
    # Same for the ordinary shutdown path (bot closed).
    health = LoopHealth('test_loop', stale_after_seconds=60)
    fake_bot = fake_bot_yielder()()
    fake_bot.bot_closed = True
    async def fake_func():
        pass
    runner = return_loop_runner(fake_func, fake_bot, logging, health=health)
    assert await runner() is None
    assert health.status == 'stopped'

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

def test_logging_config_otlp_only_false_requires_file_fields():
    '''LoggingConfig with otlp_only=False raises when log_dir/count/bytes are missing'''
    with pytest.raises(PydanticValidationError) as exc:
        LoggingConfig(log_level=30)
    assert 'Fields required' in str(exc.value)


def test_get_logger_with_otlp_logger(mocker):
    '''get_logger attaches LoggingHandler when otlp_logger is provided'''
    mock_handler = MagicMock()
    mocker.patch('discord_bot.utils.common.LoggingHandler', return_value=mock_handler)
    logging_config = LoggingConfig(log_level=30, otlp_only=True)
    logger = get_logger('test_otlp_logger', logging_config, otlp_logger=MagicMock())
    assert mock_handler in logger.handlers


def test_redis_sentinel_addrs_parses_host_port():
    '''sentinel_addrs splits each "host:port" entry into a (host, int(port)) tuple.'''
    cfg = RedisSentinelConfig(
        sentinels=['redis-sentinel:26379', 'other-host:26380'],
        service_name='mymaster',
    )
    assert cfg.sentinel_addrs() == [('redis-sentinel', 26379), ('other-host', 26380)]


def test_redis_sentinel_service_name_defaults_to_mymaster():
    '''service_name defaults to Sentinel's conventional 'mymaster'.'''
    cfg = RedisSentinelConfig(sentinels=['redis-sentinel:26379'])
    assert cfg.service_name == 'mymaster'
