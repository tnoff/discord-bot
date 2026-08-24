'''
Tests for the retry helper that speaks discord.py (async_retry_discord_message_command).

The transport-level tests moved to test_retry.py when utils/retry was split out
so the worker images could drop discord.py.
'''
from unittest.mock import AsyncMock, patch

import pytest

from aiohttp.client_exceptions import ServerDisconnectedError
from discord.errors import DiscordServerError, HTTPException, NotFound, RateLimited

from discord_bot.utils.discord_retry import async_retry_discord_message_command
from tests.helpers import FakeResponse


# ---------------------------------------------------------------------------
# async_retry_discord_message_command
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_discord_retry_success():
    """Successful Discord call returns the result."""
    func = AsyncMock(return_value='message')
    result = await async_retry_discord_message_command(func)
    assert result == 'message'


@pytest.mark.asyncio
async def test_discord_retry_not_found_without_allow_propagates():
    """NotFound propagates when allow_404=False (default)."""
    func = AsyncMock(side_effect=NotFound(FakeResponse(), 'unknown'))
    with pytest.raises(NotFound):
        await async_retry_discord_message_command(func)


@pytest.mark.asyncio
async def test_discord_retry_not_found_with_allow_returns_false():
    """NotFound is swallowed and returns False when allow_404=True."""
    func = AsyncMock(side_effect=NotFound(FakeResponse(), 'unknown'))
    result = await async_retry_discord_message_command(func, allow_404=True)
    assert result is False


@pytest.mark.asyncio
async def test_discord_retry_rate_limited_retries():
    """RateLimited sleeps retry_after then retries; success returns the result."""
    rate_limited = RateLimited(0.01)
    func = AsyncMock(side_effect=[rate_limited, 'sent'])
    with patch('discord_bot.utils.retry.async_sleep', new_callable=AsyncMock) as mock_sleep:
        result = await async_retry_discord_message_command(func, max_retries=2)
    assert result == 'sent'
    mock_sleep.assert_awaited_once_with(0.01)


@pytest.mark.asyncio
async def test_discord_retry_rate_limited_exhausted_raises():
    """RateLimited that persists past max_retries is re-raised."""
    func = AsyncMock(side_effect=RateLimited(0.01))
    with patch('discord_bot.utils.retry.async_sleep', new_callable=AsyncMock):
        with pytest.raises(RateLimited):
            await async_retry_discord_message_command(func, max_retries=1)
    assert func.await_count == 2


@pytest.mark.asyncio
async def test_discord_retry_server_error_retries():
    """DiscordServerError triggers exponential-backoff retry."""
    server_err = DiscordServerError(FakeResponse(), 'server error')
    func = AsyncMock(side_effect=[server_err, 'recovered'])
    with patch('discord_bot.utils.retry.async_sleep', new_callable=AsyncMock) as mock_sleep:
        result = await async_retry_discord_message_command(func, max_retries=2)
    assert result == 'recovered'
    mock_sleep.assert_awaited_once_with(1)  # 2**0


@pytest.mark.asyncio
async def test_discord_retry_server_disconnected_retries():
    """ServerDisconnectedError triggers exponential-backoff retry."""
    func = AsyncMock(side_effect=[ServerDisconnectedError(), 'ok'])
    with patch('discord_bot.utils.retry.async_sleep', new_callable=AsyncMock):
        result = await async_retry_discord_message_command(func, max_retries=2)
    assert result == 'ok'


@pytest.mark.asyncio
async def test_discord_retry_server_error_exhausted_raises():
    """DiscordServerError that persists past max_retries is re-raised (lines 74-76)."""
    server_err = DiscordServerError(FakeResponse(), 'server error')
    func = AsyncMock(side_effect=server_err)
    with patch('discord_bot.utils.retry.async_sleep', new_callable=AsyncMock):
        with pytest.raises(DiscordServerError):
            await async_retry_discord_message_command(func, max_retries=1)
    assert func.await_count == 2


@pytest.mark.asyncio
async def test_discord_retry_timeout_error_retries():
    """TimeoutError triggers exponential-backoff retry."""
    func = AsyncMock(side_effect=[TimeoutError(), 'ok'])
    with patch('discord_bot.utils.retry.async_sleep', new_callable=AsyncMock):
        result = await async_retry_discord_message_command(func, max_retries=2)
    assert result == 'ok'


@pytest.mark.asyncio
async def test_discord_retry_http_429_retries():
    """HTTPException with status=429 triggers exponential-backoff retry."""
    resp_429 = FakeResponse()
    resp_429.status = 429
    http_err = HTTPException(resp_429, 'rate limited')
    func = AsyncMock(side_effect=[http_err, 'ok'])
    with patch('discord_bot.utils.retry.async_sleep', new_callable=AsyncMock):
        result = await async_retry_discord_message_command(func, max_retries=2)
    assert result == 'ok'


@pytest.mark.asyncio
async def test_discord_retry_http_other_propagates_immediately():
    """HTTPException with non-429 status propagates immediately without retry."""
    resp_500 = FakeResponse()
    resp_500.status = 500
    http_err = HTTPException(resp_500, 'forbidden')
    func = AsyncMock(side_effect=http_err)
    with pytest.raises(HTTPException):
        await async_retry_discord_message_command(func, max_retries=3)
    func.assert_awaited_once()  # no retries
