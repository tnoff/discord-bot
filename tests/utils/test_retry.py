'''
Tests for the discord-free retry helpers (discord_bot/utils/retry.py).

Split out of test_discord_retry alongside the module. These drive the aiohttp
retry paths that every pod uses; the discord.errors paths stay next door.
'''
from unittest.mock import AsyncMock, Mock, patch

import pytest
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter

from aiohttp.client_exceptions import ClientConnectionError, ClientResponseError

from discord_bot.utils.retry import (
    async_retry_broker_command,
    async_retry_command,
)


def _client_response_error(status: int) -> ClientResponseError:
    return ClientResponseError(Mock(), (), status=status)


# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_async_retry_command_success():
    """Successful call returns the result immediately (lines 31-32)."""
    func = AsyncMock(return_value='ok')
    result = await async_retry_command(func)
    assert result == 'ok'
    func.assert_awaited_once()


@pytest.mark.asyncio
async def test_async_retry_command_accepted_exception_returns_false():
    """An accepted_exception is swallowed and returns False (lines 34-36)."""
    func = AsyncMock(side_effect=ValueError('swallow me'))
    result = await async_retry_command(func, accepted_exceptions=(ValueError,))
    assert result is False
    func.assert_awaited_once()


@pytest.mark.asyncio
async def test_async_retry_command_retry_then_succeed():
    """retry_exceptions trigger retries; success on a later attempt returns the result."""
    func = AsyncMock(side_effect=[RuntimeError('fail'), RuntimeError('fail'), 'done'])
    with patch('discord_bot.utils.retry.async_sleep', new_callable=AsyncMock):
        result = await async_retry_command(func, max_retries=3, retry_exceptions=(RuntimeError,))
    assert result == 'done'
    assert func.await_count == 3


@pytest.mark.asyncio
async def test_async_retry_command_exhausted_raises():
    """retry_exceptions that persist past max_retries are re-raised."""
    func = AsyncMock(side_effect=RuntimeError('always fails'))
    with patch('discord_bot.utils.retry.async_sleep', new_callable=AsyncMock):
        with pytest.raises(RuntimeError, match='always fails'):
            await async_retry_command(func, max_retries=2, retry_exceptions=(RuntimeError,))
    assert func.await_count == 3  # initial + 2 retries


@pytest.mark.asyncio
async def test_broker_retry_success():
    '''Successful call returns the result immediately.'''
    func = AsyncMock(return_value='ok')
    result = await async_retry_broker_command(func)
    assert result == 'ok'
    func.assert_awaited_once()


@pytest.mark.asyncio
async def test_broker_retry_connection_error_retries():
    '''ClientConnectionError triggers exponential-backoff retry; success returns the result.'''
    func = AsyncMock(side_effect=[ClientConnectionError(), 'recovered'])
    with patch('discord_bot.utils.retry.async_sleep', new_callable=AsyncMock) as mock_sleep:
        result = await async_retry_broker_command(func, max_retries=2)
    assert result == 'recovered'
    mock_sleep.assert_awaited_once_with(1)  # 2**0


@pytest.mark.asyncio
async def test_broker_retry_connection_error_exhausted_raises():
    '''ClientConnectionError that persists past max_retries is re-raised.'''
    func = AsyncMock(side_effect=ClientConnectionError())
    with patch('discord_bot.utils.retry.async_sleep', new_callable=AsyncMock):
        with pytest.raises(ClientConnectionError):
            await async_retry_broker_command(func, max_retries=2)
    assert func.await_count == 3  # initial + 2 retries


@pytest.mark.asyncio
async def test_broker_retry_5xx_retries():
    '''ClientResponseError with 5xx status triggers retry; success returns the result.'''
    func = AsyncMock(side_effect=[_client_response_error(503), 'ok'])
    with patch('discord_bot.utils.retry.async_sleep', new_callable=AsyncMock):
        result = await async_retry_broker_command(func, max_retries=2)
    assert result == 'ok'


@pytest.mark.asyncio
async def test_broker_retry_5xx_exhausted_raises():
    '''ClientResponseError 5xx that persists past max_retries is re-raised.'''
    func = AsyncMock(side_effect=_client_response_error(500))
    with patch('discord_bot.utils.retry.async_sleep', new_callable=AsyncMock):
        with pytest.raises(ClientResponseError):
            await async_retry_broker_command(func, max_retries=1)
    assert func.await_count == 2


@pytest.mark.asyncio
async def test_broker_retry_4xx_propagates_immediately():
    '''ClientResponseError with 4xx status propagates immediately without retry.'''
    func = AsyncMock(side_effect=_client_response_error(422))
    with pytest.raises(ClientResponseError) as exc_info:
        await async_retry_broker_command(func, max_retries=3)
    assert exc_info.value.status == 422
    func.assert_awaited_once()  # no retries


# ---------------------------------------------------------------------------
# async_retry_broker_command -- traced flag
#
# The span is emitted per CALL, so a fixed-interval poller emits at the poll
# rate rather than per unit of work: two clients at 1Hz made this span ~99% of
# the bot's span volume, and each tick against a restarting worker pod also
# landed an ERROR span for a failure the poller already tolerates.
# ---------------------------------------------------------------------------

def _recording_tracer():
    '''Throwaway SDK tracer + exporter; the suite has no global provider.'''
    exporter = InMemorySpanExporter()
    provider = TracerProvider()
    provider.add_span_processor(SimpleSpanProcessor(exporter))
    return provider.get_tracer('test'), exporter

@pytest.mark.asyncio
async def test_broker_retry_is_traced_by_default():
    '''Callers that lose work on failure keep their span -- the flag is opt-in.'''
    tracer, exporter = _recording_tracer()
    with patch('discord_bot.utils.otel.TRACER', tracer):
        await async_retry_broker_command(AsyncMock(return_value='ok'))
    assert [s.name for s in exporter.get_finished_spans()] == ['utils.retry_broker_command']


@pytest.mark.asyncio
async def test_broker_retry_untraced_emits_no_span():
    '''traced=False starts no span, and still returns the result.'''
    tracer, exporter = _recording_tracer()
    with patch('discord_bot.utils.otel.TRACER', tracer):
        result = await async_retry_broker_command(AsyncMock(return_value='ok'), traced=False)
    assert result == 'ok'
    assert not exporter.get_finished_spans()


@pytest.mark.asyncio
async def test_broker_retry_untraced_emits_no_span_on_exhausted_failure():
    '''The exhausted-retry path is the one that was stamping ERROR during a worker
    restart, so it must emit nothing too -- and must still raise.'''
    tracer, exporter = _recording_tracer()
    func = AsyncMock(side_effect=ClientConnectionError())
    with patch('discord_bot.utils.otel.TRACER', tracer):
        with patch('discord_bot.utils.retry.async_sleep', new_callable=AsyncMock):
            with pytest.raises(ClientConnectionError):
                await async_retry_broker_command(func, max_retries=2, traced=False)
    assert not exporter.get_finished_spans()
    assert func.await_count == 3


@pytest.mark.asyncio
async def test_broker_retry_untraced_still_retries():
    '''traced only controls the span; retry behaviour is unchanged.'''
    func = AsyncMock(side_effect=[ClientConnectionError(), 'recovered'])
    with patch('discord_bot.utils.retry.async_sleep', new_callable=AsyncMock) as mock_sleep:
        result = await async_retry_broker_command(func, max_retries=2, traced=False)
    assert result == 'recovered'
    mock_sleep.assert_awaited_once_with(1)
