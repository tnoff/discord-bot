'''
Retry helpers for calls that go through discord.py.

The transport-level helpers that used to live here moved to utils/retry when the
worker images dropped discord.py — see that module. They are re-exported below so
existing imports keep working, the same courtesy CheckoutResult, ClearGuildResult
and the BrokerClient Protocol got when they moved.

Importing this module pulls discord.py. Only the gateway and dispatcher should.
'''
from typing import Awaitable, Callable

from aiohttp.client_exceptions import ServerDisconnectedError
from discord.errors import DiscordServerError, HTTPException, NotFound, RateLimited
from opentelemetry.trace import SpanKind

from discord_bot.utils.otel import async_otel_span_wrapper
from discord_bot.utils.retry import (
    ACCEPTED, PROPAGATE, async_retry_broker_command, async_retry_command, run_retry_loop,
)

__all__ = ['async_retry_command', 'async_retry_broker_command',
           'async_retry_discord_message_command']

OTEL_SPAN_PREFIX = 'utils'


async def async_retry_discord_message_command(func: Callable[[], Awaitable], max_retries: int = 3, allow_404: bool = False):
    '''
    Retry discord API calls with per-exception handling:
      - RateLimited: sleep retry_after, then retry
      - DiscordServerError (5xx), TimeoutError, ServerDisconnectedError: exponential backoff retry
      - HTTPException status=429 (e.g. error code 40062): exponential backoff retry
      - HTTPException any other status: propagate immediately, no retry
      - NotFound (404) with allow_404=True: swallowed, returns False
    '''
    accepted = (NotFound,) if allow_404 else ()

    def _handle(ex, retry):
        # Order matters and is load-bearing: NotFound and DiscordServerError both
        # subclass HTTPException, so the specific cases must be tested first —
        # exactly the ordering a chain of except clauses used to give for free.
        if accepted and isinstance(ex, accepted):
            return ACCEPTED
        if isinstance(ex, RateLimited):
            return ex.retry_after
        if isinstance(ex, (DiscordServerError, TimeoutError, ServerDisconnectedError)):
            return 2 ** retry
        # Only retry 429s (e.g. error code 40062 "Service resource is being rate limited")
        if isinstance(ex, HTTPException) and ex.status != 429:
            return PROPAGATE
        return 2 ** retry

    caught = accepted + (RateLimited, DiscordServerError, TimeoutError,
                         ServerDisconnectedError, HTTPException)
    return await run_retry_loop(
        async_otel_span_wrapper(f'{OTEL_SPAN_PREFIX}.message_send_async', kind=SpanKind.CLIENT),
        func, max_retries, caught, _handle,
    )
