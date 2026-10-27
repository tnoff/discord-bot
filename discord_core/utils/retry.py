'''
Transport-level retry helpers — the ones with no discord.py in them.

Split out of utils/discord_retry so the broker, downloader and search pods stop
pulling discord.py through their HTTP clients. Every pod imports
clients/http_client_base, which needs async_retry_broker_command; that module
also held async_retry_discord_message_command, whose ``except`` clauses name
real discord.errors types and so cannot move under TYPE_CHECKING.

Nothing here knows about discord — these retry aiohttp failures. The
discord-specific helper stays in utils/discord_retry, which the gateway and
dispatcher import directly.
'''
import sys
from asyncio import sleep as async_sleep
from time import monotonic
from typing import Awaitable, Callable

from aiohttp.client_exceptions import ClientConnectionError, ClientOSError, ClientResponseError
from opentelemetry.trace import SpanKind
from opentelemetry.trace.status import StatusCode

from discord_core.utils.otel import async_otel_span_wrapper, async_untraced_span, AttributeNaming

OTEL_SPAN_PREFIX = 'utils'

# How long a refused or reset connection is retried. A pod that is rolling
# (the db is Recreate: old pod gone, new one still starting) refuses for longer
# than the 1/2/4s backoff covers, and every caller of it loses the request.
CONNECT_RETRY_BUDGET = 60.0   # seconds
CONNECT_RETRY_MAX_DELAY = 8.0  # seconds, cap on a single backoff


def _as_tuple(exceptions):
    '''Normalise an exception, tuple of exceptions, or None into a tuple.'''
    if not exceptions:
        return ()
    return exceptions if isinstance(exceptions, tuple) else (exceptions,)


# ``handle`` callbacks return one of these, or a float backoff delay.
ACCEPTED = object()   # swallow the exception and return False
PROPAGATE = None      # re-raise now, without consuming a retry


async def run_retry_loop(span_cm, func: Callable[[], Awaitable], max_retries: int,
                         retry_exceptions, handle: Callable):
    '''
    Drive ``func`` under ``span_cm``, delegating failure policy to ``handle``.

    The loop, the retry-count span attribute and the OK/ERROR status handling are
    identical across every retry helper here and in utils/discord_retry; only the
    exception policy differs. Keeping one driver is what lets the discord-specific
    helper live in another module without cloning this.

    ``retry_exceptions`` is the tuple actually caught — anything outside it
    propagates untouched, so no broad ``except`` is needed. ``handle(ex, retry)``
    returns ``ACCEPTED`` to swallow, ``PROPAGATE`` to re-raise immediately, or a
    float delay to back off and try again. Callers order their isinstance checks
    inside ``handle``, since a single ``except`` tuple loses the ordering that a
    chain of ``except`` clauses encodes.
    '''
    async with span_cm as span:
        for retry in range(max_retries + 1):
            span.set_attributes({AttributeNaming.RETRY_COUNT.value: retry})
            try:
                result = await func()
                span.set_status(StatusCode.OK)
                return result
            except retry_exceptions as ex:
                outcome = handle(ex, retry)
                if outcome is ACCEPTED:
                    span.record_exception(ex)
                    span.set_status(StatusCode.OK)
                    return False
                if outcome is PROPAGATE or retry == max_retries:
                    span.set_status(StatusCode.ERROR)
                    span.record_exception(ex)
                    raise
                await async_sleep(outcome)


async def async_retry_command(func: Callable[[], Awaitable], max_retries: int = 3,
                              retry_exceptions=None, accepted_exceptions=None):
    '''
    Retry func up to max_retries times with exponential backoff.

    func: Callable to run
    max_retries: Max retries before re-raising
    retry_exceptions: Retry on these exceptions
    accepted_exceptions: Exceptions that are swallowed (returns False)
    '''
    retry_exceptions = _as_tuple(retry_exceptions)
    accepted_exceptions = _as_tuple(accepted_exceptions)

    def _handle(ex, retry):
        # accepted wins over retry, matching the original except-clause order
        if accepted_exceptions and isinstance(ex, accepted_exceptions):
            return ACCEPTED
        return 2 ** retry

    return await run_retry_loop(
        async_otel_span_wrapper(f'{OTEL_SPAN_PREFIX}.retry_command_async', kind=SpanKind.CLIENT),
        func, max_retries, accepted_exceptions + retry_exceptions, _handle,
    )


async def async_retry_broker_command(func: Callable[[], Awaitable], max_retries: int = 3,
                                    traced: bool = True):
    '''
    Retry broker HTTP calls with per-exception handling:
      - ClientOSError (refused or reset connection, includes ClientConnectorError):
        capped exponential backoff for CONNECT_RETRY_BUDGET seconds, so a pod roll
        is ridden out rather than failing the call. Only when traced: see below
      - other ClientConnectionError (ServerDisconnectedError, ServerTimeoutError):
        exponential backoff, max_retries times
      - ClientResponseError 5xx: exponential backoff, max_retries times
      - ClientResponseError 4xx: propagate immediately (client error, won't change on retry)

    traced=False starts no span.  Reserved for fixed-interval background pollers,
    whose ticks are emitted at the poll rate rather than per unit of work: at 1Hz
    per client this span was ~99% of the bot's total span volume, and every tick
    against a restarting worker pod also landed as an ERROR span for a failure
    the poller already tolerates.  Callers that lose work when the call fails
    stay traced.  The same split applies to the connection budget: a poller
    tolerates a failed tick, so it keeps the short retry instead of blocking
    for a minute.
    '''
    connect_started = None
    failures = 0

    def _handle(ex, retry):
        nonlocal connect_started, failures
        if isinstance(ex, ClientResponseError) and ex.status < 500:
            return PROPAGATE
        if traced and isinstance(ex, ClientOSError):
            now = monotonic()
            if connect_started is None:
                connect_started = now
            if now - connect_started >= CONNECT_RETRY_BUDGET:
                return PROPAGATE
            delay = min(2 ** retry, CONNECT_RETRY_MAX_DELAY)
            return min(delay, CONNECT_RETRY_BUDGET - (now - connect_started))
        # run_retry_loop counts every attempt, connection refusals included, so
        # the fixed budget for everything else is counted here instead.
        if failures >= max_retries:
            return PROPAGATE
        failures += 1
        return 2 ** retry

    span_cm = (async_otel_span_wrapper(f'{OTEL_SPAN_PREFIX}.retry_broker_command', kind=SpanKind.CLIENT)
               if traced else async_untraced_span())
    # The loop's own count is lifted out of the way: ``_handle`` decides when to stop.
    return await run_retry_loop(
        span_cm, func, sys.maxsize,
        (ClientConnectionError, ClientResponseError), _handle,
    )
