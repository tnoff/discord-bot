from asyncio import sleep as async_sleep
from typing import Awaitable, Callable

from aiohttp.client_exceptions import ServerDisconnectedError
from discord.errors import DiscordServerError, HTTPException, NotFound, RateLimited
from opentelemetry.trace import SpanKind
from opentelemetry.trace.status import StatusCode

from discord_bot.utils.otel import otel_span_wrapper, AttributeNaming

OTEL_SPAN_PREFIX = 'utils'


class SkipRetrySleep(Exception):
    '''
    Call this to skip generic retry logic
    '''


async def async_retry_command(func: Callable[[], Awaitable], max_retries: int = 3,
                              retry_exceptions=None, post_exception_functions=None,
                              accepted_exceptions=None):
    '''
    Use retries for the command, mostly deals with db issues

    func: Callable partial function to run
    max_retries : Max retries until we fail
    retry_exceptions: Retry on these exceptions
    post_exception_functions: On retry_exceptions, run these functions
    accepted_exceptions: Exceptions that are swallowed
    '''
    retry_exceptions = retry_exceptions or ()
    post_functions = post_exception_functions or []
    accepted_exceptions = accepted_exceptions or ()
    retry = -1
    with otel_span_wrapper(f'{OTEL_SPAN_PREFIX}.retry_command_async', kind=SpanKind.CLIENT) as span:
        while True:
            retry += 1
            should_sleep = True
            span.set_attributes({
                AttributeNaming.RETRY_COUNT.value: retry
            })
            try:
                result = await func()
                span.set_status(StatusCode.OK)
                return result
            except accepted_exceptions as ex:
                span.record_exception(ex)
                span.set_status(StatusCode.OK)
                return False
            except retry_exceptions as ex:
                try:
                    for pf in post_functions:
                        await pf(ex, retry == max_retries)
                except SkipRetrySleep:
                    should_sleep = False
                if retry < max_retries:
                    if should_sleep:
                        sleep_for = 2 ** (retry - 1)
                        await async_sleep(sleep_for)
                    continue
                span.set_status(StatusCode.ERROR)
                span.record_exception(ex)
                raise


async def async_retry_discord_message_command(func: Callable[[], Awaitable], max_retries: int = 3, allow_404: bool = False):
    '''
    Retry discord send message command, catch case of rate limiting

    func: Function to retry
    max_retries: Max retry before failing
    allow_404 : 404 exceptions are fine and we can skip
    '''
    # For 429s, there is a 'retry_after' arg that tells how long to sleep before trying again
    async def check_429(ex, is_last_retry):
        if isinstance(ex, RateLimited) and not is_last_retry:
            await async_sleep(ex.retry_after)
            raise SkipRetrySleep('Skip sleep since we slept already')
        # Discord error 40062 ("Service resource is being rate limited") is raised as a plain
        # HTTPException with status 429, not as RateLimited — retry it with normal backoff.
        # All other HTTPExceptions should propagate immediately without retrying.
        if isinstance(ex, HTTPException) and ex.status != 429:
            raise ex
    post_exception_functions = [check_429]
    # These are common discord api exceptions we can retry on
    retry_exceptions = (RateLimited, DiscordServerError, TimeoutError, ServerDisconnectedError, HTTPException)
    accepted_exceptions = ()
    if allow_404:
        accepted_exceptions = NotFound
    with otel_span_wrapper(f'{OTEL_SPAN_PREFIX}.message_send_async', kind=SpanKind.CLIENT):
        return await async_retry_command(func, max_retries=max_retries,
                                         retry_exceptions=retry_exceptions, post_exception_functions=post_exception_functions,
                                         accepted_exceptions=accepted_exceptions)
