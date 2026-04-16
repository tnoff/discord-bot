from asyncio import sleep as async_sleep
from typing import Awaitable, Callable

from aiohttp.client_exceptions import ClientConnectionError, ClientResponseError, ServerDisconnectedError
from discord.errors import DiscordServerError, HTTPException, NotFound, RateLimited
from opentelemetry.trace import SpanKind
from opentelemetry.trace.status import StatusCode

from discord_bot.utils.otel import async_otel_span_wrapper, AttributeNaming

OTEL_SPAN_PREFIX = 'utils'


async def async_retry_command(func: Callable[[], Awaitable], max_retries: int = 3,
                              retry_exceptions=None, accepted_exceptions=None):
    '''
    Retry func up to max_retries times with exponential backoff.

    func: Callable to run
    max_retries: Max retries before re-raising
    retry_exceptions: Retry on these exceptions
    accepted_exceptions: Exceptions that are swallowed (returns False)
    '''
    retry_exceptions = retry_exceptions or ()
    accepted_exceptions = accepted_exceptions or ()
    async with async_otel_span_wrapper(f'{OTEL_SPAN_PREFIX}.retry_command_async', kind=SpanKind.CLIENT) as span:
        for retry in range(max_retries + 1):
            span.set_attributes({AttributeNaming.RETRY_COUNT.value: retry})
            try:
                result = await func()
                span.set_status(StatusCode.OK)
                return result
            except accepted_exceptions as ex:
                span.record_exception(ex)
                span.set_status(StatusCode.OK)
                return False
            except retry_exceptions as ex:
                if retry == max_retries:
                    span.set_status(StatusCode.ERROR)
                    span.record_exception(ex)
                    raise
                await async_sleep(2 ** retry)


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
    async with async_otel_span_wrapper(f'{OTEL_SPAN_PREFIX}.message_send_async', kind=SpanKind.CLIENT) as span:
        for retry in range(max_retries + 1):
            span.set_attributes({AttributeNaming.RETRY_COUNT.value: retry})
            try:
                result = await func()
                span.set_status(StatusCode.OK)
                return result
            except accepted as ex:
                span.record_exception(ex)
                span.set_status(StatusCode.OK)
                return False
            except RateLimited as ex:
                if retry == max_retries:
                    span.set_status(StatusCode.ERROR)
                    span.record_exception(ex)
                    raise
                await async_sleep(ex.retry_after)
            except (DiscordServerError, TimeoutError, ServerDisconnectedError) as ex:
                if retry == max_retries:
                    span.set_status(StatusCode.ERROR)
                    span.record_exception(ex)
                    raise
                await async_sleep(2 ** retry)
            except HTTPException as ex:
                # Only retry 429s (e.g. error code 40062 "Service resource is being rate limited")
                if ex.status != 429 or retry == max_retries:
                    span.set_status(StatusCode.ERROR)
                    span.record_exception(ex)
                    raise
                await async_sleep(2 ** retry)


async def async_retry_broker_command(func: Callable[[], Awaitable], max_retries: int = 3):
    '''
    Retry broker HTTP calls with per-exception handling:
      - ClientConnectionError (includes ServerDisconnectedError, ServerTimeoutError): exponential backoff retry
      - ClientResponseError 5xx: exponential backoff retry
      - ClientResponseError 4xx: propagate immediately (client error, won't change on retry)
    '''
    async with async_otel_span_wrapper(f'{OTEL_SPAN_PREFIX}.retry_broker_command', kind=SpanKind.CLIENT) as span:
        for retry in range(max_retries + 1):
            span.set_attributes({AttributeNaming.RETRY_COUNT.value: retry})
            try:
                result = await func()
                span.set_status(StatusCode.OK)
                return result
            except ClientConnectionError as ex:
                if retry == max_retries:
                    span.set_status(StatusCode.ERROR)
                    span.record_exception(ex)
                    raise
                await async_sleep(2 ** retry)
            except ClientResponseError as ex:
                if ex.status < 500 or retry == max_retries:
                    span.set_status(StatusCode.ERROR)
                    span.record_exception(ex)
                    raise
                await async_sleep(2 ** retry)
