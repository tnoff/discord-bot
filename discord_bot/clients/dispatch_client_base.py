'''Base class for dispatch clients (Redis and HTTP).

Provides shared cog queue management and the submit-request lifecycle for
fetch_history and fetch_emojis: queue lookup, OTel span, fetch, decode, error
fallback, and result delivery.  Subclasses implement _do_fetch_history and
_do_fetch_emojis to perform the actual transport-level call.
'''
import asyncio
import logging

from opentelemetry import trace

from discord_bot.types.dispatch_request import (
    DeleteRequest,
    FetchChannelHistoryRequest,
    FetchGuildEmojisRequest,
    SendRequest,
)
from discord_bot.types.dispatch_result import ChannelHistoryResult, GuildEmojisResult, decode_history_result, decode_emojis_result
from discord_bot.utils.otel import async_otel_span_wrapper

logger = logging.getLogger(__name__)

# Bound the per-cog result queues so a wedged/dead consumer applies backpressure
# (drops) instead of leaking memory without limit. Healthy consumers keep the
# depth near 0, so this only trips on the failure mode the dispatch_result_queue_depth
# gauge is there to alert on. See docs findings/2026-07-19 (OOM root cause).
RESULT_QUEUE_MAX_SIZE = 1000


class DispatchRemoteError(Exception):
    '''Raised when the dispatcher returned an error payload.'''


def _history_params(request: FetchChannelHistoryRequest) -> dict:
    return {
        'guild_id': request.guild_id,
        'channel_id': request.channel_id,
        'limit': request.limit,
        'after': request.after.isoformat() if request.after else None,
        'after_message_id': request.after_message_id,
        'oldest_first': request.oldest_first,
    }


def _emojis_params(request: FetchGuildEmojisRequest) -> dict:
    return {'guild_id': request.guild_id, 'max_retries': request.max_retries}


class DispatchClientBase:
    '''Shared cog queue registration and fetch submission for dispatch clients.'''

    _cog_queues: dict[str, asyncio.Queue]  # initialised by subclass __init__

    def register_cog_queue(self, cog_name: str) -> asyncio.Queue:
        '''Register a bounded result delivery queue for the named cog.'''
        q: asyncio.Queue = asyncio.Queue(maxsize=RESULT_QUEUE_MAX_SIZE)
        self._cog_queues[cog_name] = q
        return q

    def _deliver(self, queue: asyncio.Queue, result, cog_name: str) -> None:
        '''Deliver a result to a cog queue without blocking.

        put_nowait (not ``await put``) so a stalled/dead consumer can't
        back-pressure the fetch task into holding the result forever. On a full
        queue the result is dropped and logged — the consumer isn't draining, and
        the dispatch_result_queue_depth gauge is already pegged at maxsize.
        '''
        try:
            queue.put_nowait(result)
        except asyncio.QueueFull:
            logger.warning(
                'Dispatch result queue for cog %s is full (maxsize=%d) — dropping '
                'result; the consumer loop is not draining', cog_name, RESULT_QUEUE_MAX_SIZE)

    async def submit_request(self, request) -> None:
        '''Route a typed cog request to the appropriate send, delete, or fetch method.'''
        if isinstance(request, SendRequest):
            self._handle_send(request)
        elif isinstance(request, DeleteRequest):
            self._handle_delete(request)
        elif isinstance(request, FetchChannelHistoryRequest):
            asyncio.create_task(self._submit_history_request(request))
        elif isinstance(request, FetchGuildEmojisRequest):
            asyncio.create_task(self._submit_emojis_request(request))

    def _handle_send(self, request: SendRequest) -> None:
        '''Dispatch a SendRequest; subclasses implement the transport.'''
        raise NotImplementedError

    def _handle_delete(self, request: DeleteRequest) -> None:
        '''Dispatch a DeleteRequest; subclasses implement the transport.'''
        raise NotImplementedError

    async def _do_fetch_history(self, params: dict) -> dict:
        '''Perform the fetch_history transport call; return raw payload or raise DispatchRemoteError.'''
        raise NotImplementedError

    async def _do_fetch_emojis(self, params: dict) -> dict:
        '''Perform the fetch_emojis transport call; return raw payload or raise DispatchRemoteError.'''
        raise NotImplementedError

    async def _submit_history_request(self, request: FetchChannelHistoryRequest) -> None:
        q = self._cog_queues.get(request.cog_name)
        if q is None:
            return
        async with async_otel_span_wrapper('dispatch_client.fetch_history',
                                           kind=trace.SpanKind.CLIENT,
                                           attributes={
                                               'discord.guild': request.guild_id,
                                               'discord.channel': request.channel_id,
                                           }) as span:
            try:
                payload = await self._do_fetch_history(_history_params(request))
                result = decode_history_result(payload)
            except DispatchRemoteError as exc:
                span.record_exception(exc)
                result = ChannelHistoryResult(
                    guild_id=request.guild_id,
                    channel_id=request.channel_id,
                    messages=[],
                    after_message_id=request.after_message_id,
                    error=exc,
                )
            self._deliver(q, result, request.cog_name)

    async def _submit_emojis_request(self, request: FetchGuildEmojisRequest) -> None:
        q = self._cog_queues.get(request.cog_name)
        if q is None:
            return
        async with async_otel_span_wrapper('dispatch_client.fetch_emojis',
                                           kind=trace.SpanKind.CLIENT,
                                           attributes={'discord.guild': request.guild_id}) as span:
            try:
                payload = await self._do_fetch_emojis(_emojis_params(request))
                result = decode_emojis_result(payload)
            except DispatchRemoteError as exc:
                span.record_exception(exc)
                result = GuildEmojisResult(guild_id=request.guild_id, emojis=[], error=exc)
            self._deliver(q, result, request.cog_name)
