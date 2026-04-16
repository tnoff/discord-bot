import asyncio

import redis.asyncio as aioredis
from opentelemetry import trace
from opentelemetry.propagate import inject

from discord_bot.types.dispatch_request import (
    FetchChannelHistoryRequest,
    FetchGuildEmojisRequest,
    SendRequest,
    DeleteRequest,
)
from discord_bot.utils.dispatch_client_base import DispatchClientBase, DispatchRemoteError
from discord_bot.utils.dispatch_helpers import decode_history_result, decode_emojis_result
from discord_bot.utils.dispatch_envelope import (
    RequestType, ResultType,
    StreamEnvelope, StreamResult, new_request_id,
)
from discord_bot.utils.redis_stream_helpers import (
    input_stream_key, result_stream_key, xadd, xread_latest,
)
from discord_bot.utils.common import get_logger, LoggingConfig
from discord_bot.utils.otel import async_otel_span_wrapper, DispatchNaming


class RedisDispatchClient(DispatchClientBase):
    '''Drop-in replacement for MessageDispatcher for use in cog processes.'''

    def __init__(self, redis_client: aioredis.Redis, process_id: str, logging_config: LoggingConfig = None, shard_id: int = 0):
        self.logger = get_logger(__name__, logging_config)
        self._redis = redis_client
        self._process_id = process_id
        self._shard_id = shard_id
        self._input_key = input_stream_key(shard_id)
        self._result_key = result_stream_key(process_id)
        self._pending: dict[str, asyncio.Future] = {}
        self._cog_queues: dict[str, asyncio.Queue] = {}
        self._result_task: asyncio.Task | None = None

    async def start(self):
        '''Start background result poller.'''
        self._result_task = asyncio.create_task(self._result_poller())

    def stop(self):
        '''Cancel the background result poller task.'''
        if self._result_task:
            self._result_task.cancel()

    async def _result_poller(self):
        last_id = '$'
        while True:
            messages = await xread_latest(self._redis, self._result_key, last_id=last_id)
            for msg_id, fields in messages:
                last_id = msg_id
                result = StreamResult.decode(fields)
                fut = self._pending.pop(result.request_id, None)
                if fut and not fut.done():
                    if result.result_type == ResultType.ERROR:
                        fut.set_exception(DispatchRemoteError(result.payload.get('error', 'unknown')))
                    else:
                        fut.set_result((result.result_type, result.payload))

    async def _send_request(self, req_type: str, payload: dict, request_id: str | None = None, expect_result: bool = False):
        if request_id is None:
            request_id = new_request_id()
        async with async_otel_span_wrapper('dispatch_client.send_request',
                                           kind=trace.SpanKind.PRODUCER,
                                           attributes={
                                               'req_type': req_type,
                                               DispatchNaming.REQUEST_ID.value: request_id,
                                           }):
            carrier: dict = {}
            inject(carrier)
            envelope = StreamEnvelope(req_type, payload, self._process_id, request_id, trace_context=carrier).encode()
            if expect_result:
                loop = asyncio.get_event_loop()
                fut: asyncio.Future = loop.create_future()
                self._pending[request_id] = fut
            await xadd(self._redis, self._input_key, envelope)
            if expect_result:
                return await fut
            return None

    async def submit_request(self, request) -> None:
        '''Submit a typed cog request, routing to appropriate handler.'''
        if isinstance(request, SendRequest):
            self.send_message(request.guild_id, request.channel_id,
                              request.content, delete_after=request.delete_after)
        elif isinstance(request, DeleteRequest):
            self.delete_message(request.guild_id, request.channel_id, request.message_id)
        elif isinstance(request, FetchChannelHistoryRequest):
            asyncio.create_task(self._submit_history_request(request))
        elif isinstance(request, FetchGuildEmojisRequest):
            asyncio.create_task(self._submit_emojis_request(request))

    # ── transport implementations for DispatchClientBase ─────────────────────

    async def _do_fetch_history(self, params: dict) -> dict:
        _, payload = await self._send_request(RequestType.FETCH_HISTORY, params, expect_result=True)
        return payload

    async def _do_fetch_emojis(self, params: dict) -> dict:
        _, payload = await self._send_request(RequestType.FETCH_EMOJIS, params, expect_result=True)
        return payload

    # ── fire-and-forget ──────────────────────────────────────────────────────

    def update_mutable(self, key, guild_id, content, channel_id, sticky=True, delete_after=None):
        '''Fire-and-forget: enqueue a mutable bundle update on the dispatcher process.'''
        if not content:
            self.logger.debug('update_mutable: empty content for key=%s, routing to remove_mutable', key)
            return self.remove_mutable(key)
        req_id = new_request_id()
        trace.get_current_span().set_attribute(DispatchNaming.REQUEST_ID.value, req_id)
        self.logger.debug('update_mutable: key=%s dispatch.request_id=%s', key, req_id)
        asyncio.create_task(self._send_request(RequestType.UPDATE_MUTABLE, {
            'key': key, 'guild_id': guild_id, 'content': content,
            'channel_id': channel_id, 'sticky': sticky, 'delete_after': delete_after,
        }, request_id=req_id))
        return req_id

    def remove_mutable(self, key):
        '''Fire-and-forget: enqueue a mutable bundle removal on the dispatcher process.'''
        req_id = new_request_id()
        trace.get_current_span().set_attribute(DispatchNaming.REQUEST_ID.value, req_id)
        self.logger.debug('remove_mutable: key=%s dispatch.request_id=%s', key, req_id)
        asyncio.create_task(self._send_request(RequestType.REMOVE_MUTABLE, {'key': key}, request_id=req_id))
        return req_id

    def update_mutable_channel(self, key, guild_id, new_channel_id):
        '''Fire-and-forget: enqueue a mutable bundle channel update on the dispatcher process.'''
        asyncio.create_task(self._send_request(RequestType.UPDATE_MUTABLE_CHANNEL, {
            'key': key, 'guild_id': guild_id, 'new_channel_id': new_channel_id,
        }))

    def send_message(self, guild_id, channel_id, content, delete_after=None, allow_404=False):
        '''Fire-and-forget: enqueue a send request on the dispatcher process.'''
        asyncio.create_task(self._send_request(RequestType.SEND, {
            'guild_id': guild_id, 'channel_id': channel_id,
            'content': content, 'delete_after': delete_after, 'allow_404': allow_404,
        }))

    def delete_message(self, guild_id, channel_id, message_id):
        '''Fire-and-forget: enqueue a delete request on the dispatcher process.'''
        asyncio.create_task(self._send_request(RequestType.DELETE, {
            'guild_id': guild_id, 'channel_id': channel_id, 'message_id': message_id,
        }))

    # ── awaitable fetches ────────────────────────────────────────────────────

    async def dispatch_channel_history(self, guild_id, channel_id, limit, after=None,
                                       after_message_id=None, oldest_first=True):
        '''Fetch channel history via the dispatcher process and return a ChannelHistoryResult.'''
        _, payload = await self._send_request(RequestType.FETCH_HISTORY, {
            'guild_id': guild_id, 'channel_id': channel_id, 'limit': limit,
            'after': after.isoformat() if after else None,
            'after_message_id': after_message_id, 'oldest_first': oldest_first,
        }, expect_result=True)
        return decode_history_result(payload)

    async def dispatch_guild_emojis(self, guild_id, max_retries=3):
        '''Fetch guild emojis via the dispatcher process and return a GuildEmojisResult.'''
        _, payload = await self._send_request(RequestType.FETCH_EMOJIS, {
            'guild_id': guild_id, 'max_retries': max_retries,
        }, expect_result=True)
        return decode_emojis_result(payload)
