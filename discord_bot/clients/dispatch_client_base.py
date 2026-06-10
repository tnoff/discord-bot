'''Base class for dispatch clients (Redis and HTTP).

Provides shared cog queue management and the submit-request lifecycle for
fetch_history and fetch_emojis: queue lookup, OTel span, fetch, decode, error
fallback, and result delivery.  Subclasses implement _do_fetch_history and
_do_fetch_emojis to perform the actual transport-level call.
'''
import asyncio

from opentelemetry import trace

from discord_bot.types.dispatch_request import FetchChannelHistoryRequest, FetchGuildEmojisRequest
from discord_bot.types.dispatch_result import ChannelHistoryResult, GuildEmojisResult, decode_history_result, decode_emojis_result
from discord_bot.utils.otel import async_otel_span_wrapper


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
        '''Register a result delivery queue for the named cog.'''
        q: asyncio.Queue = asyncio.Queue()
        self._cog_queues[cog_name] = q
        return q

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
            await q.put(result)

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
            await q.put(result)
