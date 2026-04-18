'''HTTP client for cross-process dispatch via DispatchHttpServer (HA mode).'''
import asyncio
import logging

import aiohttp
from opentelemetry import trace

from discord_bot.types.dispatch_request import (
    DeleteRequest,
    FetchChannelHistoryRequest,
    FetchGuildEmojisRequest,
    SendRequest,
)
from discord_bot.clients.dispatch_client_base import DispatchClientBase, DispatchRemoteError
from discord_bot.utils.dispatch_queue import dispatch_request_id
from discord_bot.utils.discord_retry import async_retry_broker_command
from discord_bot.clients.http_client_base import HttpClientMixin
from discord_bot.utils.otel import DispatchNaming

logger = logging.getLogger(__name__)

_POLL_INTERVAL_BASE = 0.5   # seconds — first poll delay
_POLL_INTERVAL_MAX = 10.0   # seconds — cap for exponential backoff
_POLL_TIMEOUT = 300.0       # seconds — give up after this long


class HttpDispatchClient(HttpClientMixin, DispatchClientBase):
    '''
    DispatchClient that forwards calls to a remote DispatchHttpServer over HTTP.

    Fire-and-forget calls (send_message, delete_message, update_mutable, etc.) POST
    to the server and return immediately.

    Awaitable calls (via submit_request with FetchChannelHistoryRequest /
    FetchGuildEmojisRequest) submit a POST, receive a request_id, then poll
    GET /dispatch/results/{request_id} with exponential backoff until the result
    is available, then deliver it to the registered cog result queue.
    '''

    def __init__(self, base_url: str, session: aiohttp.ClientSession | None = None):
        self._base_url = base_url.rstrip('/')
        self._session = session
        self._cog_queues: dict[str, asyncio.Queue] = {}

    async def start(self) -> None:
        '''No-op — no background poller needed (polling happens per-request).'''

    def stop(self) -> None:
        '''No-op — nothing to cancel.'''

    # ------------------------------------------------------------------
    # Request routing (typed dispatch_request objects)
    # ------------------------------------------------------------------

    async def submit_request(self, request) -> None:
        '''Submit a typed cog request, routing to the appropriate HTTP call.'''
        if isinstance(request, SendRequest):
            asyncio.create_task(self._post('/dispatch/send', {
                'guild_id': request.guild_id, 'channel_id': request.channel_id,
                'content': request.content, 'delete_after': request.delete_after,
                'span_context': request.span_context,
            }))
        elif isinstance(request, DeleteRequest):
            asyncio.create_task(self._post('/dispatch/delete', {
                'guild_id': request.guild_id, 'channel_id': request.channel_id,
                'message_id': request.message_id, 'span_context': request.span_context,
            }))
        elif isinstance(request, FetchChannelHistoryRequest):
            asyncio.create_task(self._submit_history_request(request))
        elif isinstance(request, FetchGuildEmojisRequest):
            asyncio.create_task(self._submit_emojis_request(request))

    # ------------------------------------------------------------------
    # Fire-and-forget methods
    # ------------------------------------------------------------------

    def update_mutable(self, key: str, guild_id: int, content: list,
                       channel_id: int | None, sticky: bool = True, delete_after: int | None = None):
        '''Fire-and-forget: POST /dispatch/update_mutable.'''
        if not content:
            logger.debug('update_mutable: empty content for key=%s, routing to remove_mutable', key)
            return self.remove_mutable(key)
        req_id = dispatch_request_id({'key': key, 'guild_id': guild_id, 't': str(asyncio.get_event_loop().time())})
        trace.get_current_span().set_attribute(DispatchNaming.REQUEST_ID.value, req_id)
        logger.debug('update_mutable: key=%s dispatch.request_id=%s', key, req_id)
        asyncio.create_task(self._post('/dispatch/update_mutable', {
            'key': key, 'guild_id': guild_id, 'content': content,
            'channel_id': channel_id, 'sticky': sticky, 'delete_after': delete_after,
        }))
        return req_id

    def remove_mutable(self, key: str):
        '''Fire-and-forget: POST /dispatch/remove_mutable.'''
        asyncio.create_task(self._post('/dispatch/remove_mutable', {'key': key}))

    def update_mutable_channel(self, key: str, guild_id: int, new_channel_id: int):
        '''Fire-and-forget: POST /dispatch/update_mutable_channel.'''
        asyncio.create_task(self._post('/dispatch/update_mutable_channel', {
            'key': key, 'guild_id': guild_id, 'new_channel_id': new_channel_id,
        }))

    def send_message(self, guild_id: int, channel_id: int, content: str,
                     delete_after: int | None = None, allow_404: bool = False,
                     span_context: dict | None = None):
        '''Fire-and-forget: POST /dispatch/send.'''
        asyncio.create_task(self._post('/dispatch/send', {
            'guild_id': guild_id, 'channel_id': channel_id, 'content': content,
            'delete_after': delete_after, 'allow_404': allow_404, 'span_context': span_context,
        }))

    def delete_message(self, guild_id: int, channel_id: int, message_id: int,
                       span_context: dict | None = None):
        '''Fire-and-forget: POST /dispatch/delete.'''
        asyncio.create_task(self._post('/dispatch/delete', {
            'guild_id': guild_id, 'channel_id': channel_id,
            'message_id': message_id, 'span_context': span_context,
        }))

    # ------------------------------------------------------------------
    # Transport implementations for DispatchClientBase
    # ------------------------------------------------------------------

    async def _do_fetch_history(self, params: dict) -> dict:
        request_id = await self._submit_fetch('/dispatch/fetch_history', params)
        payload = await self._poll_result(request_id)
        if 'error' in payload:
            raise DispatchRemoteError(payload['error'])
        return payload

    async def _do_fetch_emojis(self, params: dict) -> dict:
        request_id = await self._submit_fetch('/dispatch/fetch_emojis', params)
        payload = await self._poll_result(request_id)
        if 'error' in payload:
            raise DispatchRemoteError(payload['error'])
        return payload

    # ------------------------------------------------------------------
    # HTTP helpers
    # ------------------------------------------------------------------

    async def _post(self, path: str, body: dict) -> None:
        '''POST *body* to *path* with retry; logs and swallows errors so callers are fire-and-forget.'''
        session = self._get_session()
        async def _call():
            async with session.post(
                f'{self._base_url}{path}',
                headers=self._trace_headers(),
                json=body,
            ) as resp:
                resp.raise_for_status()
        try:
            await async_retry_broker_command(_call)
        except Exception as exc:
            logger.error('HttpDispatchClient :: POST %s failed: %s', path, exc)

    async def _submit_fetch(self, path: str, params: dict) -> str:
        '''POST *params* to *path* and return the request_id from the 202 response.'''
        session = self._get_session()
        async def _call():
            async with session.post(
                f'{self._base_url}{path}',
                headers=self._trace_headers(),
                json=params,
            ) as resp:
                resp.raise_for_status()
                return await resp.json()
        data = await async_retry_broker_command(_call)
        return data['request_id']

    async def _poll_result(self, request_id: str) -> dict:
        '''Poll GET /dispatch/results/{request_id} with exponential backoff until available.'''
        session = self._get_session()
        interval = _POLL_INTERVAL_BASE
        deadline = asyncio.get_event_loop().time() + _POLL_TIMEOUT
        while True:
            async def _call():
                async with session.get(
                    f'{self._base_url}/dispatch/results/{request_id}',
                    headers=self._trace_headers(),
                ) as resp:
                    resp.raise_for_status()
                    return resp.status, await resp.json()
            status, data = await async_retry_broker_command(_call)
            if status == 200:
                return data
            # 202 means still pending — back off and retry
            if asyncio.get_event_loop().time() >= deadline:
                raise DispatchRemoteError(f'poll timeout for request_id={request_id}')
            await asyncio.sleep(interval)
            interval = min(interval * 2, _POLL_INTERVAL_MAX)
