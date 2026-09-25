'''HTTP client for cross-process dispatch via DispatchHttpServer (HA mode).'''
import asyncio
import logging

import aiohttp
from opentelemetry import trace

from discord_core.clients.http_client_base import HttpClientMixin
from discord_core.routes.route import Route
from discord_core.utils.circuit_breaker import CircuitBreaker, CircuitBreakerOpenError
from discord_core.utils.otel import AttributeNaming, DispatchNaming, METER_PROVIDER, MetricNaming
from discord_core.utils.retry import async_retry_broker_command

from discord_bot.seams.dispatch.clients.dispatch_client_base import DispatchClientBase, DispatchRemoteError
from discord_bot.seams.dispatch.routes import dispatch as dispatch_routes
from discord_bot.seams.dispatch.types.dispatch_request import (
    DeleteRequest,
    SendRequest,
)
from discord_bot.seams.dispatch.types.requests import (
    DeleteRequestBody, RemoveMutableRequestBody, SendRequestBody,
    UpdateMutableChannelRequestBody, UpdateMutableRequestBody,
)
from discord_bot.seams.dispatch.types.responses import (
    FetchEmojisResponse, FetchHistoryResponse,
)
from discord_bot.seams.dispatch.types.results import (
    ChannelHistoryResultBody, DispatchErrorResultBody, GuildEmojisResultBody,
)
from discord_bot.seams.dispatch.utils.dispatch_queue import dispatch_request_id

logger = logging.getLogger(__name__)

_POLL_INTERVAL_BASE = 0.5   # seconds — first poll delay
_POLL_INTERVAL_MAX = 10.0   # seconds — cap for exponential backoff
_POLL_TIMEOUT = 300.0       # seconds — give up after this long

_BREAKER_FAILURE_THRESHOLD = 5
_BREAKER_RECOVERY_TIMEOUT = 30.0  # seconds

# One breaker per process — the dispatcher URL is shared across cogs, so all
# HttpDispatchClient instances see the same up/down state.
_BREAKER = CircuitBreaker(
    name='dispatch',
    failure_threshold=_BREAKER_FAILURE_THRESHOLD,
    recovery_timeout=_BREAKER_RECOVERY_TIMEOUT,
)

_REQUEST_COUNTER = METER_PROVIDER.create_counter(
    name=MetricNaming.DISPATCH_REQUEST.value,
    description='HttpDispatchClient request outcomes, by route template',
    unit='1',
)


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

    #: The seam this client speaks, for HttpClientMixin.start_seam_check.
    SEAM = 'dispatch'
    ROUTES_CALLED = dispatch_routes.ALL

    def __init__(self, base_url: str, session: aiohttp.ClientSession | None = None,
                 seam_contract=None):
        self._base_url = base_url.rstrip('/')
        self._session = session
        self._cog_queues: dict[str, asyncio.Queue] = {}
        self._seam_contract_config = seam_contract

    async def start(self) -> None:
        '''No-op — no background poller needed (polling happens per-request).'''

    def stop(self) -> None:
        '''No-op — nothing to cancel.'''

    # ------------------------------------------------------------------
    # DispatchClientBase transport hooks
    # ------------------------------------------------------------------

    def _handle_send(self, request: SendRequest) -> None:
        # NOTE: this body now carries `allow_404: false`, which it did not before.
        # There were two client paths to POST /dispatch/send sending DIFFERENT
        # bodies -- this one omitted allow_404, send_message() included it -- and
        # the server reads `body.get('allow_404', False)`, so this path always got
        # False anyway. Sending it explicitly is the same behaviour and one body
        # shape instead of two. It is the only deliberate byte change in this
        # seam; see projects/seam-body-typing.
        asyncio.create_task(self._post(dispatch_routes.SEND, SendRequestBody(
            guild_id=request.guild_id, channel_id=request.channel_id,
            content=request.content, delete_after=request.delete_after,
            span_context=request.span_context,
        ).model_dump()))

    def _handle_delete(self, request: DeleteRequest) -> None:
        asyncio.create_task(self._post(dispatch_routes.DELETE, DeleteRequestBody(
            guild_id=request.guild_id, channel_id=request.channel_id,
            message_id=request.message_id, span_context=request.span_context,
        ).model_dump()))

    # ------------------------------------------------------------------
    # Fire-and-forget methods
    # ------------------------------------------------------------------

    def update_mutable(self, key: str, guild_id: int, content: list,
                       channel_id: int | None, sticky: bool = True, delete_after: int | None = None):
        '''Fire-and-forget: POST /dispatch/update_mutable.'''
        if not content:
            logger.debug('update_mutable: empty content for key=%s, routing to remove_mutable', key)
            return self.remove_mutable(key)
        req_id = dispatch_request_id({'key': key, 'guild_id': guild_id, 't': str(asyncio.get_running_loop().time())})
        trace.get_current_span().set_attribute(DispatchNaming.REQUEST_ID.value, req_id)
        logger.debug('update_mutable: key=%s dispatch.request_id=%s', key, req_id)
        asyncio.create_task(self._post(dispatch_routes.UPDATE_MUTABLE, UpdateMutableRequestBody(
            key=key, guild_id=guild_id, content=content,
            channel_id=channel_id, sticky=sticky, delete_after=delete_after,
        ).model_dump()))
        return req_id

    def remove_mutable(self, key: str):
        '''Fire-and-forget: POST /dispatch/remove_mutable.'''
        logger.debug('remove_mutable: key=%s', key)
        asyncio.create_task(self._post(dispatch_routes.REMOVE_MUTABLE,
                                       RemoveMutableRequestBody(key=key).model_dump()))

    def update_mutable_channel(self, key: str, guild_id: int, new_channel_id: int):
        '''Fire-and-forget: POST /dispatch/update_mutable_channel.'''
        asyncio.create_task(self._post(
            dispatch_routes.UPDATE_MUTABLE_CHANNEL, UpdateMutableChannelRequestBody(
                key=key, guild_id=guild_id, new_channel_id=new_channel_id,
            ).model_dump()))

    def send_message(self, guild_id: int, channel_id: int, content: str,
                     delete_after: int | None = None, allow_404: bool = False,
                     span_context: dict | None = None):
        '''Fire-and-forget: POST /dispatch/send.'''
        asyncio.create_task(self._post(dispatch_routes.SEND, SendRequestBody(
            guild_id=guild_id, channel_id=channel_id, content=content,
            delete_after=delete_after, allow_404=allow_404, span_context=span_context,
        ).model_dump()))

    def delete_message(self, guild_id: int, channel_id: int, message_id: int,
                       span_context: dict | None = None):
        '''Fire-and-forget: POST /dispatch/delete.'''
        asyncio.create_task(self._post(dispatch_routes.DELETE, DeleteRequestBody(
            guild_id=guild_id, channel_id=channel_id,
            message_id=message_id, span_context=span_context,
        ).model_dump()))

    # ------------------------------------------------------------------
    # Transport implementations for DispatchClientBase
    # ------------------------------------------------------------------

    async def _do_fetch_history(self, params: dict) -> dict:
        request_id = await self._submit_fetch(dispatch_routes.FETCH_HISTORY, params,
                                              FetchHistoryResponse)
        payload = await self._poll_result(request_id)
        return self._checked_result(payload, ChannelHistoryResultBody)

    async def _do_fetch_emojis(self, params: dict) -> dict:
        request_id = await self._submit_fetch(dispatch_routes.FETCH_EMOJIS, params,
                                              FetchEmojisResponse)
        payload = await self._poll_result(request_id)
        return self._checked_result(payload, GuildEmojisResultBody)

    def _checked_result(self, payload: dict, model):
        """
        Validate a polled result body, then hand back the dict callers expect.

        The polled body was the one response on this seam that reached a caller
        UNVALIDATED: `_submit_fetch` checks its 202, but the 200 from
        GET /dispatch/results/{id} went straight through to
        `DispatchClientBase.decode_*`. So a db pod shipping a changed result
        shape produced a KeyError deep in a cog rather than
        `seam_response_invalid` with the peer's name on it -- the attribution
        #953 built for every other seam, missing on this one.

        Returns the raw dict rather than the model so `decode_history_result`
        and the cog-facing DTOs are untouched: this adds a check, not a type.
        """
        if 'error' in payload:
            # Validated too. A malformed ERROR body is the case most likely to
            # be wrong and least likely to be noticed, since it is already the
            # unhappy path.
            self._validate(DispatchErrorResultBody, payload)
            raise DispatchRemoteError.from_payload(payload)
        self._validate(model, payload)
        return payload

    # ------------------------------------------------------------------
    # HTTP helpers
    # ------------------------------------------------------------------

    async def _post(self, route: Route, body: dict) -> None:
        '''POST *body* on *route* with retry+breaker; logs and swallows errors so callers are fire-and-forget.

        The `path` metric label stays the route TEMPLATE, which is byte-identical
        to the literal it replaced -- these routes take no path parameters -- so
        no existing series is orphaned.
        '''
        path = route.template
        session = self._get_session()
        async def _call():
            async with session.post(
                self._route_url(route),
                headers=self._trace_headers(),
                json=body,
            ) as resp:
                resp.raise_for_status()
        try:
            await _BREAKER.call(lambda: async_retry_broker_command(_call))
            _REQUEST_COUNTER.add(1, {AttributeNaming.OUTCOME.value: 'success', AttributeNaming.PATH.value: path})
        except CircuitBreakerOpenError:
            _REQUEST_COUNTER.add(1, {AttributeNaming.OUTCOME.value: 'breaker_open', AttributeNaming.PATH.value: path})
            logger.error('HttpDispatchClient :: dispatch breaker open, dropping POST %s', path)
        except Exception as exc:
            _REQUEST_COUNTER.add(1, {AttributeNaming.OUTCOME.value: 'failure', AttributeNaming.PATH.value: path})
            logger.error('HttpDispatchClient :: POST %s failed: %s', path, exc)

    async def _submit_fetch(self, route: Route, params: dict, response_model) -> str:
        '''
        POST *params* on *route* and return the request_id from the 202 response.

        `response_model` is passed in rather than looked up because the seam has
        one response model per route: FETCH_HISTORY and FETCH_EMOJIS happen to
        answer the same shape today, and the day one of them gains a field, the
        caller that asked for it is the one that should have to change.
        '''
        path = route.template
        session = self._get_session()
        async def _call():
            async with session.post(
                self._route_url(route),
                headers=self._trace_headers(),
                json=params,
            ) as resp:
                resp.raise_for_status()
                return await resp.json()
        try:
            data = await _BREAKER.call(lambda: async_retry_broker_command(_call))
        except CircuitBreakerOpenError:
            _REQUEST_COUNTER.add(1, {AttributeNaming.OUTCOME.value: 'breaker_open', AttributeNaming.PATH.value: path})
            raise
        except Exception:
            _REQUEST_COUNTER.add(1, {AttributeNaming.OUTCOME.value: 'failure', AttributeNaming.PATH.value: path})
            raise
        _REQUEST_COUNTER.add(1, {AttributeNaming.OUTCOME.value: 'success', AttributeNaming.PATH.value: path})
        # Through `self._validate`, not `model_validate` directly: the helper
        # reads `seam`/`prefix` off the client and emits seam_response_invalid,
        # which is the attribution #953 built. tests/clients/test_response_
        # validation.py enforces that every client response goes through it.
        return self._validate(response_model, data).request_id

    async def _poll_result(self, request_id: str) -> dict:
        '''Poll GET /dispatch/results/{request_id} with exponential backoff until available.'''
        session = self._get_session()
        interval = _POLL_INTERVAL_BASE
        deadline = asyncio.get_running_loop().time() + _POLL_TIMEOUT
        while True:
            async def _call():
                async with session.get(
                    self._route_url(dispatch_routes.GET_RESULT, request_id=request_id),
                    headers=self._trace_headers(),
                ) as resp:
                    resp.raise_for_status()
                    return resp.status, await resp.json()
            try:
                status, data = await _BREAKER.call(lambda: async_retry_broker_command(_call))
            # These four keep the bare '/dispatch/results' label rather than the
            # route template. The template carries {request_id}, so switching to it
            # would change an existing series' label value and orphan the old one
            # for no gain -- the URL comes from the registry either way.
            except CircuitBreakerOpenError:
                _REQUEST_COUNTER.add(1, {AttributeNaming.OUTCOME.value: 'breaker_open', AttributeNaming.PATH.value: '/dispatch/results'})
                raise
            except Exception:
                _REQUEST_COUNTER.add(1, {AttributeNaming.OUTCOME.value: 'failure', AttributeNaming.PATH.value: '/dispatch/results'})
                raise
            if status == 200:
                _REQUEST_COUNTER.add(1, {AttributeNaming.OUTCOME.value: 'success', AttributeNaming.PATH.value: '/dispatch/results'})
                return data
            # 202 means still pending -- back off and retry. Deliberately keyed on
            # the STATUS CODE, not on validating the body as ResultPendingResponse:
            # today an unrecognised 202 body backs off harmlessly, and validating
            # would turn it into a raise. The model documents that body; it does
            # not gate the loop.
            if asyncio.get_running_loop().time() >= deadline:
                _REQUEST_COUNTER.add(1, {AttributeNaming.OUTCOME.value: 'timeout', AttributeNaming.PATH.value: '/dispatch/results'})
                raise DispatchRemoteError(f'poll timeout for request_id={request_id}')
            await asyncio.sleep(interval)
            interval = min(interval * 2, _POLL_INTERVAL_MAX)
