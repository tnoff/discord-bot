'''
HTTP server exposing MessageDispatcher over aiohttp for cross-process dispatch.
Schedule with asyncio.create_task(server.serve()).

Fire-and-forget endpoints (POST → 202):
    /dispatch/send
    /dispatch/delete
    /dispatch/update_mutable
    /dispatch/remove_mutable
    /dispatch/update_mutable_channel

Awaitable fetch endpoints (POST → 202 with request_id, GET → 200 result | 202 pending):
    /dispatch/fetch_history
    /dispatch/fetch_emojis
    /dispatch/results/{request_id}
'''
import logging

from aiohttp import web
from opentelemetry.propagate import extract
from opentelemetry.trace import SpanKind
from discord_core.servers.base import AiohttpServerBase
from discord_core.utils.otel import otel_span_wrapper

from discord_seam_dispatch.interfaces.dispatch_protocols import WorkQueue
from discord_seam_dispatch.routes import dispatch as dispatch_routes
from discord_seam_dispatch.types import requests as dispatch_requests
from discord_seam_dispatch.types import responses as dispatch_responses
from discord_seam_dispatch.utils.dispatch_queue import dispatch_request_id

logger = logging.getLogger(__name__)


class DispatchHttpServer(AiohttpServerBase):
    '''
    aiohttp HTTP server wrapping a MessageDispatcher instance.

    Receives dispatch calls from cog pods and routes them into the shared
    Redis work queue (via the dispatcher's HTTP-mode methods).  Results for
    awaitable fetches are stored in Redis by the dispatcher workers; the
    poll endpoint reads them back so any pod can serve the response.
    '''

    # bandit B104: '0.0.0.0' default is intentional — bot pods reach the dispatcher across the docker/k8s network; callers override host via constructor arg
    def __init__(self, dispatcher, redis_queue: WorkQueue,
                 host: str = '0.0.0.0', port: int = 8082):  # nosec B104
        super().__init__()
        self._dispatcher = dispatcher
        self._redis_queue = redis_queue
        self._host = host
        self._port = port

    def route_handlers(self) -> dict:
        """Map every route on the dispatch seam to the handler that serves it.

        Keyed by the shared routes/dispatch.py symbols, so the route a client
        calls and the route this server registers are the same object.
        """
        return {
            dispatch_routes.SEND: self._handle_send,
            dispatch_routes.DELETE: self._handle_delete,
            dispatch_routes.UPDATE_MUTABLE: self._handle_update_mutable,
            dispatch_routes.REMOVE_MUTABLE: self._handle_remove_mutable,
            dispatch_routes.UPDATE_MUTABLE_CHANNEL: self._handle_update_mutable_channel,
            dispatch_routes.FETCH_HISTORY: self._handle_fetch_history,
            dispatch_routes.FETCH_EMOJIS: self._handle_fetch_emojis,
            dispatch_routes.GET_RESULT: self._handle_get_result,
        }

    def build_app(self) -> web.Application:
        '''Build and return the aiohttp Application. Exposed for testing.'''
        app = web.Application(middlewares=[self._get_drain_middleware()])
        self.register_seam_routes(app, self.route_handlers())
        self.add_contract_route(app)
        return app

    # ------------------------------------------------------------------
    # Fire-and-forget handlers
    # ------------------------------------------------------------------

    @staticmethod
    async def _body(request: web.Request, model):
        """
        Parse and validate a request body, or raise the seam's 422.

        Every handler used to do this inline with int()/str() coercion under a
        bare `except Exception`. The catch stays exactly as broad, because it
        has to cover both a non-JSON body (raised by `request.json()`) and a
        `ValidationError` from the model, and the 422 is the part that matters:
        `async_retry_broker_command` propagates 4xx immediately rather than
        laddering, so a malformed body must NOT become a retried 5xx.
        """
        try:
            return model.model_validate(await request.json())
        except Exception as exc:
            raise web.HTTPUnprocessableEntity() from exc

    async def _handle_send(self, request: web.Request) -> web.Response:
        ctx = extract(request.headers)
        body = await self._body(request, dispatch_requests.SendRequestBody)
        with otel_span_wrapper('dispatch.send', context=ctx, kind=SpanKind.SERVER):
            self._dispatcher.send_message(body.guild_id, body.channel_id, body.content,
                                          delete_after=body.delete_after, allow_404=body.allow_404,
                                          span_context=body.span_context)
        return web.json_response(dispatch_responses.SendResponse().model_dump(), status=202)

    async def _handle_delete(self, request: web.Request) -> web.Response:
        ctx = extract(request.headers)
        body = await self._body(request, dispatch_requests.DeleteRequestBody)
        with otel_span_wrapper('dispatch.delete', context=ctx, kind=SpanKind.SERVER):
            self._dispatcher.delete_message(body.guild_id, body.channel_id, body.message_id,
                                            span_context=body.span_context)
        return web.json_response(dispatch_responses.DeleteResponse().model_dump(), status=202)

    async def _handle_update_mutable(self, request: web.Request) -> web.Response:
        ctx = extract(request.headers)
        body = await self._body(request, dispatch_requests.UpdateMutableRequestBody)
        with otel_span_wrapper('dispatch.update_mutable', context=ctx, kind=SpanKind.SERVER):
            self._dispatcher.update_mutable(body.key, body.guild_id, body.content, body.channel_id,
                                            sticky=body.sticky, delete_after=body.delete_after)
        return web.json_response(dispatch_responses.UpdateMutableResponse().model_dump(), status=202)

    async def _handle_remove_mutable(self, request: web.Request) -> web.Response:
        ctx = extract(request.headers)
        body = await self._body(request, dispatch_requests.RemoveMutableRequestBody)
        with otel_span_wrapper('dispatch.remove_mutable', context=ctx, kind=SpanKind.SERVER):
            self._dispatcher.remove_mutable(body.key)
        return web.json_response(dispatch_responses.RemoveMutableResponse().model_dump(), status=202)

    async def _handle_update_mutable_channel(self, request: web.Request) -> web.Response:
        ctx = extract(request.headers)
        body = await self._body(request, dispatch_requests.UpdateMutableChannelRequestBody)
        with otel_span_wrapper('dispatch.update_mutable_channel', context=ctx, kind=SpanKind.SERVER):
            self._dispatcher.update_mutable_channel(body.key, body.guild_id, body.new_channel_id)
        return web.json_response(
            dispatch_responses.UpdateMutableChannelResponse().model_dump(), status=202)

    # ------------------------------------------------------------------
    # Awaitable fetch handlers
    # ------------------------------------------------------------------

    async def _handle_fetch_history(self, request: web.Request) -> web.Response:
        ctx = extract(request.headers)
        body = await self._body(request, dispatch_requests.FetchHistoryRequestBody)
        span_context = body.span_context
        # Built explicitly rather than from model_dump() so the hashed key SET is
        # visible here: dispatch_request_id folds this dict into the request_id,
        # and model_dump() would silently add span_context the moment the model
        # gains or loses a field. The exclusion below is the whole point.
        params = {'guild_id': body.guild_id, 'channel_id': body.channel_id, 'limit': body.limit,
                  'after': body.after, 'after_message_id': body.after_message_id,
                  'oldest_first': body.oldest_first}
        # span_context is deliberately excluded from the request_id hash: it differs
        # per trace, so folding it in would make every identical fetch a distinct
        # request and defeat result reuse.
        request_id = dispatch_request_id(params)
        with otel_span_wrapper('dispatch.fetch_history', context=ctx, kind=SpanKind.SERVER):
            await self._dispatcher.enqueue_fetch_history(request_id, **params, span_context=span_context)
        return web.json_response(
            dispatch_responses.FetchHistoryResponse(request_id=request_id).model_dump(), status=202)

    async def _handle_fetch_emojis(self, request: web.Request) -> web.Response:
        ctx = extract(request.headers)
        body = await self._body(request, dispatch_requests.FetchEmojisRequestBody)
        span_context = body.span_context
        # Same explicit build, same reason, as _handle_fetch_history above.
        params = {'guild_id': body.guild_id, 'max_retries': body.max_retries}
        # Excluded from the hash for the same reason as fetch_history above.
        request_id = dispatch_request_id(params)
        with otel_span_wrapper('dispatch.fetch_emojis', context=ctx, kind=SpanKind.SERVER):
            await self._dispatcher.enqueue_fetch_emojis(request_id, **params, span_context=span_context)
        return web.json_response(
            dispatch_responses.FetchEmojisResponse(request_id=request_id).model_dump(), status=202)

    async def _handle_get_result(self, request: web.Request) -> web.Response:
        request_id = request.match_info['request_id']
        result = await self._redis_queue.get_result(request_id)
        if result is None:
            return web.json_response(
                dispatch_responses.ResultPendingResponse().model_dump(), status=202)
        # The 200 body is the worker's stored result, returned verbatim: a
        # history payload, an emoji payload, or {'error', 'error_detail'}. It is
        # the one dispatch body still untyped, because it is produced in
        # message_dispatcher rather than here -- typing it means typing the
        # worker's result path, which is its own change.
        return web.json_response(result)
