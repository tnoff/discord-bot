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

from discord_bot.servers.base import AiohttpServerBase
from discord_bot.utils.dispatch_queue import RedisDispatchQueue, dispatch_request_id
from discord_bot.utils.otel import otel_span_wrapper

logger = logging.getLogger(__name__)


class DispatchHttpServer(AiohttpServerBase):
    '''
    aiohttp HTTP server wrapping a MessageDispatcher instance.

    Receives dispatch calls from cog pods and routes them into the shared
    Redis work queue (via the dispatcher's HTTP-mode methods).  Results for
    awaitable fetches are stored in Redis by the dispatcher workers; the
    poll endpoint reads them back so any pod can serve the response.
    '''

    def __init__(self, dispatcher, redis_queue: RedisDispatchQueue,
                 host: str = '0.0.0.0', port: int = 8082):
        self._dispatcher = dispatcher
        self._redis_queue = redis_queue
        self._host = host
        self._port = port

    def build_app(self) -> web.Application:
        '''Build and return the aiohttp Application. Exposed for testing.'''
        app = web.Application()
        app.router.add_post('/dispatch/send', self._handle_send)
        app.router.add_post('/dispatch/delete', self._handle_delete)
        app.router.add_post('/dispatch/update_mutable', self._handle_update_mutable)
        app.router.add_post('/dispatch/remove_mutable', self._handle_remove_mutable)
        app.router.add_post('/dispatch/update_mutable_channel', self._handle_update_mutable_channel)
        app.router.add_post('/dispatch/fetch_history', self._handle_fetch_history)
        app.router.add_post('/dispatch/fetch_emojis', self._handle_fetch_emojis)
        app.router.add_get('/dispatch/results/{request_id}', self._handle_get_result)
        return app

    # ------------------------------------------------------------------
    # Fire-and-forget handlers
    # ------------------------------------------------------------------

    async def _handle_send(self, request: web.Request) -> web.Response:
        ctx = extract(request.headers)
        try:
            body = await request.json()
            guild_id = int(body['guild_id'])
            channel_id = int(body['channel_id'])
            content = str(body['content'])
            delete_after = body.get('delete_after')
            allow_404 = bool(body.get('allow_404', False))
            span_context = body.get('span_context')
        except Exception as exc:
            raise web.HTTPUnprocessableEntity() from exc
        with otel_span_wrapper('dispatch.send', context=ctx, kind=SpanKind.SERVER):
            self._dispatcher.send_message(guild_id, channel_id, content,
                                          delete_after=delete_after, allow_404=allow_404,
                                          span_context=span_context)
        return web.json_response({'status': 'ok'}, status=202)

    async def _handle_delete(self, request: web.Request) -> web.Response:
        ctx = extract(request.headers)
        try:
            body = await request.json()
            guild_id = int(body['guild_id'])
            channel_id = int(body['channel_id'])
            message_id = int(body['message_id'])
            span_context = body.get('span_context')
        except Exception as exc:
            raise web.HTTPUnprocessableEntity() from exc
        with otel_span_wrapper('dispatch.delete', context=ctx, kind=SpanKind.SERVER):
            self._dispatcher.delete_message(guild_id, channel_id, message_id,
                                            span_context=span_context)
        return web.json_response({'status': 'ok'}, status=202)

    async def _handle_update_mutable(self, request: web.Request) -> web.Response:
        ctx = extract(request.headers)
        try:
            body = await request.json()
            key = str(body['key'])
            guild_id = int(body['guild_id'])
            content = list(body['content'])
            channel_id = int(body['channel_id']) if body.get('channel_id') is not None else None
            sticky = bool(body.get('sticky', True))
            delete_after = body.get('delete_after')
        except Exception as exc:
            raise web.HTTPUnprocessableEntity() from exc
        with otel_span_wrapper('dispatch.update_mutable', context=ctx, kind=SpanKind.SERVER):
            self._dispatcher.update_mutable(key, guild_id, content, channel_id,
                                            sticky=sticky, delete_after=delete_after)
        return web.json_response({'status': 'ok'}, status=202)

    async def _handle_remove_mutable(self, request: web.Request) -> web.Response:
        ctx = extract(request.headers)
        try:
            body = await request.json()
            key = str(body['key'])
        except Exception as exc:
            raise web.HTTPUnprocessableEntity() from exc
        with otel_span_wrapper('dispatch.remove_mutable', context=ctx, kind=SpanKind.SERVER):
            self._dispatcher.remove_mutable(key)
        return web.json_response({'status': 'ok'}, status=202)

    async def _handle_update_mutable_channel(self, request: web.Request) -> web.Response:
        ctx = extract(request.headers)
        try:
            body = await request.json()
            key = str(body['key'])
            guild_id = int(body['guild_id'])
            new_channel_id = int(body['new_channel_id'])
        except Exception as exc:
            raise web.HTTPUnprocessableEntity() from exc
        with otel_span_wrapper('dispatch.update_mutable_channel', context=ctx, kind=SpanKind.SERVER):
            self._dispatcher.update_mutable_channel(key, guild_id, new_channel_id)
        return web.json_response({'status': 'ok'}, status=202)

    # ------------------------------------------------------------------
    # Awaitable fetch handlers
    # ------------------------------------------------------------------

    async def _handle_fetch_history(self, request: web.Request) -> web.Response:
        ctx = extract(request.headers)
        try:
            body = await request.json()
            guild_id = int(body['guild_id'])
            channel_id = int(body['channel_id'])
            limit = int(body['limit'])
            after = body.get('after')
            after_message_id = int(body['after_message_id']) if body.get('after_message_id') is not None else None
            oldest_first = bool(body.get('oldest_first', True))
        except Exception as exc:
            raise web.HTTPUnprocessableEntity() from exc
        params = {'guild_id': guild_id, 'channel_id': channel_id, 'limit': limit,
                  'after': after, 'after_message_id': after_message_id, 'oldest_first': oldest_first}
        request_id = dispatch_request_id(params)
        with otel_span_wrapper('dispatch.fetch_history', context=ctx, kind=SpanKind.SERVER):
            await self._dispatcher.enqueue_fetch_history(request_id, **params)
        return web.json_response({'request_id': request_id}, status=202)

    async def _handle_fetch_emojis(self, request: web.Request) -> web.Response:
        ctx = extract(request.headers)
        try:
            body = await request.json()
            guild_id = int(body['guild_id'])
            max_retries = int(body.get('max_retries', 3))
        except Exception as exc:
            raise web.HTTPUnprocessableEntity() from exc
        params = {'guild_id': guild_id, 'max_retries': max_retries}
        request_id = dispatch_request_id(params)
        with otel_span_wrapper('dispatch.fetch_emojis', context=ctx, kind=SpanKind.SERVER):
            await self._dispatcher.enqueue_fetch_emojis(request_id, **params)
        return web.json_response({'request_id': request_id}, status=202)

    async def _handle_get_result(self, request: web.Request) -> web.Response:
        request_id = request.match_info['request_id']
        result = await self._redis_queue.get_result(request_id)
        if result is None:
            return web.json_response({'status': 'pending'}, status=202)
        return web.json_response(result)
