'''
HTTP server and health endpoint for the media broker process.
Schedule servers with asyncio.create_task(server.serve()).
'''
import asyncio
import logging
from dataclasses import dataclass
from pathlib import Path

from aiohttp import web
from opentelemetry.propagate import extract
from opentelemetry.trace import SpanKind

from discord_bot.clients.redis_client import RedisManager
from discord_bot.interfaces.broker_protocols import MediaBrokerBase
from discord_bot.servers.base import AiohttpServerBase, BaseHealthServer, _DbPingMixin
from discord_bot.types.download import DownloadResult, DownloadStatusUpdate
from discord_bot.types.media_request import MediaRequest
from discord_bot.utils.otel import otel_span_wrapper

logger = logging.getLogger(__name__)


class BrokerHealthServer(_DbPingMixin, BaseHealthServer):
    '''
    Lightweight HTTP health endpoint for the broker process.

    Responds 200 {"status": "ok"} when Redis is reachable and, if db_engine
    is provided, the database is also reachable. Returns 503 otherwise, with
    "redis" and optional "db" fields in the body.
    '''

    def __init__(self, redis_manager: RedisManager, port: int = 8080,
                 db_engine=None):
        super().__init__(port)
        self.redis_manager = redis_manager
        self._db_engine = db_engine
        self._last_redis_ok: bool | None = None

    async def _check_health(self) -> bool:
        try:
            await self.redis_manager.client.ping()
            self._last_redis_ok = True
        except Exception:
            self._last_redis_ok = False
        if self._db_engine is not None:
            self._last_db_ok = await self._db_ping()
            return self._last_redis_ok and self._last_db_ok
        return self._last_redis_ok

    async def _extra_body(self) -> dict:
        body = {}
        if self._last_redis_ok is not None:
            body['redis'] = 'ok' if self._last_redis_ok else 'unavailable'
        if self._last_db_ok is not None:
            body['db'] = 'ok' if self._last_db_ok else 'unavailable'
        return body


@dataclass
class _QueueItemProxy:
    '''
    Minimal stand-in for queue items passed to MediaBroker.prefetch.
    The broker only accesses item.media_request.uuid, so we return self
    as media_request and expose uuid directly.
    '''
    uuid: str

    @property
    def media_request(self):
        '''Return self so item.media_request.uuid resolves to self.uuid.'''
        return self


class BrokerHttpServer(AiohttpServerBase):
    '''
    aiohttp HTTP server wrapping a MediaBroker instance.

    Exposes endpoints for download workers (update_request_status,
    register_download_result) and music players (checkout, release, prefetch).
    All endpoints respond with JSON.

    Routes:
        POST /requests/{uuid}           register_request
        PUT  /requests/{uuid}/status    update_request_status
        POST /downloads                 register_download_result
        POST /requests/{uuid}/checkout  checkout
        POST /requests/{uuid}/release   release
        POST /prefetch                  prefetch
    '''

    def __init__(self, broker: MediaBrokerBase, host: str = '0.0.0.0', port: int = 8081,
                 result_queue: asyncio.Queue | None = None, ha_mode: bool = False):
        super().__init__()
        self._broker = broker
        self._host = host
        self._port = port
        self._result_queue = result_queue
        self._ha_mode = ha_mode

    def build_app(self) -> web.Application:
        '''Build and return the aiohttp Application. Exposed for testing.'''
        app = web.Application(middlewares=[self._get_drain_middleware()])
        app.router.add_post('/requests/{uuid}', self._handle_register_request)
        app.router.add_put('/requests/{uuid}/status', self._handle_update_status)
        app.router.add_post('/downloads', self._handle_register_download)
        app.router.add_post('/requests/{uuid}/checkout', self._handle_checkout)
        app.router.add_post('/requests/{uuid}/release', self._handle_release)
        app.router.add_post('/requests/{uuid}/remove', self._handle_remove)
        app.router.add_post('/prefetch', self._handle_prefetch)
        return app

    # ------------------------------------------------------------------
    # Route handlers
    # ------------------------------------------------------------------

    async def _handle_register_request(self, request: web.Request) -> web.Response:
        ctx = extract(request.headers)
        try:
            body = await request.json()
            media_request = MediaRequest.model_validate(body)
        except Exception as exc:
            raise web.HTTPUnprocessableEntity() from exc
        with otel_span_wrapper('broker.register_request', context=ctx, kind=SpanKind.SERVER):
            await self._broker.register_request(media_request)
        return web.json_response({'status': 'ok'}, status=201)

    async def _handle_update_status(self, request: web.Request) -> web.Response:
        ctx = extract(request.headers)
        uuid = request.match_info['uuid']
        try:
            body = await request.json()
            update = DownloadStatusUpdate.model_validate(body)
        except Exception as exc:
            raise web.HTTPUnprocessableEntity() from exc
        with otel_span_wrapper('broker.update_status', context=ctx, kind=SpanKind.SERVER):
            await self._broker.update_request_status(uuid, update)
        return web.json_response({'status': 'ok'})

    async def _handle_register_download(self, request: web.Request) -> web.Response:
        ctx = extract(request.headers)
        try:
            body = await request.json()
            result = DownloadResult.model_validate(body)
        except Exception as exc:
            raise web.HTTPUnprocessableEntity() from exc
        with otel_span_wrapper('broker.register_download', context=ctx, kind=SpanKind.SERVER):
            if self._result_queue is not None:
                self._result_queue.put_nowait(result)
            else:
                await self._broker.register_download_result(result)
        return web.json_response({'status': 'ok'}, status=202)

    async def _handle_checkout(self, request: web.Request) -> web.Response:
        ctx = extract(request.headers)
        uuid = request.match_info['uuid']
        try:
            body = await request.json()
            guild_id = int(body['guild_id'])
            guild_path = body.get('guild_path')
        except Exception as exc:
            raise web.HTTPUnprocessableEntity() from exc
        with otel_span_wrapper('broker.checkout', context=ctx, kind=SpanKind.SERVER):
            path = await self._broker.checkout(uuid, guild_id, Path(guild_path) if guild_path else None)
        if self._ha_mode:
            return web.json_response({'guild_file_path': None, 's3_key': str(path) if path else None})
        return web.json_response({'guild_file_path': str(path) if path else None, 's3_key': None})

    async def _handle_release(self, request: web.Request) -> web.Response:
        ctx = extract(request.headers)
        uuid = request.match_info['uuid']
        with otel_span_wrapper('broker.release', context=ctx, kind=SpanKind.SERVER):
            await self._broker.release(uuid)
        return web.json_response({'status': 'ok'})

    async def _handle_remove(self, request: web.Request) -> web.Response:
        ctx = extract(request.headers)
        uuid = request.match_info['uuid']
        with otel_span_wrapper('broker.remove', context=ctx, kind=SpanKind.SERVER):
            await self._broker.remove(uuid)
        return web.json_response({'status': 'ok'})

    async def _handle_prefetch(self, request: web.Request) -> web.Response:
        ctx = extract(request.headers)
        try:
            body = await request.json()
            uuids = list(body['uuids'])
            guild_id = int(body['guild_id'])
            guild_path = body.get('guild_path')
            limit = int(body['limit'])
        except Exception as exc:
            raise web.HTTPUnprocessableEntity() from exc
        items = [_QueueItemProxy(uuid=u) for u in uuids]
        with otel_span_wrapper('broker.prefetch', context=ctx, kind=SpanKind.SERVER):
            await self._broker.prefetch(items, guild_id, Path(guild_path) if guild_path else None, limit)
        return web.json_response({'status': 'ok'})
