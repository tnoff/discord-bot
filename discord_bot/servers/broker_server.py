'''
HTTP server exposing MediaBroker over aiohttp for cross-process communication.
Schedule with asyncio.create_task(server.serve()).
'''
import logging
from dataclasses import dataclass
from pathlib import Path

from aiohttp import web
from opentelemetry.propagate import extract
from opentelemetry.trace import SpanKind

from discord_bot.interfaces.broker_protocols import DownloadResultQueue, MediaBrokerBase
from discord_bot.servers.base import AiohttpServerBase
from discord_bot.types.download import DownloadResult, LifecycleStatusUpdate
from discord_bot.types.media_download import MediaDownload
from discord_bot.types.playlist_add_request import parse_media_request
from discord_bot.utils.otel import otel_span_wrapper
from discord_bot.workers.asyncio_queues import AsyncioDownloadResultQueue

logger = logging.getLogger(__name__)


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
    aiohttp HTTP server wrapping a MediaBroker instance.  Exposes the full
    BrokerClient surface so a remote bot pod (HttpBrokerClient) can drive the
    broker over HTTP.  All endpoints respond with JSON.

    Routes:
        POST   /requests/{uuid}           register_request
        PUT    /requests/{uuid}/status    update_request_status
        POST   /downloads                 register_download_result (worker)
        POST   /downloads/register        register_download (MediaDownload)
        GET    /results/next              next_result (204 when empty)
        POST   /requests/{uuid}/checkout  checkout
        POST   /requests/{uuid}/release   release
        POST   /requests/{uuid}/remove    remove
        POST   /requests/{uuid}/discard   discard
        POST   /prefetch                  prefetch
        POST   /cache/check               check_cache
        POST   /cache/cleanup             cache_cleanup
        GET    /cache/count               get_cache_count
        GET    /bundles?guild_id=N        list_bundles_for_guild
        POST   /bundles                   create_bundle
        POST   /bundles/{uuid}/finalize   finalize_bundle
        DELETE /bundles/{uuid}            delete_bundle

    checkout serialises whatever the engine returns: an in-process AsyncioBroker
    stages the file and yields CheckoutResult(local_path) -> guild_file_path; an
    HA RedisBroker yields CheckoutResult(s3_key) -> s3_key (the bot fetches it).
    No mode flag is needed — the CheckoutResult drives the response shape.
    '''

    # bandit B104: '0.0.0.0' default is intentional — worker/bot pods reach the broker across the docker/k8s network; callers override host via constructor arg
    def __init__(self, broker: MediaBrokerBase, host: str = '0.0.0.0', port: int = 8081,  # nosec B104
                 result_queue: DownloadResultQueue | None = None):
        super().__init__()
        self._broker = broker
        self._host = host
        self._port = port
        # Always have a queue.  In single-process embedded mode the cog passes
        # its InMemoryBrokerClient's AsyncioDownloadResultQueue so HTTP-arriving
        # results land where the cog drains them.  In HA the broker pod passes a
        # RedisDownloadResultQueue so multiple broker pods share a bot-ready queue.
        self._result_queue: DownloadResultQueue = (
            result_queue if result_queue is not None else AsyncioDownloadResultQueue()
        )

    def build_app(self) -> web.Application:
        '''Build and return the aiohttp Application. Exposed for testing.'''
        app = web.Application(middlewares=[self._get_drain_middleware()])
        app.router.add_post('/requests/{uuid}', self._handle_register_request)
        app.router.add_put('/requests/{uuid}/status', self._handle_update_status)
        app.router.add_post('/downloads', self._handle_register_download)
        app.router.add_post('/downloads/register', self._handle_register_download_direct)
        app.router.add_get('/results/next', self._handle_next_result)
        app.router.add_post('/requests/{uuid}/checkout', self._handle_checkout)
        app.router.add_post('/requests/{uuid}/release', self._handle_release)
        app.router.add_post('/requests/{uuid}/remove', self._handle_remove)
        app.router.add_post('/requests/{uuid}/discard', self._handle_discard)
        app.router.add_post('/prefetch', self._handle_prefetch)
        app.router.add_post('/cache/check', self._handle_check_cache)
        app.router.add_post('/cache/cleanup', self._handle_cache_cleanup)
        app.router.add_get('/cache/count', self._handle_get_cache_count)
        app.router.add_get('/bundles', self._handle_list_bundles_for_guild)
        app.router.add_post('/bundles', self._handle_create_bundle)
        app.router.add_post('/bundles/{uuid}/finalize', self._handle_finalize_bundle)
        app.router.add_delete('/bundles/{uuid}', self._handle_delete_bundle)
        return app

    # ------------------------------------------------------------------
    # Route handlers
    # ------------------------------------------------------------------

    async def _handle_register_request(self, request: web.Request) -> web.Response:
        ctx, body = await self._read_body(request)
        try:
            media_request = parse_media_request(body)
        except Exception as exc:
            raise web.HTTPUnprocessableEntity() from exc
        with otel_span_wrapper('broker.register_request', context=ctx, kind=SpanKind.SERVER):
            await self._broker.register_request(media_request)
        return web.json_response({'status': 'ok'}, status=201)

    async def _handle_update_status(self, request: web.Request) -> web.Response:
        ctx, body = await self._read_body(request)
        uuid = request.match_info['uuid']
        try:
            update = LifecycleStatusUpdate.model_validate(body)
        except Exception as exc:
            raise web.HTTPUnprocessableEntity() from exc
        with otel_span_wrapper('broker.update_status', context=ctx, kind=SpanKind.SERVER):
            await self._broker.update_request_status(uuid, update)
        return web.json_response({'status': 'ok'})

    async def _handle_register_download(self, request: web.Request) -> web.Response:
        ctx, body = await self._read_body(request)
        try:
            result = DownloadResult.model_validate(body)
        except Exception as exc:
            raise web.HTTPUnprocessableEntity() from exc
        # Persist successful media downloads (zone=AVAILABLE) so checkout finds
        # them; push every result onto the bot-ready queue for next_result —
        # failures and metadata-only PlaylistAddRequest results still need to
        # reach the bot's process_download_results router.
        with otel_span_wrapper('broker.register_download', context=ctx, kind=SpanKind.SERVER):
            if result.status.success and result.file_name is not None:
                await self._broker.register_download_result(result)
            await self._result_queue.put(result)
        return web.json_response({'status': 'ok'}, status=202)

    async def _handle_register_download_direct(self, request: web.Request) -> web.Response:
        '''POST /downloads/register — persist a MediaDownload built bot-side.'''
        ctx, body = await self._read_body(request)
        try:
            media_request = parse_media_request(body['request'])
            file_path = Path(body['file_path']) if body.get('file_path') else None
            ytdl_data = body.get('ytdl_data', {})
        except Exception as exc:
            raise web.HTTPUnprocessableEntity() from exc
        media_download = MediaDownload(file_path, ytdl_data, media_request,
                                       cache_hit=bool(body.get('cache_hit', False)))
        media_download.file_size_bytes = body.get('file_size_bytes')
        with otel_span_wrapper('broker.register_download_direct', context=ctx, kind=SpanKind.SERVER):
            await self._broker.register_download(media_download)
        return web.json_response({'status': 'ok'})

    async def _handle_next_result(self, request: web.Request) -> web.Response:
        '''GET /results/next — pop the next bot-ready DownloadResult, or 204.'''
        ctx = extract(request.headers)
        with otel_span_wrapper('broker.next_result', context=ctx, kind=SpanKind.SERVER):
            result = await self._result_queue.get_nowait()
        if result is None:
            return web.Response(status=204)
        return web.json_response(result.model_dump(mode='json'))

    async def _handle_checkout(self, request: web.Request) -> web.Response:
        ctx, body = await self._read_body(request)
        uuid = request.match_info['uuid']
        try:
            guild_id = int(body['guild_id'])
            guild_path = body.get('guild_path')
        except Exception as exc:
            raise web.HTTPUnprocessableEntity() from exc
        with otel_span_wrapper('broker.checkout', context=ctx, kind=SpanKind.SERVER):
            result = await self._broker.checkout(uuid, guild_id, Path(guild_path) if guild_path else None)
        if result is None:
            return web.json_response({'guild_file_path': None})
        if result.s3_key:
            return web.json_response({'s3_key': result.s3_key})
        return web.json_response({'guild_file_path': str(result.local_path) if result.local_path else None})

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

    async def _handle_discard(self, request: web.Request) -> web.Response:
        ctx = extract(request.headers)
        uuid = request.match_info['uuid']
        with otel_span_wrapper('broker.discard', context=ctx, kind=SpanKind.SERVER):
            await self._broker.discard(uuid)
        return web.json_response({'status': 'ok'})

    async def _handle_prefetch(self, request: web.Request) -> web.Response:
        ctx, body = await self._read_body(request)
        try:
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

    async def _handle_check_cache(self, request: web.Request) -> web.Response:
        ctx, body = await self._read_body(request)
        try:
            media_request = parse_media_request(body)
        except Exception as exc:
            raise web.HTTPUnprocessableEntity() from exc
        with otel_span_wrapper('broker.check_cache', context=ctx, kind=SpanKind.SERVER):
            cached = await self._broker.check_cache(media_request)
        if cached is None:
            return web.json_response({'hit': False})
        return web.json_response({
            'hit': True,
            'download': {
                'request': cached.media_request.model_dump(mode='json'),
                'file_path': str(cached.file_path) if cached.file_path else None,
                'file_size_bytes': cached.file_size_bytes,
                'cache_hit': cached.cache_hit,
                'ytdl_data': {
                    'id': cached.id, 'title': cached.title,
                    'webpage_url': cached.webpage_url, 'uploader': cached.uploader,
                    'duration': cached.duration, 'extractor': cached.extractor,
                },
            },
        })

    async def _handle_cache_cleanup(self, request: web.Request) -> web.Response:
        ctx = extract(request.headers)
        with otel_span_wrapper('broker.cache_cleanup', context=ctx, kind=SpanKind.SERVER):
            removed = await self._broker.cache_cleanup()
        return web.json_response({'removed': bool(removed)})

    async def _handle_get_cache_count(self, request: web.Request) -> web.Response:
        ctx = extract(request.headers)
        with otel_span_wrapper('broker.get_cache_count', context=ctx, kind=SpanKind.SERVER):
            count = await self._broker.get_cache_count()
        return web.json_response({'count': int(count)})

    async def _handle_create_bundle(self, request: web.Request) -> web.Response:
        ctx, body = await self._read_body(request)
        try:
            guild_id = int(body['guild_id'])
            channel_id = int(body['channel_id'])
            input_string = body.get('input_string')
            has_search_banner = bool(body.get('has_search_banner', False))
        except Exception as exc:
            raise web.HTTPUnprocessableEntity() from exc
        with otel_span_wrapper('broker.create_bundle', context=ctx, kind=SpanKind.SERVER):
            uuid = await self._broker.create_bundle(
                guild_id, channel_id,
                input_string=input_string, has_search_banner=has_search_banner,
            )
        return web.json_response({'uuid': uuid}, status=201)

    async def _handle_finalize_bundle(self, request: web.Request) -> web.Response:
        ctx = extract(request.headers)
        bundle_uuid = request.match_info['uuid']
        with otel_span_wrapper('broker.finalize_bundle', context=ctx, kind=SpanKind.SERVER):
            await self._broker.finalize_bundle(bundle_uuid)
        return web.json_response({'status': 'ok'})

    async def _handle_delete_bundle(self, request: web.Request) -> web.Response:
        ctx = extract(request.headers)
        bundle_uuid = request.match_info['uuid']
        with otel_span_wrapper('broker.delete_bundle', context=ctx, kind=SpanKind.SERVER):
            await self._broker.delete_bundle(bundle_uuid)
        return web.json_response({'status': 'ok'})

    async def _handle_list_bundles_for_guild(self, request: web.Request) -> web.Response:
        ctx = extract(request.headers)
        try:
            guild_id = int(request.query['guild_id'])
        except (KeyError, ValueError) as exc:
            raise web.HTTPUnprocessableEntity() from exc
        with otel_span_wrapper('broker.list_bundles_for_guild', context=ctx, kind=SpanKind.SERVER):
            uuids = await self._broker.list_bundles_for_guild(guild_id)
        return web.json_response({'uuids': uuids})
