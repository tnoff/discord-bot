'''
HTTP server for the download worker pod.

Fronts a RedisDownloadWorker so bot pods (via HttpDownloadClient) can submit /
clear / block downloads and poll queue/backoff status over HTTP.  The worker pod
also drives its own consumer loop in-process to drain the Redis queue.

Mirrors BrokerHttpServer: the AiohttpServerBase serve()/drain lifecycle,
_read_body body parsing, inline otel server spans, and a heartbeat
observable-gauge so the downloader pod has a first-class liveness series.
'''
import logging

from aiohttp import web
from opentelemetry.propagate import extract
from opentelemetry.trace import SpanKind

from discord_bot.servers.base import AiohttpServerBase
from discord_bot.types.playlist_add_request import parse_media_request
from discord_bot.utils.otel import (otel_span_wrapper, create_observable_gauge, METER_PROVIDER,
                                     MetricNaming)
from discord_bot.workers.redis_download_worker import RedisDownloadWorker

logger = logging.getLogger(__name__)


class DownloadHttpServer(AiohttpServerBase):
    '''
    aiohttp HTTP server fronting a RedisDownloadWorker.

    Routes:
        POST /downloads          submit
        POST /downloads/clear    clear_guild_queue (preserve_playlist_adds flag)
        POST /downloads/block    block_guild
        GET  /downloads/status   queue_size + failure_summary + backoff snapshot

    submit / clear / block mutate the shared Redis queue; status is a cached read
    surface the bot pod's HttpDownloadClient polls (the bot pod can't read Redis).
    '''

    # bandit B104: '0.0.0.0' default is intentional — bot pods reach the downloader
    # across the k8s network; callers override host via the constructor arg.
    def __init__(self, worker: RedisDownloadWorker, host: str = '0.0.0.0',  # nosec B104
                 port: int = 8083):
        super().__init__()
        self._worker = worker
        self._host = host
        self._port = port
        # Heartbeat so the downloader pod has a first-class liveness series like the
        # broker pod and the bot cogs — a downloader that is down (or not yet
        # accepting connections at startup) otherwise only surfaces indirectly as a
        # climbing download-queue depth.  Emitted under job="downloader".
        create_observable_gauge(METER_PROVIDER, MetricNaming.HEARTBEAT.value,
                                self.heartbeat_observations,
                                'Download HTTP server heartbeat')

    def heartbeat_observations(self, _options=None):
        '''OTEL observable-gauge callback: 1 while the HTTP server is up and
        accepting requests, else 0. Public so it can be exercised directly.'''
        return self._serving_heartbeat_observations('downloader')

    def build_app(self) -> web.Application:
        '''Build and return the aiohttp Application. Exposed for testing.'''
        app = web.Application(middlewares=[self._get_drain_middleware()])
        app.router.add_post('/downloads', self._handle_submit)
        app.router.add_post('/downloads/clear', self._handle_clear)
        app.router.add_post('/downloads/block', self._handle_block)
        app.router.add_get('/downloads/status', self._handle_status)
        return app

    async def _handle_submit(self, request: web.Request) -> web.Response:
        '''POST /downloads — enqueue a MediaRequest on the Redis download queue.'''
        ctx, body = await self._read_body(request)
        try:
            guild_id = int(body['guild_id'])
            priority = body.get('priority')
            media_request = parse_media_request(body['media_request'])
        except Exception as exc:
            raise web.HTTPUnprocessableEntity() from exc
        with otel_span_wrapper('downloader.submit', context=ctx, kind=SpanKind.SERVER):
            await self._worker.submit(guild_id, media_request, priority=priority)
        return web.json_response({'status': 'ok'}, status=202)

    async def _handle_clear(self, request: web.Request) -> web.Response:
        '''POST /downloads/clear — drop pending requests for a guild.

        Body: {guild_id, preserve_playlist_adds}.  A predicate can't cross HTTP,
        so preserve_playlist_adds=True translates server-side to preserving the
        metadata-only (non-downloading) requests — i.e. playlist-add items — which
        is exactly what the cog's `not req.download_file` predicate keeps.  The
        dropped MediaRequests are returned so the cog can still push their
        DISCARDED lifecycle states from the bot pod.
        '''
        ctx, body = await self._read_body(request)
        try:
            guild_id = int(body['guild_id'])
            preserve_playlist_adds = bool(body.get('preserve_playlist_adds', False))
        except Exception as exc:
            raise web.HTTPUnprocessableEntity() from exc
        preserve = (lambda mr: not mr.download_file) if preserve_playlist_adds else None
        with otel_span_wrapper('downloader.clear', context=ctx, kind=SpanKind.SERVER):
            dropped = await self._worker.clear_guild_queue(guild_id, preserve_predicate=preserve)
        return web.json_response(
            {'dropped': [mr.model_dump(mode='json') for mr in dropped]}, status=200,
        )

    async def _handle_block(self, request: web.Request) -> web.Response:
        '''POST /downloads/block — refuse new submits for a guild.'''
        ctx, body = await self._read_body(request)
        try:
            guild_id = int(body['guild_id'])
        except Exception as exc:
            raise web.HTTPUnprocessableEntity() from exc
        with otel_span_wrapper('downloader.block', context=ctx, kind=SpanKind.SERVER):
            await self._worker.block_guild(guild_id)
        return web.json_response({'status': 'ok'}, status=200)

    async def _handle_status(self, request: web.Request) -> web.Response:
        '''GET /downloads/status — live queue/backoff/failure snapshot for the bot
        pod's HttpDownloadClient poller.'''
        ctx = extract(request.headers)
        with otel_span_wrapper('downloader.status', context=ctx, kind=SpanKind.SERVER):
            snapshot = await self._worker.status_snapshot()
        return web.json_response(snapshot, status=200)
