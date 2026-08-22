'''
Shared HTTP surface for the pod-hosted queue workers.

The downloader pod (DownloadHttpServer) and the YouTube-Music search pod
(YoutubeMusicSearchHttpServer) front structurally identical engines: a
Redis-backed per-guild queue that the bot pod can submit to, clear, block, and
read status from — but never pop, because the consumer loop runs in the worker
pod.  The four handlers are the same modulo the route prefix and the span name,
so they live here once and the two servers supply constants.

That is not a style preference: pylint's duplicate-code (R0801) check compares
modules pairwise, and two copies of these handlers trip it.  Sharing rather than
disabling follows the same call made for workers/redis_guild_queue.py.

Subclasses set ROUTE_PREFIX / SPAN_PREFIX / HEARTBEAT_JOB / DEFAULT_PORT; the
AiohttpServerBase serve()/drain lifecycle, _read_body parsing, inline otel server
spans, and the heartbeat observable-gauge all come from here.
'''
import logging
from typing import ClassVar

from aiohttp import web
from opentelemetry.trace import SpanKind

from discord_bot.servers.base import AiohttpServerBase
from discord_bot.types.playlist_add_request import parse_media_request
from discord_bot.types.queue import PutsBlocked, QueueFull, submit_rejection_status
from discord_bot.utils.otel import (otel_span_wrapper, create_observable_gauge, METER_PROVIDER,
                                     MetricNaming, AttributeNaming)

logger = logging.getLogger(__name__)


class QueueWorkerHttpServer(AiohttpServerBase):
    '''
    aiohttp HTTP server fronting a Redis-backed queue worker.

    Routes (all relative to ROUTE_PREFIX):
        POST {prefix}          submit
        POST {prefix}/clear    clear_guild_queue (preserve_playlist_adds flag)
        POST {prefix}/block    block_guild
        GET  {prefix}/status   queue_size + failure_summary + backoff snapshot

    submit / clear / block mutate the shared Redis queue; status is a read
    surface the bot pod's HTTP client polls (the bot pod can't read Redis).
    '''

    # Route prefix ('/downloads', '/search/ytmusic') and otel span prefix
    # ('downloader', 'youtube_music_search') — set by the subclass.
    ROUTE_PREFIX: ClassVar[str]
    SPAN_PREFIX: ClassVar[str]
    # background_job label on this pod's heartbeat gauge.
    HEARTBEAT_JOB: ClassVar[str]
    HEARTBEAT_DESCRIPTION: ClassVar[str]
    DEFAULT_PORT: ClassVar[int]

    # bandit B104: '0.0.0.0' default is intentional — bot pods reach the worker
    # pod across the k8s network; callers override host via the constructor arg.
    def __init__(self, worker, host: str = '0.0.0.0',  # nosec B104
                 port: int | None = None):
        super().__init__()
        self._worker = worker
        self._host = host
        self._port = port if port is not None else self.DEFAULT_PORT
        # Heartbeat so the worker pod has a first-class liveness series like the
        # broker pod and the bot cogs — a worker pod that is down (or not yet
        # accepting connections at startup) otherwise only surfaces indirectly as
        # a climbing queue depth.
        create_observable_gauge(METER_PROVIDER, MetricNaming.HEARTBEAT.value,
                                self.heartbeat_observations,
                                self.HEARTBEAT_DESCRIPTION)

    def heartbeat_observations(self, _options=None):
        '''OTEL observable-gauge callback: 1 while the HTTP server is up and
        accepting requests, else 0. Public so it can be exercised directly.'''
        return self._serving_heartbeat_observations(self.HEARTBEAT_JOB)

    def build_app(self) -> web.Application:
        '''Build and return the aiohttp Application. Exposed for testing.'''
        app = web.Application(middlewares=[self._get_drain_middleware()])
        app.router.add_post(self.ROUTE_PREFIX, self._handle_submit)
        app.router.add_post(f'{self.ROUTE_PREFIX}/clear', self._handle_clear)
        app.router.add_post(f'{self.ROUTE_PREFIX}/block', self._handle_block)
        app.router.add_get(f'{self.ROUTE_PREFIX}/status', self._handle_status)
        return app

    async def _handle_submit(self, request: web.Request) -> web.Response:
        '''POST {prefix} — enqueue a MediaRequest on the worker's Redis queue.'''
        ctx, body = await self._read_body(request)
        try:
            guild_id = int(body['guild_id'])
            priority = body.get('priority')
            media_request = parse_media_request(body['media_request'])
        except Exception as exc:
            raise web.HTTPUnprocessableEntity() from exc
        rejection: Exception | None = None
        with otel_span_wrapper(f'{self.SPAN_PREFIX}.submit', context=ctx, kind=SpanKind.SERVER) as span:
            try:
                await self._worker.submit(guild_id, media_request, priority=priority)
            except (PutsBlocked, QueueFull) as exc:
                # Spelled out rather than derived from SUBMIT_REJECTION_STATUS:
                # pylint cannot infer that a dict's keys are exception classes and
                # rejects `except tuple(...)` (E0712). Drift is guarded by the
                # end-to-end test, which parametrises over that dict -- adding an
                # entry there without adding it here fails the test.
                #
                # Caught INSIDE the span on purpose. otel_span_wrapper marks any
                # escaping exception ERROR, and a queue refusal is an expected
                # answer the cog handles -- letting it escape would make every
                # blocked-guild submit read as a server fault on the error-rate
                # panels and alerts.
                rejection = exc
                span.set_attributes({AttributeNaming.SUBMIT_REJECTION.value: type(exc).__name__})
        if rejection is not None:
            return web.json_response(
                {'status': 'rejected', 'reason': type(rejection).__name__, 'detail': str(rejection)},
                status=submit_rejection_status(rejection))
        return web.json_response({'status': 'ok'}, status=202)

    async def _handle_clear(self, request: web.Request) -> web.Response:
        '''POST {prefix}/clear — drop pending requests for a guild.

        Body: {guild_id, preserve_playlist_adds}.  A predicate can't cross HTTP,
        so preserve_playlist_adds=True translates server-side to preserving the
        metadata-only (non-downloading) requests — i.e. playlist-add items —
        which is exactly what the cog's `not req.download_file` predicate keeps.
        The response returns the dropped MediaRequests (so the cog can still push
        their DISCARDED lifecycle states from the bot pod) plus the bundle_uuids
        of the preserved items (so the cog can skip deleting those bundles — the
        reconciliation it does in-process when the worker is local).
        '''
        ctx, body = await self._read_body(request)
        try:
            guild_id = int(body['guild_id'])
            preserve_playlist_adds = bool(body.get('preserve_playlist_adds', False))
        except Exception as exc:
            raise web.HTTPUnprocessableEntity() from exc
        preserved_bundle_uuids: set[str] = set()
        if preserve_playlist_adds:
            def preserve(media_request):
                keep = not media_request.download_file
                if keep and media_request.bundle_uuid:
                    preserved_bundle_uuids.add(media_request.bundle_uuid)
                return keep
        else:
            preserve = None
        with otel_span_wrapper(f'{self.SPAN_PREFIX}.clear', context=ctx, kind=SpanKind.SERVER):
            dropped = await self._worker.clear_guild_queue(guild_id, preserve_predicate=preserve)
        return web.json_response(
            {
                'dropped': [mr.model_dump(mode='json') for mr in dropped],
                'preserved_bundle_uuids': sorted(preserved_bundle_uuids),
            },
            status=200,
        )

    async def _handle_block(self, request: web.Request) -> web.Response:
        '''POST {prefix}/block — refuse new submits for a guild.'''
        ctx, body = await self._read_body(request)
        try:
            guild_id = int(body['guild_id'])
        except Exception as exc:
            raise web.HTTPUnprocessableEntity() from exc
        with otel_span_wrapper(f'{self.SPAN_PREFIX}.block', context=ctx, kind=SpanKind.SERVER):
            await self._worker.block_guild(guild_id)
        return web.json_response({'status': 'ok'}, status=200)

    async def _handle_status(self, request: web.Request) -> web.Response:
        '''GET {prefix}/status — live queue/backoff/failure snapshot for the bot
        pod's polling client.

        Deliberately unspanned, the server half of the same decision as
        HttpQueueWorkerClient._poll_status_loop_once: the bot polls this route
        every second per worker whether or not anything changed, so a span here
        is emitted at the poll rate rather than per unit of work — ~3.5k/hour on
        each of the downloader and search pods, against single-digit hourly rates
        for the spans that describe real work.  The client no longer starts a
        span for the poll either, so there is no longer a caller context to
        continue: every one of these would be a root span describing an
        unremarkable cache refresh.  Routes that mutate queue state keep theirs.
        '''
        del request
        snapshot = await self._worker.status_snapshot()
        return web.json_response(snapshot, status=200)
