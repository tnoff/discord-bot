'''
Shared bot-side HTTP client for the pod-hosted queue workers.

The mirror image of servers/queue_worker_server.py: HttpDownloadClient and
HttpYoutubeMusicSearchClient talk to structurally identical pods, so the producer
surface (submit / block_guild / clear_guild_queue) and the cached read surface
(queue_size / failure_summary / backoff_seconds_remaining, refreshed by a
background status poller) live here once, parameterised by route + span prefix.
Two copies would trip pylint's duplicate-code check (R0801); sharing rather than
disabling follows the call made for workers/redis_guild_queue.py.

Scoped to what the bot pod can do from outside the worker pod.  There is no
`run` and no `local_worker`: the consumer loop belongs to the pod that owns the
queue.  start() / stop() drive the status poller, mirroring the in-memory
clients' run() lifecycle.

The poller deliberately does NOT register a LoopHealth.  Every other background
loop does, but this one runs in the *bot* pod and only refreshes display values;
a wedged poller that failed the bot's livenessProbe would restart the bot pod
over a worker-pod outage a bot restart cannot fix — the failure mode
utils/loop_health.py exists to prevent.  What a stall could actually come from is
an unbounded request, so each poll carries its own timeout instead
(STATUS_REQUEST_TIMEOUT_SECONDS).
'''
import asyncio
import logging
from typing import Callable, ClassVar

from opentelemetry.trace import SpanKind

from discord_bot.clients.http_client_base import HttpClientMixin
from discord_bot.types.clear_guild_result import ClearGuildResult
from discord_bot.types.media_request import MediaRequest
from discord_bot.types.playlist_add_request import parse_media_request
from discord_bot.utils.otel import async_otel_span_wrapper

logger = logging.getLogger(__name__)

_DEFAULT_FAILURE_SUMMARY = '0 failures in queue'


class HttpQueueWorkerClient(HttpClientMixin):
    '''
    Forwards the cog-facing queue-client surface to a remote worker pod.

    Producer methods POST to the pod's QueueWorkerHttpServer and await the result
    inline.  The sync read surface is served from a per-client cache the poller
    refreshes from GET {prefix}/status — the bot pod doesn't run the worker loop,
    so it can't read the shared Redis queue/backoff state directly.
    '''

    # Route prefix ('/downloads', '/search/ytmusic') and otel span prefix
    # ('downloader', 'youtube_music_search') — set by the subclass.
    ROUTE_PREFIX: ClassVar[str]
    SPAN_PREFIX: ClassVar[str]

    POLL_INTERVAL_SECONDS: ClassVar[float] = 1.0
    # Cap on a single status request, retries included.  Without it an aiohttp
    # request inherits the 5-minute default, so a blackholed worker pod could
    # park one poll iteration for far longer than the poll interval.
    STATUS_REQUEST_TIMEOUT_SECONDS: ClassVar[float] = 10.0

    def __init__(self, base_url: str, session=None):
        self._base_url = base_url.rstrip('/')
        self._session = session
        self._cached_failure_summary: str = _DEFAULT_FAILURE_SUMMARY
        self._cached_backoff_seconds: int | None = None
        self._cached_queue_sizes: dict[int, int] = {}
        self._poller_task: asyncio.Task | None = None
        self._poller_stop: asyncio.Event = asyncio.Event()

    @property
    def _submit_url(self) -> str:
        return f'{self._base_url}{self.ROUTE_PREFIX}'

    @staticmethod
    def _build_submit_body(guild_id: int, media_request: MediaRequest,
                           priority: int | None) -> dict:
        '''Canonical submit body: guild + priority + serialised request.'''
        return {
            'guild_id': guild_id,
            'priority': priority,
            'media_request': media_request.model_dump(mode='json'),
        }

    async def submit(self, guild_id: int, media_request: MediaRequest,
                     priority: int | None = None) -> None:
        '''POST {prefix} — the worker pod enqueues the request on its Redis queue.'''
        async with async_otel_span_wrapper(f'{self.SPAN_PREFIX}.submit', kind=SpanKind.CLIENT):
            await self._http('POST', self._submit_url,
                             self._build_submit_body(guild_id, media_request, priority))

    async def block_guild(self, guild_id: int) -> bool:
        '''POST {prefix}/block — refuse subsequent submits for this guild.'''
        async with async_otel_span_wrapper(f'{self.SPAN_PREFIX}.block', kind=SpanKind.CLIENT):
            await self._http('POST', f'{self._submit_url}/block', {'guild_id': guild_id})
        return True

    async def clear_guild_queue(self, guild_id: int,
                                preserve_predicate: Callable[[MediaRequest], bool] | None = None,
                                ) -> ClearGuildResult:
        '''POST {prefix}/clear — drop the guild's pending requests.

        A predicate can't cross HTTP, so we forward preserve_playlist_adds=True when
        a predicate is supplied (the cog only passes one to preserve the metadata-only
        playlist-add items); the worker pod translates that to a server-side predicate.
        The response carries the dropped requests (so the cog can still push their
        DISCARDED lifecycle states from the bot pod) and the bundle_uuids the pod
        preserved (so the cog skips deleting those bundles — the reconciliation the
        in-memory client does in-process).
        '''
        body = {'guild_id': guild_id, 'preserve_playlist_adds': preserve_predicate is not None}
        async with async_otel_span_wrapper(f'{self.SPAN_PREFIX}.clear', kind=SpanKind.CLIENT):
            resp = await self._http('POST', f'{self._submit_url}/clear', body)
        resp = resp or {}
        return ClearGuildResult(
            dropped=[parse_media_request(item) for item in resp.get('dropped', [])],
            preserved_bundle_uuids=set(resp.get('preserved_bundle_uuids', [])),
        )

    async def queue_size(self, guild_id: int) -> int:
        '''Cached pending count for a guild, refreshed by the background poller.'''
        return self._cached_queue_sizes.get(guild_id, 0)

    @property
    def failure_summary(self) -> str:
        '''Cached failure-queue summary refreshed by the background poller.'''
        return self._cached_failure_summary

    @property
    def backoff_seconds_remaining(self) -> int | None:
        '''Cached backoff seconds refreshed by the background poller.'''
        return self._cached_backoff_seconds

    async def start(self, bot=None, shutdown_event: asyncio.Event | None = None) -> None:
        '''Start the background status poller.  Idempotent.  bot / shutdown_event are
        accepted for symmetry with the in-memory client's run() driver but unused —
        the worker loop runs in the worker pod.'''
        del bot, shutdown_event
        if self._poller_task is not None and not self._poller_task.done():
            return
        self._poller_stop.clear()
        self._poller_task = asyncio.create_task(self._poll_status_loop())

    async def stop(self) -> None:
        '''Stop the poller and close the http session.'''
        self._poller_stop.set()
        if self._poller_task and not self._poller_task.done():
            self._poller_task.cancel()
            try:
                await self._poller_task
            except asyncio.CancelledError:
                pass
        await self.close()

    async def _poll_status_loop_once(self) -> None:
        '''One status-refresh iteration.  Errors are swallowed so a transient
        worker-pod hiccup doesn't kill the poller.  Factored out so tests can drive
        the cache-update logic without the loop timing.'''
        try:
            status = await asyncio.wait_for(
                self._http('GET', f'{self._submit_url}/status'),
                timeout=self.STATUS_REQUEST_TIMEOUT_SECONDS)
        except Exception as exc:
            logger.warning('%s status poller error: %s', self.SPAN_PREFIX, exc)
            return
        if not status:
            return
        self._cached_failure_summary = status.get('failure_summary', _DEFAULT_FAILURE_SUMMARY)
        self._cached_backoff_seconds = status.get('backoff_seconds_remaining')
        self._cached_queue_sizes = {
            int(k): int(v) for k, v in status.get('queue_sizes', {}).items()
        }

    async def _poll_status_loop(self) -> None:
        '''Periodically refresh cached read values until stop() fires.'''
        while not self._poller_stop.is_set():
            await self._poll_status_loop_once()
            try:
                await asyncio.wait_for(self._poller_stop.wait(),
                                       timeout=self.POLL_INTERVAL_SECONDS)
            except asyncio.TimeoutError:
                continue
