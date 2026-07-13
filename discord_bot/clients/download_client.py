'''
Cog-facing download client.

InMemoryDownloadClient is a thin wrapper around a DownloadWorkerBase engine
(AsyncioDownloadWorker in single-process) — it forwards the cog-facing
DownloadClient Protocol surface to the worker, mirroring how InMemoryBrokerClient
wraps a MediaBrokerBase.  A future HttpDownloadClient will forward the same
surface to a remote downloader pod for HA.

The download exceptions, yt-dlp helpers, and the DownloadWorkerBase engine live
in interfaces/download_protocols.py; they are re-exported here so existing
`from discord_bot.clients.download_client import ...` call sites keep working.
'''
import asyncio
import logging
from typing import Callable, ClassVar

from opentelemetry.trace import SpanKind

from discord_bot.clients.http_client_base import HttpClientMixin
from discord_bot.types.playlist_add_request import parse_media_request
from discord_bot.utils.otel import async_otel_span_wrapper
from discord_bot.interfaces.download_protocols import (
    DownloadClient,
    DownloadWorkerBase,
    DirectItemAvailableException,
    DownloadClientException,
    DownloadTerminalException,
    RetryableException,
    RetryLimitExceeded,
    InvalidFormatException,
    VideoNotFoundException,
    MetadataCheckFailedException,
    VideoAgeRestrictedException,
    VideoUnavailableException,
    VideoViolatedTermsException,
    PrivateVideoException,
    VideoTooLong,
    VideoBanned,
    BotDownloadFlagged,
    match_generator,
    OTEL_SPAN_PREFIX,
    YTDLP_OUTPUT_TEMPLATE,
    YTDLP_SOURCE_ADDRESS,
)
from discord_bot.types.media_request import MediaRequest

logger = logging.getLogger(__name__)

__all__ = [
    'DownloadClient',
    'DownloadWorkerBase',
    'InMemoryDownloadClient',
    'HttpDownloadClient',
    'DirectItemAvailableException',
    'DownloadClientException',
    'DownloadTerminalException',
    'RetryableException',
    'RetryLimitExceeded',
    'InvalidFormatException',
    'VideoNotFoundException',
    'MetadataCheckFailedException',
    'VideoAgeRestrictedException',
    'VideoUnavailableException',
    'VideoViolatedTermsException',
    'PrivateVideoException',
    'VideoTooLong',
    'VideoBanned',
    'BotDownloadFlagged',
    'match_generator',
    'OTEL_SPAN_PREFIX',
    'YTDLP_OUTPUT_TEMPLATE',
    'YTDLP_SOURCE_ADDRESS',
]


class InMemoryDownloadClient:
    '''
    Single-process DownloadClient: a thin wrapper around a DownloadWorkerBase.

    Forwards the cog-facing Protocol surface (submit / block_guild /
    clear_guild_queue / queue_size / failure_summary / backoff_seconds_remaining
    / run) straight to the worker, which owns the yt-dlp pipeline and input
    queues.  local_worker exposes the wrapped engine for single-process callers
    that need it (there is no equivalent on a future HttpDownloadClient).
    '''
    def __init__(self, worker: DownloadWorkerBase):
        self._worker = worker

    @property
    def local_worker(self) -> DownloadWorkerBase:
        '''The wrapped engine — only meaningful in single-process mode.'''
        return self._worker

    async def submit(self, guild_id: int, media_request: MediaRequest,
                     priority: int | None = None) -> None:
        '''Enqueue a MediaRequest for download; results are reported to the broker.'''
        await self._worker.submit(guild_id, media_request, priority=priority)

    async def block_guild(self, guild_id: int) -> bool:
        '''Block new submissions for a guild (used during shutdown/cleanup).'''
        return await self._worker.block_guild(guild_id)

    async def clear_guild_queue(self, guild_id: int,
                                preserve_predicate: Callable[[MediaRequest], bool] | None = None,
                                ) -> list[MediaRequest]:
        '''Clear the input queue for a guild, returning the dropped requests.'''
        return await self._worker.clear_guild_queue(guild_id, preserve_predicate=preserve_predicate)

    async def queue_size(self, guild_id: int) -> int:
        '''Return the number of pending requests for a guild, or 0 if none.'''
        return await self._worker.queue_size(guild_id)

    @property
    def failure_summary(self) -> str:
        '''Human-readable summary of the download failure queue.'''
        return self._worker.failure_summary

    @property
    def backoff_seconds_remaining(self) -> int | None:
        '''Seconds remaining in the current backoff period, or None if not set.'''
        return self._worker.backoff_seconds_remaining

    async def run(self, shutdown_event: asyncio.Event) -> None:
        '''Consume one queued request and download it; driven as a background loop.'''
        await self._worker.run(shutdown_event)


class HttpDownloadClient(HttpClientMixin):
    '''
    DownloadClient that forwards the cog-facing surface to a remote downloader pod.

    Producer methods (submit / block_guild / clear_guild_queue) POST to the
    downloader's DownloadHttpServer and await the result inline (the DownloadClient
    Protocol is async).  The sync read surface (failure_summary /
    backoff_seconds_remaining / queue_size) is served from a per-client cache a
    background poller refreshes from GET /downloads/status — the bot pod doesn't run
    the worker loop, so it can't read the shared Redis backoff/queue state directly.

    There is no `run` or `local_worker`: the download consumer loop runs in the
    downloader pod (owned by its CLI entrypoint), not on the bot side.  start() /
    stop() drive the status poller, mirroring the local client's run() lifecycle.
    '''

    POLL_INTERVAL_SECONDS: ClassVar[float] = 1.0

    def __init__(self, base_url: str, session=None):
        self._base_url = base_url.rstrip('/')
        self._session = session
        self._cached_failure_summary: str = '0 failures in queue'
        self._cached_backoff_seconds: int | None = None
        self._cached_queue_sizes: dict[int, int] = {}
        self._poller_task: asyncio.Task | None = None
        self._poller_stop: asyncio.Event = asyncio.Event()

    @staticmethod
    def _build_submit_body(guild_id: int, media_request: MediaRequest,
                           priority: int | None) -> dict:
        '''Canonical POST /downloads body: guild + priority + serialised request.'''
        return {
            'guild_id': guild_id,
            'priority': priority,
            'media_request': media_request.model_dump(mode='json'),
        }

    async def submit(self, guild_id: int, media_request: MediaRequest,
                     priority: int | None = None) -> None:
        '''POST /downloads — the downloader pod enqueues the request on its Redis queue.'''
        async with async_otel_span_wrapper('downloader.submit', kind=SpanKind.CLIENT):
            await self._http('POST', f'{self._base_url}/downloads',
                             self._build_submit_body(guild_id, media_request, priority))

    async def block_guild(self, guild_id: int) -> bool:
        '''POST /downloads/block — refuse subsequent submits for this guild.'''
        async with async_otel_span_wrapper('downloader.block', kind=SpanKind.CLIENT):
            await self._http('POST', f'{self._base_url}/downloads/block',
                             {'guild_id': guild_id})
        return True

    async def clear_guild_queue(self, guild_id: int,
                                preserve_predicate: Callable[[MediaRequest], bool] | None = None,
                                ) -> list[MediaRequest]:
        '''POST /downloads/clear — drop the guild's pending requests, returning them.

        A predicate can't cross HTTP, so we forward preserve_playlist_adds=True when a
        predicate is supplied (the cog only passes one to preserve the metadata-only
        playlist-add items); the downloader translates that to a server-side predicate.
        The dropped requests are returned so the cog can still push their DISCARDED
        lifecycle states from the bot pod.

        NOTE (HA gap for the cog cutover, MR 5): the cog's predicate also records the
        bundle_uuids of *preserved* items to skip deleting their bundles.  Preserved
        items stay on the downloader pod and are not returned here, so that side-effect
        does not run in HA mode — the bundle-preservation reconciliation moves to the
        cog cutover.
        '''
        body = {'guild_id': guild_id, 'preserve_playlist_adds': preserve_predicate is not None}
        async with async_otel_span_wrapper('downloader.clear', kind=SpanKind.CLIENT):
            resp = await self._http('POST', f'{self._base_url}/downloads/clear', body)
        return [parse_media_request(item) for item in (resp or {}).get('dropped', [])]

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
        accepted for symmetry with the local client's run() driver but unused — the
        worker loop runs in the downloader pod.'''
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
        downloader hiccup doesn't kill the poller.  Factored out so tests can drive
        the cache-update logic without the loop timing.'''
        try:
            status = await self._http('GET', f'{self._base_url}/downloads/status')
        except Exception as exc:
            logger.warning('download status poller error: %s', exc)
            return
        if not status:
            return
        self._cached_failure_summary = status.get('failure_summary', '0 failures in queue')
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
