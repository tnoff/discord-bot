'''
Abstract base class for the media broker plus the cog-facing BrokerClient
Protocol.

MediaBrokerBase is the broker *engine* interface — both the in-process
(asyncio) and Redis-backed implementations satisfy it.

BrokerClient is the cog-facing handle.  InMemoryBrokerClient wraps a
MediaBrokerBase directly for single-process deployments; HttpBrokerClient
forwards calls to a remote BrokerHttpServer.  The cog only depends on
BrokerClient — config picks which impl is wired in.
'''
import asyncio
import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass
from enum import Enum
from pathlib import Path
from typing import Any, List, Protocol

from opentelemetry.trace import SpanKind

from discord_bot.cogs.music_helpers.video_cache_client import VideoCacheClient
from discord_bot.interfaces.result_queue import DownloadResultQueue
from discord_bot.types.download import DownloadResult, LifecycleStatusUpdate
from discord_bot.types.media_download import MediaDownload
from discord_bot.types.media_request import MediaRequest
from discord_bot.utils.integrations.s3 import delete_file
from discord_bot.utils.otel import async_otel_span_wrapper
from discord_bot.workers.media_bundle import BundleRenderer, BundleState

logger = logging.getLogger(__name__)

# Re-exported so callers that already import DownloadResultQueue from
# broker_protocols keep working.  The canonical home is interfaces/result_queue.py
# — kept separate to avoid dragging the broker engine's deps (sqlalchemy via
# VideoCacheClient) into the dispatcher pod, which only needs the Redis
# WorkQueue/BundleStore from workers/redis_queues.
__all__ = ['DownloadResultQueue']


@dataclass
class CheckoutResult:
    '''
    Result of a broker checkout operation.

    Exactly one of local_path or s3_key will be set. local_path means the file
    is already staged on local disk and ready to play. s3_key means the file
    lives in S3; bucket_name is set alongside it so the caller can download
    without needing separate S3 configuration.
    '''
    local_path: Path | None = None
    s3_key: str | None = None
    bucket_name: str | None = None


class BrokerClient(Protocol):
    '''
    Cog-facing handle for the MediaBroker.  Two implementations exist:

      InMemoryBrokerClient (clients/broker_client.py) — wraps a local
        MediaBrokerBase directly.  Used in single-process deployments.

      HttpBrokerClient (clients/broker_client.py) — forwards calls to a
        remote BrokerHttpServer over HTTP.  Used in HA deployments where
        the broker pod runs separately.

    Both shapes satisfy this Protocol; the cog only depends on the Protocol
    and lets config decide which impl is constructed.
    '''
    async def register_request(self, media_request) -> None:
        '''Register a new MediaRequest entering the pipeline.'''
    async def update_request_status(self, uuid: str, update: LifecycleStatusUpdate) -> None:
        '''Apply a lifecycle status update from the download worker.'''
    async def register_download(self, media_download: MediaDownload) -> None:
        '''Persist a downloaded MediaDownload on the broker (zone=AVAILABLE).'''
    async def register_download_result(self, result: DownloadResult) -> MediaDownload | None:
        '''Register a completed DownloadResult.  For success the broker stores the
        entry as AVAILABLE; in either case the result is pushed onto the bot-ready
        queue served by next_result.  Returns None — consumers build their own
        MediaDownload from the result they receive.'''
    async def next_result(self) -> DownloadResult | None:
        '''Pop the next bot-ready DownloadResult, or None if nothing is ready.
        Non-blocking — callers poll on their own cadence.'''
    async def checkout(self, uuid: str, guild_id: int, guild_path: str | None = None) -> CheckoutResult | None:
        '''Mark a request CHECKED_OUT; returns a CheckoutResult with local_path or s3_key set.'''
    async def release(self, uuid: str) -> None:
        '''Release a CHECKED_OUT entry and clean up the guild-specific file.'''
    async def remove(self, uuid: str) -> None:
        '''Remove an entry from the registry without touching any files.'''
    async def discard(self, uuid: str) -> None:
        '''Drop an entry that was registered but cannot be enqueued; deletes the
        underlying file unless a VideoCache is keeping it.'''
    async def prefetch(self, queue_items: list, guild_id: int, guild_path: str | None, limit: int) -> None:
        '''Pre-stage the next limit items from the queue to local disk.'''
    async def check_cache(self, media_request) -> MediaDownload | None:
        '''Look up a cached MediaDownload by webpage URL; returns None on miss.'''
    async def cache_cleanup(self) -> bool:
        '''Evict stale cache entries.  Returns True if at least one was removed.'''
    async def get_cache_count(self) -> int:
        '''Return the current VideoCache entry count (0 if no cache configured).'''
    async def create_bundle(self, guild_id: int, channel_id: int,
                            input_string: str | None = None,
                            has_search_banner: bool = False) -> str:
        '''Create a new bundle on the broker; returns the bundle uuid.'''
    async def finalize_bundle(self, bundle_uuid: str) -> None:
        '''Lock pagination and trigger the first full render of a bundle.'''
    async def delete_bundle(self, bundle_uuid: str) -> None:
        '''Tear down a bundle (broker also drops its Discord messages).'''
    async def list_bundles_for_guild(self, guild_id: int) -> list[str]:
        '''Return uuids of every bundle currently stored for this guild.'''


class BundleDispatchSink(Protocol):
    '''Subset of dispatcher methods the broker uses to push bundle UI updates.

    Both MessageDispatcher (in-process, cog-side) and HttpDispatchClient
    (HA, broker-side) satisfy it without extending — duck-typed.
    '''
    def update_mutable(self, key: str, guild_id: int, content: list,
                       channel_id: int | None,
                       sticky: bool = True, delete_after: int | None = None) -> Any:
        '''Edit (or send) the mutable Discord message for the given key.'''

    def remove_mutable(self, key: str) -> Any:
        '''Delete the mutable Discord message(s) for the given key.'''

    def send_message(self, guild_id: int, channel_id: int, content: str,
                     delete_after: int | None = None,
                     allow_404: bool = False,
                     span_context: dict | None = None) -> Any:
        '''Send a plain Discord message (not part of a mutable bundle).'''


class Zone(Enum):
    '''Lifecycle zone for a tracked media item.'''
    IN_FLIGHT = 'in_flight'
    AVAILABLE = 'available'
    CHECKED_OUT = 'checked_out'


@dataclass
class BrokerEntry:
    '''Single entry in the broker registry.'''
    request: MediaRequest
    download: MediaDownload | None = None
    zone: Zone = Zone.IN_FLIGHT
    checked_out_by: int | None = None
    guild_file_path: Path | None = None


class MediaBrokerBase(ABC):
    '''
    Abstract base for media broker implementations.

    Provides shared cache helpers (check_cache, cache_cleanup) as concrete
    template methods that call the abstract can_evict_base. All other broker
    operations are declared as abstract methods.

    Concrete subclasses must set video_cache and bucket_name in __init__.
    '''

    video_cache: VideoCacheClient | None
    bucket_name: str | None

    # ------------------------------------------------------------------
    # Shared cache helpers (template methods)
    #
    # Concrete on the base now that AsyncioBroker is the single in-process
    # engine: both it and the future RedisBroker share this eviction body, so
    # it lives here rather than being duplicated per impl.
    # ------------------------------------------------------------------

    async def check_cache(self, media_request: MediaRequest) -> MediaDownload | None:
        '''Return a cached MediaDownload for the request URL, or None if no cache hit.'''
        if not self.video_cache:
            return None
        return await self.video_cache.get_webpage_url_item(media_request)

    async def _get_evictable_entries(self) -> list:
        return [
            vc
            for vc in await self.video_cache.get_deletable_entries()
            if await self.can_evict_base(vc.video_url)
        ]

    async def cache_cleanup(self) -> bool:
        '''Evict stale cache entries. Returns True if at least one file was removed.'''
        if not self.video_cache:
            return False
        async with async_otel_span_wrapper('music.broker.cache_cleanup', kind=SpanKind.INTERNAL) as span:
            await self.video_cache.ready_remove()
            to_delete = await self._get_evictable_entries()
            span.set_attribute('music.broker.evicted_count', len(to_delete))
            if not to_delete:
                return False
            for vc in to_delete:
                delete_file(self.bucket_name, str(vc.base_path))
            await self.video_cache.remove_video_cache([vc.id for vc in to_delete])
            return True

    # ------------------------------------------------------------------
    # Abstract interface
    # ------------------------------------------------------------------

    @abstractmethod
    async def register_request(self, media_request: MediaRequest) -> None:
        '''Register a new MediaRequest entering the pipeline. Idempotent.'''

    @abstractmethod
    async def update_request_status(self, request_uuid: str, update: LifecycleStatusUpdate) -> None:
        '''Apply a lifecycle status update from the download worker.'''

    @abstractmethod
    async def register_download_result(self, result: DownloadResult) -> MediaDownload:
        '''Create a MediaDownload from a completed DownloadResult and register it.'''

    @abstractmethod
    async def register_download(self, media_download: MediaDownload) -> None:
        '''Associate a completed download with its request and move it to AVAILABLE.'''

    @abstractmethod
    async def checkout(self, media_request_uuid: str, guild_id: int,
                       guild_path: Path | None = None) -> Path | None:
        '''Mark an entry CHECKED_OUT; optionally stage the file and return its path.'''

    @abstractmethod
    async def remove(self, media_request_uuid: str) -> None:
        '''Remove an entry from the registry without touching any files.'''

    @abstractmethod
    async def release(self, media_request_uuid: str) -> None:
        '''Release a CHECKED_OUT entry and clean up the guild-specific file.'''

    @abstractmethod
    async def discard(self, media_request_uuid: str) -> None:
        '''Remove an entry that was registered but could not be enqueued.'''

    @abstractmethod
    async def prefetch(self, queue_items: list, guild_id: int,
                       guild_path: Path | None, limit: int) -> None:
        '''Pre-stage the next limit AVAILABLE items from the queue to local disk.'''

    @abstractmethod
    async def can_evict_request(self, media_request_uuid: str) -> bool:
        '''True if the guild-specific copy for this request is safe to delete.'''

    @abstractmethod
    async def can_evict_base(self, webpage_url: str) -> bool:
        '''True if the shared cached base file for this URL is safe to delete.'''

    @abstractmethod
    async def get_entry(self, media_request_uuid: str) -> BrokerEntry | None:
        '''Return the registry entry for a given UUID, or None if not present.'''

    @abstractmethod
    async def get_cache_count(self) -> int:
        '''Return the current VideoCache entry count, or 0 if no cache is configured.'''

    @abstractmethod
    async def get_checked_out_by(self, guild_id: int) -> List[BrokerEntry]:
        '''Return all entries currently checked out by the given guild.'''

    # ------------------------------------------------------------------
    # Bundle lifecycle (broker-owned MultiMediaRequestBundle replacement)
    #
    # The lifecycle methods below are concrete and route storage through the
    # abstract _load_bundle / _save_bundle / _drop_bundle hooks each impl
    # provides.  Keeping the orchestration here avoids duplicating the
    # "construct renderer, mutate, persist" pattern across AsyncioBroker
    # and RedisBroker.
    # ------------------------------------------------------------------

    def __init__(self, video_cache: VideoCacheClient | None = None,
                 bucket_name: str | None = None,
                 dispatcher: 'BundleDispatchSink | None' = None,
                 download_max_retries: int = 3,
                 search_max_retries: int = 3):
        '''Common construction for all broker impls.

        dispatcher: pushes bundle-UI updates to Discord.  None disables UI
        emission (helper methods short-circuit) so unit tests can run without
        one.

        download_max_retries / search_max_retries drive BundleRenderer's
        retry-summary "attempt N/M" rendering.
        '''
        self.video_cache = video_cache
        self.bucket_name = bucket_name
        self.dispatcher = dispatcher
        self.download_max_retries = download_max_retries
        self.search_max_retries = search_max_retries
        # Per-bundle locks for the in-memory default impl.  Without
        # serialisation, register_request and the download worker's
        # lifecycle pushes can both read the same snapshot, both modify
        # it, and the loser's update gets silently dropped — that's the
        # bug behind "playlist queue with 3 items only counts 2".  The
        # RedisBroker override switches to a redis-backed lock so the
        # serialisation works across multiple broker pods in HA.
        self._bundle_locks: dict[str, asyncio.Lock] = {}

    def _bundle_lock(self, bundle_uuid: str):
        '''Return an async context manager serializing mutations to bundle_uuid.

        Default implementation is a process-local asyncio.Lock — correct for
        in-memory brokers (AsyncioBroker) and for single-pod deployments of
        any other impl.  RedisBroker overrides this to use a redis-side
        SET NX EX lock so multiple broker pods stay in sync.'''
        lock = self._bundle_locks.get(bundle_uuid)
        if lock is None:
            lock = asyncio.Lock()
            self._bundle_locks[bundle_uuid] = lock
        return lock

    @abstractmethod
    async def _load_bundle(self, bundle_uuid: str) -> BundleState | None:
        '''Read the bundle state from the impl's storage, or None if absent.'''

    @abstractmethod
    async def _save_bundle(self, state: BundleState) -> None:
        '''Persist the bundle state in the impl's storage.'''

    @abstractmethod
    async def _drop_bundle(self, bundle_uuid: str) -> None:
        '''Delete the bundle from the impl's storage.'''

    @abstractmethod
    async def list_bundles_for_guild(self, guild_id: int) -> list[str]:
        '''Return uuids of every bundle currently stored for this guild.

        Point-in-time snapshot; bundles may be added or removed concurrently.
        Used by the cog on guild cleanup to know which bundles to tear down.
        '''

    async def _render_and_dispatch_bundle(self, bundle_uuid: str) -> None:
        '''Re-render a bundle and push its content to the dispatcher.

        Idempotent — safe to call repeatedly.  Always advances counters /
        stored_status based on the latest lifecycle stages; dispatch + summary
        emission are skipped when no dispatcher is wired.  No-op when the
        bundle is absent or shut down.

        The whole load-render-save flow runs under the per-bundle lock so it
        is serialised against concurrent register_request / lifecycle pushes
        — otherwise a stale snapshot can overwrite a freshly-added request.
        '''
        async with self._bundle_lock(bundle_uuid):
            await self._render_and_dispatch_bundle_locked(bundle_uuid)

    async def _render_and_dispatch_bundle_locked(self, bundle_uuid: str) -> None:
        '''Body of _render_and_dispatch_bundle, expected to be called with the
        per-bundle lock already held.  Helpers that already hold the lock
        (register_request etc.) call this directly to avoid re-entry.'''
        state = await self._load_bundle(bundle_uuid)
        if state is None or state.is_shutdown:
            return
        renderer = BundleRenderer(state)
        renderer.update_request_status()
        # Persist counter / stored_status updates regardless of dispatcher —
        # tests and HA-mode consumers may inspect state without a dispatcher
        # ever firing.
        await self._save_bundle(renderer.state)
        if self.dispatcher is None:
            return
        # Defer dispatches during the enqueue phase of multi-track bundles.
        # create_bundle has already dispatched the initial "Processing X"
        # banner; without this guard, every register_request + lifecycle push
        # in the loop would re-edit the Discord message, racing cache hits
        # and downloads to produce visual jitter.  finalize_bundle flips
        # all_requests_enqueued=True and is the next dispatch.
        if (state.has_search_banner and not state.all_requests_enqueued
                and state.bundled_requests):
            return
        content = renderer.print()
        if content:
            self.dispatcher.update_mutable(
                f'request_bundle-{bundle_uuid}',
                renderer.state.guild_id, content, renderer.state.channel_id,
                sticky=False,
            )
        # Best-effort failure / retry summaries — these are separate Discord
        # messages, not edits, so they go through send_message.
        failure = renderer.get_failure_summary()
        if failure:
            for msg in failure:
                self.dispatcher.send_message(
                    renderer.state.guild_id, renderer.state.channel_id, msg,
                )
        retries = renderer.get_retry_summary(
            self.download_max_retries, self.search_max_retries,
        )
        if retries:
            for msg in retries:
                self.dispatcher.send_message(
                    renderer.state.guild_id, renderer.state.channel_id, msg,
                )
        # Resave to preserve sent flags after summary emission.
        await self._save_bundle(renderer.state)
        # Single-track bundles render to '' once their request reaches a
        # terminal stage — there's no banner to keep visible.  Without this
        # the previous IN_PROGRESS render ("Downloading and processing…")
        # sits forever as a stale Discord message.  Tear it down here so the
        # cog doesn't have to chase per-completion cleanup.
        if not content and renderer.finished:
            self.dispatcher.remove_mutable(f'request_bundle-{bundle_uuid}')
            await self._drop_bundle(bundle_uuid)

    async def _maybe_render_bundle(self, media_request: MediaRequest | None) -> None:
        '''Render the request's bundle if it has one.'''
        if media_request is None or not media_request.bundle_uuid:
            return
        await self._render_and_dispatch_bundle(media_request.bundle_uuid)

    async def create_bundle(self, guild_id: int, channel_id: int,
                            input_string: str | None = None,
                            has_search_banner: bool = False) -> str:
        '''Create a new bundle and return its uuid.

        input_string drives the placeholder / banner row; has_search_banner
        flips the bundle into multi-track mode where the banner persists and
        carries progress counters.
        '''
        state = BundleState(
            guild_id=guild_id, channel_id=channel_id,
            input_string=input_string, has_search_banner=has_search_banner,
        )
        async with self._bundle_lock(state.uuid):
            # Constructing the renderer seeds row 0 (placeholder or banner);
            # we only persist the BundleState — the table is rebuilt on demand.
            BundleRenderer(state)
            await self._save_bundle(state)
            # Initial dispatch — user sees the "Processing X" banner immediately.
            # Subsequent register_request / lifecycle pushes during the enqueue
            # loop are suppressed by the defer guard so the message doesn't
            # churn before finalize_bundle.
            await self._render_and_dispatch_bundle_locked(state.uuid)
        return state.uuid

    async def finalize_bundle(self, bundle_uuid: str) -> None:
        '''Lock pagination on a bundle: mark all_requests_enqueued and trigger
        the first full render.  Called once after every register_request for
        the bundle has been issued.'''
        async with self._bundle_lock(bundle_uuid):
            state = await self._load_bundle(bundle_uuid)
            if state is None:
                logger.warning('finalize_bundle called for unknown bundle %s', bundle_uuid)
                return
            renderer = BundleRenderer(state)
            renderer.all_requests_added()
            await self._save_bundle(renderer.state)
            await self._render_and_dispatch_bundle_locked(bundle_uuid)

    async def delete_bundle(self, bundle_uuid: str) -> None:
        '''Tear down a bundle: tell the dispatcher to drop its Discord messages
        and wipe the broker's stored state.'''
        if self.dispatcher is not None:
            self.dispatcher.remove_mutable(f'request_bundle-{bundle_uuid}')
        async with self._bundle_lock(bundle_uuid):
            await self._drop_bundle(bundle_uuid)
