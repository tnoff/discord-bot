'''
In-memory (asyncio) media broker backed by a plain dict registry. A test double
for RedisBroker.
'''
import asyncio
import hashlib
import logging
from pathlib import Path
from shutil import copyfile
from typing import List

from opentelemetry.trace import SpanKind

from discord_core.types.download import LifecycleEvent, DownloadResult, LifecycleStatusUpdate
from discord_core.types.media_download import MediaDownload, media_download_attributes
from discord_core.types.media_request import MediaRequest
from discord_core.utils.otel import async_otel_span_wrapper, otel_span_wrapper

from discord_core.types.player_session import PlayerSession
from discord_core.utils.integrations.s3 import delete_file, get_file

from discord_broker.interfaces.broker_protocols import BrokerEntry, CheckoutResult, MediaBrokerBase, Zone
from discord_broker.workers.media_bundle import BundleRenderer, BundleState

logger = logging.getLogger(__name__)


def _copy_and_checksum(src: Path, dst: Path) -> tuple[str, str]:
    '''Copy src to dst and return (src_md5, dst_md5). Runs inside asyncio.to_thread.'''
    copyfile(str(src), str(dst))
    # bandit B324: local copyfile integrity check, not used for security
    return (
        hashlib.md5(src.read_bytes(), usedforsecurity=False).hexdigest(),
        hashlib.md5(dst.read_bytes(), usedforsecurity=False).hexdigest(),
    )


class AsyncioBroker(MediaBrokerBase):
    '''
    In-memory media broker backed by a plain dict registry.

    All state lives in memory. A test double for RedisBroker; production never
    builds it.
    '''

    def __init__(self, **kwargs):
        '''Forward all base-class kwargs (video_cache, bucket_name, dispatcher,
        download_max_retries, search_max_retries) to MediaBrokerBase.'''
        super().__init__(**kwargs)
        self._registry: dict[str, BrokerEntry] = {}
        # Files stage_checkout copied under a guild dir, by request uuid.
        self._staged: dict[str, Path] = {}
        self._bundles: dict[str, BundleState] = {}
        self._player_sessions: dict[int, PlayerSession] = {}

    # ------------------------------------------------------------------
    # Registration
    # ------------------------------------------------------------------

    async def register_request(self, media_request: MediaRequest) -> None:
        key = str(media_request.uuid)
        if key not in self._registry:
            self._registry[key] = BrokerEntry(request=media_request)
        # If the request belongs to a known bundle, attach it.  Loading the
        # renderer rebuilds the table from BundleState, mutates it via
        # add_media_request, and we keep the (mutated) state in the dict.
        bundle_uuid = media_request.bundle_uuid
        if bundle_uuid and bundle_uuid in self._bundles:
            renderer = BundleRenderer(self._bundles[bundle_uuid])
            renderer.add_media_request(media_request)
            self._bundles[bundle_uuid] = renderer.state
            await self._maybe_render_bundle(media_request)

    async def update_request_status(self, request_uuid: str, update: LifecycleStatusUpdate) -> None:
        entry = self._registry.get(request_uuid)
        if entry is None:
            logger.warning('update_request_status called for unknown uuid %s', request_uuid)
            return
        if update.event == LifecycleEvent.QUEUED:
            entry.request.state_machine.mark_queued()
        elif update.event == LifecycleEvent.BACKOFF:
            entry.request.state_machine.mark_backoff()
        elif update.event == LifecycleEvent.IN_PROGRESS:
            entry.request.state_machine.mark_in_progress()
        elif update.event == LifecycleEvent.RETRY:
            entry.request.state_machine.mark_retry_download(
                update.error_detail, update.backoff_seconds, update.retry_count,
                update.max_retries)
        elif update.event == LifecycleEvent.RETRY_SEARCH:
            entry.request.state_machine.mark_retry_search(
                update.error_detail, update.backoff_seconds, update.retry_count,
                update.max_retries)
        elif update.event == LifecycleEvent.DISCARDED:
            entry.request.state_machine.mark_discarded()
        elif update.event == LifecycleEvent.COMPLETED:
            entry.request.state_machine.mark_completed()
        elif update.event == LifecycleEvent.FAILED:
            entry.request.state_machine.mark_failed(update.failure_reason, rejected=update.rejected)
        # A DISCARDED/FAILED request is terminal — drop its registry entry so
        # de-duplicated / failed requests don't accumulate. We keep the local
        # `entry` reference, so the bundle sync/render below still works.
        if update.event in (LifecycleEvent.DISCARDED, LifecycleEvent.FAILED):
            self._registry.pop(request_uuid, None)
        # Re-point the bundle's stored copy of this request at the registry's
        # authoritative object before rendering.  We *usually* share a Python
        # reference with bundled_requests, but that alias breaks whenever the
        # registry entry was rebuilt from a deserialised request — e.g. the
        # register_download entry-absent branch fed a DownloadResult's
        # media_request.  When it breaks, the bundle keeps rendering the stale
        # snapshot (stuck "Downloading and processing…") and never reaches
        # finished, so the message is never cleared.  Syncing here makes the
        # render reflect the lifecycle stage we just applied, alias or not.
        bundle_uuid = entry.request.bundle_uuid
        if bundle_uuid:
            async with self._bundle_lock(bundle_uuid):
                await self._sync_request_into_bundle(entry.request)
                await self._render_and_dispatch_bundle_locked(bundle_uuid)
        else:
            await self._maybe_render_bundle(entry.request)

    async def _sync_request_into_bundle(self, media_request: MediaRequest) -> None:
        '''Re-attach the registry's authoritative request into its bundle.

        Caller must already hold the per-bundle lock.  Here the bundle's
        bundled_requests normally share a Python reference with the registry
        entry, but that alias is lost when the entry is rebuilt from a
        deserialised request; this write-back restores it so the renderer sees
        the latest lifecycle_stage / failure_reason.  No-op when the bundle or a
        matching request row is absent.  The caller only invokes this for a
        request that already has a bundle_uuid, so we don't re-check it here.
        '''
        state = self._bundles.get(media_request.bundle_uuid)
        if state is not None:
            state.sync_request(media_request)

    async def register_download_result(self, result: DownloadResult) -> MediaDownload:
        media_download = MediaDownload(result.file_name, result.ytdlp_data, result.media_request)
        media_download.file_size_bytes = result.file_size_bytes
        await self.register_download(media_download)
        return media_download

    async def register_download(self, media_download: MediaDownload) -> None:
        async with async_otel_span_wrapper('music.broker.register_download', kind=SpanKind.INTERNAL,
                               attributes=media_download_attributes(media_download)):
            key = str(media_download.media_request.uuid)
            entry = self._registry.get(key)
            if entry is None:
                self._registry[key] = BrokerEntry(
                    request=media_download.media_request,
                    download=media_download,
                    zone=Zone.AVAILABLE,
                )
            else:
                entry.download = media_download
                entry.zone = Zone.AVAILABLE
            if self.video_cache:
                await self.video_cache.iterate_file(media_download)
            await self._maybe_render_bundle(media_download.media_request)

    # ------------------------------------------------------------------
    # Player lifecycle
    # ------------------------------------------------------------------

    async def checkout(self, media_request_uuid: str, guild_id: int) -> CheckoutResult | None:
        '''Test double for the Redis broker's checkout: mark the entry CHECKED_OUT
        and answer with the download's path as the s3_key. Returns None for an
        unknown entry or one with no download. Use stage_checkout to copy the file.'''
        entry = self._registry.get(media_request_uuid)
        if entry is None or entry.download is None or not entry.download.file_path:
            return None
        entry.zone = Zone.CHECKED_OUT
        entry.checked_out_by = guild_id
        return CheckoutResult(s3_key=str(entry.download.file_path), bucket_name=self.bucket_name)

    async def stage_checkout(self, media_request_uuid: str, guild_id: int,
                             guild_path: Path | None = None) -> Path | None:
        '''Mark the entry CHECKED_OUT, copying the file under guild_path when given.
        Returns the staged path, or None for an unknown entry.'''
        entry = self._registry.get(media_request_uuid)
        if entry is None:
            return None
        staged = self._staged.get(media_request_uuid)
        if entry.zone == Zone.CHECKED_OUT and staged and staged.exists():
            return staged
        attributes = {
            'music.media_request.uuid': media_request_uuid,
            'music.guild_id': guild_id,
            'music.broker.s3_mode': bool(self.bucket_name),
        }
        if entry.download:
            attributes.update(media_download_attributes(entry.download))
        with otel_span_wrapper('music.broker.checkout', kind=SpanKind.INTERNAL, attributes=attributes):
            if guild_path is not None and entry.download is not None and entry.download.file_path:
                guild_path.mkdir(exist_ok=True)
                uuid_path = guild_path / f'{entry.download.media_request.uuid}{"".join(i for i in entry.download.file_path.suffixes)}'
                if self.bucket_name:
                    await asyncio.to_thread(
                        get_file, self.bucket_name, str(entry.download.file_path), uuid_path
                    )
                else:
                    if not entry.download.file_path.exists():
                        raise FileNotFoundError('Unable to locate base path')
                    src_md5, dst_md5 = await asyncio.to_thread(
                        _copy_and_checksum, entry.download.file_path, uuid_path
                    )
                    if src_md5 != dst_md5:
                        logger.warning('Checksum mismatch after copyfile: src=%s dst=%s src_md5=%s dst_md5=%s',
                                       entry.download.file_path, uuid_path, src_md5, dst_md5)
                self._staged[media_request_uuid] = uuid_path
            entry.zone = Zone.CHECKED_OUT
            entry.checked_out_by = guild_id
            return self._staged.get(media_request_uuid)

    async def remove(self, media_request_uuid: str) -> None:
        entry = self._registry.pop(media_request_uuid, None)
        if entry is not None:
            await self._maybe_render_bundle(entry.request)

    async def release(self, media_request_uuid: str) -> None:
        entry = self._registry.pop(media_request_uuid, None)
        staged = self._staged.pop(media_request_uuid, None)
        if staged:
            await asyncio.to_thread(staged.unlink, missing_ok=True)
        if entry is not None:
            await self._maybe_render_bundle(entry.request)

    async def discard(self, media_request_uuid: str) -> None:
        entry = self._registry.pop(media_request_uuid, None)
        if entry and entry.download and not self.video_cache:
            if entry.download.file_path:
                if self.bucket_name:
                    await asyncio.to_thread(delete_file, self.bucket_name, str(entry.download.file_path))
                else:
                    await asyncio.to_thread(entry.download.file_path.unlink, missing_ok=True)
        if entry is not None:
            await self._maybe_render_bundle(entry.request)

    # ------------------------------------------------------------------
    # Eviction queries
    # ------------------------------------------------------------------

    async def can_evict_request(self, media_request_uuid: str) -> bool:
        entry = self._registry.get(media_request_uuid)
        if entry is None:
            return True
        return entry.zone == Zone.AVAILABLE

    async def can_evict_base(self, webpage_url: str) -> bool:
        for entry in self._registry.values():
            if entry.download and entry.download.webpage_url == webpage_url:
                if entry.zone in (Zone.AVAILABLE, Zone.CHECKED_OUT):
                    return False
        return True

    # ------------------------------------------------------------------
    # Queries
    # ------------------------------------------------------------------

    async def get_entry(self, media_request_uuid: str) -> BrokerEntry | None:
        return self._registry.get(media_request_uuid)

    async def get_cache_count(self) -> int:
        if not self.video_cache:
            return 0
        return await self.video_cache.get_cache_count()

    def __len__(self) -> int:
        return len(self._registry)

    async def get_checked_out_by(self, guild_id: int) -> List[BrokerEntry]:
        return [
            entry for entry in self._registry.values()
            if entry.checked_out_by == guild_id
        ]

    # ------------------------------------------------------------------
    # Player sessions
    #
    # This fake stores sessions in memory, so they do not survive the restart
    # they exist for (RedisBroker's do).  That is fine for a test double: the
    # surface only has to behave, so code under test can call the same methods
    # it calls against the real broker.
    # ------------------------------------------------------------------

    async def save_player_session(self, session: PlayerSession) -> None:
        self._player_sessions[session.guild_id] = session

    async def list_player_sessions(self) -> List[PlayerSession]:
        return list(self._player_sessions.values())

    async def delete_player_session(self, guild_id: int) -> None:
        self._player_sessions.pop(guild_id, None)

    # ------------------------------------------------------------------
    # Bundle storage (the lifecycle methods live on MediaBrokerBase)
    # ------------------------------------------------------------------

    async def _load_bundle(self, bundle_uuid: str) -> BundleState | None:
        return self._bundles.get(bundle_uuid)

    async def _save_bundle(self, state: BundleState) -> None:
        self._bundles[state.uuid] = state

    async def _drop_bundle(self, bundle_uuid: str) -> None:
        self._bundles.pop(bundle_uuid, None)

    async def list_bundles_for_guild(self, guild_id: int) -> list[str]:
        return [
            uuid for uuid, state in self._bundles.items()
            if state.guild_id == guild_id
        ]

    def get_bundle_state(self, bundle_uuid: str) -> BundleState | None:
        '''Test/inspection helper — return the stored BundleState or None.'''
        return self._bundles.get(bundle_uuid)
