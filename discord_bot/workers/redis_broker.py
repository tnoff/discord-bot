'''
Redis-backed media broker for HA multi-pod deployments.

Checkout returns the S3 object key rather than staging files locally —
file I/O is the caller's responsibility (HttpBrokerClient). S3 is required;
local-disk mode will not work across separate pods.
'''
import asyncio
import logging
from pathlib import Path
from typing import List

from discord_bot.interfaces.broker_protocols import BrokerEntry, CheckoutResult, MediaBrokerBase, Zone
from discord_bot.types.download import LifecycleEvent, DownloadResult, LifecycleStatusUpdate
from discord_bot.types.media_download import MediaDownload
from discord_bot.types.media_request import MediaRequest
from discord_bot.types.playlist_add_request import parse_media_request
from discord_bot.utils.integrations.s3 import delete_file
from discord_bot.workers.broker_registry import RedisBrokerRegistry
from discord_bot.workers.media_bundle import BundleRenderer, BundleState

logger = logging.getLogger(__name__)


def _download_to_dict(media_download: MediaDownload) -> dict:
    return {
        'file_path': str(media_download.file_path) if media_download.file_path else None,
        'webpage_url': media_download.webpage_url,
        'title': media_download.title,
        'id': media_download.id,
        'duration': media_download.duration,
        'uploader': media_download.uploader,
        'extractor': media_download.extractor,
        'file_size_bytes': media_download.file_size_bytes,
    }


def _download_from_dict(data: dict, media_request: MediaRequest) -> MediaDownload:
    ytdl_data = {
        'id': data.get('id'),
        'title': data.get('title'),
        'webpage_url': data.get('webpage_url'),
        'uploader': data.get('uploader'),
        'duration': data.get('duration'),
        'extractor': data.get('extractor'),
    }
    file_path = Path(data['file_path']) if data.get('file_path') else None
    md = MediaDownload(file_path, ytdl_data, media_request)
    md.file_size_bytes = data.get('file_size_bytes')
    return md


def _entry_from_dict(data: dict) -> BrokerEntry:
    '''Reconstruct a BrokerEntry from a Redis-stored dict.'''
    media_request = parse_media_request(data['request'])
    download = _download_from_dict(data['download'], media_request) if data.get('download') else None
    guild_file_path = Path(data['guild_file_path']) if data.get('guild_file_path') else None
    return BrokerEntry(
        request=media_request,
        download=download,
        zone=Zone(data['zone']),
        checked_out_by=data.get('checked_out_by'),
        guild_file_path=guild_file_path,
    )


class RedisBroker(MediaBrokerBase):
    '''
    Media broker backed by RedisBrokerRegistry for HA multi-pod deployments.

    All registry state lives in Redis; no local dict is maintained.
    '''

    def __init__(self, registry: RedisBrokerRegistry, **kwargs):
        '''Forward all base-class kwargs (video_cache, bucket_name, dispatcher,
        download_max_retries, search_max_retries) to MediaBrokerBase.'''
        super().__init__(**kwargs)
        self._registry = registry

    def _bundle_lock(self, bundle_uuid: str):
        '''Use the redis-backed bundle lock so multiple broker pods stay
        serialised on the same bundle.  The base-class default is a local
        asyncio.Lock — fine for AsyncioBroker (in-memory) but useless across
        broker pods in HA.'''
        return self._registry.bundle_lock(bundle_uuid)

    # ------------------------------------------------------------------
    # Registration
    # ------------------------------------------------------------------

    async def register_request(self, media_request: MediaRequest) -> None:
        uuid = str(media_request.uuid)
        if await self._registry.get_entry(uuid) is None:
            await self._registry.set_entry(uuid, {
                'zone': 'in_flight',
                'checked_out_by': None,
                'guild_file_path': None,
                'request': media_request.model_dump(mode='json'),
                'download': None,
            })
        # Attach to its bundle if one exists.  Roundtrip through BundleRenderer
        # to mutate counters / bundled_requests, then persist — under the
        # per-bundle lock so concurrent register_request / lifecycle pushes
        # don't clobber each other's snapshot.
        bundle_uuid = media_request.bundle_uuid
        if bundle_uuid:
            async with self._bundle_lock(bundle_uuid):
                raw = await self._registry.get_bundle(bundle_uuid)
                if raw is None:
                    return
                renderer = BundleRenderer(BundleState.model_validate(raw))
                renderer.add_media_request(media_request)
                await self._registry.set_bundle(
                    bundle_uuid, renderer.state.model_dump(mode='json')
                )
                await self._render_and_dispatch_bundle_locked(bundle_uuid)

    async def update_request_status(self, request_uuid: str, update: LifecycleStatusUpdate) -> None:
        data = await self._registry.get_entry(request_uuid)
        if data is None:
            logger.warning('update_request_status called for unknown uuid %s', request_uuid)
            return
        media_request = parse_media_request(data['request'])
        if update.event == LifecycleEvent.QUEUED:
            media_request.state_machine.mark_queued()
        elif update.event == LifecycleEvent.BACKOFF:
            media_request.state_machine.mark_backoff()
        elif update.event == LifecycleEvent.IN_PROGRESS:
            media_request.state_machine.mark_in_progress()
        elif update.event == LifecycleEvent.RETRY:
            media_request.state_machine.mark_retry_download(
                update.error_detail, update.backoff_seconds, update.retry_count
            )
        elif update.event == LifecycleEvent.RETRY_SEARCH:
            media_request.state_machine.mark_retry_search(
                update.error_detail, update.backoff_seconds, update.retry_count
            )
        elif update.event == LifecycleEvent.DISCARDED:
            media_request.state_machine.mark_discarded()
        elif update.event == LifecycleEvent.COMPLETED:
            media_request.state_machine.mark_completed()
        elif update.event == LifecycleEvent.FAILED:
            media_request.state_machine.mark_failed(update.failure_reason)
        data['request'] = media_request.model_dump(mode='json')
        # A DISCARDED/FAILED request is terminal — it will never yield a playable
        # download — so drop its registry entry instead of leaving it parked in
        # the in_flight zone until the 24h TTL. (The download worker emits
        # DISCARDED for every de-duplicated request, so these otherwise pile up.)
        # The bundle keeps its own synced copy below, so the UI still reflects
        # the final state.
        if update.event in (LifecycleEvent.DISCARDED, LifecycleEvent.FAILED):
            await self._registry.delete_entry(request_uuid)
        else:
            await self._registry.set_entry(request_uuid, data)
        # Sync the bundle's copy of this request so the renderer sees the new
        # lifecycle_stage / failure_reason.  In single-process the registry
        # entry and bundled_requests share a Python reference; in Redis they
        # are independent JSON blobs and would otherwise drift forever.
        # Both the sync and the render run under the per-bundle lock so they
        # don't clobber a concurrent register_request.
        bundle_uuid = media_request.bundle_uuid
        if bundle_uuid:
            async with self._bundle_lock(bundle_uuid):
                await self._sync_request_into_bundle(media_request)
                await self._render_and_dispatch_bundle_locked(bundle_uuid)

    async def _sync_request_into_bundle(self, media_request: MediaRequest) -> None:
        '''Replace the bundle's stored copy of this request with the latest one.

        Caller must already hold the per-bundle lock.  In single-process the
        bundle's bundled_requests share a Python reference with the registry
        entry, so a mutation is visible everywhere; in Redis the two are
        independent JSON blobs and would drift forever without this explicit
        write-back.
        '''
        bundle_uuid = media_request.bundle_uuid
        if not bundle_uuid:
            return
        raw = await self._registry.get_bundle(bundle_uuid)
        if raw is None:
            return
        state = BundleState.model_validate(raw)
        if not state.sync_request(media_request):
            return
        await self._registry.set_bundle(bundle_uuid, state.model_dump(mode='json'))

    async def register_download_result(self, result: DownloadResult) -> MediaDownload:
        media_download = MediaDownload(result.file_name, result.ytdlp_data, result.media_request)
        media_download.file_size_bytes = result.file_size_bytes
        await self.register_download(media_download)
        return media_download

    async def register_download(self, media_download: MediaDownload) -> None:
        key = str(media_download.media_request.uuid)
        data = await self._registry.get_entry(key)
        download_dict = _download_to_dict(media_download)
        if data is None:
            await self._registry.set_entry(key, {
                'zone': 'available',
                'checked_out_by': None,
                'guild_file_path': None,
                'request': media_download.media_request.model_dump(mode='json'),
                'download': download_dict,
            })
        else:
            data['download'] = download_dict
            data['zone'] = 'available'
            await self._registry.set_entry(key, data)
        if self.video_cache:
            await self.video_cache.iterate_file(media_download)
        # Render under the bundle lock so it doesn't race with concurrent
        # register_request / status pushes touching the same bundle.
        bundle_uuid = media_download.media_request.bundle_uuid
        if bundle_uuid:
            async with self._bundle_lock(bundle_uuid):
                await self._render_and_dispatch_bundle_locked(bundle_uuid)

    # ------------------------------------------------------------------
    # Player lifecycle
    # ------------------------------------------------------------------

    async def checkout(self, media_request_uuid: str, guild_id: int,
                       guild_path: Path | None = None) -> CheckoutResult | None:
        '''
        Atomically mark the entry CHECKED_OUT and return CheckoutResult(s3_key=...).

        guild_path is accepted for interface compatibility but ignored — file
        staging is the caller's responsibility (the bot downloads from S3 via the
        returned s3_key + bucket_name).
        '''
        succeeded = await self._registry.atomic_checkout(media_request_uuid, guild_id)
        if not succeeded:
            return None
        data = await self._registry.get_entry(media_request_uuid)
        if data is None or not data.get('download') or not data['download'].get('file_path'):
            return None
        return CheckoutResult(s3_key=data['download']['file_path'], bucket_name=self.bucket_name)

    async def remove(self, media_request_uuid: str) -> None:
        data = await self._registry.get_entry(media_request_uuid)
        await self._registry.delete_entry(media_request_uuid)
        if data is not None:
            await self._maybe_render_bundle(parse_media_request(data['request']))

    async def release(self, media_request_uuid: str) -> None:
        data = await self._registry.get_entry(media_request_uuid)
        await self._registry.delete_entry(media_request_uuid)
        if data is not None:
            await self._maybe_render_bundle(parse_media_request(data['request']))

    async def discard(self, media_request_uuid: str) -> None:
        data = await self._registry.get_entry(media_request_uuid)
        await self._registry.delete_entry(media_request_uuid)
        if data and data.get('download') and not self.video_cache:
            file_path = data['download'].get('file_path')
            if file_path and self.bucket_name:
                await asyncio.to_thread(delete_file, self.bucket_name, file_path)
        if data is not None:
            await self._maybe_render_bundle(parse_media_request(data['request']))

    async def prefetch(self, queue_items: list, guild_id: int,
                       guild_path: Path | None, limit: int) -> None:
        '''No-op — S3 prefetch staging is handled by the bot pod.'''

    # ------------------------------------------------------------------
    # Eviction queries
    # ------------------------------------------------------------------

    async def can_evict_request(self, media_request_uuid: str) -> bool:
        data = await self._registry.get_entry(media_request_uuid)
        if data is None:
            return True
        return data.get('zone') == 'available'

    async def can_evict_base(self, webpage_url: str) -> bool:
        for data in await self._registry.all_entries():
            if data.get('download') and data['download'].get('webpage_url') == webpage_url:
                if data.get('zone') in ('available', 'checked_out'):
                    return False
        return True

    # ------------------------------------------------------------------
    # Queries
    # ------------------------------------------------------------------

    async def get_entry(self, media_request_uuid: str) -> BrokerEntry | None:
        data = await self._registry.get_entry(media_request_uuid)
        if data is None:
            return None
        return _entry_from_dict(data)

    async def get_cache_count(self) -> int:
        if not self.video_cache:
            return 0
        return await self.video_cache.get_cache_count()

    async def get_checked_out_by(self, guild_id: int) -> List[BrokerEntry]:
        entries = await self._registry.all_entries()
        return [
            _entry_from_dict(data) for data in entries
            if data.get('checked_out_by') == guild_id and data.get('zone') == 'checked_out'
        ]

    # ------------------------------------------------------------------
    # Bundle storage (the lifecycle methods live on MediaBrokerBase)
    # ------------------------------------------------------------------

    async def _load_bundle(self, bundle_uuid: str) -> BundleState | None:
        raw = await self._registry.get_bundle(bundle_uuid)
        if raw is None:
            return None
        return BundleState.model_validate(raw)

    async def _save_bundle(self, state: BundleState) -> None:
        await self._registry.set_bundle(state.uuid, state.model_dump(mode='json'))

    async def _drop_bundle(self, bundle_uuid: str) -> None:
        await self._registry.delete_bundle(bundle_uuid)

    async def list_bundles_for_guild(self, guild_id: int) -> list[str]:
        bundles = await self._registry.all_bundles()
        return [b['uuid'] for b in bundles if b.get('guild_id') == guild_id]

    async def get_bundle_state(self, bundle_uuid: str) -> BundleState | None:
        '''Test/inspection helper — return the stored BundleState or None.'''
        return await self._load_bundle(bundle_uuid)
