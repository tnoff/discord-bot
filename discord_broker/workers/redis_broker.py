'''
Redis-backed media broker for HA multi-pod deployments.

Checkout returns the S3 object key rather than staging files locally —
file I/O is the caller's responsibility (HttpBrokerClient). S3 is required;
local-disk mode will not work across separate pods.
'''
import asyncio
import copy
import logging
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import List

from discord_core.types.download import LifecycleEvent, DownloadResult, LifecycleStatusUpdate
from discord_core.types.media_download import MediaDownload
from discord_core.types.media_request import MediaRequest
from discord_core.types.playlist_add_request import parse_media_request

from discord_core.types.player_session import PlayerSession
from discord_core.utils.integrations.s3 import ObjectStorageException, delete_file, get_file, upload_file

from discord_broker.interfaces.broker_protocols import (
    CACHE_KEY_PREFIX, QUEUE_KEY_PREFIX, BrokerEntry, CheckoutResult, MediaBrokerBase, Zone, bucket_for_key,
)
from discord_broker.workers.broker_registry import RedisBrokerRegistry
from discord_broker.workers.media_bundle import BundleRenderer, BundleState

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
        'cache_hit': media_download.cache_hit,
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
    md.cache_hit = bool(data.get('cache_hit', False))
    return md


def _entry_from_dict(data: dict) -> BrokerEntry:
    '''Reconstruct a BrokerEntry from a Redis-stored dict.'''
    media_request = parse_media_request(data['request'])
    download = _download_from_dict(data['download'], media_request) if data.get('download') else None
    return BrokerEntry(
        request=media_request,
        download=download,
        zone=Zone(data['zone']),
        checked_out_by=data.get('checked_out_by'),
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
                update.error_detail, update.backoff_seconds, update.retry_count,
                update.max_retries
            )
        elif update.event == LifecycleEvent.RETRY_SEARCH:
            media_request.state_machine.mark_retry_search(
                update.error_detail, update.backoff_seconds, update.retry_count,
                update.max_retries
            )
        elif update.event == LifecycleEvent.DISCARDED:
            media_request.state_machine.mark_discarded()
        elif update.event == LifecycleEvent.COMPLETED:
            media_request.state_machine.mark_completed()
        elif update.event == LifecycleEvent.FAILED:
            media_request.state_machine.mark_failed(update.failure_reason, rejected=update.rejected)
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
        # lifecycle_stage / failure_reason.  The registry entry and
        # bundled_requests are independent JSON blobs and would otherwise drift
        # forever.
        # Both the sync and the render run under the per-bundle lock so they
        # don't clobber a concurrent register_request.
        bundle_uuid = media_request.bundle_uuid
        if bundle_uuid:
            async with self._bundle_lock(bundle_uuid):
                await self._sync_request_into_bundle(media_request)
                await self._render_and_dispatch_bundle_locked(bundle_uuid)

    async def _sync_request_into_bundle(self, media_request: MediaRequest) -> None:
        '''Replace the bundle's stored copy of this request with the latest one.

        Caller must already hold the per-bundle lock.  The bundle's
        bundled_requests and the registry entry are independent JSON blobs and
        would drift forever without this explicit write-back.
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
                'request': media_download.media_request.model_dump(mode='json'),
                'download': download_dict,
            })
        else:
            data['download'] = download_dict
            data['zone'] = 'available'
            await self._registry.set_entry(key, data)
        # With split buckets a fresh download is not cached yet: it joins the catalog when it has
        # played (retire_download). A cache hit is already in the catalog, and this bumps its use.
        if self.video_cache and (not self.cache_bucket_name or media_download.cache_hit):
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

    async def checkout(self, media_request_uuid: str, guild_id: int) -> CheckoutResult | None:
        '''
        Atomically mark the entry CHECKED_OUT and return CheckoutResult(s3_key=...).

        File staging is the caller's responsibility (the bot downloads from S3 via
        the returned s3_key + bucket_name).
        '''
        succeeded = await self._registry.atomic_checkout(media_request_uuid, guild_id)
        if not succeeded:
            return None
        data = await self._registry.get_entry(media_request_uuid)
        if data is None or not data.get('download') or not data['download'].get('file_path'):
            return None
        key = data['download']['file_path']
        return CheckoutResult(s3_key=key, bucket_name=bucket_for_key(key, self.bucket_name, self.cache_bucket_name))

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
        if data and data.get('download') and (not self.video_cache or self.cache_bucket_name):
            file_path = data['download'].get('file_path')
            # With split buckets only the queue's own objects are garbage here: a cached object is
            # still the cache's, however this request ended.
            if file_path and self.bucket_name and (not self.cache_bucket_name or str(file_path).startswith(QUEUE_KEY_PREFIX)):
                await asyncio.to_thread(delete_file, self.bucket_name, file_path)
        if data is not None:
            await self._maybe_render_bundle(parse_media_request(data['request']))

    async def retire_download(self, download: MediaDownload | None, played: bool) -> None:
        '''
        Dispose of a song's object in the queue bucket once its queue entry is gone.

        A song that played to the end (played=True) is first copied into the cache bucket under
        cache/ and entered in the catalog, so the cache holds exactly the songs that have played.
        The queue object is then deleted, unless another entry still queues the same key.

        Does nothing without split buckets, or for an object that is not in the queue bucket
        (a cache hit, or a key from before the split). Failures are logged and never raised: this
        runs after the track is done, and the queue bucket's expiry removes what is left behind.

        download : The finished entry's download, read before the entry was released
        played : False for a skipped, cleared or closed track
        '''
        if download is None or not download.file_path or not self.cache_bucket_name:
            return
        key = str(download.file_path)
        if not key.startswith(QUEUE_KEY_PREFIX):
            return
        try:
            if played and self.video_cache and not download.cache_hit:
                await self._promote_to_cache(download, key)
            if not await self._key_in_use(key):
                await asyncio.to_thread(delete_file, self.bucket_name, key)
        except ObjectStorageException:
            logger.warning('Could not retire %s from the queue bucket', key, exc_info=True)

    async def _promote_to_cache(self, download: MediaDownload, queue_key: str) -> None:
        '''Copy the queue object into the cache bucket and catalog it under its new key.'''
        cache_key = CACHE_KEY_PREFIX + queue_key[len(QUEUE_KEY_PREFIX):]
        with TemporaryDirectory() as tmp_dir:
            local_path = Path(tmp_dir) / Path(queue_key).name
            await asyncio.to_thread(get_file, self.bucket_name, queue_key, local_path)
            await asyncio.to_thread(upload_file, self.cache_bucket_name, local_path, cache_key)
        promoted = copy.copy(download)
        promoted.file_path = Path(cache_key)
        try:
            await self.video_cache.iterate_file(promoted)
        except Exception:
            # Not cataloged, so the cache could never evict it: drop the object just uploaded.
            await asyncio.to_thread(delete_file, self.cache_bucket_name, cache_key)
            raise
        # The catalog keeps one row per video. A second copy of a video promoted while the first is
        # cataloged (different audio, so a different digest in the key) is not recorded, and nothing
        # would ever evict that object, so it goes.
        kept = await self.video_cache.get_webpage_url_item(download.media_request)
        if kept is not None and str(kept.file_path) != cache_key:
            await asyncio.to_thread(delete_file, self.cache_bucket_name, cache_key)

    async def _key_in_use(self, key: str) -> bool:
        '''True if a registry entry still points at this object key.'''
        for data in await self._registry.all_entries():
            if data.get('download') and data['download'].get('file_path') == key:
                return True
        return False

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

    async def get_entries(self, media_request_uuids: List[str]) -> List[BrokerEntry | None]:
        '''Several entries in one round trip, None where an entry is gone.'''
        return [_entry_from_dict(data) if data else None
                for data in await self._registry.get_entries(media_request_uuids)]

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
    # Player sessions
    # ------------------------------------------------------------------

    async def save_player_session(self, session: PlayerSession) -> None:
        await self._registry.set_session(session.guild_id, session.model_dump(mode='json'))

    async def list_player_sessions(self) -> List[PlayerSession]:
        return [PlayerSession.model_validate(data) for data in await self._registry.all_sessions()]

    async def delete_player_session(self, guild_id: int) -> None:
        await self._registry.delete_session(guild_id)

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
