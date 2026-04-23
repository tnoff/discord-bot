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

from discord_bot.cogs.music_helpers.video_cache_client import VideoCacheClient
from discord_bot.interfaces.broker_protocols import BrokerEntry, MediaBrokerBase, Zone
from discord_bot.types.download import DownloadEvent, DownloadResult, DownloadStatusUpdate
from discord_bot.types.media_download import MediaDownload
from discord_bot.types.media_request import MediaRequest
from discord_bot.utils.integrations.s3 import delete_file
from discord_bot.workers.broker_registry import RedisBrokerRegistry

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
    media_request = MediaRequest.model_validate(data['request'])
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

    def __init__(self, registry: RedisBrokerRegistry,
                 video_cache: VideoCacheClient | None = None,
                 bucket_name: str | None = None):
        self._registry = registry
        self.video_cache: VideoCacheClient | None = video_cache
        self.bucket_name: str | None = bucket_name

    # ------------------------------------------------------------------
    # Registration
    # ------------------------------------------------------------------

    async def register_request(self, media_request: MediaRequest) -> None:
        uuid = str(media_request.uuid)
        if await self._registry.get_entry(uuid) is not None:
            return
        await self._registry.set_entry(uuid, {
            'zone': 'in_flight',
            'checked_out_by': None,
            'guild_file_path': None,
            'request': media_request.model_dump(mode='json'),
            'download': None,
        })

    async def update_request_status(self, request_uuid: str, update: DownloadStatusUpdate) -> None:
        data = await self._registry.get_entry(request_uuid)
        if data is None:
            logger.warning('update_request_status called for unknown uuid %s', request_uuid)
            return
        media_request = MediaRequest.model_validate(data['request'])
        if update.event == DownloadEvent.BACKOFF:
            media_request.state_machine.mark_backoff()
        elif update.event == DownloadEvent.IN_PROGRESS:
            media_request.state_machine.mark_in_progress()
        elif update.event == DownloadEvent.RETRY:
            media_request.state_machine.mark_retry_download(
                update.error_detail, update.backoff_seconds
            )
        elif update.event == DownloadEvent.DISCARDED:
            media_request.state_machine.mark_discarded()
        data['request'] = media_request.model_dump(mode='json')
        await self._registry.set_entry(request_uuid, data)

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

    async def _get_evictable_entries(self) -> list:
        entries = await self._registry.all_entries()
        return [
            vc
            for vc in await self.video_cache.get_deletable_entries()
            if await self.can_evict_base(vc.video_url, _entries=entries)
        ]

    # ------------------------------------------------------------------
    # Player lifecycle
    # ------------------------------------------------------------------

    async def checkout(self, media_request_uuid: str, guild_id: int,
                       guild_path: Path | None = None) -> Path | None:
        '''
        Atomically mark the entry CHECKED_OUT and return the S3 object key as a Path.

        guild_path is accepted for interface compatibility but ignored — file
        staging is the caller's responsibility (HttpBrokerClient downloads from S3).
        '''
        succeeded = await self._registry.atomic_checkout(media_request_uuid, guild_id)
        if not succeeded:
            return None
        data = await self._registry.get_entry(media_request_uuid)
        if data is None or not data.get('download') or not data['download'].get('file_path'):
            return None
        return Path(data['download']['file_path'])

    async def remove(self, media_request_uuid: str) -> None:
        await self._registry.delete_entry(media_request_uuid)

    async def release(self, media_request_uuid: str) -> None:
        await self._registry.delete_entry(media_request_uuid)

    async def discard(self, media_request_uuid: str) -> None:
        data = await self._registry.get_entry(media_request_uuid)
        await self._registry.delete_entry(media_request_uuid)
        if data and data.get('download') and not self.video_cache:
            file_path = data['download'].get('file_path')
            if file_path and self.bucket_name:
                await asyncio.to_thread(delete_file, self.bucket_name, file_path)

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

    async def can_evict_base(self, webpage_url: str, _entries: list | None = None) -> bool:
        entries = _entries if _entries is not None else await self._registry.all_entries()
        for data in entries:
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
