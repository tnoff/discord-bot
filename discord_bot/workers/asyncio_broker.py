'''
In-process (asyncio) media broker backed by a plain dict registry.
'''
import asyncio
import hashlib
import logging
from pathlib import Path
from shutil import copyfile
from typing import List

from opentelemetry.trace import SpanKind

from discord_bot.cogs.music_helpers.video_cache_client import VideoCacheClient
from discord_bot.interfaces.broker_protocols import BrokerEntry, MediaBrokerBase, Zone
from discord_bot.types.download import DownloadEvent, DownloadResult, DownloadStatusUpdate
from discord_bot.types.media_download import MediaDownload, media_download_attributes
from discord_bot.types.media_request import MediaRequest
from discord_bot.utils.integrations.s3 import delete_file, get_file
from discord_bot.utils.otel import async_otel_span_wrapper, otel_span_wrapper

logger = logging.getLogger(__name__)


def _copy_and_checksum(src: Path, dst: Path) -> tuple[str, str]:
    '''Copy src to dst and return (src_md5, dst_md5). Runs inside asyncio.to_thread.'''
    copyfile(str(src), str(dst))
    return (
        hashlib.md5(src.read_bytes()).hexdigest(),
        hashlib.md5(dst.read_bytes()).hexdigest(),
    )


class AsyncioBroker(MediaBrokerBase):
    '''
    In-process media broker backed by a plain dict registry.

    All state lives in memory; suitable for single-process deployments.
    '''

    def __init__(self, video_cache: VideoCacheClient | None = None,
                 bucket_name: str | None = None):
        self._registry: dict[str, BrokerEntry] = {}
        self.video_cache: VideoCacheClient | None = video_cache
        self.bucket_name: str | None = bucket_name

    # ------------------------------------------------------------------
    # Registration
    # ------------------------------------------------------------------

    async def register_request(self, media_request: MediaRequest) -> None:
        key = str(media_request.uuid)
        if key not in self._registry:
            self._registry[key] = BrokerEntry(request=media_request)

    async def update_request_status(self, request_uuid: str, update: DownloadStatusUpdate) -> None:
        entry = self._registry.get(request_uuid)
        if entry is None:
            logger.warning('update_request_status called for unknown uuid %s', request_uuid)
            return
        if update.event == DownloadEvent.BACKOFF:
            entry.request.state_machine.mark_backoff()
        elif update.event == DownloadEvent.IN_PROGRESS:
            entry.request.state_machine.mark_in_progress()
        elif update.event == DownloadEvent.RETRY:
            entry.request.state_machine.mark_retry_download(update.error_detail, update.backoff_seconds)
        elif update.event == DownloadEvent.DISCARDED:
            entry.request.state_machine.mark_discarded()

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

    # ------------------------------------------------------------------
    # Player lifecycle
    # ------------------------------------------------------------------

    async def checkout(self, media_request_uuid: str, guild_id: int,
                       guild_path: Path | None = None) -> Path | None:
        entry = self._registry.get(media_request_uuid)
        if entry is None:
            return None
        if entry.zone == Zone.CHECKED_OUT and entry.guild_file_path and entry.guild_file_path.exists():
            return entry.guild_file_path
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
                entry.guild_file_path = uuid_path
            entry.zone = Zone.CHECKED_OUT
            entry.checked_out_by = guild_id
            return entry.guild_file_path

    async def remove(self, media_request_uuid: str) -> None:
        self._registry.pop(media_request_uuid, None)

    async def release(self, media_request_uuid: str) -> None:
        entry = self._registry.pop(media_request_uuid, None)
        if entry and entry.guild_file_path:
            await asyncio.to_thread(entry.guild_file_path.unlink, missing_ok=True)

    async def discard(self, media_request_uuid: str) -> None:
        entry = self._registry.pop(media_request_uuid, None)
        if entry and entry.download and not self.video_cache:
            if entry.download.file_path:
                if self.bucket_name:
                    await asyncio.to_thread(delete_file, self.bucket_name, str(entry.download.file_path))
                else:
                    await asyncio.to_thread(entry.download.file_path.unlink, missing_ok=True)

    async def prefetch(self, queue_items: list, guild_id: int,
                       guild_path: Path | None, limit: int) -> None:
        if not guild_path or not self.bucket_name:
            return
        with otel_span_wrapper('music.broker.prefetch', kind=SpanKind.INTERNAL,
                               attributes={'music.guild_id': guild_id, 'music.prefetch_limit': limit}):
            staged = 0
            for item in queue_items:
                if staged >= limit:
                    break
                entry = self._registry.get(str(item.media_request.uuid))
                if entry is None:
                    continue
                if entry.zone == Zone.CHECKED_OUT:
                    staged += 1
                elif entry.zone == Zone.AVAILABLE:
                    await self.checkout(str(item.media_request.uuid), guild_id, guild_path)
                    staged += 1

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
