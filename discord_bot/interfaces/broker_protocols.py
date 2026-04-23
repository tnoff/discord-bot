'''
Abstract base class for the media broker.

MediaBrokerBase defines the interface that both the in-process (asyncio)
and Redis-backed implementations must satisfy. Zone, BrokerEntry, and the
shared cache helpers also live here.
'''
from abc import ABC, abstractmethod
from dataclasses import dataclass
from enum import Enum
from pathlib import Path
from typing import List

from opentelemetry.trace import SpanKind

from discord_bot.cogs.music_helpers.video_cache_client import VideoCacheClient
from discord_bot.types.download import DownloadResult, DownloadStatusUpdate
from discord_bot.types.media_download import MediaDownload
from discord_bot.types.media_request import MediaRequest
from discord_bot.utils.integrations.s3 import delete_file
from discord_bot.utils.otel import async_otel_span_wrapper


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
    async def update_request_status(self, request_uuid: str, update: DownloadStatusUpdate) -> None:
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
