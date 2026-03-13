from dataclasses import dataclass
from enum import Enum
from pathlib import Path
from typing import List

from discord_bot.types.media_download import MediaDownload
from discord_bot.types.media_request import MediaRequest
from discord_bot.types.download import DownloadResult
from discord_bot.cogs.music_helpers.video_cache_client import VideoCacheClient


class Zone(Enum):
    '''
    Lifecycle zone for a tracked media item
    '''
    IN_FLIGHT = 'in_flight'       # Request active, no file on disk yet
    AVAILABLE = 'available'       # File on disk, sitting in player queue
    CHECKED_OUT = 'checked_out'   # Player has dequeued it and is playing


@dataclass
class BrokerEntry:
    '''
    Single entry in the broker registry
    '''
    request: MediaRequest
    download: MediaDownload | None = None
    zone: Zone = Zone.IN_FLIGHT
    checked_out_by: int | None = None  # guild_id


class MediaBroker:
    '''
    Tracks all media in flight across the system.

    Maintains a registry keyed on MediaRequest.uuid that spans the full
    lifecycle from request creation through file eviction.  Two eviction
    queries are provided:

      can_evict_request  -- is the guild-specific copy safe to delete
      can_evict_base     -- is the shared cached base file safe to delete

    The base-file check is separate because multiple requests for the
    same URL share the same underlying base_path; the file must not be
    evicted while any of those requests are still AVAILABLE or CHECKED_OUT.
    '''

    def __init__(self, file_dir: Path | None = None, video_cache: VideoCacheClient | None = None):
        self._registry: dict[str, BrokerEntry] = {}
        self.file_dir: Path | None = file_dir
        self.video_cache: VideoCacheClient | None = video_cache

    # ------------------------------------------------------------------
    # Registration
    # ------------------------------------------------------------------

    def register_request(self, media_request: MediaRequest):
        '''
        Register a new MediaRequest entering the pipeline.
        Idempotent: a second call for the same UUID is ignored.
        '''
        key = str(media_request.uuid)
        if key not in self._registry:
            self._registry[key] = BrokerEntry(request=media_request)

    def register_download_result(self, result: DownloadResult) -> MediaDownload:
        '''
        Create a MediaDownload from a successful DownloadResult and register it.
        '''
        media_download = MediaDownload(result.file_name, result.ytdlp_data, result.media_request)
        self.register_download(media_download)
        return media_download

    def register_download(self, media_download: MediaDownload):
        '''
        Associate a completed MediaDownload with its request entry and
        move it to the AVAILABLE zone.

        If the entry does not yet exist (e.g. a cache hit that bypassed
        the normal pipeline) a new AVAILABLE entry is created.
        '''
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
            self.video_cache.iterate_file(media_download)

    def check_cache(self, media_request: MediaRequest) -> MediaDownload | None:
        '''
        Look up a completed download in the video cache.

        Returns a MediaDownload if the URL was previously downloaded and is
        still on disk, or None if the cache is disabled or there is no hit.
        '''
        if not self.video_cache:
            return None
        return self.video_cache.get_webpage_url_item(media_request)

    def cache_cleanup(self) -> bool:
        '''
        Mark old cache entries for deletion and evict those that are no
        longer referenced by any active player.

        Returns True if at least one file was removed, False otherwise.
        '''
        if not self.video_cache:
            return False
        self.video_cache.ready_remove()
        to_delete = [
            vc.id
            for vc in self.video_cache.get_deletable_entries()
            if self.can_evict_base(vc.video_url)
        ]
        if to_delete:
            self.video_cache.remove_video_cache(to_delete)
        if self.video_cache.object_storage_enabled:
            for vc in self.video_cache.get_entries_without_backup():
                self.video_cache.object_storage_backup(vc.id)
        return bool(to_delete)

    # ------------------------------------------------------------------
    # Player lifecycle
    # ------------------------------------------------------------------

    def checkout(self, media_request_uuid: str, guild_id: int, guild_path: Path | None = None):
        '''
        Mark an entry as CHECKED_OUT when a player dequeues it to play.

        If guild_path is given, the broker copies the base file into the
        guild-specific directory via ready_file() before marking CHECKED_OUT.
        This is the point at which the broker hands the guild copy to the player.
        '''
        entry = self._registry.get(media_request_uuid)
        if entry is None:
            return
        if guild_path is not None and entry.download is not None:
            entry.download.ready_file(guild_path)
        entry.zone = Zone.CHECKED_OUT
        entry.checked_out_by = guild_id

    def remove(self, media_request_uuid: str):
        '''
        Remove an entry from the registry.

        Called when a player finishes with a file (guild copy deleted)
        or when a request reaches a terminal stage with no file
        (FAILED, DISCARDED).
        '''
        self._registry.pop(media_request_uuid, None)

    # ------------------------------------------------------------------
    # Eviction queries
    # ------------------------------------------------------------------

    def can_evict_request(self, media_request_uuid: str) -> bool:
        '''
        True if the guild-specific copy for this request is safe to delete.

        Returns True if the entry is not present (already gone) or is in
        the AVAILABLE zone (player queue copy, not currently being played).
        Returns False if CHECKED_OUT (player is actively using it) or
        IN_FLIGHT (no file exists yet, nothing to evict).
        '''
        entry = self._registry.get(media_request_uuid)
        if entry is None:
            return True
        return entry.zone == Zone.AVAILABLE

    def can_evict_base(self, webpage_url: str) -> bool:
        '''
        True if the shared cached base file for this URL is safe to delete.

        Returns False if any entry referencing this URL is AVAILABLE or
        CHECKED_OUT, meaning at least one player still needs the base file.
        '''
        for entry in self._registry.values():
            if entry.download and entry.download.webpage_url == webpage_url:
                if entry.zone in (Zone.AVAILABLE, Zone.CHECKED_OUT):
                    return False
        return True

    # ------------------------------------------------------------------
    # Queries
    # ------------------------------------------------------------------

    def get_entry(self, media_request_uuid: str) -> 'BrokerEntry | None':
        '''
        Return the registry entry for a given UUID, or None if not present.
        Intended for inspection in tests and diagnostics.
        '''
        return self._registry.get(media_request_uuid)

    def __len__(self) -> int:
        return len(self._registry)

    def get_checked_out_by(self, guild_id: int) -> List[BrokerEntry]:
        '''
        Return all entries currently checked out by the given guild.
        Used for player restart / queue refresh.
        '''
        return [
            entry for entry in self._registry.values()
            if entry.checked_out_by == guild_id
        ]
