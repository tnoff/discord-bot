from dataclasses import dataclass
from enum import Enum
from pathlib import Path
from shutil import copyfile
from typing import List

from discord_bot.types.media_download import MediaDownload
from discord_bot.types.media_request import MediaRequest
from discord_bot.types.download import DownloadResult
from discord_bot.cogs.music_helpers.video_cache_client import VideoCacheClient
from discord_bot.utils.integrations.s3 import get_file, delete_file


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
    guild_file_path: Path | None = None


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

    When bucket_name is set (S3 mode), DownloadClient has already uploaded
    the file and set file_path to the S3 object key before register_download
    is called. MediaBroker uses bucket_name for checkout (get_file) and
    eviction (delete_file). Without bucket_name local-disk behaviour is used.
    '''

    def __init__(self, file_dir: Path | None = None, video_cache: VideoCacheClient | None = None,
                 bucket_name: str | None = None):
        self._registry: dict[str, BrokerEntry] = {}
        self.file_dir: Path | None = file_dir
        self.video_cache: VideoCacheClient | None = video_cache
        self.bucket_name: str | None = bucket_name

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
        media_download.file_size_bytes = result.file_size_bytes
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
        still available, or None if the cache is disabled or there is no hit.
        '''
        if not self.video_cache:
            return None
        return self.video_cache.get_webpage_url_item(media_request)

    def cache_cleanup(self) -> bool:
        '''
        Mark old cache entries for deletion and evict those that are no
        longer referenced by any active player.

        In S3 mode deletes the S3 object; in local-disk mode deletes the
        local file. In both cases the DB record is removed.

        Returns True if at least one file was removed, False otherwise.
        '''
        if not self.video_cache:
            return False
        self.video_cache.ready_remove()
        to_delete = [
            vc
            for vc in self.video_cache.get_deletable_entries()
            if self.can_evict_base(vc.video_url)
        ]
        if not to_delete:
            return False
        for vc in to_delete:
            delete_file(self.bucket_name, str(vc.base_path))
        self.video_cache.remove_video_cache([vc.id for vc in to_delete])
        return True

    # ------------------------------------------------------------------
    # Player lifecycle
    # ------------------------------------------------------------------

    def checkout(self, media_request_uuid: str, guild_id: int, guild_path: Path | None = None) -> Path | None:
        '''
        Mark an entry as CHECKED_OUT when a player dequeues it to play.

        If guild_path is given, stages the file into the guild-specific
        directory (copying from local disk or downloading from S3) and
        stores the resulting path in entry.guild_file_path.

        Returns the guild-specific file path, or None if no copy was made.
        '''
        entry = self._registry.get(media_request_uuid)
        if entry is None:
            return None
        if entry.zone == Zone.CHECKED_OUT and entry.guild_file_path and entry.guild_file_path.exists():
            return entry.guild_file_path
        if guild_path is not None and entry.download is not None and entry.download.file_path:
            guild_path.mkdir(exist_ok=True)
            uuid_path = guild_path / f'{entry.download.media_request.uuid}{"".join(i for i in entry.download.file_path.suffixes)}'
            if self.bucket_name:
                get_file(self.bucket_name, str(entry.download.file_path), uuid_path)
            else:
                if not entry.download.file_path.exists():
                    raise FileNotFoundError('Unable to locate base path')
                copyfile(str(entry.download.file_path), str(uuid_path))
            entry.guild_file_path = uuid_path
        entry.zone = Zone.CHECKED_OUT
        entry.checked_out_by = guild_id
        return entry.guild_file_path

    def remove(self, media_request_uuid: str):
        '''
        Remove an entry from the registry without touching any files.

        Use for AVAILABLE entries (not yet checked out) where the base
        file lifecycle is managed by the video cache or a later discard.
        '''
        self._registry.pop(media_request_uuid, None)

    def release(self, media_request_uuid: str):
        '''
        Release a CHECKED_OUT entry: delete the guild-specific copy and
        remove from the registry.

        Called when a player finishes with or abandons a file after checkout.
        '''
        entry = self._registry.pop(media_request_uuid, None)
        if entry and entry.guild_file_path:
            entry.guild_file_path.unlink(missing_ok=True)

    def discard(self, media_request_uuid: str):
        '''
        Remove an entry that was registered but could not be enqueued.

        If no video cache is configured the base file is also deleted from
        the appropriate store (S3 or local disk), since nothing else will
        manage its lifecycle. With a cache the file is kept as a valid
        cache entry.
        '''
        entry = self._registry.pop(media_request_uuid, None)
        if entry and entry.download and not self.video_cache:
            if entry.download.file_path:
                if self.bucket_name:
                    delete_file(self.bucket_name, str(entry.download.file_path))
                else:
                    entry.download.file_path.unlink(missing_ok=True)

    def prefetch(self, queue_items: list, guild_id: int, guild_path: 'Path | None', limit: int):
        '''
        Pre-stage the next `limit` AVAILABLE items from the queue to local disk.
        Already-staged (CHECKED_OUT) items count toward the limit.
        '''
        if not guild_path or not self.bucket_name:
            return  # local mode: no-op (copyfile is fast, no benefit)
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
                self.checkout(str(item.media_request.uuid), guild_id, guild_path)
                staged += 1

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
