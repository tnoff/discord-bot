from dataclasses import dataclass
from enum import Enum
from typing import List

from discord_bot.cogs.music_helpers.media_download import MediaDownload
from discord_bot.cogs.music_helpers.media_request import MediaRequest


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

    def __init__(self):
        self._registry: dict[str, BrokerEntry] = {}

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
            return
        entry.download = media_download
        entry.zone = Zone.AVAILABLE

    # ------------------------------------------------------------------
    # Player lifecycle
    # ------------------------------------------------------------------

    def checkout(self, media_request_uuid: str, guild_id: int):
        '''
        Mark an entry as CHECKED_OUT when a player dequeues it to play.
        '''
        entry = self._registry.get(media_request_uuid)
        if entry is None:
            return
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
