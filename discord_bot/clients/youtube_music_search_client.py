'''
Cog-facing YouTube-Music search client.

InMemoryYoutubeMusicSearchClient is a thin wrapper around a
YoutubeMusicSearchWorkerBase engine (AsyncioYoutubeMusicSearchWorker in
single-process) — it forwards the cog-facing YoutubeMusicSearchClient Protocol
surface to the worker, mirroring how InMemoryDownloadClient wraps a
DownloadWorkerBase.  A future HttpYoutubeMusicSearchClient will forward the same
surface to a remote search pod for HA.
'''
import asyncio
from typing import Callable

from discord_bot.interfaces.youtube_music_search_protocols import (
    YoutubeMusicSearchClient, YoutubeMusicSearchWorkerBase,
)
from discord_bot.types.media_request import MediaRequest

__all__ = [
    'YoutubeMusicSearchClient',
    'YoutubeMusicSearchWorkerBase',
    'InMemoryYoutubeMusicSearchClient',
]


class InMemoryYoutubeMusicSearchClient:
    '''
    Single-process YoutubeMusicSearchClient: a thin wrapper around a
    YoutubeMusicSearchWorkerBase.

    Forwards the cog-facing Protocol surface (submit / get_input_nowait / resolve
    / backoff_wait / block_guild / clear_guild_queue / queue_size + the
    failure_summary / backoff_seconds_remaining reads) straight to the worker,
    which owns the ytmusicapi call and input queue.  local_worker exposes the
    wrapped engine for single-process callers that need it (there is no
    equivalent on a future HttpYoutubeMusicSearchClient).
    '''
    def __init__(self, worker: YoutubeMusicSearchWorkerBase):
        self._worker = worker

    @property
    def local_worker(self) -> YoutubeMusicSearchWorkerBase:
        '''The wrapped engine — only meaningful in single-process mode.'''
        return self._worker

    async def submit(self, guild_id: int, media_request: MediaRequest,
                     priority: int | None = None) -> None:
        '''Enqueue a search request for resolution on the search loop.'''
        await self._worker.submit(guild_id, media_request, priority=priority)

    def get_input_nowait(self) -> MediaRequest:
        '''Pop the next pending search request, raising asyncio.QueueEmpty if none.'''
        return self._worker.get_input_nowait()

    async def resolve(self, media_request: MediaRequest) -> str | None:
        '''Resolve a request to a videoId (or None); re-raises on a 429.'''
        return await self._worker.resolve(media_request)

    async def backoff_wait(self, shutdown_event: asyncio.Event) -> None:
        '''Sleep out any active search backoff window.'''
        await self._worker.backoff_wait(shutdown_event)

    def set_wait_timestamp(self, backoff_multiplier: int = 1) -> None:
        '''Arm the search backoff window.'''
        self._worker.set_wait_timestamp(backoff_multiplier=backoff_multiplier)

    async def block_guild(self, guild_id: int) -> bool:
        '''Block new search submissions for a guild (used during cleanup).'''
        return await self._worker.block_guild(guild_id)

    async def clear_guild_queue(self, guild_id: int,
                                preserve_predicate: Callable[[MediaRequest], bool] | None = None,
                                ) -> list[MediaRequest]:
        '''Clear the search queue for a guild, returning the dropped requests.'''
        return await self._worker.clear_guild_queue(guild_id, preserve_predicate=preserve_predicate)

    async def queue_size(self, guild_id: int) -> int:
        '''Pending search-request count for a guild.'''
        return await self._worker.queue_size(guild_id)

    @property
    def failure_summary(self) -> str:
        '''Human-readable summary of the search failure queue.'''
        return self._worker.failure_summary

    @property
    def backoff_seconds_remaining(self) -> int | None:
        '''Seconds remaining in the current search backoff window, or None.'''
        return self._worker.backoff_seconds_remaining
