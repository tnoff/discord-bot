'''
Cog-facing YouTube-Music search client.

InMemoryYoutubeMusicSearchClient is a thin wrapper around a
YoutubeMusicSearchWorkerBase engine (AsyncioYoutubeMusicSearchWorker in
single-process) — it forwards the cog-facing YoutubeMusicSearchClient Protocol
surface to the worker, mirroring how InMemoryDownloadClient wraps a
DownloadWorkerBase.  HttpYoutubeMusicSearchClient forwards the submit/clear/block
half of that surface to a standalone search pod for HA, over the shared
HttpQueueWorkerClient base.
'''
import asyncio
from typing import ClassVar

from discord_bot.clients.http_queue_worker_client import HttpQueueWorkerClient
from discord_bot.clients.in_memory_queue_worker_client import InMemoryQueueWorkerClient
from discord_bot.interfaces.youtube_music_search_protocols import (
    YoutubeMusicSearchClient, YoutubeMusicSearchWorkerBase,
)
from discord_bot.types.media_request import MediaRequest

__all__ = [
    'YoutubeMusicSearchClient',
    'YoutubeMusicSearchWorkerBase',
    'InMemoryYoutubeMusicSearchClient',
    'HttpYoutubeMusicSearchClient',
]


class InMemoryYoutubeMusicSearchClient(InMemoryQueueWorkerClient):
    '''
    Single-process YoutubeMusicSearchClient: a thin wrapper around a
    YoutubeMusicSearchWorkerBase.

    The shared forwarding surface (submit / block_guild / clear_guild_queue /
    queue_size / failure_summary / backoff_seconds_remaining) comes
    from InMemoryQueueWorkerClient; the worker owns the ytmusicapi call and input
    queue.  What is specific to search is the pop/resolve half below — the loop
    that consumes it runs in the cog in single-process mode, and in the search pod
    under HA (which is why HttpYoutubeMusicSearchClient has no counterpart).
    '''

    @property
    def local_worker(self) -> YoutubeMusicSearchWorkerBase:
        '''The wrapped engine — only meaningful in single-process mode.'''
        return self._worker

    async def get_input_nowait(self) -> MediaRequest:
        '''Pop the next pending search request, raising asyncio.QueueEmpty if none.'''
        return await self._worker.get_input_nowait()

    async def resolve(self, media_request: MediaRequest) -> str | None:
        '''Resolve a request to a videoId (or None); re-raises on a 429.'''
        return await self._worker.resolve(media_request)

    async def backoff_wait(self, shutdown_event: asyncio.Event,
                           max_wait_seconds: float | None = None) -> None:
        '''Sleep out any active search backoff window, at most max_wait_seconds.'''
        await self._worker.backoff_wait(shutdown_event, max_wait_seconds=max_wait_seconds)

    def set_wait_timestamp(self, backoff_multiplier: int = 1) -> None:
        '''Arm the search backoff window.'''
        self._worker.set_wait_timestamp(backoff_multiplier=backoff_multiplier)


class HttpYoutubeMusicSearchClient(HttpQueueWorkerClient):
    '''
    YoutubeMusicSearchClient that forwards the cog-facing surface to a remote
    search pod.

    Producer calls (submit / block_guild / clear_guild_queue) POST to the pod's
    YoutubeMusicSearchHttpServer; the cached read surface (failure_summary /
    backoff_seconds_remaining / queue_size) is refreshed by the shared background
    poller from GET /search/ytmusic/status.  Everything comes from HttpQueueWorkerClient —
    only the route and span prefixes differ from the downloader's client.

    **Deliberately narrower than the Protocol**: no resolve / get_input_nowait /
    backoff_wait / set_wait_timestamp.  Those four belong to whoever drives the
    search loop, and that is the search pod: it owns the ytmusicapi client, the
    queue it pops from, and the shared 429 window.  The bot receives resolutions
    back through the broker's search-result queue, not through this client, and
    registers no search loop at all — the cog used to run one itself when no
    search url was configured, and that in-process shape is gone
    (projects/discord-bot-ha-only).  Calling one of the four here is a programming error,
    not a runtime fallback, so they are absent rather than raising stubs.
    '''

    ROUTE_PREFIX: ClassVar[str] = '/search/ytmusic'
    SPAN_PREFIX: ClassVar[str] = 'youtube_music_search'
