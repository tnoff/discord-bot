'''
Single-process YoutubeMusicSearchClient: a wrapper around a search worker.

Retired from production by the HA rollout and kept only as a test double.
The pod-facing implementation is HttpYoutubeMusicSearchClient
(clients/youtube_music_search_client.py), which forwards the submit/clear/block
half of the surface to the search pod.
'''
import asyncio

from discord_core.types.media_request import MediaRequest

from discord_seam_media_search.interfaces.youtube_music_search_protocols import YoutubeMusicSearchWorkerBase

from tests.fakes.in_memory_queue_worker_client import InMemoryQueueWorkerClient


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
