'''
In-process YouTube-Music search engine backed by a DistributedQueue.

AsyncioYoutubeMusicSearchWorker is the single-process
YoutubeMusicSearchWorkerBase impl: it owns the per-guild input queue and runs in
the same process as the cog.  All the resolution / 429-backoff logic lives on the
base; this class only supplies the queue surface.  A future
RedisYoutubeMusicSearchWorker will supply the same surface backed by Redis for HA.
'''
from typing import Callable

from discord_bot.interfaces.youtube_music_search_protocols import YoutubeMusicSearchWorkerBase
from discord_bot.types.media_request import MediaRequest
from discord_bot.utils.distributed_queue import DistributedQueue


class AsyncioYoutubeMusicSearchWorker(YoutubeMusicSearchWorkerBase):
    '''
    Single-process search engine backed by an in-memory DistributedQueue.

    Search requests are lightweight, so a single per-guild queue (no DIRECT
    fast-path like the download worker) is enough; the cog sizes it larger than
    the play queue.
    '''
    def __init__(self, *args, queue_max_size: int = 100, **kwargs):
        '''
        Forward the resolution / backoff kwargs to YoutubeMusicSearchWorkerBase,
        then build the in-memory queue.

        queue_max_size : Per-guild capacity for the input queue.
        '''
        super().__init__(*args, **kwargs)
        self._input_queue: DistributedQueue[MediaRequest] = DistributedQueue(queue_max_size)

    async def _enqueue(self, guild_id: int, media_request: MediaRequest,
                       priority: int | None = None) -> None:
        '''Append a request to the guild's input queue.'''
        self._input_queue.put_nowait(guild_id, media_request, priority=priority)

    def get_input_nowait(self) -> MediaRequest:
        '''Pop the next pending request, raising asyncio.QueueEmpty if none.'''
        return self._input_queue.get_nowait()

    async def block_guild(self, guild_id: int) -> bool:
        '''Block new submissions for a guild (used during shutdown).'''
        return self._input_queue.block(guild_id)

    async def clear_guild_queue(self, guild_id: int,
                                preserve_predicate: Callable[[MediaRequest], bool] | None = None,
                                ) -> list[MediaRequest]:
        '''Clear the input queue for a guild, returning the dropped requests.'''
        return self._input_queue.clear_queue(guild_id, preserve_predicate=preserve_predicate)

    async def queue_size(self, guild_id: int) -> int:
        '''Return the number of pending requests for a guild, or 0 if none.'''
        return self._input_queue.size(guild_id) or 0
