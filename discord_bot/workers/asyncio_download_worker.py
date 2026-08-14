'''
In-process download engine backed by DistributedQueues.

AsyncioDownloadWorker is the single-process DownloadWorkerBase impl: it owns the
per-guild input queues (regular + DIRECT) and the _direct_available event that
lets a DIRECT item interrupt an active backoff.  All the yt-dlp / backoff /
consumer-loop logic lives on DownloadWorkerBase; this class only supplies the
queue surface.  A future RedisDownloadWorker will supply the same surface backed
by Redis for HA.
'''
import asyncio
from asyncio import QueueEmpty
from datetime import datetime, timezone
from typing import Callable

from discord_bot.cogs.music_helpers.common import SearchType
from discord_bot.exceptions import ExitEarlyException
from discord_bot.interfaces.download_protocols import (
    DownloadWorkerBase, DirectItemAvailableException,
)
from discord_bot.types.media_request import MediaRequest
from discord_bot.utils.distributed_queue import DistributedQueue


class AsyncioDownloadWorker(DownloadWorkerBase):
    '''
    Single-process download engine backed by an in-process yt-dlp pipeline.

    Owns the input queues and runs the download worker loop (via the inherited
    run()) in the same process as the cog.  The regular and DIRECT queues are
    plain in-memory DistributedQueues; _direct_available wakes backoff_wait when
    a DIRECT item arrives so it can bypass an active backoff period.
    '''
    def __init__(self, *args, queue_max_size: int = 100, **kwargs):
        '''
        Forward all yt-dlp / backoff / broker kwargs to DownloadWorkerBase, then
        build the in-memory queues.

        queue_max_size : Per-guild capacity for the input queues
        '''
        super().__init__(*args, **kwargs)
        self._input_queue: DistributedQueue[MediaRequest] = DistributedQueue(queue_max_size)
        self._direct_input_queue: DistributedQueue[MediaRequest] = DistributedQueue(queue_max_size)
        self._direct_available: asyncio.Event = asyncio.Event()
        # Retries waiting out their hold-off, as (ready_at, guild_id, request).
        # In-process is the whole storage story here: this worker's input queues
        # are memory too, so a restart loses a deferred retry exactly as it loses
        # a queued one. The Redis worker, whose queue is durable, persists instead.
        self._deferred_retries: list[tuple[float, int, MediaRequest]] = []

    @property
    def has_direct_pending(self) -> bool:
        '''True when at least one DIRECT item is waiting to bypass backoff.'''
        return self._direct_available.is_set()

    async def backoff_wait(self, shutdown_event: asyncio.Event) -> None:
        '''
        Wait until the backoff timestamp elapses, the shutdown event fires, or a
        DIRECT item becomes available.

        Raises ExitEarlyException if shutdown is signalled.
        Raises DirectItemAvailableException if _direct_available fires during the wait.
        '''
        if self._wait_timestamp is None:
            return

        now = datetime.now(timezone.utc).timestamp()
        sleep_duration = max(0, self._wait_timestamp - now)

        if shutdown_event.is_set():
            raise ExitEarlyException('Exiting bot wait loop')

        if sleep_duration == 0:
            return

        _, pending = await asyncio.wait(
            {
                asyncio.ensure_future(shutdown_event.wait()),
                asyncio.ensure_future(self._direct_available.wait()),
            },
            timeout=sleep_duration,
            return_when=asyncio.FIRST_COMPLETED,
        )
        for task in pending:
            task.cancel()

        if shutdown_event.is_set():
            raise ExitEarlyException('Exiting bot wait loop')
        if self._direct_available.is_set():
            raise DirectItemAvailableException()

    # ------------------------------------------------------------------
    # Queue interface
    # ------------------------------------------------------------------

    async def _enqueue_request(self, guild_id: int, media_request: MediaRequest,
                               priority: int | None = None) -> None:
        '''Route a MediaRequest to the correct input queue based on its search type.'''
        if media_request.search_result.search_type == SearchType.DIRECT:
            self._direct_input_queue.put_nowait(guild_id, media_request, priority=priority)
            self._direct_available.set()
        else:
            self._input_queue.put_nowait(guild_id, media_request, priority=priority)

    async def _enqueue_deferred_request(self, guild_id: int, media_request: MediaRequest,
                                        ready_at: float) -> None:
        '''Park a retry until ready_at instead of queueing it now.'''
        self._deferred_retries.append((ready_at, guild_id, media_request))

    async def _promote_ready_retries(self) -> None:
        '''Queue every parked retry whose hold-off has elapsed.'''
        if not self._deferred_retries:
            return
        now = datetime.now(timezone.utc).timestamp()
        still_waiting = []
        for ready_at, guild_id, media_request in self._deferred_retries:
            if ready_at > now:
                still_waiting.append((ready_at, guild_id, media_request))
                continue
            await self._enqueue_request(guild_id, media_request)
        self._deferred_retries = still_waiting

    async def block_guild(self, guild_id: int) -> bool:
        '''Block new submissions for a guild (used during shutdown).'''
        a = self._input_queue.block(guild_id)
        b = self._direct_input_queue.block(guild_id)
        return a and b

    async def clear_guild_queue(self, guild_id: int,
                                preserve_predicate: Callable[[MediaRequest], bool] | None = None,
                                ) -> list[MediaRequest]:
        '''Clear the input queue for a guild, returning the dropped requests.'''
        dropped = self._input_queue.clear_queue(guild_id, preserve_predicate=preserve_predicate)
        dropped += self._direct_input_queue.clear_queue(guild_id, preserve_predicate=preserve_predicate)
        # Deferred retries are part of this guild's pending work — leaving them
        # parked would resurrect a cleared request minutes after the clear.
        dropped += self._drop_deferred_for_guild(guild_id, preserve_predicate)
        # If no DIRECT items remain across any guild, disarm the wakeup event so
        # backoff_wait does not spuriously raise DirectItemAvailableException.
        if self._direct_input_queue.total_size() == 0:
            self._direct_available.clear()
        return dropped

    def _drop_deferred_for_guild(self, guild_id: int,
                                 preserve_predicate: Callable[[MediaRequest], bool] | None,
                                 ) -> list[MediaRequest]:
        '''Remove and return this guild's deferred retries, honouring the predicate.'''
        dropped = []
        kept = []
        for entry in self._deferred_retries:
            _, entry_guild, media_request = entry
            if entry_guild == guild_id and not (preserve_predicate and preserve_predicate(media_request)):
                dropped.append(media_request)
                continue
            kept.append(entry)
        self._deferred_retries = kept
        return dropped

    async def queue_size(self, guild_id: int) -> int:
        '''Return the number of pending requests for a guild, or 0 if none.

        Deferred retries count: they are pending work the guild is still waiting
        on, and a caller that reads 0 concludes the guild has drained.
        '''
        deferred = sum(1 for _, entry_guild, _ in self._deferred_retries if entry_guild == guild_id)
        return (self._input_queue.size(guild_id) or 0) + (self._direct_input_queue.size(guild_id) or 0) + deferred

    async def _dequeue_direct(self) -> MediaRequest:
        '''Dequeue from the direct queue and clear the wakeup event if it is now empty.'''
        result = self._direct_input_queue.get_nowait()
        if self._direct_input_queue.total_size() == 0:
            self._direct_available.clear()
        return result

    async def _merged_get_nowait(self) -> MediaRequest:
        '''
        Dequeue the next item across both queues ordered by submission timestamp,
        raising QueueEmpty if both are empty.
        '''
        direct_ts = self._direct_input_queue.next_timestamp()
        regular_ts = self._input_queue.next_timestamp()
        if direct_ts is None and regular_ts is None:
            raise QueueEmpty('No items in queue')
        if direct_ts is not None and (regular_ts is None or direct_ts <= regular_ts):
            return await self._dequeue_direct()
        return self._input_queue.get_nowait()
