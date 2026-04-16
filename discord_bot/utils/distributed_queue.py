from asyncio import QueueEmpty
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Callable, Generic, TypeVar

from discord_bot.utils.queue import Queue

T = TypeVar('T')

@dataclass
class DistributedQueueItem(Generic[T]):
    '''
    Distributed Queue Item for Server
    '''
    created_at: datetime
    max_size: int
    priority: int

    def __post_init__(self):
        self.queue: Queue[T] = Queue(maxsize=self.max_size)
        self.iterated_at: datetime | None = None


class DistributedQueue(Generic[T]):
    '''
    Balance between queues in multiple servers/guilds
    '''
    def __init__(self, max_size: int, default_priority = 100):
        '''
        Distribute Traffic between multiple queues for different servers
        Keeps a queue per server, when item requested, returns FIFO of the last server processed

        When priority passed in, higher priority items will return first, regardless of timestamps
        If priority matches, then FIFO will win out

        max_size : Max size of each individual queue
        default_priority : Default priority of queues
        '''
        self.queues: dict[int, DistributedQueueItem[T]] = {}
        self.max_size = max_size
        self.default_priority = default_priority

    def block(self, guild_id: int):
        '''
        Block downloads for guild id
        '''
        try:
            self.queues[guild_id].queue.block()
            return True
        except KeyError:
            return False

    def put_nowait(self, guild_id: int, entry: T, priority: int | None = None):
        '''
        Put into the download queue for proper download queue

        guild_id : Guild ID for queue
        entry: Item to place into queue
        priority: Priority of queue item
        '''
        if guild_id not in self.queues:
            self.queues[guild_id] = DistributedQueueItem(datetime.now(timezone.utc),
                                                         self.max_size,
                                                         priority or self.default_priority)
        self.queues[guild_id].queue.put_nowait(entry)
        return True

    def size(self, guild_id: int):
        '''
        Check queue size for server
        '''
        try:
            return self.queues[guild_id].queue.size()
        except KeyError:
            return 0

    def total_size(self) -> int:
        '''Total number of items across all guild queues.'''
        return sum(item.queue.size() for item in self.queues.values())

    def _find_next_guild(self) -> tuple[int | None, datetime | None]:
        '''
        Return the (guild_id, comparison_timestamp) of the item get_nowait() would select,
        or (None, None) if all queues are empty.
        '''
        check_priority = None
        check_timestamp = None
        check_guild_id = None
        for guild_id, item in self.queues.items():
            if item.queue.size() < 1:
                continue
            comparison_value = item.iterated_at or item.created_at
            if check_priority is None:
                check_timestamp = comparison_value
                check_guild_id = guild_id
                check_priority = item.priority
                continue
            if check_priority > item.priority:
                continue
            if check_priority < item.priority or comparison_value < check_timestamp:
                check_timestamp = comparison_value
                check_guild_id = guild_id
                check_priority = item.priority
        return check_guild_id, check_timestamp

    def next_timestamp(self) -> datetime | None:
        '''
        Return the comparison timestamp of the item that get_nowait() would select,
        without dequeuing it. Returns None if the queue is empty.
        '''
        _, timestamp = self._find_next_guild()
        return timestamp

    def get_nowait(self) -> T:
        '''
        Get download item from server thats been waiting longest
        '''
        guild_id, _ = self._find_next_guild()
        if guild_id is None:
            raise QueueEmpty('No items in queue')
        self.queues[guild_id].iterated_at = datetime.now(timezone.utc)
        result = self.queues[guild_id].queue.get_nowait()
        # Clear queue if nothing present
        if self.queues[guild_id].queue.size() == 0:
            self.queues.pop(guild_id, None)
        return result

    def clear_queue(self, guild_id: int, preserve_predicate: Callable[[T], bool] | None = None) -> list[T]:
        '''
        Clear queue for guild, returning removed items.
        If preserve_predicate given, items for which it returns True are kept in the queue
        and excluded from the return value.
        '''
        guild_info = self.queues.get(guild_id)
        if not guild_info:
            return []
        if preserve_predicate is None:
            self.queues.pop(guild_id, None)
            items: list[T] = []
            while True:
                try:
                    items.append(guild_info.queue.get_nowait())
                except QueueEmpty:
                    return items
        dropped: list[T] = []
        kept: list[T] = []
        while True:
            try:
                item = guild_info.queue.get_nowait()
                if preserve_predicate(item):
                    kept.append(item)
                else:
                    dropped.append(item)
            except QueueEmpty:
                break
        if kept:
            for item in kept:
                guild_info.queue.put_nowait(item)
        else:
            self.queues.pop(guild_id, None)
        return dropped
