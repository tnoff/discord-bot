from asyncio import QueueEmpty
from dataclasses import dataclass
from datetime import datetime, timezone

from discord_bot.utils.queue import Queue

@dataclass
class DistributedQueueItem():
    '''
    Distributed Queue Item for Server
    '''
    created_at: datetime
    max_size: int
    priority: int

    def __post_init__(self):
        self.queue : Queue = Queue(maxsize=self.max_size)
        self.iterated_at : datetime = None


class DistributedQueue():
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
        self.queues = {}
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

    def put_nowait(self, guild_id: int, entry, priority: int = None):
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

    def get_nowait(self):
        '''
        Get download item from server thats been waiting longest
        '''
        check_priority = None
        check_timestamp = None
        check_guild_id = None
        for guild_id, item in self.queues.items():
            # If no queue data, continue
            if item.queue.size() < 1:
                continue
            comparison_value = item.iterated_at or item.created_at
            # If no item, priority higher, or timestamp before current, set result
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
                continue
        if not check_guild_id:
            raise QueueEmpty('No items in queue')

        self.queues[check_guild_id].iterated_at = datetime.now(timezone.utc)
        result = self.queues[check_guild_id].queue.get_nowait()
        # Clear queue if nothing present
        if self.queues[check_guild_id].queue.size() == 0:
            self.queues.pop(check_guild_id, None)
        return result

    def clear_queue(self, guild_id: int):
        '''
        Clear queue for guild
        '''
        # Clear and return items
        guild_info = self.queues.pop(guild_id, None)
        if not guild_info:
            return []
        items = []
        while True:
            try:
                items.append(guild_info.queue.get_nowait())
            except QueueEmpty:
                return items
