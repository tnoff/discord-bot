from asyncio import QueueEmpty
from datetime import datetime, timezone

from discord_bot.utils.queue import Queue

class DistributedQueue():
    '''
    Balance between queues in multiple servers/guilds
    '''
    def __init__(self, max_size: int, number_shuffles: int = 5, default_priority = 100):
        '''
        Distribute Traffic between multiple queues for different servers


        max_size : Max size of each individual queue
        number_shuffles : Number of shuffles for queues
        default_priority : Default priority of queues
        '''
        self.queues = {}
        self.max_size = max_size
        self.number_shuffles = number_shuffles
        self.default_priority = default_priority

    def block(self, guild_id: int):
        '''
        Block downloads for guild id
        '''
        try:
            self.queues[guild_id]['queue'].block()
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
            self.queues[guild_id] = {
                'created_at': datetime.now(timezone.utc),
                'last_iterated_at': None,
                'queue': Queue(maxsize=self.max_size, num_shuffles=self.number_shuffles),
                'priority': priority or self.default_priority,
            }
        self.queues[guild_id]['queue'].put_nowait(entry)
        return True

    def get_queue_size(self, guild_id: int):
        '''
        Check queue size for server
        '''
        try:
            return self.queues[guild_id]['queue'].size()
        except KeyError:
            return 0

    def get_nowait(self):
        '''
        Get download item from queue thats been waiting longest
        '''
        oldest_timestamp = None
        oldest_guild = None
        item = None
        current_priority = None
        for guild_id, data in self.queues.items():
            # If no queue data, continue
            if data['queue'].size() < 1:
                continue
            # Get timestamp to check against
            check_time = data['last_iterated_at'] or data['created_at']
            # Set default if we dont have anything yet
            # Check for oldest time
            # Also check priority isn't higher
            if (oldest_timestamp is None) or (check_time < oldest_timestamp and data['priority'] >= current_priority) or (data['priority'] > current_priority):
                oldest_timestamp = check_time
                oldest_guild = guild_id
                current_priority = data['priority']
                continue

        # Check if no available queues
        if oldest_timestamp is None:
            raise QueueEmpty('No items in queue')
        # Return values if present
        # Update timestamps
        item = self.queues[oldest_guild]['queue'].get_nowait()
        self.queues[oldest_guild]['last_iterated_at'] = datetime.now(timezone.utc)
        # Check if queue now empty and we can remove
        if self.queues[oldest_guild]['queue'].empty():
            self.queues.pop(oldest_guild)
        return item

    def clear_queue(self, guild_id: int):
        '''
        Clear queue for guild
        '''
        # Check if queue exists at all
        if guild_id not in self.queues:
            return []

        # Clear and return items
        items = []
        guild_info = self.queues.pop(guild_id, None)
        if not guild_info:
            return []
        while True:
            try:
                items.append(guild_info['queue'].get_nowait())
            except QueueEmpty:
                return items
