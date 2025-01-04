from asyncio import QueueEmpty
from datetime import datetime, timezone

from discord_bot.utils.queue import Queue

class DistributedQueue():
    '''
    Balance between queues in multiple servers/guilds
    '''
    def __init__(self, max_size: int):
        '''
        Distribute Traffic between multiple queues for different servers


        max_size : Max size of each individual queue
        '''
        self.queues = {}
        self.max_size = max_size

    def block(self, guild_id: str):
        '''
        Block downloads for guild id
        '''
        try:
            self.queues[guild_id]['queue'].block()
            return True
        except KeyError:
            return False

    def put_nowait(self, guild_id: str, entry):
        '''
        Put into the download queue for proper download queue

        guild_id : Guild ID for queue
        entry: Item to place into queue
        '''
        if guild_id not in self.queues:
            self.queues[guild_id] = {
                'created_at': datetime.now(timezone.utc),
                'last_iterated_at': None,
                'queue': Queue(maxsize=self.max_size),
            }
        self.queues[guild_id]['queue'].put_nowait(entry)
        return True

    def get_nowait(self):
        '''
        Get download item from queue thats been waiting longest
        '''
        oldest_timestamp = None
        oldest_guild = None
        item = None
        remove_guilds = []
        for guild_id, data in self.queues.items():
            # If no queue data, continue
            if data['queue'].size() < 1:
                remove_guilds.append(guild_id)
                continue
            # Get timestamp to check against
            check_time = data['last_iterated_at'] or data['created_at']
            # Set default if we dont have anything yet
            # Check for oldest time
            if oldest_timestamp is None or check_time < oldest_timestamp:
                oldest_timestamp = check_time
                oldest_guild = guild_id
                continue
        # Remove guild items with no data
        for guild_id in remove_guilds:
            # Double check its empty again
            if self.queues[guild_id]['queue'].empty():
                self.queues.pop(guild_id)

        # Check if no available queues
        if oldest_timestamp is None:
            raise QueueEmpty('No items in queue')
        # Return values if present
        # Update timestamps
        item = self.queues[oldest_guild]['queue'].get_nowait()
        self.queues[oldest_guild]['last_iterated_at'] = datetime.now(timezone.utc)
        return item

    def clear_queue(self, guild_id: str):
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
