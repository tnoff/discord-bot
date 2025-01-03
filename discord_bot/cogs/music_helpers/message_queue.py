from datetime import datetime, timezone
from enum import Enum
from typing import Callable

from discord_bot.utils.queue import Queue

from discord_bot.cogs.music_helpers.source_dict import SourceDict


class SourceLifecycleStages(Enum):
    SEND = 'send'
    EDIT = 'edit'
    DELETE = 'delete'

class MessageTypes(Enum):
    SOURCE_LIFECYCLE = 'source_lifecycle'

class MessageQueue():
    '''
    Message queue to handle diff types of messages
    '''
    def __init__(self):
        self.source_lifecycle_queue = {}
    
    def iterate_source_lifecycle(self, source_dict: SourceDict, func: Callable) -> bool:
        '''
        Add source lifecycle to queue

        source_dict : Original source dict
        func : Function to call
        '''
        self.source_lifecycle_queue.setdefault(source_dict.uuid, {
            'item': None,
            'created_at': datetime.now(timezone.utc),
            'last_iterated_at': datetime.now(timezone.utc),
        })
        if not self.source_lifecycle_queue[source_dict.uuid]['item']:
            self.source_lifecycle_queue[source_dict.uuid]['item'] = func
            return True
        current_func = func.func.__name__
        current_args = func.func.args
    
    def get_source_lifecycle(self) -> Callable:
        '''
        Get latest from source lifecycle
        '''
        oldest_item = None
        oldest_timestamp = None
        for uuid, data in self.source_lifecycle_queue.items():
            if oldest_item is None:
                oldest_item = data
                oldest_timestamp = data['last_iterated_at']
                continue
            timecheck = data['last_iterated_at']
            if oldest_timestamp < timecheck:
                oldest_item = data
                oldest_timestamp = timecheck
                continue
        return oldest_item['item']