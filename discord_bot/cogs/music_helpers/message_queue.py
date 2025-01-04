from asyncio import QueueEmpty
from datetime import datetime, timezone
from enum import Enum
from typing import Callable

from discord_bot.utils.queue import Queue

from discord_bot.cogs.music_helpers.source_dict import SourceDict


class SourceLifecycleStage(Enum):
    SEND = 'send'
    EDIT = 'edit'
    DELETE = 'delete'

class MessageTypes(Enum):
    SOURCE_LIFECYCLE = 'source_lifecycle'

class MessageItem():
    '''
    Message item class
    '''
    def __init__(self, lifecycle_state: SourceLifecycleStage, function: Callable,
                 message_content: str, delete_after: int):
        self.lifecycle_state = lifecycle_state
        self.function = function
        self.message_content = message_content
        self.delete_after= delete_after
        self.created_at = datetime.now(timezone.utc)
        self.last_iterated_at = datetime.now(timezone.utc)

    def update_item(self, message_content: str, delete_after: int, function: Callable = None):
        '''
        Update items content
        '''
        self.message_content = message_content
        self.delete_after = delete_after
        self.last_iterated_at = datetime.now(timezone.utc)
        if function:
            self.function = function

class MessageQueue():
    '''
    Message queue to handle diff types of messages
    '''
    def __init__(self):
        self.source_lifecycle_queue = {}
        self.single_message_queue = Queue()

    def iterate_single_message(self, function: Callable) -> bool:
        '''
        Add message to single message queue
        '''
        self.single_message_queue.put_nowait(function)
        return True

    def iterate_source_lifecycle(self, source_dict: SourceDict, lifecycle_state: SourceLifecycleStage, function: Callable,
                                 message_content: str, delete_after: int = None) -> bool:
        '''
        Add source lifecycle to queue

        source_dict : Original source dict
        lifecycle_state : Lifecycle state of call
        function : Function to call
        message_content: message content
        delete_after: Delete message after
        '''
        if str(source_dict.uuid) not in self.source_lifecycle_queue:
            self.source_lifecycle_queue[str(source_dict.uuid)] = MessageItem(lifecycle_state, function, message_content, delete_after)
            return True
        current_value = self.source_lifecycle_queue[str(source_dict.uuid)]
        # If existing value is send and new value is edit, override the send with new content
        if current_value.lifecycle_state == SourceLifecycleStage.SEND and lifecycle_state != SourceLifecycleStage.DELETE:
            current_value.update_item(message_content, delete_after)
            return True
        # If sending existing value and deleting, just remove
        if current_value.lifecycle_state == SourceLifecycleStage.SEND and lifecycle_state == SourceLifecycleStage.DELETE:
            self.source_lifecycle_queue.pop(str(source_dict.uuid))
            return True
        # If editing, update the edit
        if current_value.lifecycle_state == SourceLifecycleStage.EDIT and lifecycle_state != SourceLifecycleStage.DELETE:
            current_value.update_item(message_content, delete_after)
            return True
        if current_value.lifecycle_state == SourceLifecycleStage.EDIT and lifecycle_state == SourceLifecycleStage.DELETE:
            current_value.update_item('', 0, function=function)
            return True
        return False


    def get_source_lifecycle(self) -> Callable:
        '''
        Get latest from source lifecycle
        '''
        oldest_item = None
        oldest_timestamp = None
        for uuid, data in self.source_lifecycle_queue.items():
            if oldest_item is None:
                oldest_item = data
                oldest_timestamp = data.last_iterated_at
                continue
            timecheck = data.last_iterated_at
            if oldest_timestamp < timecheck:
                oldest_item = data
                oldest_timestamp = timecheck
                continue
        return oldest_item

    def get_single_message(self) -> Callable:
        '''
        Get one off queue messages
        '''
        try:
            return self.get_one_off_queue.get_nowait()
        except QueueEmpty:
            return None