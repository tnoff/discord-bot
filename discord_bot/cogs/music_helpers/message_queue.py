from asyncio import QueueEmpty
from datetime import datetime, timezone
from enum import Enum
from typing import Callable, List

from discord_bot.utils.queue import Queue

from discord_bot.cogs.music_helpers.message_context import MessageContext

class MessageLifecycleStage(Enum):
    '''
    Stages of a source message lifecycle
    '''
    SEND = 'send'
    EDIT = 'edit'
    DELETE = 'delete'

class MessageType(Enum):
    '''
    Types of messages queue returns
    '''
    MULTIPLE_MUTABLE = 'multiple_mutable'
    SINGLE_MUTABLE = 'single_mutable'
    SINGLE_IMMUTABLE = 'single_immutable'

class MessageItem():
    '''
    Message item class
    '''
    def __init__(self, message_context: MessageContext, lifecycle_stage: MessageLifecycleStage, function: Callable,
                 message_content: str, delete_after: int):
        self.message_context = message_context
        self.lifecycle_stage = lifecycle_stage
        self.function = function
        self.message_content = message_content
        self.delete_after = delete_after
        self.created_at = datetime.now(timezone.utc)
        self.last_iterated_at = datetime.now(timezone.utc)

    def update_item(self, message_content: str, delete_after: int, lifecycle_stage: MessageLifecycleStage = None):
        '''
        Update items content
        '''
        self.message_content = message_content
        self.delete_after = delete_after
        self.last_iterated_at = datetime.now(timezone.utc)
        if lifecycle_stage:
            self.lifecycle_stage = lifecycle_stage

class MessageQueue():
    '''
    Message queue to handle diff types of messages
    '''
    def __init__(self):
        self.single_mutable_queue = {}
        self.multiple_mutable_queue = {}
        self.single_immutable_queue = Queue()

    def get_next_message(self):
        '''
        Return type of message
        '''
        item = self.get_next_multiple_mutable()
        if item:
            return MessageType.MULTIPLE_MUTABLE, item
        item = self.get_next_single_mutable()
        if item:
            return MessageType.SINGLE_MUTABLE, item
        item = self.get_single_immutable()
        if item:
            return MessageType.SINGLE_IMMUTABLE, item
        return None, None

    def update_multiple_mutable(self, guild_id: int) -> bool:
        '''
        Iterate play order queue
        guild_id : Guild id to iterate
        '''
        if guild_id not in self.multiple_mutable_queue:
            self.multiple_mutable_queue[guild_id] = datetime.now(timezone.utc)
            return True
        return True

    def get_next_multiple_mutable(self) -> str:
        '''
        Return guild id with oldest value
        '''
        oldest_timestamp = None
        oldest_guild = None
        for guild_id, timestamp in self.multiple_mutable_queue.items():
            if oldest_guild is None:
                oldest_guild = guild_id
                oldest_timestamp = timestamp
                continue
            if timestamp < oldest_timestamp:
                oldest_guild = guild_id
                oldest_timestamp = timestamp
                continue
        if not oldest_guild:
            return None
        self.multiple_mutable_queue.pop(oldest_guild)
        return oldest_guild

    def send_single_immutable(self, function_list: List[Callable]) -> bool:
        '''
        Add message to single message queue
        '''
        if not function_list:
            return True
        self.single_immutable_queue.put_nowait(function_list)
        return True

    def update_single_mutable(self, message_context: MessageContext, lifecycle_stage: MessageLifecycleStage, function: Callable,
                                 message_content: str, delete_after: int = None) -> bool:
        '''
        Add source lifecycle to queue

        message_context : Message Context to use
        lifecycle_stage : Lifecycle state of call
        function : Function to call
        message_content: message content
        delete_after: Delete message after
        custom_uuid: Use custom uuid instead of source dicts
        '''
        if str(message_context.uuid) not in self.single_mutable_queue:
            self.single_mutable_queue[str(message_context.uuid)] = MessageItem(message_context, lifecycle_stage, function, message_content, delete_after)
            return True
        current_value = self.single_mutable_queue[str(message_context.uuid)]
        # If existing value is send and new value is edit, override the send with new content
        if current_value.lifecycle_stage == MessageLifecycleStage.SEND and lifecycle_stage != MessageLifecycleStage.DELETE:
            current_value.update_item(message_content, delete_after)
            return True
        # If sending existing value and deleting, just remove
        if current_value.lifecycle_stage == MessageLifecycleStage.SEND and lifecycle_stage == MessageLifecycleStage.DELETE:
            self.single_mutable_queue.pop(str(message_context.uuid))
            return True
        # If editing, update the edit
        if current_value.lifecycle_stage == MessageLifecycleStage.EDIT and lifecycle_stage != MessageLifecycleStage.DELETE:
            current_value.update_item(message_content, delete_after)
            return True
        if current_value.lifecycle_stage == MessageLifecycleStage.EDIT and lifecycle_stage == MessageLifecycleStage.DELETE:
            current_value.update_item('', 0, lifecycle_stage=MessageLifecycleStage.DELETE)
            current_value.function = function
            return True
        return False

    def get_next_single_mutable(self) -> Callable:
        '''
        Get latest from source lifecycle
        '''
        oldest_item = None
        oldest_timestamp = None
        for uuid, data in self.single_mutable_queue.items():
            if oldest_item is None:
                oldest_item = uuid
                oldest_timestamp = data.created_at
                continue
            timecheck = data.created_at
            if oldest_timestamp > timecheck:
                oldest_item = uuid
                oldest_timestamp = timecheck
                continue
        if not oldest_item:
            return None
        item = self.single_mutable_queue.pop(oldest_item)
        return item

    def get_single_immutable(self) -> List[Callable]:
        '''
        Get one off queue messages
        '''
        try:
            return self.single_immutable_queue.get_nowait()
        except QueueEmpty:
            return None
