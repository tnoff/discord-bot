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
    PLAY_ORDER = 'play_order'
    SOURCE_LIFECYCLE = 'source_lifecycle'
    SINGLE_MESSAGE = 'single_message'

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

    def update_item(self, message_content: str, delete_after: int, lifecycle_stage: MessageLifecycleStage = None, function: Callable = None):
        '''
        Update items content
        '''
        self.message_content = message_content
        self.delete_after = delete_after
        self.last_iterated_at = datetime.now(timezone.utc)
        if function:
            self.function = function
        if lifecycle_stage:
            self.lifecycle_stage = lifecycle_stage

class MessageQueue():
    '''
    Message queue to handle diff types of messages
    '''
    def __init__(self):
        self.source_lifecycle_queue = {}
        self.play_order_queue = {}
        self.single_message_queue = Queue()

    def get_next_message(self):
        '''
        Return type of message
        '''
        item = self.get_play_order()
        if item:
            return MessageType.PLAY_ORDER, item
        item = self.get_source_lifecycle()
        if item:
            return MessageType.SOURCE_LIFECYCLE, item
        item = self.get_single_message()
        if item:
            return MessageType.SINGLE_MESSAGE, item
        return None, None

    def iterate_play_order(self, guild_id: int) -> bool:
        '''
        Iterate play order queue
        guild_id : Guild id to iterate
        '''
        if guild_id not in self.play_order_queue:
            self.play_order_queue[guild_id] = datetime.now(timezone.utc)
            return True
        return True

    def get_play_order(self) -> str:
        '''
        Return guild id with oldest value
        '''
        oldest_timestamp = None
        oldest_guild = None
        for guild_id, timestamp in self.play_order_queue.items():
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
        self.play_order_queue.pop(oldest_guild)
        return oldest_guild

    def iterate_single_message(self, function_list: List[Callable]) -> bool:
        '''
        Add message to single message queue
        '''
        if not function_list:
            return True
        self.single_message_queue.put_nowait(function_list)
        return True

    def iterate_source_lifecycle(self, message_context: MessageContext, lifecycle_stage: MessageLifecycleStage, function: Callable,
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
        if str(message_context.uuid) not in self.source_lifecycle_queue:
            self.source_lifecycle_queue[str(message_context.uuid)] = MessageItem(message_context, lifecycle_stage, function, message_content, delete_after)
            return True
        current_value = self.source_lifecycle_queue[str(message_context.uuid)]
        # If existing value is send and new value is edit, override the send with new content
        if current_value.lifecycle_stage == MessageLifecycleStage.SEND and lifecycle_stage != MessageLifecycleStage.DELETE:
            current_value.update_item(message_content, delete_after)
            return True
        # If sending existing value and deleting, just remove
        if current_value.lifecycle_stage == MessageLifecycleStage.SEND and lifecycle_stage == MessageLifecycleStage.DELETE:
            self.source_lifecycle_queue.pop(str(message_context.uuid))
            return True
        # If editing, update the edit
        if current_value.lifecycle_stage == MessageLifecycleStage.EDIT and lifecycle_stage != MessageLifecycleStage.DELETE:
            current_value.update_item(message_content, delete_after)
            return True
        if current_value.lifecycle_stage == MessageLifecycleStage.EDIT and lifecycle_stage == MessageLifecycleStage.DELETE:
            current_value.update_item('', 0, function=function, lifecycle_stage=MessageLifecycleStage.DELETE)
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
        item = self.source_lifecycle_queue.pop(oldest_item)
        return item

    def get_single_message(self) -> List[Callable]:
        '''
        Get one off queue messages
        '''
        try:
            return self.single_message_queue.get_nowait()
        except QueueEmpty:
            return None
