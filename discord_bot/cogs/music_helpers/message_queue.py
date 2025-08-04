from asyncio import QueueEmpty
from datetime import datetime, timezone
from enum import Enum
from typing import Callable, List

from discord_bot.utils.queue import Queue

from discord_bot.cogs.music_helpers.source_dict import SourceDict
from discord_bot.cogs.music_helpers.batched_message import BatchedMessageItem
from discord_bot.cogs.music_helpers.message_formatter import MessageStatus


class SourceLifecycleStage(Enum):
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
    BATCHED_MESSAGE = 'batched_message'

class MessageItem():
    '''
    Message item class
    '''
    def __init__(self, source_dict: SourceDict, lifecycle_stage: SourceLifecycleStage, function: Callable,
                 message_content: str, delete_after: int):
        self.source_dict = source_dict
        self.lifecycle_stage = lifecycle_stage
        self.function = function
        self.message_content = message_content
        self.delete_after= delete_after
        self.created_at = datetime.now(timezone.utc)
        self.last_iterated_at = datetime.now(timezone.utc)

    def update_item(self, message_content: str, delete_after: int, lifecycle_stage: SourceLifecycleStage = None, function: Callable = None):
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
    def __init__(self, batch_size: int = 15, batch_timeout: int = 30, delete_after: int = 300):
        self.source_lifecycle_queue = {}
        self.play_order_queue = {}
        self.single_message_queue = Queue()

        # Batching system
        self.pending_batches: dict[int, BatchedMessageItem] = {}  # guild_id -> batch
        self.active_batches: dict[str, BatchedMessageItem] = {}  # batch_id -> batch
        self.batch_updates_queue = Queue()  # Queue of batches needing updates
        self.batch_size = batch_size
        self.batch_timeout = batch_timeout
        self.delete_after = delete_after

    def get_next_message(self):
        '''
        Return type of message
        '''
        item = self.get_play_order()
        if item:
            return MessageType.PLAY_ORDER, item
        item = self.get_batch_update()
        if item:
            return MessageType.BATCHED_MESSAGE, item
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

    def iterate_source_lifecycle(self, source_dict: SourceDict, lifecycle_stage: SourceLifecycleStage, function: Callable,
                                 message_content: str, delete_after: int = None) -> bool:
        '''
        Add source lifecycle to queue

        source_dict : Original source dict
        lifecycle_stage : Lifecycle state of call
        function : Function to call
        message_content: message content
        delete_after: Delete message after
        custom_uuid: Use custom uuid instead of source dicts
        '''
        if str(source_dict.uuid) not in self.source_lifecycle_queue:
            self.source_lifecycle_queue[str(source_dict.uuid)] = MessageItem(source_dict, lifecycle_stage, function, message_content, delete_after)
            return True
        current_value = self.source_lifecycle_queue[str(source_dict.uuid)]
        # If existing value is send and new value is edit, override the send with new content
        if current_value.lifecycle_stage == SourceLifecycleStage.SEND and lifecycle_stage != SourceLifecycleStage.DELETE:
            current_value.update_item(message_content, delete_after)
            return True
        # If sending existing value and deleting, just remove
        if current_value.lifecycle_stage == SourceLifecycleStage.SEND and lifecycle_stage == SourceLifecycleStage.DELETE:
            self.source_lifecycle_queue.pop(str(source_dict.uuid))
            return True
        # If editing, update the edit
        if current_value.lifecycle_stage == SourceLifecycleStage.EDIT and lifecycle_stage != SourceLifecycleStage.DELETE:
            current_value.update_item(message_content, delete_after)
            return True
        if current_value.lifecycle_stage == SourceLifecycleStage.EDIT and lifecycle_stage == SourceLifecycleStage.DELETE:
            current_value.update_item('', 0, function=function, lifecycle_stage=SourceLifecycleStage.DELETE)
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

    # Batching Methods

    def should_batch_items(self, guild_id: int, num_items: int) -> bool:
        '''
        Determine if items should be batched based on count and timing
        '''
        # Always batch if we have 2 or more items
        if num_items >= 2:
            return True

        # Check if we have a pending batch that's been waiting
        if guild_id in self.pending_batches:
            pending_batch = self.pending_batches[guild_id]
            time_waiting = datetime.now(timezone.utc) - pending_batch.created_at
            if time_waiting.total_seconds() >= self.batch_timeout:
                return True

        return False

    def add_items_to_batch(self, guild_id: int, source_dicts: List[SourceDict], send_function: Callable, channel_id: int = None) -> str:
        '''
        Add multiple SourceDicts to a batch, create new batch if needed
        Returns batch_id of the first finalized batch
        '''
        # Get or create batch for this guild
        if guild_id not in self.pending_batches:
            self.pending_batches[guild_id] = BatchedMessageItem(guild_id, self.batch_size, self.delete_after, channel_id)

        batch = self.pending_batches[guild_id]
        first_finalized_batch_id = None

        # Add items to batch
        for source_dict in source_dicts:
            if not batch.add_source_dict(source_dict):
                # Batch is full, finalize it
                self._finalize_batch(batch, send_function)
                if first_finalized_batch_id is None:
                    first_finalized_batch_id = batch.batch_id
                # Create a new batch and add the current item
                batch = BatchedMessageItem(guild_id, self.batch_size, self.delete_after, channel_id)
                self.pending_batches[guild_id] = batch
                batch.add_source_dict(source_dict)

        # If current batch should be finalized (either full or meets batching criteria), finalize it
        if batch.is_batch_full() or self.should_batch_items(guild_id, len(batch.source_dicts)):
            self._finalize_batch(batch, send_function)
            self.pending_batches.pop(guild_id, None)
            if first_finalized_batch_id is None:
                first_finalized_batch_id = batch.batch_id

        # Return the first finalized batch ID, or current batch ID if none finalized
        return first_finalized_batch_id or batch.batch_id

    def _finalize_batch(self, batch: BatchedMessageItem, send_function: Callable):
        '''
        Move batch from pending to active and queue initial send
        '''
        self.active_batches[batch.batch_id] = batch

        # Create initial message send function
        def send_batch_message():
            content = batch.generate_message_content()
            return send_function(content)

        # Add to updates queue for initial send
        batch.send_function = send_batch_message
        batch.lifecycle_stage = SourceLifecycleStage.SEND
        self.batch_updates_queue.put_nowait(batch)

    def update_batch_item(self, batch_id: str, source_uuid: str, status: MessageStatus, error_msg: str = None) -> bool:
        '''
        Update status of individual item in batch
        '''
        if batch_id not in self.active_batches:
            return False

        batch = self.active_batches[batch_id]
        if batch.update_item_status(source_uuid, status, error_msg):
            # Queue batch for message update
            batch.lifecycle_stage = SourceLifecycleStage.EDIT
            self.batch_updates_queue.put_nowait(batch)
            return True

        return False

    def get_batch_update(self) -> BatchedMessageItem:
        '''
        Get next batch that needs message update
        '''
        try:
            return self.batch_updates_queue.get_nowait()
        except QueueEmpty:
            return None

    def cleanup_completed_batch(self, batch_id: str):
        '''
        Remove completed batch from active batches
        '''
        if batch_id in self.active_batches:
            batch = self.active_batches[batch_id]
            if batch.is_processing_complete():
                self.active_batches.pop(batch_id)
                return True
        return False
