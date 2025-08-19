from asyncio import QueueEmpty
from datetime import datetime, timezone
from functools import partial
from typing import Callable, List

from discord import TextChannel

from discord_bot.utils.queue import Queue
from discord_bot.utils.common import async_retry_discord_message_command

from discord_bot.cogs.music_helpers.common import MessageType, MessageLifecycleStage
from discord_bot.cogs.music_helpers.message_context import MessageContext, MessageMutableBundle

class MessageQueue():
    '''
    Message queue to handle diff types of messages
    '''
    def __init__(self):
        self.single_mutable_queue = {}
        self.mutable_bundles = {}
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

    def update_multiple_mutable(self, index_name: str, text_channel: TextChannel, delete_after: int = None) -> bool:
        '''
        Update multiple mutable messages using MessageMutableBundle
        index_name : Index Name for mutable bundle (e.g., 'play_order-<guild-id>')
        text_channel : Discord TextChannel object
        delete_after : Delete messages after X seconds
        '''
        # Create check_last_message_func that uses text_channel.history
        async def check_last_message_func(count: int):
            async def fetch_messages():
                return [m async for m in text_channel.history(limit=count)]
            return await async_retry_discord_message_command(fetch_messages)

        # Create send_function that uses text_channel.send
        async def send_function(content: str, delete_after: int = None):
            return await async_retry_discord_message_command(
                partial(text_channel.send, content, delete_after=delete_after)
            )

        # Create and register bundle if it doesn't exist
        if index_name not in self.mutable_bundles:
            bundle = MessageMutableBundle(
                guild_id=text_channel.guild.id,
                channel_id=text_channel.id,
                check_last_message_func=check_last_message_func,
                send_function=send_function,
                delete_after=delete_after
            )
            bundle.is_queued_for_processing = True
            self.mutable_bundles[index_name] = bundle
        else:
            # Update the timestamp for existing bundle and queue it for processing
            self.mutable_bundles[index_name].updated_at = datetime.now(timezone.utc)
            self.mutable_bundles[index_name].is_queued_for_processing = True

        return True

    async def update_mutable_bundle_content(self, index_name: str, message_content: List[str]) -> List[Callable]:
        '''
        Update a MessageMutableBundle with new content and get dispatch functions

        index_name: Unique identifier for the bundle
        message_content: List of message content strings
        '''
        bundle = self.mutable_bundles.get(index_name)
        if not bundle:
            return []

        # Check if we should clear messages first (sticky check)
        should_clear = await async_retry_discord_message_command(partial(bundle.should_clear_messages))

        # Get dispatch functions for content updates (including clear if needed)
        dispatch_functions = bundle.get_message_dispatch(message_content, clear_existing=should_clear)
        return dispatch_functions

    async def update_mutable_bundle_references(self, index_name: str, results: List) -> bool:
        '''
        Update message references in a bundle after dispatch functions have been executed

        index_name: Unique identifier for the bundle
        results: List of results from executing dispatch functions (Message objects for sends, booleans for deletes)
        '''
        bundle = self.mutable_bundles.get(index_name)
        if not bundle:
            return False

        # Update message references only for Message objects (send/update operations)
        send_results = [r for r in results if r and hasattr(r, 'id')]

        # Set message references for the contexts that correspond to new messages
        for i, message in enumerate(send_results):
            if i < len(bundle.message_contexts):
                bundle.message_contexts[i].set_message(message)

        return True

    async def update_mutable_bundle_channel(self, index_name: str, new_text_channel: TextChannel) -> bool:
        '''
        Update the text channel for an existing MessageMutableBundle and queue it for processing

        index_name: Unique identifier for the bundle
        new_text_channel: The new TextChannel to move messages to
        '''
        bundle = self.mutable_bundles.get(index_name)
        if not bundle:
            return False

        # Update the bundle's text channel and get delete functions
        delete_functions = bundle.update_text_channel(new_text_channel)

        # Execute delete functions immediately
        for delete_func in delete_functions:
            await async_retry_discord_message_command(delete_func)

        # Update the timestamp to queue this bundle for processing
        bundle.updated_at = datetime.now(timezone.utc)
        bundle.is_queued_for_processing = True

        return True

    def get_next_multiple_mutable(self) -> str:
        '''
        Return bundle index that was least recently sent and is queued for processing
        '''
        oldest_timestamp = None
        oldest_index = None
        for index_name, bundle in self.mutable_bundles.items():
            # Only consider bundles that are queued for processing
            if not bundle.is_queued_for_processing:
                continue

            # Use last_sent timestamp, or created_at if never sent
            bundle_timestamp = bundle.last_sent if bundle.last_sent else bundle.created_at

            if oldest_index is None:
                oldest_index = index_name
                oldest_timestamp = bundle_timestamp
                continue
            if bundle_timestamp < oldest_timestamp:
                oldest_index = index_name
                oldest_timestamp = bundle_timestamp
                continue
        if not oldest_index:
            return None

        # Update last_sent timestamp and mark as no longer queued when bundle is returned for processing
        self.mutable_bundles[oldest_index].last_sent = datetime.now(timezone.utc)
        self.mutable_bundles[oldest_index].is_queued_for_processing = False
        return oldest_index

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
            message_context.function = function
            message_context.message_content = message_content
            message_context.delete_after = delete_after
            message_context.lifecycle_stage = lifecycle_stage
            self.single_mutable_queue[str(message_context.uuid)] = message_context
            return True
        current_value = self.single_mutable_queue[str(message_context.uuid)]
        # If existing value is send and new value is edit, override the send with new content
        if current_value.lifecycle_stage == MessageLifecycleStage.SEND and lifecycle_stage != MessageLifecycleStage.DELETE:
            current_value.message_content = message_content
            current_value.delete_after = delete_after
            return True
        # If sending existing value and deleting, just remove
        if current_value.lifecycle_stage == MessageLifecycleStage.SEND and lifecycle_stage == MessageLifecycleStage.DELETE:
            self.single_mutable_queue.pop(str(message_context.uuid))
            return True
        # If editing, update the edit
        if current_value.lifecycle_stage == MessageLifecycleStage.EDIT and lifecycle_stage != MessageLifecycleStage.DELETE:
            current_value.message_content = message_content
            current_value.delete_after = delete_after
            return True
        if current_value.lifecycle_stage == MessageLifecycleStage.EDIT and lifecycle_stage == MessageLifecycleStage.DELETE:
            current_value.message_content = None
            current_value.delete_after = None
            current_value.function = function
            current_value.lifecycle_stage = MessageLifecycleStage.DELETE
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
