from datetime import datetime, timezone
from functools import partial
from typing import Callable, List
from uuid import uuid4

from discord import Message, TextChannel

from discord_bot.utils.common import async_retry_discord_message_command

class MessageContext():
    '''
    Keep track of metadata messages that are sent and then later edited or deleted
    '''
    def __init__(self, guild_id: int, channel_id: int):
        self.guild_id = guild_id
        self.channel_id = channel_id
        self.uuid = f'context.{uuid4()}'
        self.created_at = datetime.now(timezone.utc)

        # Set after
        self.message_id = None
        self.message = None
        self.message_content = None
        self.delete_after = None
        self.function = None

    def set_message(self, message: Message):
        '''
        Set message that was sent to channel when video was requested

        message : Message object (can be None for failed messages)
        '''
        self.message = message
        self.message_id = message.id if message else None

    async def delete_message(self, _message_content: str, **_kwargs):
        '''
        Delete message if existing
        '''
        if not self.message:
            return False

        await self.message.delete()
        return True

    async def edit_message(self, content: str, delete_after: int = None):
        '''
        Edit message contents

        content : Message content
        delete_after : Delete after X seconds
        '''
        if not self.message:
            return False
        await self.message.edit(content=content, delete_after=delete_after)
        return True


class MuableBundleInvalidMessageContent(Exception):
    '''
    Update has invalid message content
    '''

class MessageMutableBundle():
    '''
    Collection of multiple mutable messages
    '''
    def __init__(self, guild_id: int, channel_id: int, check_last_message_func: Callable,
                 send_function: Callable,
                 sticky_messages: bool = True):
        '''
        guild_id : Server ID
        channel_id: Channel ID with mutable messages
        check_last_message_func: Use this function to get the last message of the channel
        send_function: Use this function to send new messages
        delete_after: Delete after for new messages, if any
        sticky_messages: If messages should always stick to the last message in the channel
        '''
        self.guild_id = guild_id
        self.channel_id = channel_id
        self.check_last_message_func = check_last_message_func
        self.send_function = send_function
        self.created_at = datetime.now(timezone.utc)
        self.updated_at = datetime.now(timezone.utc)
        self.last_sent = None  # Track when this bundle was last processed
        self.is_queued_for_processing = False
        self.sticky_messages = sticky_messages

        self.message_contexts = []

    async def should_clear_messages(self) -> bool:
        '''
        Check if messages should be cleared (sticky check)
        Returns True if messages are not at the end of the channel
        '''
        if not self.message_contexts:
            return False
        if not self.sticky_messages:
            return False

        # Get the last N messages from the channel where N is the number of our messages
        history_messages = await async_retry_discord_message_command(partial(self.check_last_message_func, len(self.message_contexts)))

        # Compare our messages with the channel history in reverse order
        for count, hist_message in enumerate(history_messages):
            index = len(self.message_contexts) - 1 - count
            if index < 0:
                break
            context = self.message_contexts[index]
            if not context.message or context.message.id != hist_message.id:
                return True
        return False

    def get_message_dispatch(self, message_content: List[str], clear_existing: bool = False, delete_after: int = None) -> List[Callable]:
        '''
        Return list of functions to handle message updates
        Compares new content with existing messages and returns appropriate functions
        
        message_content: List of new message content
        clear_existing: If True, clear all existing messages before sending new ones (for sticky behavior)
        delete_after: Set delete after on message
        '''
        dispatch_functions = []

        # Handle sticky clear behavior - delete all existing messages first
        if clear_existing and self.message_contexts:
            for context in self.message_contexts:
                if context.message:
                    delete_func = partial(context.delete_message, "")
                    dispatch_functions.append(delete_func)
            # Clear contexts after deleting
            self.message_contexts = []

        # Handle the case where we have no existing messages
        if not self.message_contexts:
            for content in message_content:
                mc = MessageContext(self.guild_id, self.channel_id)
                mc.message_content = content
                mc.delete_after = delete_after
                send_func = partial(self.send_function, content=content, delete_after=delete_after)
                mc.function = send_func
                self.message_contexts.append(mc)
                dispatch_functions.append(send_func)
            return dispatch_functions

        # Use content-aware diffing to minimize operations
        dispatch_functions.extend(self._get_content_aware_dispatch(message_content, delete_after))

    def _find_exact_content_matches(self, old_content: List[str], new_content: List[str]) -> dict:
        '''
        Find exact content matches between old and new content lists.
        Returns mapping of old_index -> new_index for exact matches.

        old_content: List of existing message content
        new_content: List of new message content

        Returns: dict mapping old_index -> new_index for exact matches
        '''
        matches = {}
        used_new_indices = set()

        # For each old content, find first unused exact match in new content
        for old_idx, old_text in enumerate(old_content):
            for new_idx, new_text in enumerate(new_content):
                if new_idx not in used_new_indices and old_text == new_text:
                    matches[old_idx] = new_idx
                    used_new_indices.add(new_idx)
                    break

        return matches

    def _get_content_aware_dispatch(self, message_content: List[str], delete_after: int = None) -> List[Callable]:
        '''
        Generate dispatch functions using content-aware diffing to minimize operations.

        message_content: List of new message content
        delete_after: Set delete after on message

        Returns: List of dispatch functions
        '''
        dispatch_functions = []
        existing_count = len(self.message_contexts)
        new_count = len(message_content)

        # Get current content for comparison
        old_content = [ctx.message_content for ctx in self.message_contexts]

        # Find exact content matches
        exact_matches = self._find_exact_content_matches(old_content, message_content)

        # Determine which old messages to keep, edit, or delete
        old_messages_to_keep = set(exact_matches.keys())
        new_positions_filled = set(exact_matches.values())

        # Create new context list for the final state
        new_contexts = [None] * new_count

        # Phase 1: Place exact matches in their new positions
        for old_idx, new_idx in exact_matches.items():
            old_context = self.message_contexts[old_idx]
            new_contexts[new_idx] = old_context

            # Handle delete_after updates for exact matches
            if delete_after and not old_context.delete_after:
                edit_func = partial(old_context.edit_message, content=old_context.message_content, delete_after=delete_after)
                old_context.function = edit_func
                old_context.delete_after = delete_after
                dispatch_functions.append(edit_func)

        # Phase 2: Handle remaining old messages (delete unused ones)
        for old_idx in range(existing_count):
            if old_idx not in old_messages_to_keep:
                context = self.message_contexts[old_idx]
                if context.message:
                    delete_func = partial(context.delete_message, "")
                    dispatch_functions.append(delete_func)

        # Phase 3: Handle remaining new positions (edit existing or send new)
        for new_idx in range(new_count):
            if new_idx not in new_positions_filled:
                new_content_text = message_content[new_idx]

                # Try to find an existing unused message to edit
                available_context = None
                for old_idx in range(existing_count):
                    if (old_idx not in old_messages_to_keep and
                        self.message_contexts[old_idx].message and
                        self.message_contexts[old_idx] not in [nc for nc in new_contexts if nc is not None]):
                        available_context = self.message_contexts[old_idx]
                        old_messages_to_keep.add(old_idx)  # Mark as used
                        break

                if available_context:
                    # Edit existing message
                    available_context.message_content = new_content_text
                    edit_func = partial(available_context.edit_message, content=new_content_text, delete_after=delete_after)
                    available_context.function = edit_func
                    available_context.delete_after = delete_after
                    new_contexts[new_idx] = available_context
                    dispatch_functions.append(edit_func)
                else:
                    # Create new message
                    mc = MessageContext(self.guild_id, self.channel_id)
                    mc.message_content = new_content_text
                    mc.delete_after = delete_after
                    send_func = partial(self.send_function, content=new_content_text, delete_after=delete_after)
                    mc.function = send_func
                    new_contexts[new_idx] = mc
                    dispatch_functions.append(send_func)

        # Update the message contexts to the new arrangement
        self.message_contexts = new_contexts

        return dispatch_functions

    def clear_all_messages(self) -> List[Callable]:
        '''
        Return functions to delete all managed messages
        '''
        delete_functions = []
        for context in self.message_contexts:
            if context.message:
                delete_func = partial(context.delete_message, "")
                delete_functions.append(delete_func)
        self.message_contexts = []
        return delete_functions

    def get_message_count(self) -> int:
        '''
        Get the number of managed messages
        '''
        return len(self.message_contexts)

    def has_messages(self) -> bool:
        '''
        Check if there are any managed messages
        '''
        return len(self.message_contexts) > 0

    def update_text_channel(self, new_text_channel: TextChannel) -> List[Callable]:
        '''
        Update the text channel for this bundle and return functions to:
        1. Delete all existing messages
        2. Send new messages to the new channel
        
        new_text_channel: The new TextChannel to send messages to
        Returns: List of async functions to execute (delete old, then send new)
        '''
        dispatch_functions = []

        # First, add delete functions for all existing messages
        delete_functions = self.clear_all_messages()
        dispatch_functions.extend(delete_functions)

        # Update the channel references
        self.guild_id = new_text_channel.guild.id
        self.channel_id = new_text_channel.id

        # Create new check_last_message_func for the new channel
        async def new_check_last_message_func(count: int):
            async def fetch_messages():
                return [m async for m in new_text_channel.history(limit=count)]
            return await async_retry_discord_message_command(fetch_messages)

        # Create new send_function for the new channel
        async def new_send_function(content: str, delete_after: int = None):
            return await async_retry_discord_message_command(
                partial(new_text_channel.send, content, delete_after=delete_after)
            )

        # Update the functions
        self.check_last_message_func = new_check_last_message_func
        self.send_function = new_send_function

        # Reset message contexts since we're moving to a new channel
        self.message_contexts = []

        return dispatch_functions
