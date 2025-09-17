from datetime import datetime, timezone
from functools import partial
from typing import Callable, List
from uuid import uuid4

from discord import Message, TextChannel
from discord.errors import NotFound

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

        try:
            await self.message.delete()
        except NotFound:
            return True
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

        # Compare existing messages with new content
        existing_count = len(self.message_contexts)
        new_count = len(message_content)

        # Handle deletion of extra messages
        if existing_count > new_count:
            for i in range(new_count, existing_count):
                context = self.message_contexts[i]
                if context.message:
                    delete_func = partial(context.delete_message, "")
                    dispatch_functions.append(delete_func)
            # Remove the extra contexts
            self.message_contexts = self.message_contexts[:new_count]

        # Handle updating existing messages
        for i in range(min(existing_count, new_count)):
            context = self.message_contexts[i]
            new_content = message_content[i]

            # Check if content is different
            if context.message_content != new_content:
                context.message_content = new_content
                if context.message:
                    # Message exists, edit it
                    edit_func = partial(context.edit_message, content=new_content, delete_after=delete_after)
                    context.function = edit_func
                    context.delete_after = delete_after
                    dispatch_functions.append(edit_func)
                else:
                    # Message doesn't exist yet, send it
                    send_func = partial(self.send_function, content=new_content, delete_after=delete_after)
                    context.function = send_func
                    context.delete_after = delete_after
                    dispatch_functions.append(send_func)

            # Delete after might be passed in on a new call
            # If message existed and was actively edited, and now is final message
            elif delete_after and not context.delete_after:
                edit_func = partial(context.edit_message, content=new_content, delete_after=delete_after)
                context.function = edit_func
                context.delete_after = delete_after
                dispatch_functions.append(edit_func)

            # If content is the same, no action needed (no-op)

        # Handle adding new messages
        if new_count > existing_count:
            for i in range(existing_count, new_count):
                content = message_content[i]
                mc = MessageContext(self.guild_id, self.channel_id)
                mc.message_content = content
                mc.delete_after = delete_after
                send_func = partial(self.send_function, content=content, delete_after=delete_after)
                mc.function = send_func
                self.message_contexts.append(mc)
                dispatch_functions.append(send_func)

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
