from copy import deepcopy
from datetime import datetime, timezone
from typing import Dict, List, Optional
from uuid import uuid4

from discord import Message

from discord_bot.cogs.music_helpers.source_dict import SourceDict
from discord_bot.cogs.music_helpers.message_formatter import MessageFormatter, MessageStatus


class BatchedMessageItem:
    '''
    Manages a batch of SourceDicts as a single Discord message
    Items are removed from display once completed successfully
    '''

    def __init__(self, guild_id: int, batch_size: int = 15, channel_id: int = None, items_per_message: int = 10):
        self.batch_id = str(uuid4())
        self.guild_id = guild_id
        self.channel_id = channel_id
        self.source_dicts: List[SourceDict] = []
        self.status_map: Dict[str, MessageStatus] = {}  # uuid -> status
        self.error_map: Dict[str, str] = {}  # uuid -> error message
        self.messages: List[Optional[Message]] = []  # Multiple Discord messages
        self.message_ids: List[Optional[int]] = []  # Message IDs for each message
        self.created_at = datetime.now(timezone.utc)
        self.last_updated = datetime.now(timezone.utc)
        self.batch_size = batch_size
        self.items_per_message = items_per_message

        # Counters
        self.total_items = 0
        self.completed_count = 0
        self.failed_count = 0

    def add_source_dict(self, source_dict: SourceDict) -> bool:
        '''
        Add a SourceDict to this batch
        Returns True if successfully added, False if batch is full
        '''
        if len(self.source_dicts) >= self.batch_size:
            return False

        self.source_dicts.append(source_dict)
        self.status_map[str(source_dict.uuid)] = MessageStatus.PENDING
        self.total_items += 1
        source_dict.batch_id = self.batch_id  # Link back to batch
        return True

    def update_item_status(self, source_uuid: str, status: MessageStatus, error_msg: str = None) -> bool:
        '''
        Update status of individual item in batch
        Returns True if batch needs message update
        '''
        if source_uuid not in self.status_map:
            return False

        old_status = deepcopy(self.status_map[source_uuid])
        self.status_map[source_uuid] = status
        self.last_updated = datetime.now(timezone.utc)

        # Update counters
        if old_status != MessageStatus.COMPLETED and status == MessageStatus.COMPLETED:
            self.completed_count += 1
        elif old_status != MessageStatus.FAILED and status == MessageStatus.FAILED:
            self.failed_count += 1
            if error_msg:
                self.error_map[source_uuid] = error_msg

        return True

    def get_visible_items(self) -> List[tuple]:
        '''
        Get items that should be visible in the message
        Returns list of (index, source_dict, status, error_msg) tuples
        Excludes completed items
        '''
        visible_items = []
        for i, source_dict in enumerate(self.source_dicts):
            status = self.status_map[str(source_dict.uuid)]

            # Skip completed items - they're removed from display
            if status == MessageStatus.COMPLETED:
                continue

            error_msg = self.error_map.get(str(source_dict.uuid))
            visible_items.append((i + 1, source_dict, status, error_msg))

        return visible_items

    def get_message_groups(self) -> List[List[tuple]]:
        '''
        Split visible items into groups for multiple Discord messages
        Returns list of groups, each containing up to items_per_message items
        '''
        visible_items = self.get_visible_items()
        groups = []

        for i in range(0, len(visible_items), self.items_per_message):
            group = visible_items[i:i + self.items_per_message]
            groups.append(group)

        return groups

    def generate_message_content(self, message_index: int = 0) -> str:
        '''
        Generate the formatted message content for a specific message
        Shows only pending, downloading, failed, and skipped items
        Completed items are hidden
        '''
        message_groups = self.get_message_groups()

        # Special case: if processing is complete and no visible items, show completion summary
        if self.is_processing_complete() and len(message_groups) == 0:
            if message_index > 0:
                return ""  # Only show completion summary on first message
            items_for_message = []
            total_messages = 1
        else:
            # Handle case where message_index is out of range
            if message_index >= len(message_groups):
                return ""
            items_for_message = message_groups[message_index]
            total_messages = len(message_groups)

        # Header with progress (same across all messages)
        if self.is_processing_complete():
            if self.failed_count > 0:
                header = f"Multi-video Input Processing Complete ({self.completed_count}/{self.total_items} succeeded, {self.failed_count} failed)"
            else:
                header = f"Multi-video Input Processing Complete ({self.completed_count}/{self.total_items} succeeded)"
        else:
            processed_count = self.completed_count + self.failed_count
            if self.failed_count > 0:
                header = f"Processing ({processed_count}/{self.total_items} items, {self.completed_count} succeeded, {self.failed_count} failed)"
            else:
                header = f"Processing ({processed_count}/{self.total_items} items)"

        # Add message indicator if multiple messages
        if total_messages > 1:
            header += f" [Message {message_index + 1}/{total_messages}]"

        lines = [header]

        # Add visible items for this message
        for item_num, source_dict, status, error_msg in items_for_message:
            # Use MessageFormatter for consistent formatting
            formatted_line = MessageFormatter.format_single_message(source_dict, status, error_msg)
            line = f"{status.value} {item_num}. {formatted_line[2:]}"  # Keep emoji, add number, remove original emoji
            lines.append(line)

        # Add completion summary if done (only on last message)
        if self.is_processing_complete() and message_index == total_messages - 1:
            lines.append("")
            if self.completed_count > 0:
                video_word = "video" if self.completed_count == 1 else "videos"
                lines.append(f"✅ {self.completed_count} {video_word} successfully added to queue")
            if self.failed_count > 0:
                failed_word = "video" if self.failed_count == 1 else "videos"
                lines.append(f"❌ {self.failed_count} {failed_word} failed to process")

        return "\n".join(lines)

    def generate_all_message_contents(self) -> List[str]:
        '''
        Generate content for all messages
        Returns list of message contents
        '''
        message_groups = self.get_message_groups()
        contents = []

        for i in range(len(message_groups)):
            content = self.generate_message_content(i)
            contents.append(content)

        return contents

    def is_batch_full(self) -> bool:
        '''Check if batch has reached capacity'''
        return len(self.source_dicts) >= self.batch_size

    def is_processing_complete(self) -> bool:
        '''Check if all items have been processed (completed, failed, or skipped)'''
        # Empty batch is not considered complete
        if not self.status_map:
            return False

        for status in self.status_map.values():
            if status in [MessageStatus.PENDING, MessageStatus.DOWNLOADING]:
                return False
        return True

    def should_auto_delete(self) -> bool:
        '''Check if message should be auto-deleted'''
        return self.is_processing_complete()

    def set_message(self, message: Message, message_index: int = 0):
        '''Set a Discord message for this batch at specific index'''
        # Ensure we have enough slots for this message index
        while len(self.messages) <= message_index:
            self.messages.append(None)
            self.message_ids.append(None)

        self.messages[message_index] = message
        self.message_ids[message_index] = message.id

    def set_messages(self, messages: List[Message]):
        '''Set multiple Discord messages for this batch'''
        self.messages = messages
        self.message_ids = [msg.id if msg else None for msg in messages]

    async def delete_message(self, message_index: int = None):
        '''Delete Discord message(s)'''
        if message_index is not None:
            # Delete specific message
            if message_index < len(self.messages) and self.messages[message_index]:
                await self.messages[message_index].delete()
                self.messages[message_index] = None
                self.message_ids[message_index] = None
        else:
            # Delete all messages
            for i, message in enumerate(self.messages):
                if message:
                    await message.delete()
                    self.messages[i] = None
                    self.message_ids[i] = None

    async def edit_message(self, content: str, message_index: int = 0, delete_after: int = None):
        '''Edit a specific Discord message content'''
        if message_index < len(self.messages) and self.messages[message_index]:
            await self.messages[message_index].edit(content=content, delete_after=delete_after)

    async def edit_all_messages(self, contents: List[str], delete_after: int = None):
        '''Edit all Discord messages with corresponding content'''
        for i, content in enumerate(contents):
            if i < len(self.messages) and self.messages[i]:
                await self.messages[i].edit(content=content, delete_after=delete_after)

    def get_required_message_count(self) -> int:
        '''Get the number of Discord messages needed for all visible items'''
        visible_items = self.get_visible_items()
        if not visible_items:
            return 0
        return (len(visible_items) + self.items_per_message - 1) // self.items_per_message
