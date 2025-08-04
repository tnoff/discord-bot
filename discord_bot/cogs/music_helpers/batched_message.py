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

    def __init__(self, guild_id: int, batch_size: int = 15, auto_delete_after: int = 30, channel_id: int = None):
        self.batch_id = str(uuid4())
        self.guild_id = guild_id
        self.channel_id = channel_id
        self.source_dicts: List[SourceDict] = []
        self.status_map: Dict[str, MessageStatus] = {}  # uuid -> status
        self.error_map: Dict[str, str] = {}  # uuid -> error message
        self.message: Optional[Message] = None
        self.message_id: Optional[int] = None
        self.created_at = datetime.now(timezone.utc)
        self.last_updated = datetime.now(timezone.utc)
        self.batch_size = batch_size
        self.auto_delete_after = auto_delete_after

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

    def generate_message_content(self) -> str:
        '''
        Generate the formatted message content
        Shows only pending, downloading, failed, and skipped items
        Completed items are hidden
        '''
        visible_items = self.get_visible_items()

        # Header with progress
        if self.is_processing_complete():
            header = f"Multi-video Input Processing Complete ({self.completed_count}/{self.total_items} items succeeded)"
        else:
            header = f"Processing ({self.completed_count}/{self.total_items} items)"

        lines = [header]

        # Add visible items (with Discord 2000 char limit protection)
        DISCORD_CHAR_LIMIT = 2000
        RESERVE_CHARS = 200  # Reserve space for completion summary and safety margin
        current_length = len(header) + 1  # +1 for newline

        for item_num, source_dict, status, error_msg in visible_items:
            # Use MessageFormatter for consistent formatting
            formatted_line = MessageFormatter.format_single_message(source_dict, status, error_msg)
            line = f"{status.value} {item_num}. {formatted_line[2:]}"  # Keep emoji, add number, remove original emoji

            # Check if adding this line would exceed Discord's limit
            line_length = len(line) + 1  # +1 for newline
            if current_length + line_length + RESERVE_CHARS > DISCORD_CHAR_LIMIT:
                # Add truncation notice
                remaining_items = len(visible_items) - len(lines) + 1  # +1 because header is first line
                lines.append(f"... and {remaining_items} more items (truncated due to message length)")
                break

            lines.append(line)
            current_length += line_length

        # Add completion summary if done
        if self.is_processing_complete():
            if self.completed_count > 0:
                lines.append("")
                video_word = "video" if self.completed_count == 1 else "videos"
                lines.append(f"{self.completed_count} {video_word} successfully added to queue")


        return "\n".join(lines)

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

    def get_delete_after(self) -> Optional[int]:
        '''Get delete_after value if message should auto-delete'''
        if self.should_auto_delete():
            return self.auto_delete_after
        return None

    def set_message(self, message: Message):
        '''Set the Discord message for this batch'''
        self.message = message
        self.message_id = message.id

    async def delete_message(self):
        '''Delete the Discord message'''
        if self.message:
            await self.message.delete()

    async def edit_message(self, content: str, delete_after: int = None):
        '''Edit the Discord message content'''
        if self.message:
            await self.message.edit(content=content, delete_after=delete_after)
