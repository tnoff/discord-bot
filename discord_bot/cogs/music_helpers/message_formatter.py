from enum import Enum
import re
from typing import Optional

from discord_bot.cogs.music_helpers.source_dict import SourceDict


def _format_search_string_for_discord(search_string: str) -> str:
    '''
    Format search string to prevent Discord embeds for URLs
    Wraps URLs in <> to suppress embeds while keeping other text unchanged
    '''
    # Pattern to match URLs that are NOT already wrapped in <>
    # Uses negative lookbehind (?<!<) and negative lookahead (?!>)
    url_pattern = r'(?<!<)https?://[^\s<>"{}|\\^`\[\]]+(?!>)'

    def wrap_url(match):
        url = match.group(0)
        return f'<{url}>'

    return re.sub(url_pattern, wrap_url, search_string)


class MessageStatus(Enum):
    '''
    Status indicators for different message types
    '''
    PENDING = "⏳"      # Waiting to process
    DOWNLOADING = "🔄"  # Currently downloading
    FAILED = "❌"       # Failed with error
    SKIPPED = "⏭️"      # Skipped (duplicate, etc.)


class MessageFormatter:
    '''
    Common message formatting utilities for consistent messaging across single and batch systems
    '''

    @staticmethod
    def format_single_message(source_dict: SourceDict, status: MessageStatus,
                             error_msg: Optional[str] = None) -> str:
        '''
        Format a single item message with emoji status indicators

        Args:
            source_dict: The source being processed
            status: Status indicator
            error_msg: Error message for failed items

        Returns:
            Formatted message string
        '''
        formatted_search_string = _format_search_string_for_discord(source_dict.search_string)

        if status == MessageStatus.FAILED and error_msg:
            return f'{status.value} {formatted_search_string} (failed: {error_msg})'
        if status == MessageStatus.SKIPPED and error_msg:
            return f'{status.value} {formatted_search_string} (skipped: {error_msg})'
        if status == MessageStatus.DOWNLOADING:
            return f'{status.value} {formatted_search_string} (downloading...)'
        return f'{status.value} {formatted_search_string}'

    @staticmethod
    def format_downloading_message(source_dict: SourceDict) -> str:
        '''
        Format message for items being downloaded
        '''
        return MessageFormatter.format_single_message(source_dict, MessageStatus.DOWNLOADING)

    @staticmethod
    def format_queue_full_message(source_dict: SourceDict) -> str:
        '''
        Format message for queue full errors
        '''
        return MessageFormatter.format_single_message(source_dict, MessageStatus.FAILED, "queue is full")

    @staticmethod
    def format_play_queue_full_message(source_dict: SourceDict) -> str:
        '''
        Format message for play queue full errors
        '''
        return MessageFormatter.format_single_message(source_dict, MessageStatus.FAILED, "play queue is full")

    @staticmethod
    def format_download_failed_message(source_dict: SourceDict) -> str:
        '''
        Format message for download failures
        '''
        return MessageFormatter.format_single_message(source_dict, MessageStatus.FAILED, "download failed")

    @staticmethod
    def format_playlist_item_failed_message(source_dict: SourceDict, reason: str = "issue generating source") -> str:
        '''
        Format message for playlist item failures
        '''
        return MessageFormatter.format_single_message(source_dict, MessageStatus.FAILED, reason)

    @staticmethod
    def format_playlist_item_exists_message(source_dict: SourceDict) -> str:
        '''
        Format message for duplicate playlist items
        '''
        return MessageFormatter.format_single_message(source_dict, MessageStatus.SKIPPED, "already exists")

    @staticmethod
    def format_playlist_max_size_message() -> str:
        '''
        Format message for max playlist size reached
        '''
        return f'{MessageStatus.FAILED.value} Cannot add more items to playlist, already max size'

    @staticmethod
    def format_playlist_item_added_message(title: str) -> str:
        '''
        Format message for successfully added playlist items
        '''
        return f'✨ Added item "{title}" to playlist'

    @staticmethod
    def format_history_playlist_error() -> str:
        '''
        Format message for history playlist addition errors
        '''
        return f'{MessageStatus.FAILED.value} Cannot add to history playlist, is reserved and cannot be added to manually'

    @staticmethod
    def format_no_videos_message() -> str:
        '''
        Format message when no videos are available
        '''
        return f'{MessageStatus.FAILED.value} There are no videos to add to playlist'

    @staticmethod
    def format_finished_playlist_message(playlist_name: str) -> str:
        '''
        Format completion message for playlist operations
        '''
        return f'✅ Finished adding items to playlist "{playlist_name}"'
