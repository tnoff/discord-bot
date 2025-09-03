"""Message formatting utilities for Discord bot music functionality."""

class MessageFormatter:
    """Handles formatting of various messages for the music cog."""

    @staticmethod
    def format_play_queue_full_message(item_str: str, failed_reason: str = None) -> str:
        """
        Format a message for when the play queue is full.
        
        Args:
            item_str: String representation of the item that failed to be added
            failed_reason: Optional specific reason for failure (defaults to queue full)
            
        Returns:
            Formatted error message
        """
        reason = failed_reason or "play queue is full"
        return f'{item_str} (failed: {reason})'

    @staticmethod
    def format_download_queue_full_message(item_str: str) -> str:
        """
        Format a message for when the download queue is full.
        
        Args:
            item_str: String representation of the item that failed to be added
            
        Returns:
            Formatted error message
        """
        return f'Unable to add "{item_str}" to queue, download queue is full'

    @staticmethod
    def format_video_download_issue_message(item_str: str, error_message: str = None) -> str:
        """
        Format a message for when there's an issue downloading a video.
        
        Args:
            item_str: String representation of the item that failed to download
            error_message: Error message to show

        Returns:
            Formatted error message
        """
        mess = f'Issue downloading video "{item_str}"'
        if error_message:
            mess = f'{mess}, error: "{error_message}"'
        return mess

    @staticmethod
    def format_downloading_message(item_str: str) -> str:
        """
        Format a message for when starting to download and process an item.
        
        Args:
            item_str: String representation of the item being downloaded
            
        Returns:
            Formatted status message
        """
        return f'Downloading and processing "{item_str}"'

    @staticmethod
    def format_downloading_for_playlist_message(item_str: str) -> str:
        """
        Format a message for when downloading an item to add to a playlist.
        
        Args:
            item_str: String representation of the item being downloaded
            
        Returns:
            Formatted status message
        """
        return f'Downloading and processing "{item_str}" to add to playlist'

    @staticmethod
    def format_playlist_item_added_message(title: str) -> str:
        """
        Format a message for when an item is successfully added to a playlist.
        
        Args:
            title: Title of the item that was added
            
        Returns:
            Formatted success message
        """
        return f'Added item "{title}" to playlist'

    @staticmethod
    def format_playlist_item_add_failed_message(reason: str = "likely already exists") -> str:
        """
        Format a message for when adding an item to a playlist fails.
        
        Args:
            item_str: String representation of the item that failed to be added
            reason: Reason for the failure
            
        Returns:
            Formatted error message
        """
        return f'Unable to add playlist item: {reason}'

    @staticmethod
    def format_playlist_max_length_message() -> str:
        """
        Format a message for when a playlist has reached its maximum length.
        
        Returns:
            Formatted error message
        """
        return 'Cannot add more items to playlist, already max size'

    @staticmethod
    def format_playlist_generation_issue_message(item_str: str) -> str:
        """
        Format a message for when there's an issue generating source for a playlist item.
        
        Args:
            item_str: String representation of the item that failed
            
        Returns:
            Formatted error message
        """
        return f'Unable to add playlist item "{item_str}", issue generating source'
