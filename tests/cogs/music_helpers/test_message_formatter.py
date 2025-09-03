from discord_bot.cogs.music_helpers.message_formatter import MessageFormatter

def test_format_play_queue_full_message_default_reason():
    """Test format_play_queue_full_message with default reason."""
    item_str = "test song"
    result = MessageFormatter.format_play_queue_full_message(item_str)
    expected = "test song (failed: play queue is full)"
    assert result == expected

def test_format_play_queue_full_message_custom_reason():
    """Test format_play_queue_full_message with custom reason."""
    item_str = "test song"
    custom_reason = "server overloaded"
    result = MessageFormatter.format_play_queue_full_message(item_str, custom_reason)
    expected = "test song (failed: server overloaded)"
    assert result == expected

def test_format_play_queue_full_message_empty_item():
    """Test format_play_queue_full_message with empty item string."""
    item_str = ""
    result = MessageFormatter.format_play_queue_full_message(item_str)
    expected = " (failed: play queue is full)"
    assert result == expected

def test_format_play_queue_full_message_special_characters():
    """Test format_play_queue_full_message with special characters in item."""
    item_str = "Song with \"quotes\" & symbols"
    result = MessageFormatter.format_play_queue_full_message(item_str)
    expected = "Song with \"quotes\" & symbols (failed: play queue is full)"
    assert result == expected

def test_format_download_queue_full_message_basic():
    """Test format_download_queue_full_message with basic input."""
    item_str = "test song"
    result = MessageFormatter.format_download_queue_full_message(item_str)
    expected = 'Unable to add "test song" to queue, download queue is full'
    assert result == expected

def test_format_download_queue_full_message_empty_item():
    """Test format_download_queue_full_message with empty item string."""
    item_str = ""
    result = MessageFormatter.format_download_queue_full_message(item_str)
    expected = 'Unable to add "" to queue, download queue is full'
    assert result == expected

def test_format_download_queue_full_message_special_characters():
    """Test format_download_queue_full_message with special characters in item."""
    item_str = "Song with \"quotes\" & symbols"
    result = MessageFormatter.format_download_queue_full_message(item_str)
    expected = 'Unable to add "Song with "quotes" & symbols" to queue, download queue is full'
    assert result == expected

def test_format_download_queue_full_message_long_item():
    """Test format_download_queue_full_message with long item string."""
    item_str = "Very long song title that goes on and on with lots of words"
    result = MessageFormatter.format_download_queue_full_message(item_str)
    expected = 'Unable to add "Very long song title that goes on and on with lots of words" to queue, download queue is full'
    assert result == expected

def test_format_play_queue_full_message_none_item():
    """Test format_play_queue_full_message handles None item gracefully."""
    # This should convert None to string "None"
    result = MessageFormatter.format_play_queue_full_message(None)
    expected = "None (failed: play queue is full)"
    assert result == expected

def test_format_download_queue_full_message_none_item():
    """Test format_download_queue_full_message handles None item gracefully."""
    # This should convert None to string "None"
    result = MessageFormatter.format_download_queue_full_message(None)
    expected = 'Unable to add "None" to queue, download queue is full'
    assert result == expected

def test_format_play_queue_full_message_numeric_item():
    """Test format_play_queue_full_message with numeric item."""
    item_str = 123
    result = MessageFormatter.format_play_queue_full_message(item_str)
    expected = "123 (failed: play queue is full)"
    assert result == expected

def test_format_download_queue_full_message_numeric_item():
    """Test format_download_queue_full_message with numeric item."""
    item_str = 456
    result = MessageFormatter.format_download_queue_full_message(item_str)
    expected = 'Unable to add "456" to queue, download queue is full'
    assert result == expected

def test_format_video_download_issue_message():
    """Test format_video_download_issue_message."""
    item_str = "Test Song"
    result = MessageFormatter.format_video_download_issue_message(item_str)
    expected = 'Issue downloading video "Test Song"'
    assert result == expected

def test_format_downloading_message():
    """Test format_downloading_message."""
    item_str = "Test Song"
    result = MessageFormatter.format_downloading_message(item_str)
    expected = 'Downloading and processing "Test Song"'
    assert result == expected

def test_format_downloading_for_playlist_message():
    """Test format_downloading_for_playlist_message."""
    item_str = "Test Song"
    result = MessageFormatter.format_downloading_for_playlist_message(item_str)
    expected = 'Downloading and processing "Test Song" to add to playlist'
    assert result == expected

def test_format_playlist_item_added_message():
    """Test format_playlist_item_added_message."""
    title = "My Favorite Song"
    result = MessageFormatter.format_playlist_item_added_message(title)
    expected = 'Added item "My Favorite Song" to playlist'
    assert result == expected

def test_format_playlist_item_add_failed_message_default_reason():
    """Test format_playlist_item_add_failed_message with default reason."""
    result = MessageFormatter.format_playlist_item_add_failed_message()
    expected = 'Unable to add playlist item: likely already exists'
    assert result == expected

def test_format_playlist_item_add_failed_message_custom_reason():
    """Test format_playlist_item_add_failed_message with custom reason."""
    reason = "invalid format"
    result = MessageFormatter.format_playlist_item_add_failed_message(reason)
    expected = 'Unable to add playlist item: invalid format'
    assert result == expected

def test_format_playlist_max_length_message():
    """Test format_playlist_max_length_message."""
    result = MessageFormatter.format_playlist_max_length_message()
    expected = 'Cannot add more items to playlist, already max size'
    assert result == expected

def test_format_playlist_generation_issue_message():
    """Test format_playlist_generation_issue_message."""
    item_str = "Test Song"
    result = MessageFormatter.format_playlist_generation_issue_message(item_str)
    expected = 'Unable to add playlist item "Test Song", issue generating source'
    assert result == expected

def test_all_methods_handle_empty_strings():
    """Test that all methods handle empty strings gracefully."""
    # Test methods that take item_str parameter
    assert MessageFormatter.format_video_download_issue_message("") == 'Issue downloading video ""'
    assert MessageFormatter.format_downloading_message("") == 'Downloading and processing ""'
    assert MessageFormatter.format_downloading_for_playlist_message("") == 'Downloading and processing "" to add to playlist'
    assert MessageFormatter.format_playlist_item_added_message("") == 'Added item "" to playlist'
    assert MessageFormatter.format_playlist_item_add_failed_message("") == 'Unable to add playlist item: '
    assert MessageFormatter.format_playlist_generation_issue_message("") == 'Unable to add playlist item "", issue generating source'

def test_all_methods_handle_special_characters():
    """Test that all methods handle special characters properly."""
    item_with_quotes = 'Song "with quotes" & symbols'

    assert MessageFormatter.format_video_download_issue_message(item_with_quotes) == 'Issue downloading video "Song "with quotes" & symbols"'
    assert MessageFormatter.format_downloading_message(item_with_quotes) == 'Downloading and processing "Song "with quotes" & symbols"'
    assert MessageFormatter.format_downloading_for_playlist_message(item_with_quotes) == 'Downloading and processing "Song "with quotes" & symbols" to add to playlist'
    assert MessageFormatter.format_playlist_item_added_message(item_with_quotes) == 'Added item "Song "with quotes" & symbols" to playlist'
    assert MessageFormatter.format_playlist_item_add_failed_message(item_with_quotes) == 'Unable to add playlist item: Song "with quotes" & symbols'
    assert MessageFormatter.format_playlist_generation_issue_message(item_with_quotes) == 'Unable to add playlist item "Song "with quotes" & symbols", issue generating source'
