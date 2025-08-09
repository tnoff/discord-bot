from discord_bot.cogs.music_helpers.message_formatter import (
    MessageFormatter,
    MessageStatus,
    _format_search_string_for_discord
)
from discord_bot.cogs.music_helpers.common import SearchType
from tests.helpers import fake_source_dict, generate_fake_context


class TestFormatSearchStringForDiscord:
    '''Test the URL formatting function'''

    def test_format_single_url(self):
        '''Test formatting a single URL'''
        result = _format_search_string_for_discord('https://youtube.com/watch?v=123')
        assert result == '<https://youtube.com/watch?v=123>'

    def test_format_http_url(self):
        '''Test formatting HTTP URL'''
        result = _format_search_string_for_discord('http://example.com')
        assert result == '<http://example.com>'

    def test_format_url_with_text(self):
        '''Test formatting URL with surrounding text'''
        result = _format_search_string_for_discord('Check out https://example.com for info')
        assert result == 'Check out <https://example.com> for info'

    def test_format_multiple_urls(self):
        '''Test formatting multiple URLs in one string'''
        result = _format_search_string_for_discord('https://site1.com and https://site2.com')
        assert result == '<https://site1.com> and <https://site2.com>'

    def test_format_already_wrapped_url(self):
        '''Test that already wrapped URLs are not double-wrapped'''
        result = _format_search_string_for_discord('Already wrapped: <https://example.com>')
        assert result == 'Already wrapped: <https://example.com>'

    def test_format_mixed_wrapped_unwrapped(self):
        '''Test mix of wrapped and unwrapped URLs'''
        result = _format_search_string_for_discord('<https://wrapped.com> https://unwrapped.com')
        assert result == '<https://wrapped.com> <https://unwrapped.com>'

    def test_format_no_urls(self):
        '''Test string with no URLs'''
        result = _format_search_string_for_discord('Just some text')
        assert result == 'Just some text'

    def test_format_empty_string(self):
        '''Test empty string'''
        result = _format_search_string_for_discord('')
        assert result == ''

    def test_format_complex_url(self):
        '''Test complex URL with parameters and fragments'''
        url = 'https://youtube.com/watch?v=123&t=45&list=456#comment'
        result = _format_search_string_for_discord(url)
        assert result == f'<{url}>'

    def test_format_url_with_special_chars_in_text(self):
        '''Test URL with special characters in surrounding text'''
        result = _format_search_string_for_discord('Song: "Title" - https://youtube.com/watch?v=123')
        assert result == 'Song: "Title" - <https://youtube.com/watch?v=123>'


class TestMessageStatus:
    '''Test the MessageStatus enum'''

    def test_enum_values(self):
        '''Test that all enum values have correct emoji values'''
        assert MessageStatus.PENDING.value == "⏳"
        assert MessageStatus.DOWNLOADING.value == "🔄"
        assert MessageStatus.FAILED.value == "❌"
        assert MessageStatus.SKIPPED.value == "⏭️"

    def test_enum_comparison(self):
        '''Test enum comparison works correctly'''
        assert MessageStatus.FAILED == MessageStatus.FAILED
        assert MessageStatus.FAILED != MessageStatus.DOWNLOADING


class TestMessageFormatterCore:
    '''Test the core format_single_message method'''

    def test_format_single_message_failed_with_error(self):
        '''Test formatting failed message with error'''
        fake_context = generate_fake_context()
        source_dict = fake_source_dict(fake_context)
        result = MessageFormatter.format_single_message(source_dict, MessageStatus.FAILED, "test error")

        expected = f"❌ {source_dict.search_string} (failed: test error)"
        assert result == expected

    def test_format_single_message_skipped_with_reason(self):
        '''Test formatting skipped message with reason'''
        fake_context = generate_fake_context()
        source_dict = fake_source_dict(fake_context)
        result = MessageFormatter.format_single_message(source_dict, MessageStatus.SKIPPED, "already exists")

        expected = f"⏭️ {source_dict.search_string} (skipped: already exists)"
        assert result == expected

    def test_format_single_message_downloading(self):
        '''Test formatting downloading message'''
        fake_context = generate_fake_context()
        source_dict = fake_source_dict(fake_context)
        result = MessageFormatter.format_single_message(source_dict, MessageStatus.DOWNLOADING)

        expected = f"🔄 {source_dict.search_string} (downloading...)"
        assert result == expected

    def test_format_single_message_basic_status(self):
        '''Test formatting with basic status only'''
        fake_context = generate_fake_context()
        source_dict = fake_source_dict(fake_context)
        result = MessageFormatter.format_single_message(source_dict, MessageStatus.PENDING)

        expected = f"⏳ {source_dict.search_string}"
        assert result == expected

    def test_format_single_message_failed_without_error(self):
        '''Test formatting failed message without error message'''
        fake_context = generate_fake_context()
        source_dict = fake_source_dict(fake_context)
        result = MessageFormatter.format_single_message(source_dict, MessageStatus.FAILED)

        expected = f"❌ {source_dict.search_string}"
        assert result == expected

    def test_format_single_message_skipped_without_reason(self):
        '''Test formatting skipped message without reason'''
        fake_context = generate_fake_context()
        source_dict = fake_source_dict(fake_context)
        result = MessageFormatter.format_single_message(source_dict, MessageStatus.SKIPPED)

        expected = f"⏭️ {source_dict.search_string}"
        assert result == expected


class TestMessageFormatterMethods:
    '''Test all the specific formatter methods'''

    def test_format_downloading_message(self):
        '''Test downloading message formatter'''
        fake_context = generate_fake_context()
        source_dict = fake_source_dict(fake_context)
        result = MessageFormatter.format_downloading_message(source_dict)

        expected = f"🔄 {source_dict.search_string} (downloading...)"
        assert result == expected

    def test_format_queue_full_message(self):
        '''Test queue full message formatter'''
        fake_context = generate_fake_context()
        source_dict = fake_source_dict(fake_context)
        result = MessageFormatter.format_play_queue_full_message(source_dict)

        expected = f"❌ {source_dict.search_string} (failed: play queue is full)"
        assert result == expected

    def test_format_play_queue_full_message(self):
        '''Test play queue full message formatter'''
        fake_context = generate_fake_context()
        source_dict = fake_source_dict(fake_context)
        result = MessageFormatter.format_play_queue_full_message(source_dict)

        expected = f"❌ {source_dict.search_string} (failed: play queue is full)"
        assert result == expected

    def test_format_download_failed_message(self):
        '''Test download failed message formatter'''
        fake_context = generate_fake_context()
        source_dict = fake_source_dict(fake_context)
        result = MessageFormatter.format_download_failed_message(source_dict)

        expected = f"❌ {source_dict.search_string} (failed: download failed)"
        assert result == expected

    def test_format_playlist_item_failed_message_default(self):
        '''Test playlist item failed message with default reason'''
        fake_context = generate_fake_context()
        source_dict = fake_source_dict(fake_context)
        result = MessageFormatter.format_playlist_item_failed_message(source_dict)

        expected = f"❌ {source_dict.search_string} (failed: issue generating source)"
        assert result == expected

    def test_format_playlist_item_failed_message_custom(self):
        '''Test playlist item failed message with custom reason'''
        fake_context = generate_fake_context()
        source_dict = fake_source_dict(fake_context)
        result = MessageFormatter.format_playlist_item_failed_message(source_dict, "custom error")

        expected = f"❌ {source_dict.search_string} (failed: custom error)"
        assert result == expected

    def test_format_playlist_item_exists_message(self):
        '''Test playlist item exists message formatter'''
        fake_context = generate_fake_context()
        source_dict = fake_source_dict(fake_context)
        result = MessageFormatter.format_playlist_item_exists_message(source_dict)

        expected = f"⏭️ {source_dict.search_string} (skipped: already exists)"
        assert result == expected

    def test_format_playlist_max_size_message(self):
        '''Test playlist max size message formatter'''
        result = MessageFormatter.format_playlist_max_size_message()

        expected = "❌ Cannot add more items to playlist, already max size"
        assert result == expected

    def test_format_playlist_item_added_message(self):
        '''Test playlist item added message formatter'''
        title = "Test Song Title"
        result = MessageFormatter.format_playlist_item_added_message(title)

        expected = f'Added item "{title}" to playlist'
        assert result == expected

    def test_format_history_playlist_error(self):
        '''Test history playlist error message formatter'''
        result = MessageFormatter.format_history_playlist_error()

        expected = "❌ Cannot add to history playlist, is reserved and cannot be added to manually"
        assert result == expected

    def test_format_no_videos_message(self):
        '''Test no videos message formatter'''
        result = MessageFormatter.format_no_videos_message()

        expected = "❌ There are no videos to add to playlist"
        assert result == expected

    def test_format_finished_playlist_message(self):
        '''Test finished playlist message formatter'''
        playlist_name = "My Playlist"
        result = MessageFormatter.format_finished_playlist_message(playlist_name)

        expected = f'Finished adding items to playlist "{playlist_name}"'
        assert result == expected


class TestMessageFormatterEdgeCases:
    '''Test edge cases and special scenarios'''

    def test_format_with_special_characters_in_search_string(self):
        '''Test formatting with special characters in search string'''
        fake_context = generate_fake_context()
        source_dict = fake_source_dict(fake_context)
        source_dict.search_string = 'Song "With Quotes" & Special-Chars!'

        result = MessageFormatter.format_downloading_message(source_dict)
        expected = '🔄 Song "With Quotes" & Special-Chars! (downloading...)'
        assert result == expected

    def test_format_with_url_in_search_string(self):
        '''Test formatting when search string contains URL'''
        fake_context = generate_fake_context()
        source_dict = fake_source_dict(fake_context)
        source_dict.search_string = 'https://youtube.com/watch?v=12345'

        result = MessageFormatter.format_downloading_message(source_dict)
        expected = '🔄 <https://youtube.com/watch?v=12345> (downloading...)'
        assert result == expected

    def test_format_with_empty_error_message(self):
        '''Test formatting with empty error message'''
        fake_context = generate_fake_context()
        source_dict = fake_source_dict(fake_context)
        result = MessageFormatter.format_single_message(source_dict, MessageStatus.FAILED, "")

        # Empty error message should not trigger the error format
        expected = f"❌ {source_dict.search_string}"
        assert result == expected

    def test_format_with_none_error_message(self):
        '''Test formatting with None error message'''
        fake_context = generate_fake_context()
        source_dict = fake_source_dict(fake_context)
        result = MessageFormatter.format_single_message(source_dict, MessageStatus.FAILED, None)

        expected = f"❌ {source_dict.search_string}"
        assert result == expected

    def test_format_with_long_search_string(self):
        '''Test formatting with very long search string'''
        fake_context = generate_fake_context()
        source_dict = fake_source_dict(fake_context)
        source_dict.search_string = "This is a very long search string " * 10

        result = MessageFormatter.format_downloading_message(source_dict)
        expected = f"🔄 {source_dict.search_string} (downloading...)"
        assert result == expected

    def test_format_with_unicode_characters(self):
        '''Test formatting with unicode characters'''
        fake_context = generate_fake_context()
        source_dict = fake_source_dict(fake_context)
        source_dict.search_string = "Song with émojis 🎵 and üñíçødé"

        result = MessageFormatter.format_downloading_message(source_dict)
        expected = "🔄 Song with émojis 🎵 and üñíçødé (downloading...)"
        assert result == expected

    def test_format_playlist_item_added_with_special_title(self):
        '''Test playlist item added with special characters in title'''
        title = 'Song "Title" with & special chars!'
        result = MessageFormatter.format_playlist_item_added_message(title)

        expected = f'Added item "{title}" to playlist'
        assert result == expected

    def test_format_finished_playlist_with_special_name(self):
        '''Test finished playlist with special characters in name'''
        playlist_name = 'Playlist "Name" with & special chars!'
        result = MessageFormatter.format_finished_playlist_message(playlist_name)

        expected = f'Finished adding items to playlist "{playlist_name}"'
        assert result == expected


class TestMessageFormatterIntegration:
    '''Integration tests with different SearchType values'''

    def test_format_with_spotify_search_type(self):
        '''Test formatting with Spotify search type'''
        fake_context = generate_fake_context()
        source_dict = fake_source_dict(fake_context)
        source_dict.search_type = SearchType.SPOTIFY
        source_dict.search_string = "https://open.spotify.com/track/123456"

        result = MessageFormatter.format_downloading_message(source_dict)
        expected = "🔄 <https://open.spotify.com/track/123456> (downloading...)"
        assert result == expected

    def test_format_with_youtube_search_type(self):
        '''Test formatting with YouTube search type'''
        fake_context = generate_fake_context()
        source_dict = fake_source_dict(fake_context)
        source_dict.search_type = SearchType.YOUTUBE
        source_dict.search_string = "https://youtube.com/watch?v=123"

        result = MessageFormatter.format_downloading_message(source_dict)
        expected = "🔄 <https://youtube.com/watch?v=123> (downloading...)"
        assert result == expected

    def test_format_with_search_search_type(self):
        '''Test formatting with generic search type'''
        fake_context = generate_fake_context()
        source_dict = fake_source_dict(fake_context)
        source_dict.search_type = SearchType.SEARCH
        source_dict.search_string = "some song name"

        result = MessageFormatter.format_downloading_message(source_dict)
        expected = "🔄 some song name (downloading...)"
        assert result == expected

    def test_format_with_direct_search_type(self):
        '''Test formatting with direct URL search type'''
        fake_context = generate_fake_context()
        source_dict = fake_source_dict(fake_context, is_direct_search=True)
        source_dict.search_type = SearchType.DIRECT
        source_dict.search_string = "https://example.com/audio.mp3"

        result = MessageFormatter.format_downloading_message(source_dict)
        expected = "🔄 <https://example.com/audio.mp3> (downloading...)"
        assert result == expected
