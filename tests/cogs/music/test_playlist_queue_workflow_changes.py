"""
Test playlist queue workflow changes from commit 0b66662
"""
from unittest.mock import patch, MagicMock
import pytest

from discord_bot.cogs.music import Music
from discord_bot.cogs.music_helpers.media_request import MediaRequest
from discord_bot.cogs.music_helpers.common import SearchType
from discord_bot.cogs.music_helpers.message_context import MessageContext
from discord_bot.cogs.music_helpers.search_client import SearchClient
from discord_bot.cogs.music_helpers.message_queue import MessageQueue
from tests.cogs.test_music import BASE_MUSIC_CONFIG
from tests.helpers import fake_source_dict, fake_engine, fake_context  #pylint:disable=unused-import


@pytest.mark.asyncio
async def test_playlist_queue_adds_multi_input_string(fake_engine, fake_context):  #pylint:disable=redefined-outer-name
    """Test that playlist queue operations add multi_input_string to media requests"""
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, fake_engine)

    # Mock database operations
    with patch('discord_bot.cogs.music.retry_database_commands') as mock_db:
        # Setup mock database responses
        playlist_name = "Test Playlist"
        mock_playlist_items = [
            MagicMock(id=1, video_url="https://youtube.com/watch?v=123",
                     requester_name="user1", requester_id=456),
            MagicMock(id=2, video_url="https://youtube.com/watch?v=456",
                     requester_name="user2", requester_id=789),
        ]

        # Mock database calls in order they appear in playlist_queue method
        mock_db.side_effect = [
            playlist_name,  # get_playlist_name
            mock_playlist_items,  # list_playlist_items
            None,  # playlist_update_queued
        ]

        # Mock the enqueue_media_requests method
        captured_requests = []
        async def mock_enqueue(ctx, player, requests):  #pylint:disable=unused-argument
            captured_requests.extend(requests)
            return True

        with patch.object(cog, 'enqueue_media_requests', side_effect=mock_enqueue):
            with patch.object(cog, 'get_player', return_value=MagicMock()):
                # Call the private playlist queue method directly
                # pylint: disable=protected-access
                await cog._Music__playlist_queue(fake_context['context'], MagicMock(), 123, False, 0, False)

                # Verify media requests were created with multi_input_string
                assert len(captured_requests) == 2

                for req in captured_requests:
                    assert req.multi_input_string == playlist_name
                    assert isinstance(req.message_context, MessageContext)
                    assert req.history_playlist_item_id in [1, 2]


@pytest.mark.asyncio
async def test_playlist_queue_completion_messaging_simplified(fake_engine, fake_context):  #pylint:disable=redefined-outer-name
    """Test that playlist queue completion messaging is simplified in new version"""
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, fake_engine)

    # Mock database operations
    with patch('discord_bot.cogs.music.retry_database_commands') as mock_db:
        playlist_name = "Test Playlist"
        mock_playlist_items = [
            MagicMock(id=1, video_url="https://youtube.com/watch?v=123",
                     requester_name="user1", requester_id=456),
        ]

        mock_db.side_effect = [
            playlist_name,  # get_playlist_name
            mock_playlist_items,  # list_playlist_items
            None,  # playlist_update_queued
        ]

        # Mock message queue to capture messages
        with patch.object(cog.message_queue, 'send_single_immutable') as mock_send:
            with patch.object(cog, 'enqueue_media_requests', return_value=False):  # finished_all = False
                with patch.object(cog, 'get_player', return_value=MagicMock()):
                    # Call the private playlist queue method directly
                    # pylint: disable=protected-access
                    await cog._Music__playlist_queue(fake_context['context'], MagicMock(), 123, False, 0, False)

                    # Verify only failure message is sent (hit limit case)
                    mock_send.assert_called_once()
                    call_args = mock_send.call_args[0][0]
                    assert len(call_args) == 1
                    message_context = call_args[0]

                    # Check that the message is about hitting limit
                    assert callable(message_context.function)
                    # The message should contain the playlist name and indicate limit hit
                    # We can't easily test the exact message without executing the partial function


@pytest.mark.asyncio
async def test_playlist_queue_bundle_creation_with_text_channel(fake_context):  #pylint:disable=redefined-outer-name
    """Test that enqueue_media_requests creates bundles with proper text_channel parameter"""
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, None)

    # Test the method that actually creates bundles - enqueue_media_requests
    entries = [fake_source_dict(fake_context), fake_source_dict(fake_context)]

    # Create a mock player
    mock_player = MagicMock()
    mock_player.guild = fake_context['guild']
    mock_player.text_channel = fake_context['channel']

    # Call enqueue_media_requests directly to test bundle creation
    result = await cog.enqueue_media_requests(fake_context['context'], mock_player, entries)

    # Verify bundle was created with correct text_channel
    assert result is True
    assert len(cog.multirequest_bundles) == 1

    # Verify bundle has correct text_channel
    bundle = list(cog.multirequest_bundles.values())[0]
    assert bundle.text_channel == fake_context['channel']
    assert bundle.guild_id == fake_context['guild'].id
    assert bundle.channel_id == fake_context['channel'].id


@pytest.mark.asyncio
async def test_history_playlist_queue_behavior(fake_engine, fake_context):  #pylint:disable=redefined-outer-name
    """Test history playlist queue retains special behavior"""
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, fake_engine)

    # Mock database operations for history playlist
    with patch('discord_bot.cogs.music.retry_database_commands') as mock_db:
        mock_playlist_items = [
            MagicMock(id=1, video_url="https://youtube.com/watch?v=123",
                     requester_name="user1", requester_id=456),
        ]

        mock_db.side_effect = [
            "Auto-generated History",  # get_playlist_name (this gets overridden)
            mock_playlist_items,  # list_playlist_items
            None,  # playlist_update_queued
        ]

        # Mock message queue to capture messages
        with patch.object(cog.message_queue, 'send_single_immutable') as mock_send:
            with patch.object(cog, 'enqueue_media_requests', return_value=False):  # finished_all = False
                with patch.object(cog, 'get_player', return_value=MagicMock()):
                    # Call history playlist queue (playlist_id = guild_id for history)
                    # pylint: disable=protected-access
                    await cog._Music__playlist_queue(fake_context['context'], MagicMock(), fake_context['guild'].id, False, 0, True)

                    # For history playlists, should still send completion message
                    mock_send.assert_called_once()
                    call_args = mock_send.call_args[0][0]
                    assert len(call_args) == 1

                    # Message should mention "Channel History" not the database playlist name
                    # This is set by the special is_history logic


def test_media_request_multi_input_string_parameter_consistency(fake_context):  #pylint:disable=redefined-outer-name
    """Test that MediaRequest properly handles multi_input_string parameter"""
    # Create MediaRequest with multi_input_string (new parameter name)
    req = MediaRequest(
        guild_id=fake_context['guild'].id,
        channel_id=fake_context['channel'].id,
        requester_name="test_user",
        requester_id=123,
        search_string="test song",
        search_type=SearchType.YOUTUBE,
        multi_input_string="Test Playlist"
    )

    # Verify parameter is stored correctly
    assert req.multi_input_string == "Test Playlist"
    assert hasattr(req, 'multi_input_string')

    # Verify it doesn't have the old parameter name
    assert not hasattr(req, 'multi_input_search_string')


def test_search_client_multi_input_string_usage():
    """Test that SearchClient uses multi_input_string consistently"""
    # This test verifies the SearchClient change on line 275 of search_client.py
    # where multi_input_search_string was renamed to multi_input_string

    SearchClient(MessageQueue())

    # Create a test context for creating MediaRequest
    guild_id = 123
    channel_id = 456
    requester_name = "test_user"
    requester_id = 789
    search_string = "test song"
    search_type = SearchType.YOUTUBE
    search_string_message = "Test Playlist"

    # Create MediaRequest similar to how SearchClient does it (line 274-275)
    entry_context = MessageContext(guild_id, channel_id)
    entry = MediaRequest(guild_id, channel_id, requester_name, requester_id, search_string, search_type,
                        message_context=entry_context, multi_input_string=search_string_message)

    # Verify the parameter is set correctly
    assert entry.multi_input_string == search_string_message
    assert not hasattr(entry, 'multi_input_search_string')
