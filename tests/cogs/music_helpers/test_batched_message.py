from unittest.mock import MagicMock
import pytest

from discord_bot.cogs.music_helpers.batched_message import BatchedMessageItem
from discord_bot.cogs.music_helpers.message_formatter import MessageStatus, _format_search_string_for_discord

from tests.helpers import generate_fake_context, fake_source_dict
from tests.helpers import fake_engine #pylint:disable=unused-import


def test_batched_message_item_creation():
    """Test basic BatchedMessageItem creation"""
    guild_id = 12345
    batch = BatchedMessageItem(guild_id, batch_size=5, auto_delete_after=15)

    assert batch.guild_id == guild_id
    assert batch.batch_size == 5
    assert batch.auto_delete_after == 15
    assert batch.total_items == 0
    assert batch.completed_count == 0
    assert batch.failed_count == 0
    assert not batch.is_batch_full()
    assert not batch.is_processing_complete()


def test_add_source_dict_to_batch():
    """Test adding SourceDicts to batch"""
    fake_context = generate_fake_context()
    batch = BatchedMessageItem(fake_context['guild'].id, batch_size=3)

    # Add first item
    source_dict1 = fake_source_dict(fake_context)
    result = batch.add_source_dict(source_dict1)

    assert result is True
    assert batch.total_items == 1
    assert len(batch.source_dicts) == 1
    assert str(source_dict1.uuid) in batch.status_map
    assert batch.status_map[str(source_dict1.uuid)] == MessageStatus.PENDING
    assert source_dict1.batch_id == batch.batch_id

    # Add more items until full
    source_dict2 = fake_source_dict(fake_context)
    source_dict3 = fake_source_dict(fake_context)
    batch.add_source_dict(source_dict2)
    batch.add_source_dict(source_dict3)

    assert batch.total_items == 3
    assert batch.is_batch_full()

    # Try to add one more (should fail)
    source_dict4 = fake_source_dict(fake_context)
    result = batch.add_source_dict(source_dict4)
    assert result is False
    assert batch.total_items == 3


def test_update_item_status():
    """Test updating individual item status"""
    fake_context = generate_fake_context()
    batch = BatchedMessageItem(fake_context['guild'].id)

    source_dict = fake_source_dict(fake_context)
    batch.add_source_dict(source_dict)

    # Update to downloading
    result = batch.update_item_status(str(source_dict.uuid), MessageStatus.DOWNLOADING)
    assert result is True
    assert batch.status_map[str(source_dict.uuid)] == MessageStatus.DOWNLOADING

    # Update to completed
    result = batch.update_item_status(str(source_dict.uuid), MessageStatus.COMPLETED)
    assert result is True
    assert batch.status_map[str(source_dict.uuid)] == MessageStatus.COMPLETED
    assert batch.completed_count == 1

    # Try to update non-existent item
    result = batch.update_item_status("fake-uuid", MessageStatus.FAILED)
    assert result is False


def test_update_item_status_with_errors():
    """Test updating item status with error messages"""
    fake_context = generate_fake_context()
    batch = BatchedMessageItem(fake_context['guild'].id)

    source_dict = fake_source_dict(fake_context)
    batch.add_source_dict(source_dict)

    # Update to failed with error
    error_msg = "video unavailable"
    result = batch.update_item_status(str(source_dict.uuid), MessageStatus.FAILED, error_msg)
    assert result is True
    assert batch.status_map[str(source_dict.uuid)] == MessageStatus.FAILED
    assert batch.error_map[str(source_dict.uuid)] == error_msg
    assert batch.failed_count == 1


def test_get_visible_items():
    """Test getting items that should be visible in message"""
    fake_context = generate_fake_context()
    batch = BatchedMessageItem(fake_context['guild'].id)

    # Add multiple items with different statuses
    pending_dict = fake_source_dict(fake_context)
    downloading_dict = fake_source_dict(fake_context)
    completed_dict = fake_source_dict(fake_context)
    failed_dict = fake_source_dict(fake_context)

    batch.add_source_dict(pending_dict)
    batch.add_source_dict(downloading_dict)
    batch.add_source_dict(completed_dict)
    batch.add_source_dict(failed_dict)

    # Update statuses
    batch.update_item_status(str(downloading_dict.uuid), MessageStatus.DOWNLOADING)
    batch.update_item_status(str(completed_dict.uuid), MessageStatus.COMPLETED)
    batch.update_item_status(str(failed_dict.uuid), MessageStatus.FAILED, "error")

    visible_items = batch.get_visible_items()

    # Should see pending, downloading, failed - but NOT completed
    assert len(visible_items) == 3
    visible_uuids = [str(item[1].uuid) for item in visible_items]
    assert str(pending_dict.uuid) in visible_uuids
    assert str(downloading_dict.uuid) in visible_uuids
    assert str(failed_dict.uuid) in visible_uuids
    assert str(completed_dict.uuid) not in visible_uuids


def test_generate_message_content_initial():
    """Test generating initial message content"""
    fake_context = generate_fake_context()
    batch = BatchedMessageItem(fake_context['guild'].id)

    # Add some items
    for i in range(3):
        source_dict = fake_source_dict(fake_context)
        source_dict.search_string = f"Test Song {i+1}"
        batch.add_source_dict(source_dict)

    content = batch.generate_message_content()

    assert "Processing (0/3 items)" in content
    assert "⏳ 1. Test Song 1" in content
    assert "⏳ 2. Test Song 2" in content
    assert "⏳ 3. Test Song 3" in content


def test_generate_message_content_in_progress():
    """Test generating message content during processing"""
    fake_context = generate_fake_context()
    batch = BatchedMessageItem(fake_context['guild'].id)

    # Add items with different statuses
    pending_dict = fake_source_dict(fake_context)
    pending_dict.search_string = "Pending Song"
    downloading_dict = fake_source_dict(fake_context)
    downloading_dict.search_string = "Downloading Song"
    completed_dict = fake_source_dict(fake_context)
    completed_dict.search_string = "Completed Song"
    failed_dict = fake_source_dict(fake_context)
    failed_dict.search_string = "Failed Song"

    batch.add_source_dict(pending_dict)
    batch.add_source_dict(downloading_dict)
    batch.add_source_dict(completed_dict)
    batch.add_source_dict(failed_dict)

    # Update statuses
    batch.update_item_status(str(downloading_dict.uuid), MessageStatus.DOWNLOADING)
    batch.update_item_status(str(completed_dict.uuid), MessageStatus.COMPLETED)
    batch.update_item_status(str(failed_dict.uuid), MessageStatus.FAILED, "video unavailable")

    content = batch.generate_message_content()

    assert "Processing (2/4 items, 1 succeeded, 1 failed)" in content
    assert "⏳ 1. Pending Song" in content
    assert "🔄 2. Downloading Song (downloading...)" in content
    # Completed song should NOT appear
    assert "Completed Song" not in content
    assert "❌ 4. Failed Song (failed: video unavailable)" in content


def test_generate_message_content_complete():
    """Test generating final message content when processing is complete"""
    fake_context = generate_fake_context()
    batch = BatchedMessageItem(fake_context['guild'].id, auto_delete_after=30)

    # Add items
    completed_dict = fake_source_dict(fake_context)
    completed_dict.search_string = "Success Song"
    failed_dict = fake_source_dict(fake_context)
    failed_dict.search_string = "Failed Song"

    batch.add_source_dict(completed_dict)
    batch.add_source_dict(failed_dict)

    # Complete processing
    batch.update_item_status(str(completed_dict.uuid), MessageStatus.COMPLETED)
    batch.update_item_status(str(failed_dict.uuid), MessageStatus.FAILED, "age restricted")

    content = batch.generate_message_content()

    assert "Multi-video Input Processing Complete (1/2 succeeded, 1 failed)" in content
    assert "❌ 2. Failed Song (failed: age restricted)" in content
    assert "✅ 1 video successfully added to queue" in content
    assert "❌ 1 video failed to process" in content
    # Completed song should not appear
    assert "Success Song" not in content


def test_is_processing_complete():
    """Test checking if all items are processed"""
    fake_context = generate_fake_context()
    batch = BatchedMessageItem(fake_context['guild'].id)

    source_dict1 = fake_source_dict(fake_context)
    source_dict2 = fake_source_dict(fake_context)
    batch.add_source_dict(source_dict1)
    batch.add_source_dict(source_dict2)

    # Initially not complete
    assert not batch.is_processing_complete()

    # One downloading, still not complete
    batch.update_item_status(str(source_dict1.uuid), MessageStatus.DOWNLOADING)
    assert not batch.is_processing_complete()

    # Both completed/failed, now complete
    batch.update_item_status(str(source_dict1.uuid), MessageStatus.COMPLETED)
    batch.update_item_status(str(source_dict2.uuid), MessageStatus.FAILED)
    assert batch.is_processing_complete()


def test_should_auto_delete():
    """Test auto-delete logic"""
    fake_context = generate_fake_context()
    batch = BatchedMessageItem(fake_context['guild'].id)

    source_dict = fake_source_dict(fake_context)
    batch.add_source_dict(source_dict)

    # Not complete, should not auto-delete
    assert not batch.should_auto_delete()
    assert batch.get_delete_after() is None

    # Complete, should auto-delete
    batch.update_item_status(str(source_dict.uuid), MessageStatus.COMPLETED)
    assert batch.should_auto_delete()
    assert batch.get_delete_after() == batch.auto_delete_after


def test_character_limit_protection():
    """Test that batch splits items across multiple messages"""
    fake_context = generate_fake_context()
    batch = BatchedMessageItem(fake_context['guild'].id, batch_size=50, items_per_message=10)  # Large batch with 10 items per message

    # Add many items to test multi-message functionality
    for i in range(25):
        source_dict = fake_source_dict(fake_context)
        source_dict.search_string = f"Song Title {i+1} - Artist Name"
        batch.add_source_dict(source_dict)

    # Should need 3 messages for 25 items (10 + 10 + 5)
    assert batch.get_required_message_count() == 3

    # Test message groups
    message_groups = batch.get_message_groups()
    assert len(message_groups) == 3
    assert len(message_groups[0]) == 10  # First message has 10 items
    assert len(message_groups[1]) == 10  # Second message has 10 items
    assert len(message_groups[2]) == 5   # Third message has 5 items

    # Test content generation for each message
    content_0 = batch.generate_message_content(0)
    content_1 = batch.generate_message_content(1)
    content_2 = batch.generate_message_content(2)

    # All messages should have the same header with message indicator
    assert "[Message 1/3]" in content_0
    assert "[Message 2/3]" in content_1
    assert "[Message 3/3]" in content_2


@pytest.mark.asyncio
async def test_message_operations():
    """Test Discord message operations"""
    fake_context = generate_fake_context()
    batch = BatchedMessageItem(fake_context['guild'].id)

    # Mock Discord message with async methods
    mock_message = MagicMock()
    mock_message.edit = MagicMock(return_value=None)
    mock_message.delete = MagicMock(return_value=None)

    # Make the methods awaitable
    async def mock_edit(*args, **kwargs):
        mock_message.edit(*args, **kwargs)

    async def mock_delete(*args, **kwargs):
        mock_message.delete(*args, **kwargs)

    mock_message.edit = mock_edit
    mock_message.delete = mock_delete

    batch.set_message(mock_message)

    assert batch.messages[0] == mock_message

    # Test edit message (can't easily test call args with async mock)
    await batch.edit_message("test content", delete_after=30)

    # Test delete message
    await batch.delete_message()


def test_failed_items_display():
    """Test that failed items are displayed correctly with counts"""
    fake_context = generate_fake_context()
    batch = BatchedMessageItem(fake_context['guild'].id, items_per_message=10)

    # Add multiple items with mixed success/failure
    for i in range(5):
        source_dict = fake_source_dict(fake_context)
        source_dict.search_string = f"Song {i+1}"
        batch.add_source_dict(source_dict)

    # Set different statuses: 2 completed, 2 failed, 1 pending
    uuids = [str(sd.uuid) for sd in batch.source_dicts]
    batch.update_item_status(uuids[0], MessageStatus.COMPLETED)
    batch.update_item_status(uuids[1], MessageStatus.COMPLETED)
    batch.update_item_status(uuids[2], MessageStatus.FAILED, "network error")
    batch.update_item_status(uuids[3], MessageStatus.FAILED, "video unavailable")
    # uuids[4] stays PENDING

    # Test in-progress header
    content = batch.generate_message_content()
    assert "Processing (4/5 items, 2 succeeded, 2 failed)" in content

    # Complete the last item
    batch.update_item_status(uuids[4], MessageStatus.COMPLETED)

    # Test completed header and summary
    content = batch.generate_message_content()
    assert "Multi-video Input Processing Complete (3/5 succeeded, 2 failed)" in content
    assert "✅ 3 videos successfully added to queue" in content
    assert "❌ 2 videos failed to process" in content


def test_failed_items_only_success_display():
    """Test display when only successful items exist"""
    fake_context = generate_fake_context()
    batch = BatchedMessageItem(fake_context['guild'].id)

    # Add items that all succeed
    for i in range(3):
        source_dict = fake_source_dict(fake_context)
        source_dict.search_string = f"Success Song {i+1}"
        batch.add_source_dict(source_dict)

    # Complete all successfully
    for source_dict in batch.source_dicts:
        batch.update_item_status(str(source_dict.uuid), MessageStatus.COMPLETED)

    content = batch.generate_message_content()
    assert "Multi-video Input Processing Complete (3/3 succeeded)" in content
    assert "✅ 3 videos successfully added to queue" in content
    assert "❌" not in content  # No failure message should appear


def test_failed_items_only_failure_display():
    """Test display when only failed items exist"""  
    fake_context = generate_fake_context()
    batch = BatchedMessageItem(fake_context['guild'].id)

    # Add items that all fail
    for i in range(2):
        source_dict = fake_source_dict(fake_context)
        source_dict.search_string = f"Failed Song {i+1}"
        batch.add_source_dict(source_dict)

    # Fail all items
    batch.update_item_status(str(batch.source_dicts[0].uuid), MessageStatus.FAILED, "error 1")
    batch.update_item_status(str(batch.source_dicts[1].uuid), MessageStatus.FAILED, "error 2")

    content = batch.generate_message_content()
    assert "Multi-video Input Processing Complete (0/2 succeeded, 2 failed)" in content
    assert "✅" not in content  # No success message should appear
    assert "❌ 2 videos failed to process" in content


def test_format_search_string_for_discord():
    """Test URL formatting to prevent Discord embeds"""
    # Test HTTP URL
    result = _format_search_string_for_discord("https://www.youtube.com/watch?v=aB3cD4eF5gH")
    assert result == "<https://www.youtube.com/watch?v=aB3cD4eF5gH>"

    # Test HTTPS URL
    result = _format_search_string_for_discord("http://example.com/test")
    assert result == "<http://example.com/test>"

    # Test regular text (no change)
    result = _format_search_string_for_discord("Test Song Artist Name")
    assert result == "Test Song Artist Name"

    # Test mixed content
    result = _format_search_string_for_discord("Check out https://youtube.com/watch?v=123 for music")
    assert result == "Check out <https://youtube.com/watch?v=123> for music"

    # Test already wrapped URL (should not double-wrap)
    result = _format_search_string_for_discord("<https://youtube.com/watch?v=456>")
    assert result == "<https://youtube.com/watch?v=456>"

    # Test Spotify URI (should not be wrapped)
    result = _format_search_string_for_discord("https://open.spotify.com/track/1a2B3c4D5e6F7g8H9i0J1k")
    assert result == "<https://open.spotify.com/track/1a2B3c4D5e6F7g8H9i0J1k>"

    # Test multiple URLs
    result = _format_search_string_for_discord("Visit https://site1.com and https://site2.com")
    assert result == "Visit <https://site1.com> and <https://site2.com>"


def test_delete_after_behavior():
    """Test that delete_after is only applied when processing is complete"""

    fake_context = generate_fake_context()
    batch = BatchedMessageItem(
        guild_id=fake_context['guild'].id,
        channel_id=fake_context['channel'].id,
        auto_delete_after=30
    )

    # Add some items
    source_dict1 = fake_source_dict(fake_context)
    source_dict2 = fake_source_dict(fake_context)
    batch.add_source_dict(source_dict1)
    batch.add_source_dict(source_dict2)

    # Initially, processing not complete, should return None for delete_after
    assert not batch.is_processing_complete()
    assert batch.get_delete_after() is None

    # Complete one item, still not complete overall
    batch.update_item_status(str(source_dict1.uuid), MessageStatus.COMPLETED)
    assert not batch.is_processing_complete()  # Still has one pending
    assert batch.get_delete_after() is None

    # Complete the second item, now processing is complete
    batch.update_item_status(str(source_dict2.uuid), MessageStatus.COMPLETED)
    assert batch.is_processing_complete()  # All items processed
    assert batch.get_delete_after() == 30  # Should return the timeout value


def test_delete_after_behavior_with_failures():
    """Test delete_after behavior when processing completes with failures"""

    fake_context = generate_fake_context()
    batch = BatchedMessageItem(
        guild_id=fake_context['guild'].id,
        channel_id=fake_context['channel'].id,
        auto_delete_after=60
    )

    source_dict1 = fake_source_dict(fake_context)
    source_dict2 = fake_source_dict(fake_context)
    batch.add_source_dict(source_dict1)
    batch.add_source_dict(source_dict2)

    # Complete one, fail one - should be considered complete
    batch.update_item_status(str(source_dict1.uuid), MessageStatus.COMPLETED)
    batch.update_item_status(str(source_dict2.uuid), MessageStatus.FAILED, "test error")

    assert batch.is_processing_complete()
    assert batch.get_delete_after() == 60
