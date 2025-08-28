"""
Test edge cases for media request functionality
"""
import pytest

from discord_bot.cogs.music_helpers.media_request import chunk_list, MultiMediaRequestBundle, MediaRequest
from discord_bot.cogs.music_helpers.common import MediaRequestLifecycleStage
from discord_bot.cogs.music_helpers.message_queue import MessageQueue, MessageQueueException
from tests.helpers import fake_source_dict, fake_context  #pylint:disable=unused-import


def test_chunk_list_edge_cases():
    """Test chunk_list function with edge cases"""
    # Test empty list
    result = chunk_list([], 5)
    assert result == []

    # Test size 0 (should be clamped to 1)
    result = chunk_list([1, 2, 3], 0)
    assert result == [[1], [2], [3]]

    # Test negative size (should be clamped to 1)
    result = chunk_list([1, 2, 3], -5)
    assert result == [[1], [2], [3]]

    # Test size larger than list
    result = chunk_list([1, 2], 10)
    assert result == [[1, 2]]

    # Test exact divisible chunks
    result = chunk_list([1, 2, 3, 4], 2)
    assert result == [[1, 2], [3, 4]]

    # Test non-divisible chunks
    result = chunk_list([1, 2, 3, 4, 5], 2)
    assert result == [[1, 2], [3, 4], [5]]


def test_bundle_override_message_functionality(fake_context):  #pylint:disable=redefined-outer-name
    """Test override_message functionality in bundle print"""
    bundle = MultiMediaRequestBundle(fake_context['guild'].id, fake_context['channel'].id, fake_context['channel'])

    # Add request with override message
    req = fake_source_dict(fake_context)
    bundle.add_media_request(req)

    # Update with override message
    bundle.update_request_status(req, MediaRequestLifecycleStage.FAILED,
                                failure_reason="Original failure",
                                override_message="Custom override message")

    # Test that override message is used instead of default formatting
    messages = bundle.print()
    assert len(messages) == 1
    assert "Custom override message" in messages[0]
    assert "Original failure" not in messages[0]  # Original failure reason should be ignored


def test_bundle_items_per_message_edge_cases(fake_context):  #pylint:disable=redefined-outer-name
    """Test bundle with various items_per_message edge cases"""
    # Test zero gets clamped to 1
    bundle_zero = MultiMediaRequestBundle(123, 456, fake_context['channel'], items_per_message=0)
    assert bundle_zero.items_per_message == 1

    # Test negative gets clamped to 1
    bundle_negative = MultiMediaRequestBundle(123, 456, fake_context['channel'], items_per_message=-5)
    assert bundle_negative.items_per_message == 1

    # Test large number gets clamped to 5
    bundle_large = MultiMediaRequestBundle(123, 456, fake_context['channel'], items_per_message=100)
    assert bundle_large.items_per_message == 5

    # Test edge of valid range
    bundle_valid = MultiMediaRequestBundle(123, 456, fake_context['channel'], items_per_message=3)
    assert bundle_valid.items_per_message == 3


def test_bundle_empty_message_list(fake_context):  #pylint:disable=redefined-outer-name
    """Test bundle when all items are completed/discarded (empty message list)"""
    bundle = MultiMediaRequestBundle(123, 456, fake_context['channel'])

    # Add request that will be completed (shouldn't appear in messages)
    req = MediaRequest(123, 456, "user", 1, "search", "search", download_file=True)
    bundle.add_media_request(req)
    bundle.update_request_status(req, MediaRequestLifecycleStage.COMPLETED)

    # Print should return empty list when no messages to display
    messages = bundle.print()
    assert not messages


def test_bundle_single_item_no_status_header(fake_context):  #pylint:disable=redefined-outer-name
    """Test that single-item bundles don't include status header"""
    bundle = MultiMediaRequestBundle(123, 456, fake_context['channel'])

    # Add single failed request
    req = MediaRequest(123, 456, "user", 1, "search", "search", download_file=True)
    bundle.add_media_request(req)
    bundle.update_request_status(req, MediaRequestLifecycleStage.FAILED, failure_reason="Test failure")

    messages = bundle.print()
    # Should not include top-level status since total == 1
    assert len(messages) == 1
    assert "Test failure" in messages[0]
    assert "downloaded successfully" not in messages[0]  # No status header


def test_bundle_multiple_items_includes_status_header(fake_context):  #pylint:disable=redefined-outer-name
    """Test that multi-item bundles include status header"""
    bundle = MultiMediaRequestBundle(fake_context['guild'].id, fake_context['channel'].id, fake_context['channel'])
    bundle.multi_input_string = "test-playlist"

    # Add multiple requests
    for i in range(3):
        req = fake_source_dict(fake_context)
        bundle.add_media_request(req)
        if i == 0:
            bundle.update_request_status(req, MediaRequestLifecycleStage.COMPLETED)
        elif i == 1:
            bundle.update_request_status(req, MediaRequestLifecycleStage.FAILED)
        else:
            bundle.update_request_status(req, MediaRequestLifecycleStage.QUEUED)

    messages = bundle.print()
    # Should include status header since total > 1
    full_message = "\n".join(messages)
    assert "Downloading" in full_message
    assert "test-playlist" in full_message
    assert "1/3 items downloaded successfully, 1 failed" in full_message


def test_message_queue_none_channel_validation(fake_context):  #pylint:disable=redefined-outer-name
    """Test that MessageQueue properly validates None text_channel parameter with MessageQueueException"""
    message_queue = MessageQueue()

    # Test 1: Creating new bundle with valid channel should work
    bundle_name = "test-bundle-1"
    result = message_queue.update_multiple_mutable(bundle_name, fake_context['channel'])
    assert result is True
    assert bundle_name in message_queue.mutable_bundles

    # Test 2: Updating existing bundle with None channel should work (bundle already exists)
    result = message_queue.update_multiple_mutable(bundle_name, None)
    assert result is True

    # Test 3: Creating new bundle with None channel should raise MessageQueueException
    new_bundle_name = "test-bundle-2"
    with pytest.raises(MessageQueueException) as exc_info:
        message_queue.update_multiple_mutable(new_bundle_name, None)

    assert "Cannot create new message bundle" in str(exc_info.value)
    assert new_bundle_name in str(exc_info.value)
    assert new_bundle_name not in message_queue.mutable_bundles  # Bundle should not be created


def test_bundle_finished_successfully_property(fake_context):  #pylint:disable=redefined-outer-name
    """Test the new finished_successfully property behavior"""
    bundle = MultiMediaRequestBundle(fake_context['guild'].id, fake_context['channel'].id, fake_context['channel'])

    # Empty bundle is considered "finished successfully" (0 == 0)
    assert bundle.finished_successfully

    # Add multiple requests
    req1 = fake_source_dict(fake_context)
    req2 = fake_source_dict(fake_context)
    req3 = fake_source_dict(fake_context)
    bundle.add_media_request(req1)
    bundle.add_media_request(req2)
    bundle.add_media_request(req3)

    # Still not finished
    assert not bundle.finished_successfully

    # Complete first request - still not finished
    bundle.update_request_status(req1, MediaRequestLifecycleStage.COMPLETED)
    assert not bundle.finished_successfully

    # Fail second request - still not finished successfully (has failures)
    bundle.update_request_status(req2, MediaRequestLifecycleStage.FAILED)
    assert not bundle.finished_successfully

    # Discard third request - still not finished successfully (has failures)
    bundle.update_request_status(req3, MediaRequestLifecycleStage.DISCARDED)
    assert not bundle.finished_successfully

    # Test scenario where all are completed or discarded (no failures)
    bundle2 = MultiMediaRequestBundle(fake_context['guild'].id, fake_context['channel'].id, fake_context['channel'])
    req4 = fake_source_dict(fake_context)
    req5 = fake_source_dict(fake_context)
    bundle2.add_media_request(req4)
    bundle2.add_media_request(req5)

    bundle2.update_request_status(req4, MediaRequestLifecycleStage.COMPLETED)
    bundle2.update_request_status(req5, MediaRequestLifecycleStage.DISCARDED)

    # Now should be finished successfully (no failures)
    assert bundle2.finished_successfully


def test_bundle_text_channel_parameter_storage(fake_context):  #pylint:disable=redefined-outer-name
    """Test that text_channel parameter is properly stored in bundle"""
    test_channel = fake_context['channel']
    bundle = MultiMediaRequestBundle(fake_context['guild'].id, fake_context['channel'].id, test_channel)

    # Verify text_channel is stored
    assert bundle.text_channel == test_channel
    assert bundle.text_channel.id == fake_context['channel'].id


def test_bundle_print_completion_messages(fake_context):  #pylint:disable=redefined-outer-name
    """Test new completion messaging in bundle print method"""
    bundle = MultiMediaRequestBundle(fake_context['guild'].id, fake_context['channel'].id, fake_context['channel'])
    bundle.multi_input_string = "test-playlist"

    # Add multiple requests to trigger multi-item messaging
    req1 = fake_source_dict(fake_context)
    req2 = fake_source_dict(fake_context)
    bundle.add_media_request(req1)
    bundle.add_media_request(req2)

    # Test in-progress messaging
    messages = bundle.print()
    full_message = "\n".join(messages)
    assert "Downloading" in full_message
    assert "test-playlist" in full_message

    # Complete all requests
    bundle.update_request_status(req1, MediaRequestLifecycleStage.COMPLETED)
    bundle.update_request_status(req2, MediaRequestLifecycleStage.COMPLETED)

    # Test completion messaging
    messages = bundle.print()
    full_message = "\n".join(messages)
    assert "Completed download of" in full_message
    assert "test-playlist" in full_message
    assert "2/2 items downloaded successfully, 0 failed" in full_message


def test_bundle_url_formatting_in_print(fake_context):  #pylint:disable=redefined-outer-name
    """Test URL formatting with angle brackets in bundle print"""
    bundle = MultiMediaRequestBundle(fake_context['guild'].id, fake_context['channel'].id, fake_context['channel'])

    # Test URL gets wrapped in angle brackets
    bundle.multi_input_string = "https://example.com/playlist"

    req1 = fake_source_dict(fake_context)
    req2 = fake_source_dict(fake_context)
    bundle.add_media_request(req1)
    bundle.add_media_request(req2)

    messages = bundle.print()
    full_message = "\n".join(messages)
    assert "<https://example.com/playlist>" in full_message

    # Test non-URL doesn't get wrapped
    bundle2 = MultiMediaRequestBundle(fake_context['guild'].id, fake_context['channel'].id, fake_context['channel'])
    bundle2.multi_input_string = "My Playlist"

    req3 = fake_source_dict(fake_context)
    req4 = fake_source_dict(fake_context)
    bundle2.add_media_request(req3)
    bundle2.add_media_request(req4)

    messages2 = bundle2.print()
    full_message2 = "\n".join(messages2)
    assert "\"My Playlist\"" in full_message2
    assert "<My Playlist>" not in full_message2
