import pytest

from discord_bot.cogs.music_helpers.common import SearchType
from discord_bot.cogs.music_helpers.media_request import MultiMediaRequestBundle, MediaRequest, chunk_list
from discord_bot.cogs.music_helpers.message_queue import MessageQueue, MessageQueueException
from discord_bot.cogs.music_helpers.common import MediaRequestLifecycleStage

from tests.helpers import fake_source_dict
from tests.helpers import fake_context #pylint:disable=unused-import

@pytest.mark.asyncio
async def test_media_request_basics(fake_context): #pylint:disable=redefined-outer-name
    x = fake_source_dict(fake_context)
    assert x.download_file is True

    assert str(x) == x.search_string
    x_direct = fake_source_dict(fake_context, is_direct_search=True)
    assert str(x_direct) == f'<{x_direct.search_string}>'

@pytest.mark.asyncio
async def test_media_request_bundle_single(fake_context): #pylint:disable=redefined-outer-name
    x = fake_source_dict(fake_context)
    b = MultiMediaRequestBundle(fake_context['guild'].id, fake_context['channel'].id, fake_context['channel'])
    b.add_media_request(x)
    assert b.print()[0] == f'Media request queued for download: "{x.raw_search_string}"'

    b.update_request_status(x, MediaRequestLifecycleStage.IN_PROGRESS)
    assert b.print()[0] == f'Downloading and processing media request: "{x.raw_search_string}"'

    b.update_request_status(x, MediaRequestLifecycleStage.COMPLETED)
    assert not b.print()

@pytest.mark.asyncio
async def test_media_request_bundle(fake_context): #pylint:disable=redefined-outer-name
    multi_input_string = 'https://foo.example.com/playlist'
    x = fake_source_dict(fake_context)
    y = fake_source_dict(fake_context)
    z = fake_source_dict(fake_context)
    x.multi_input_string = multi_input_string
    y.multi_input_string = multi_input_string
    z.multi_input_string = multi_input_string


    b = MultiMediaRequestBundle(fake_context['guild'].id, fake_context['channel'].id, fake_context['channel'])
    b.add_search_request(multi_input_string)
    b.finish_search_request()
    b.add_media_request(x)
    b.add_media_request(y)
    b.add_media_request(z)

    assert x.bundle_uuid == b.uuid
    assert b.finished is False

    # Check that the status header and URL formatting are correct with new format
    print_output = b.print()
    full_output = '\n'.join(print_output)
    assert 'Processing ' in full_output
    assert '<https://foo.example.com/playlist>' in full_output
    assert '0/3 items processed successfully, 0 failed' in full_output

    b.update_request_status(x, MediaRequestLifecycleStage.IN_PROGRESS)
    print_output = b.print()
    full_output = '\n'.join(print_output)
    assert 'Processing ' in full_output
    assert '0/3 items processed successfully, 0 failed' in full_output
    assert 'Downloading and processing media request:' in full_output

    b.update_request_status(x, MediaRequestLifecycleStage.COMPLETED)
    b.update_request_status(y, MediaRequestLifecycleStage.IN_PROGRESS)
    print_output = b.print()
    full_output = '\n'.join(print_output)
    assert '1/3 items processed successfully, 0 failed' in full_output

    b.update_request_status(y, MediaRequestLifecycleStage.FAILED, failure_reason='cats ate the chords')
    b.update_request_status(z, MediaRequestLifecycleStage.IN_PROGRESS)
    print_output = b.print()
    full_output = '\n'.join(print_output)
    assert '1/3 items processed successfully, 1 failed' in full_output
    assert 'cats ate the chords' in full_output

    b.update_request_status(z, MediaRequestLifecycleStage.COMPLETED)
    print_output = b.print()
    full_output = '\n'.join(print_output)
    assert '2/3 items processed successfully, 1 failed' in full_output
    assert b.finished is True


@pytest.mark.asyncio
async def test_media_request_bundle_blanks_removed(fake_context): #pylint:disable=redefined-outer-name
    multi_input_string = 'https://foo.example.com/playlist'
    x = fake_source_dict(fake_context)
    y = fake_source_dict(fake_context)
    z = fake_source_dict(fake_context)
    a = fake_source_dict(fake_context)
    c = fake_source_dict(fake_context)

    b = MultiMediaRequestBundle(fake_context['guild'].id, fake_context['channel'].id, fake_context['channel'], items_per_message=2)
    b.add_search_request(multi_input_string)
    b.finish_search_request()
    b.add_media_request(x)
    b.add_media_request(y)
    b.add_media_request(z)
    b.add_media_request(a)
    b.add_media_request(c)

    initial_print = b.print()
    assert len(initial_print) == 4

    b.update_request_status(x, MediaRequestLifecycleStage.COMPLETED)
    b.update_request_status(y, MediaRequestLifecycleStage.COMPLETED)
    b.update_request_status(z, MediaRequestLifecycleStage.COMPLETED)

    # Top row changed, bottom row the same
    new_print = b.print()
    assert initial_print[0] != new_print[0]
    assert initial_print[-1] == new_print[-1]
    assert len(new_print) == 3

@pytest.mark.asyncio
async def test_media_request_bundle_multi_message(fake_context): #pylint:disable=redefined-outer-name
    multi_input_string = 'https://foo.example.com/playlist'
    x = fake_source_dict(fake_context)
    y = fake_source_dict(fake_context)
    z = fake_source_dict(fake_context)
    x.multi_input_string = multi_input_string
    y.multi_input_string = multi_input_string
    z.multi_input_string = multi_input_string


    b = MultiMediaRequestBundle(fake_context['guild'].id, fake_context['channel'].id, fake_context['channel'], items_per_message=2)
    b.add_search_request(multi_input_string)
    b.finish_search_request()
    b.add_media_request(x)
    b.add_media_request(y)
    b.add_media_request(z)
    assert b.finished is False

    assert b.print()[0] == 'Processing "<https://foo.example.com/playlist>"\n0/3 items processed successfully, 0 failed'
    assert b.print()[1] == f'Media request queued for download: "{x.raw_search_string}"\nMedia request queued for download: "{y.raw_search_string}"'
    assert b.print()[2] == f'Media request queued for download: "{z.raw_search_string}"'

@pytest.mark.asyncio
async def test_media_request_bundle_shutdown(fake_context): #pylint:disable=redefined-outer-name
    """Test that bundle shutdown functionality clears messages"""
    multi_input_string = 'https://foo.example.com/playlist'
    x = fake_source_dict(fake_context)
    y = fake_source_dict(fake_context)
    x.multi_input_string = multi_input_string
    y.multi_input_string = multi_input_string

    b = MultiMediaRequestBundle(fake_context['guild'].id, fake_context['channel'].id, fake_context['channel'])
    b.add_media_request(x)
    b.add_media_request(y)

    # Initially should have messages
    assert len(b.print()) > 0
    assert b.is_shutdown is False

    # After shutdown, should return empty messages
    b.shutdown()
    assert b.is_shutdown is True
    assert not b.print()

    # Even if we update status, should still return empty
    b.update_request_status(x, MediaRequestLifecycleStage.IN_PROGRESS)
    assert not b.print()

@pytest.mark.asyncio
async def test_media_request_bundle_shutdown_single_item(fake_context): #pylint:disable=redefined-outer-name
    """Test shutdown behavior with single item bundle"""
    x = fake_source_dict(fake_context)

    b = MultiMediaRequestBundle(fake_context['guild'].id, fake_context['channel'].id, fake_context['channel'])
    b.add_media_request(x)

    # Initially should have message for single item
    assert len(b.print()) == 1
    assert f'Media request queued for download: "{x.raw_search_string}"' in b.print()[0]

    # After shutdown, should return empty
    b.shutdown()
    assert not b.print()

@pytest.mark.asyncio
async def test_media_request_bundle_shutdown_initialization(fake_context): #pylint:disable=redefined-outer-name
    """Test that bundle starts with shutdown=False"""
    b = MultiMediaRequestBundle(fake_context['guild'].id, fake_context['channel'].id, fake_context['channel'])

    # Should start as not shutdown
    assert b.is_shutdown is False
    assert hasattr(b, 'is_shutdown')  # Verify attribute exists

    # Should work normally before shutdown
    x = fake_source_dict(fake_context)
    b.add_media_request(x)
    assert len(b.print()) > 0

@pytest.fixture
def media_request_bundle(fake_context):  #pylint:disable=redefined-outer-name
    """Create a media request bundle for testing"""
    bundle = MultiMediaRequestBundle(
        fake_context['guild'].id,
        fake_context['channel'].id,
        fake_context['channel'],
        items_per_message=3
    )
    # Set up search state for testing - bundles created in tests should be ready for use
    bundle.search_finished = True
    return bundle


def test_media_request_bundle_finished_property_empty(media_request_bundle):  #pylint:disable=redefined-outer-name
    """Test finished property when bundle is empty"""
    # Empty bundle is considered finished (0 processed out of 0 total)
    assert media_request_bundle.finished


def test_media_request_bundle_finished_property_all_completed(media_request_bundle, fake_context):  #pylint:disable=redefined-outer-name
    """Test finished property when all items are completed"""
    # Add a media request
    media_request = MediaRequest(
        fake_context['guild'].id,
        fake_context['channel'].id,
        'test_user',
        123456,
        'test search',
        'test search',
        SearchType.SEARCH
    )
    media_request_bundle.add_media_request(media_request)

    # Mark as completed
    media_request_bundle.update_request_status(media_request, MediaRequestLifecycleStage.COMPLETED)

    assert media_request_bundle.finished


def test_media_request_bundle_finished_property_mixed_status(media_request_bundle, fake_context):  #pylint:disable=redefined-outer-name
    """Test finished property with mixed status items"""
    # Add multiple media requests
    for i in range(3):
        media_request = MediaRequest(
            fake_context['guild'].id,
            fake_context['channel'].id,
            'test_user',
            123456,
            f'test search {i}',
            f'test search {i}',
            SearchType.SEARCH
        )
        media_request_bundle.add_media_request(media_request)

    # Mark different statuses
    requests = list(media_request_bundle.media_requests)

    # Create actual MediaRequest objects to update status
    media_request_1 = MediaRequest(
        fake_context['guild'].id,
        fake_context['channel'].id,
        'test_user',
        123456,
        'test search 0',
        'test search 0',
        SearchType.SEARCH
    )
    media_request_1.uuid = requests[0]['uuid']

    media_request_2 = MediaRequest(
        fake_context['guild'].id,
        fake_context['channel'].id,
        'test_user',
        123456,
        'test search 1',
        'test search 1',
        SearchType.SEARCH
    )
    media_request_2.uuid = requests[1]['uuid']

    media_request_bundle.update_request_status(media_request_1, MediaRequestLifecycleStage.COMPLETED)
    media_request_bundle.update_request_status(media_request_2, MediaRequestLifecycleStage.FAILED)

    # Third one remains queued, so not finished
    assert not media_request_bundle.finished


def test_media_request_bundle_print_shutdown(media_request_bundle):  #pylint:disable=redefined-outer-name
    """Test print method when bundle is shutdown"""
    media_request_bundle.shutdown()
    result = media_request_bundle.print()
    assert result == []


def test_media_request_bundle_print_single_item(media_request_bundle, fake_context):  #pylint:disable=redefined-outer-name
    """Test print method with single item (no top message)"""
    media_request = MediaRequest(
        fake_context['guild'].id,
        fake_context['channel'].id,
        'test_user',
        123456,
        'single test',
        'single test',
        SearchType.SEARCH
    )
    media_request_bundle.add_media_request(media_request)

    result = media_request_bundle.print()
    assert len(result) == 1
    assert 'Media request queued for download: "single test"' in result[0]


def test_media_request_bundle_print_multiple_items_with_status(media_request_bundle, fake_context):  #pylint:disable=redefined-outer-name
    """Test print method with multiple items showing top message and status"""
    media_request_bundle.add_search_request('playlist test')
    media_request_bundle.finish_search_request()

    # Add multiple requests
    for i in range(3):
        media_request = MediaRequest(
            fake_context['guild'].id,
            fake_context['channel'].id,
            'test_user',
            123456,
            f'test {i}',
            f'test {i}',
            SearchType.SEARCH
        )
        media_request_bundle.add_media_request(media_request)

    result = media_request_bundle.print()

    # Should have top message
    assert any('Processing "playlist test"' in msg for msg in result)
    assert any('0/3 items processed successfully, 0 failed' in msg for msg in result)


def test_media_request_bundle_print_with_different_statuses(media_request_bundle, fake_context):  #pylint:disable=redefined-outer-name
    """Test print method with different request statuses"""
    media_request_bundle.multi_input_string = 'mixed status test'

    # Add requests with different statuses
    statuses_to_test = [
        (MediaRequestLifecycleStage.QUEUED, 'queued'),
        (MediaRequestLifecycleStage.IN_PROGRESS, 'Downloading and processing'),
        (MediaRequestLifecycleStage.FAILED, 'failed download'),
        (MediaRequestLifecycleStage.COMPLETED, None),  # Completed items don't show in messages
        (MediaRequestLifecycleStage.DISCARDED, None)   # Discarded items don't show in messages
    ]

    media_requests = []
    for i, (status, expected_text) in enumerate(statuses_to_test):
        media_request = MediaRequest(
            fake_context['guild'].id,
            fake_context['channel'].id,
            'test_user',
            123456,
            f'request {i}',
            f'request {i}',
            SearchType.SEARCH
        )
        media_request_bundle.add_media_request(media_request)
        media_requests.append(media_request)

    # Update statuses
    for media_request, (status, _) in zip(media_requests, statuses_to_test):
        if status == MediaRequestLifecycleStage.FAILED:
            media_request_bundle.update_request_status(media_request, status, failure_reason="test failure")
        else:
            media_request_bundle.update_request_status(media_request, status)

    result = media_request_bundle.print()
    result_text = ' '.join(result)

    # Check that expected texts appear
    for _, expected_text in statuses_to_test:
        if expected_text:
            assert expected_text in result_text

    # Check that completed and discarded items don't appear
    assert 'request 3' not in result_text  # completed
    assert 'request 4' not in result_text  # discarded


def test_media_request_bundle_print_with_failure_reason(media_request_bundle, fake_context):  #pylint:disable=redefined-outer-name
    """Test print method shows failure reason for failed requests"""
    media_request = MediaRequest(
        fake_context['guild'].id,
        fake_context['channel'].id,
        'test_user',
        123456,
        'failed request',
        'failed request',
        SearchType.SEARCH
    )
    media_request_bundle.add_media_request(media_request)

    # Mark as failed with reason
    media_request_bundle.update_request_status(
        media_request,
        MediaRequestLifecycleStage.FAILED,
        failure_reason="Video too long"
    )

    result = media_request_bundle.print()
    result_text = ' '.join(result)

    assert 'Media request failed download: "failed request"' in result_text
    assert 'Video too long' in result_text


def test_media_request_bundle_print_items_per_message_chunking(media_request_bundle, fake_context):  #pylint:disable=redefined-outer-name
    """Test that print method respects items_per_message limit"""
    # Bundle is configured for 3 items per message

    # Add 7 requests (should result in 3 messages: 2+3+2 when including top messages)
    for i in range(7):
        media_request = MediaRequest(
            fake_context['guild'].id,
            fake_context['channel'].id,
            'test_user',
            123456,
            f'request {i}',
            f'request {i}',
            SearchType.SEARCH
        )
        media_request_bundle.add_media_request(media_request)

    result = media_request_bundle.print()

    # Should have multiple messages due to chunking
    assert len(result) > 1

    # Each message should not exceed items_per_message when counting lines
    for message in result:
        lines = message.split('\n')
        # Account for top message and status line taking up space
        assert len(lines) <= media_request_bundle.items_per_message + 2


def test_media_request_bundle_print_url_formatting(media_request_bundle, fake_context):  #pylint:disable=redefined-outer-name
    """Test that URLs are properly formatted with angle brackets"""
    media_request = MediaRequest(
        fake_context['guild'].id,
        fake_context['channel'].id,
        'test_user',
        123456,
        'https://example.com/video',
        'https://example.com/video',
        SearchType.DIRECT
    )
    media_request_bundle.add_media_request(media_request)

    result = media_request_bundle.print()
    result_text = ' '.join(result)

    # URL should be wrapped in angle brackets
    assert '<https://example.com/video>' in result_text


def test_media_request_bundle_print_with_backoff_status(media_request_bundle, fake_context):  #pylint:disable=redefined-outer-name
    """Test that BACKOFF status shows appropriate message"""
    media_request = MediaRequest(
        fake_context['guild'].id,
        fake_context['channel'].id,
        'test_user',
        123456,
        'test search string',
        'test search string',
        SearchType.SEARCH
    )
    media_request_bundle.add_media_request(media_request)

    # Set to BACKOFF status
    media_request_bundle.update_request_status(media_request, MediaRequestLifecycleStage.BACKOFF)

    result = media_request_bundle.print()
    result_text = ' '.join(result)

    # Should contain backoff message
    assert 'Waiting for youtube backoff time before processing media request: "test search string"' in result_text


def test_media_request_bundle_print_with_all_lifecycle_stages(media_request_bundle, fake_context):  #pylint:disable=redefined-outer-name
    """Test bundle print with all possible lifecycle stages"""
    # Create requests for each lifecycle stage
    lifecycle_stages = [
        MediaRequestLifecycleStage.QUEUED,
        MediaRequestLifecycleStage.IN_PROGRESS,
        MediaRequestLifecycleStage.BACKOFF,
        MediaRequestLifecycleStage.COMPLETED,
        MediaRequestLifecycleStage.FAILED,
        MediaRequestLifecycleStage.DISCARDED
    ]

    for i, stage in enumerate(lifecycle_stages):
        media_request = MediaRequest(
            fake_context['guild'].id,
            fake_context['channel'].id,
            'test_user',
            123456,
            f'test search {i}',
            f'test search {i}',
            SearchType.SEARCH
        )
        media_request_bundle.add_media_request(media_request)
        media_request_bundle.update_request_status(media_request, stage)

    result = media_request_bundle.print()
    result_text = ' '.join(result)

    # Should contain expected messages for visible stages
    assert 'Media request queued for download: "test search 0"' in result_text
    assert 'Downloading and processing media request: "test search 1"' in result_text
    assert 'Waiting for youtube backoff time before processing media request: "test search 2"' in result_text
    # COMPLETED items are skipped from output, so should not appear
    assert 'test search 3' not in result_text
    assert 'Media request failed download: "test search 4"' in result_text
    # DISCARDED items should not appear in output
    assert 'test search 5' not in result_text


def test_media_request_bundle_finished_property_with_backoff(media_request_bundle, fake_context):  #pylint:disable=redefined-outer-name
    """Test that BACKOFF status doesn't mark bundle as finished"""
    media_request = MediaRequest(
        fake_context['guild'].id,
        fake_context['channel'].id,
        'test_user',
        123456,
        'test search',
        'test search',
        SearchType.SEARCH
    )
    media_request_bundle.add_media_request(media_request)

    # BACKOFF status should not be considered finished
    media_request_bundle.update_request_status(media_request, MediaRequestLifecycleStage.BACKOFF)
    assert not media_request_bundle.finished

    # IN_PROGRESS status should not be considered finished
    media_request_bundle.update_request_status(media_request, MediaRequestLifecycleStage.IN_PROGRESS)
    assert not media_request_bundle.finished

    # COMPLETED status should be considered finished
    media_request_bundle.update_request_status(media_request, MediaRequestLifecycleStage.COMPLETED)
    assert media_request_bundle.finished

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
    req = MediaRequest(123, 456, "user", 1, "search", "search", SearchType.SEARCH, download_file=True)
    bundle.add_media_request(req)
    bundle.update_request_status(req, MediaRequestLifecycleStage.COMPLETED)

    # Print should return empty list when no messages to display
    messages = bundle.print()
    assert not messages


def test_bundle_single_item_no_status_header(fake_context):  #pylint:disable=redefined-outer-name
    """Test that single-item bundles don't include status header"""
    bundle = MultiMediaRequestBundle(123, 456, fake_context['channel'])

    # Add single failed request
    req = MediaRequest(123, 456, "user", 1, "search", "search", SearchType.SEARCH, download_file=True)
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
    bundle.add_search_request("test-playlist")
    bundle.finish_search_request()

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
    assert "Processing" in full_message
    assert "test-playlist" in full_message
    assert "1/3 items processed successfully, 1 failed" in full_message


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
    bundle.add_search_request("test-playlist")
    bundle.finish_search_request()

    # Add multiple requests to trigger multi-item messaging
    req1 = fake_source_dict(fake_context)
    req2 = fake_source_dict(fake_context)
    bundle.add_media_request(req1)
    bundle.add_media_request(req2)

    # Test in-progress messaging
    messages = bundle.print()
    full_message = "\n".join(messages)
    assert "Processing" in full_message
    assert "test-playlist" in full_message

    # Complete all requests
    bundle.update_request_status(req1, MediaRequestLifecycleStage.COMPLETED)
    bundle.update_request_status(req2, MediaRequestLifecycleStage.COMPLETED)

    # Test completion messaging
    messages = bundle.print()
    full_message = "\n".join(messages)
    assert "Completed processing of" in full_message
    assert "test-playlist" in full_message
    assert "2/2 items processed successfully, 0 failed" in full_message


def test_bundle_url_formatting_in_print(fake_context):  #pylint:disable=redefined-outer-name
    """Test URL formatting with angle brackets in bundle print"""
    bundle = MultiMediaRequestBundle(fake_context['guild'].id, fake_context['channel'].id, fake_context['channel'])

    # Test URL gets wrapped in angle brackets
    bundle.add_search_request("https://example.com/playlist")
    bundle.finish_search_request()

    req1 = fake_source_dict(fake_context)
    req2 = fake_source_dict(fake_context)
    bundle.add_media_request(req1)
    bundle.add_media_request(req2)

    messages = bundle.print()
    full_message = "\n".join(messages)
    assert "<https://example.com/playlist>" in full_message

    # Test non-URL doesn't get wrapped
    bundle2 = MultiMediaRequestBundle(fake_context['guild'].id, fake_context['channel'].id, fake_context['channel'])
    bundle2.add_search_request("My Playlist")
    bundle2.finish_search_request()

    req3 = fake_source_dict(fake_context)
    req4 = fake_source_dict(fake_context)
    bundle2.add_media_request(req3)
    bundle2.add_media_request(req4)

    messages2 = bundle2.print()
    full_message2 = "\n".join(messages2)
    assert "\"My Playlist\"" in full_message2
    assert "<My Playlist>" not in full_message2
