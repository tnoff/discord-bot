import pytest

from discord_bot.cogs.music_helpers.common import SearchType
from discord_bot.cogs.music_helpers.media_request import MultiMediaRequestBundle, MediaRequest, chunk_list
from discord_bot.cogs.music_helpers.message_queue import MessageQueue, MessageQueueException
from discord_bot.cogs.music_helpers.common import MediaRequestLifecycleStage
from discord_bot.common import DISCORD_MAX_MESSAGE_LENGTH

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
async def test_media_request_retry_count_initialization(fake_context): #pylint:disable=redefined-outer-name
    """Test that retry_count is always initialized to 0"""
    x = fake_source_dict(fake_context)
    assert x.retry_count == 0

@pytest.mark.asyncio
async def test_media_request_retry_count_increments(fake_context): #pylint:disable=redefined-outer-name
    """Test that retry_count can be incremented"""
    x = fake_source_dict(fake_context)
    assert x.retry_count == 0

    x.retry_count += 1
    assert x.retry_count == 1

    x.retry_count += 1
    assert x.retry_count == 2

@pytest.mark.asyncio
async def test_media_request_bundle_single(fake_context): #pylint:disable=redefined-outer-name
    x = fake_source_dict(fake_context)
    b = MultiMediaRequestBundle(fake_context['guild'].id, fake_context['channel'].id, fake_context['channel'])
    b.set_initial_search(x.raw_search_string)
    b.set_multi_input_request()
    b.add_media_request(x)
    b.all_requests_added()
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

    b = MultiMediaRequestBundle(fake_context['guild'].id, fake_context['channel'].id, fake_context['channel'])
    b.set_initial_search(multi_input_string)
    b.set_multi_input_request()
    b.add_media_request(x)
    b.add_media_request(y)
    b.add_media_request(z)
    b.all_requests_added()

    assert x.bundle_uuid == b.uuid
    assert b.finished is False

    # Check that the status header and URL formatting are correct with new format
    print_output = b.print()
    full_output = '\n'.join(print_output)
    assert 'Processing ' in full_output
    assert '<https://foo.example.com/playlist>' in full_output
    assert '0/3 media requests processed successfully, 0 failed' in full_output

    b.update_request_status(x, MediaRequestLifecycleStage.IN_PROGRESS)
    print_output = b.print()
    full_output = '\n'.join(print_output)
    assert 'Processing ' in full_output
    assert '0/3 media requests processed successfully, 0 failed' in full_output
    assert 'Downloading and processing media request:' in full_output

    b.update_request_status(x, MediaRequestLifecycleStage.COMPLETED)
    b.update_request_status(y, MediaRequestLifecycleStage.IN_PROGRESS)
    print_output = b.print()
    full_output = '\n'.join(print_output)
    assert '1/3 media requests processed successfully, 0 failed' in full_output

    b.update_request_status(y, MediaRequestLifecycleStage.FAILED, failure_reason='cats ate the chords')
    b.update_request_status(z, MediaRequestLifecycleStage.IN_PROGRESS)
    print_output = b.print()
    full_output = '\n'.join(print_output)
    assert '1/3 media requests processed successfully, 1 failed' in full_output
    # Failure reason should NOT be in print output (sent separately)
    assert 'cats ate the chords' not in full_output
    # But should be available via get_failure_summary()
    summary = b.get_failure_summary()
    assert summary is not None
    summary_text = '\n'.join(summary)
    assert 'cats ate the chords' in summary_text

    b.update_request_status(z, MediaRequestLifecycleStage.COMPLETED)
    print_output = b.print()
    full_output = '\n'.join(print_output)
    assert '2/3 media requests processed successfully, 1 failed' in full_output
    assert b.finished is True

@pytest.mark.asyncio
async def test_media_request_bundle_retry_lifecycle(fake_context): #pylint:disable=redefined-outer-name
    """Test that RETRY lifecycle stage shows correct message"""
    x = fake_source_dict(fake_context)
    y = fake_source_dict(fake_context)
    z = fake_source_dict(fake_context)

    b = MultiMediaRequestBundle(fake_context['guild'].id, fake_context['channel'].id, fake_context['channel'])
    b.set_initial_search('https://foo.example.com/playlist')
    b.set_multi_input_request()
    b.add_media_request(x)
    b.add_media_request(y)
    b.add_media_request(z)
    b.all_requests_added()

    # First request starts processing
    b.update_request_status(x, MediaRequestLifecycleStage.IN_PROGRESS)
    print_output = b.print()
    full_output = '\n'.join(print_output)
    assert 'Downloading and processing media request:' in full_output

    # First request fails and will retry
    b.update_request_status(x, MediaRequestLifecycleStage.RETRY)
    print_output = b.print()
    full_output = '\n'.join(print_output)
    assert 'Failed, will retry:' in full_output
    assert x.search_string in full_output

    # Second request completes successfully
    b.update_request_status(y, MediaRequestLifecycleStage.IN_PROGRESS)
    b.update_request_status(y, MediaRequestLifecycleStage.COMPLETED)
    print_output = b.print()
    full_output = '\n'.join(print_output)
    assert '1/3 media requests processed successfully, 0 failed' in full_output

    # First request (after retry) completes successfully
    b.update_request_status(x, MediaRequestLifecycleStage.IN_PROGRESS)
    b.update_request_status(x, MediaRequestLifecycleStage.COMPLETED)

    # Third request completes
    b.update_request_status(z, MediaRequestLifecycleStage.IN_PROGRESS)
    b.update_request_status(z, MediaRequestLifecycleStage.COMPLETED)

    print_output = b.print()
    full_output = '\n'.join(print_output)
    assert '3/3 media requests processed successfully, 0 failed' in full_output
    assert b.finished is True


@pytest.mark.asyncio
async def test_media_request_bundle_shutdown(fake_context): #pylint:disable=redefined-outer-name
    """Test that bundle shutdown functionality clears messages"""
    x = fake_source_dict(fake_context)
    y = fake_source_dict(fake_context)

    b = MultiMediaRequestBundle(fake_context['guild'].id, fake_context['channel'].id, fake_context['channel'])
    b.set_initial_search("test search")
    b.set_multi_input_request()
    b.add_media_request(x)
    b.add_media_request(y)
    b.all_requests_added()

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
    b.set_initial_search(x.raw_search_string)
    b.set_multi_input_request()
    b.add_media_request(x)
    b.all_requests_added()

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
    b.set_initial_search(x.raw_search_string)
    b.set_multi_input_request()
    b.add_media_request(x)
    b.all_requests_added()
    assert len(b.print()) > 0

@pytest.fixture
def media_request_bundle(fake_context):  #pylint:disable=redefined-outer-name
    """Create a media request bundle for testing"""
    bundle = MultiMediaRequestBundle(
        fake_context['guild'].id,
        fake_context['channel'].id,
        fake_context['channel']
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
    media_request_1.uuid = requests[0].uuid

    media_request_2 = MediaRequest(
        fake_context['guild'].id,
        fake_context['channel'].id,
        'test_user',
        123456,
        'test search 1',
        'test search 1',
        SearchType.SEARCH
    )
    media_request_2.uuid = requests[1].uuid

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
    media_request_bundle.set_initial_search('single test')
    media_request_bundle.set_multi_input_request()
    media_request_bundle.add_media_request(media_request)
    media_request_bundle.all_requests_added()

    result = media_request_bundle.print()
    assert len(result) == 1
    assert 'Media request queued for download: "single test"' in result[0]


def test_media_request_bundle_print_multiple_items_with_status(media_request_bundle, fake_context):  #pylint:disable=redefined-outer-name
    """Test print method with multiple items showing top message and status"""
    media_request_bundle.set_initial_search('playlist test')
    media_request_bundle.set_multi_input_request()

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
    media_request_bundle.all_requests_added()

    result = media_request_bundle.print()

    # Should have top message
    assert any('Processing "playlist test"' in msg for msg in result)
    assert any('0/3 media requests processed successfully, 0 failed' in msg for msg in result)


def test_media_request_bundle_print_with_different_statuses(media_request_bundle, fake_context):  #pylint:disable=redefined-outer-name
    """Test print method with different request statuses"""
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

    media_request_bundle.all_requests_added()

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
    """Test print method shows failure message but NOT the failure reason (kept separate)"""
    media_request = MediaRequest(
        fake_context['guild'].id,
        fake_context['channel'].id,
        'test_user',
        123456,
        'failed request',
        'failed request',
        SearchType.SEARCH
    )
    media_request_bundle.set_initial_search('failed request')
    media_request_bundle.set_multi_input_request()
    media_request_bundle.add_media_request(media_request)
    media_request_bundle.all_requests_added()

    # Mark as failed with reason
    media_request_bundle.update_request_status(
        media_request,
        MediaRequestLifecycleStage.FAILED,
        failure_reason="Video too long"
    )

    result = media_request_bundle.print()
    result_text = ' '.join(result)

    # Should show failed message
    assert 'Media request failed download: "failed request"' in result_text
    # But failure reason should NOT be in the table (to keep message short)
    assert 'Video too long' not in result_text

    # Failure reason should be available via get_failure_summary()
    summary = media_request_bundle.get_failure_summary()
    assert summary is not None
    summary_text = '\n'.join(summary)
    assert 'Video too long' in summary_text


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
    media_request_bundle.set_initial_search('https://example.com/video')
    media_request_bundle.set_multi_input_request()
    media_request_bundle.add_media_request(media_request)
    media_request_bundle.all_requests_added()

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
    media_request_bundle.set_initial_search('test search string')
    media_request_bundle.set_multi_input_request()
    media_request_bundle.add_media_request(media_request)
    media_request_bundle.all_requests_added()

    # Set to BACKOFF status
    media_request_bundle.update_request_status(media_request, MediaRequestLifecycleStage.BACKOFF)

    result = media_request_bundle.print()
    result_text = ' '.join(result)

    # Should contain backoff message
    assert 'Waiting to process: "test search string"' in result_text


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
    media_request_bundle.all_requests_added()

    for i, (media_request, stage) in enumerate(zip(media_request_bundle.media_requests, lifecycle_stages)):
        # Reconstruct MediaRequest object with the stored UUID
        mr = MediaRequest(
            fake_context['guild'].id,
            fake_context['channel'].id,
            'test_user',
            123456,
            f'test search {i}',
            f'test search {i}',
            SearchType.SEARCH
        )
        mr.uuid = media_request.uuid
        media_request_bundle.update_request_status(mr, stage)

    result = media_request_bundle.print()
    result_text = ' '.join(result)

    # Should contain expected messages for visible stages
    assert 'Media request queued for download: "test search 0"' in result_text
    assert 'Downloading and processing media request: "test search 1"' in result_text
    assert 'Waiting to process: "test search 2"' in result_text
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
    bundle.set_initial_search(req.raw_search_string)
    bundle.set_multi_input_request()
    bundle.add_media_request(req)
    bundle.all_requests_added()

    # Update with override message
    bundle.update_request_status(req, MediaRequestLifecycleStage.FAILED,
                                failure_reason="Original failure",
                                override_message="Custom override message")

    # Test that override message is used instead of default formatting
    messages = bundle.print()
    assert len(messages) == 1
    assert "Custom override message" in messages[0]
    assert "Original failure" not in messages[0]  # Original failure reason should be ignored


def test_bundle_empty_message_list(fake_context):  #pylint:disable=redefined-outer-name
    """Test bundle when all items are completed/discarded (empty message list)"""
    bundle = MultiMediaRequestBundle(123, 456, fake_context['channel'])

    # Add request that will be completed (shouldn't appear in messages)
    req = MediaRequest(123, 456, "user", 1, "search", "search", SearchType.SEARCH, download_file=True)
    bundle.set_initial_search("search")
    bundle.set_multi_input_request()
    bundle.add_media_request(req)
    bundle.all_requests_added()
    bundle.update_request_status(req, MediaRequestLifecycleStage.COMPLETED)

    # Print should return empty list when no messages to display
    messages = bundle.print()
    assert not messages


def test_bundle_single_item_no_status_header(fake_context):  #pylint:disable=redefined-outer-name
    """Test that single-item bundles don't include status header"""
    bundle = MultiMediaRequestBundle(123, 456, fake_context['channel'])

    # Add single failed request
    req = MediaRequest(123, 456, "user", 1, "search", "search", SearchType.SEARCH, download_file=True)
    bundle.set_initial_search("search")
    bundle.set_multi_input_request()
    bundle.add_media_request(req)
    bundle.all_requests_added()
    bundle.update_request_status(req, MediaRequestLifecycleStage.FAILED, failure_reason="Test failure")

    messages = bundle.print()
    # Should not include top-level status since total == 1
    assert len(messages) == 1
    # Failure reason should NOT be in print output (sent separately)
    assert "Test failure" not in messages[0]
    # But should be available via get_failure_summary()
    summary = bundle.get_failure_summary()
    assert summary is not None
    summary_text = '\n'.join(summary)
    assert "Test failure" in summary_text
    assert "downloaded successfully" not in messages[0]  # No status header


def test_bundle_multiple_items_includes_status_header(fake_context):  #pylint:disable=redefined-outer-name
    """Test that multi-item bundles include status header"""
    bundle = MultiMediaRequestBundle(fake_context['guild'].id, fake_context['channel'].id, fake_context['channel'])
    bundle.set_initial_search("test-playlist")
    bundle.set_multi_input_request()

    # Add multiple requests
    requests = []
    for _ in range(3):
        req = fake_source_dict(fake_context)
        bundle.add_media_request(req)
        requests.append(req)
    bundle.all_requests_added()

    bundle.update_request_status(requests[0], MediaRequestLifecycleStage.COMPLETED)
    bundle.update_request_status(requests[1], MediaRequestLifecycleStage.FAILED)
    bundle.update_request_status(requests[2], MediaRequestLifecycleStage.QUEUED)

    messages = bundle.print()
    # Should include status header since total > 1
    full_message = "\n".join(messages)
    assert "Processing" in full_message
    assert "test-playlist" in full_message
    assert "1/3 media requests processed successfully, 1 failed" in full_message


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
    bundle.set_initial_search("test-playlist")
    bundle.set_multi_input_request()

    # Add multiple requests to trigger multi-item messaging
    req1 = fake_source_dict(fake_context)
    req2 = fake_source_dict(fake_context)
    bundle.add_media_request(req1)
    bundle.add_media_request(req2)
    bundle.all_requests_added()

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
    assert "2/2 media requests processed successfully, 0 failed" in full_message


def test_bundle_url_formatting_in_print(fake_context):  #pylint:disable=redefined-outer-name
    """Test URL formatting with angle brackets in bundle print"""
    bundle = MultiMediaRequestBundle(fake_context['guild'].id, fake_context['channel'].id, fake_context['channel'])

    # Test URL gets wrapped in angle brackets
    bundle.set_initial_search("https://example.com/playlist")
    bundle.set_multi_input_request()

    req1 = fake_source_dict(fake_context)
    req2 = fake_source_dict(fake_context)
    bundle.add_media_request(req1)
    bundle.add_media_request(req2)
    bundle.all_requests_added()

    messages = bundle.print()
    full_message = "\n".join(messages)
    assert "<https://example.com/playlist>" in full_message

    # Test non-URL doesn't get wrapped
    bundle2 = MultiMediaRequestBundle(fake_context['guild'].id, fake_context['channel'].id, fake_context['channel'])
    bundle2.set_initial_search("My Playlist")
    bundle2.set_multi_input_request()

    req3 = fake_source_dict(fake_context)
    req4 = fake_source_dict(fake_context)
    bundle2.add_media_request(req3)
    bundle2.add_media_request(req4)
    bundle2.all_requests_added()

    messages2 = bundle2.print()
    full_message2 = "\n".join(messages2)
    assert "\"My Playlist\"" in full_message2
    assert "<My Playlist>" not in full_message2


def test_bundle_pagination_length_parameter(fake_context):  #pylint:disable=redefined-outer-name
    """Test that pagination_length parameter is properly stored and used"""
    custom_length = 500
    bundle = MultiMediaRequestBundle(
        fake_context['guild'].id,
        fake_context['channel'].id,
        fake_context['channel'],
        pagination_length=custom_length
    )

    # Verify pagination_length is stored
    assert bundle.pagination_length == custom_length

    # Verify default value
    default_bundle = MultiMediaRequestBundle(
        fake_context['guild'].id,
        fake_context['channel'].id,
        fake_context['channel']
    )
    assert default_bundle.pagination_length == DISCORD_MAX_MESSAGE_LENGTH


def test_bundle_pagination_length_creates_multiple_pages(fake_context):  #pylint:disable=redefined-outer-name
    """Test that short pagination_length splits content into multiple pages"""
    # Use very short pagination length to force multiple pages
    bundle = MultiMediaRequestBundle(
        fake_context['guild'].id,
        fake_context['channel'].id,
        fake_context['channel'],
        pagination_length=100  # Very short to trigger pagination
    )
    bundle.set_initial_search("test-playlist")
    bundle.set_multi_input_request()

    # Add multiple requests with medium-length strings
    for i in range(5):
        req = MediaRequest(
            fake_context['guild'].id,
            fake_context['channel'].id,
            'test_user',
            123456,
            f'test search item with some length {i}',
            f'test search item with some length {i}',
            SearchType.SEARCH
        )
        bundle.add_media_request(req)
    bundle.all_requests_added()

    result = bundle.print()

    # Should create multiple pages due to short pagination length
    assert len(result) > 1, "Short pagination length should create multiple pages"

    # Verify all content is present across pages
    full_output = '\n'.join(result)
    assert 'test-playlist' in full_output
    for i in range(5):
        assert f'test search item with some length {i}' in full_output


def test_bundle_completed_items_removed_from_output(fake_context):  #pylint:disable=redefined-outer-name
    """Test that completed/discarded items are removed from row_collections output"""
    bundle = MultiMediaRequestBundle(
        fake_context['guild'].id,
        fake_context['channel'].id,
        fake_context['channel'],
        pagination_length=150  # Short enough to create pagination
    )
    bundle.set_initial_search("test-playlist")
    bundle.set_multi_input_request()

    # Add requests
    requests = []
    for i in range(4):
        req = MediaRequest(
            fake_context['guild'].id,
            fake_context['channel'].id,
            'test_user',
            123456,
            f'item{i}',
            f'item{i}',
            SearchType.SEARCH
        )
        bundle.add_media_request(req)
        requests.append(req)
    bundle.all_requests_added()

    initial_result = bundle.print()
    initial_output = '\n'.join(initial_result)

    # All items should be in initial output
    for i in range(4):
        assert f'item{i}' in initial_output

    # Complete first two items
    bundle.update_request_status(requests[0], MediaRequestLifecycleStage.COMPLETED)
    bundle.update_request_status(requests[1], MediaRequestLifecycleStage.COMPLETED)

    new_result = bundle.print()
    new_output = '\n'.join(new_result)

    # Completed items should not appear
    assert 'item0' not in new_output
    assert 'item1' not in new_output

    # Remaining items should still appear
    assert 'item2' in new_output
    assert 'item3' in new_output


def test_bundle_pagination_stability_with_completions(fake_context):  #pylint:disable=redefined-outer-name
    """Test that completing items in middle doesn't affect later pages unnecessarily"""
    bundle = MultiMediaRequestBundle(
        fake_context['guild'].id,
        fake_context['channel'].id,
        fake_context['channel'],
        pagination_length=200  # Create multiple pages
    )
    bundle.set_initial_search("test-playlist")
    bundle.set_multi_input_request()

    # Add many requests to span multiple pages
    requests = []
    for i in range(8):
        req = MediaRequest(
            fake_context['guild'].id,
            fake_context['channel'].id,
            'test_user',
            123456,
            f'media_request_item_{i:02d}',
            f'media_request_item_{i:02d}',
            SearchType.SEARCH
        )
        bundle.add_media_request(req)
        requests.append(req)
    bundle.all_requests_added()

    # Complete some items from the beginning
    bundle.update_request_status(requests[0], MediaRequestLifecycleStage.COMPLETED)
    bundle.update_request_status(requests[1], MediaRequestLifecycleStage.COMPLETED)

    new_result = bundle.print()
    new_output = '\n'.join(new_result)

    # Later items should still be present
    for i in range(2, 8):
        assert f'media_request_item_{i:02d}' in new_output, f"Item {i} should still be in output"

    # Completed items should not be present
    assert 'media_request_item_00' not in new_output
    assert 'media_request_item_01' not in new_output


def test_bundle_ready_for_print_during_search_phase(fake_context):  #pylint:disable=redefined-outer-name
    """Test that bundle is ready_for_print during search phase before any media requests are added"""
    bundle = MultiMediaRequestBundle(
        fake_context['guild'].id,
        fake_context['channel'].id,
        fake_context['channel']
    )

    # Initially, bundle has no search and no requests
    # DapperTable.print() returns [''] for empty table, which is expected behavior
    initial_print = bundle.print()
    # Empty table returns a list with empty string
    assert initial_print == [''] or not initial_print

    # Add search request - bundle should now have something to print even without media requests
    bundle.set_initial_search("spotify:album:123abc")

    # During search phase (before set_multi_input_request), print should show processing message
    result = bundle.print()
    assert len(result) == 1
    assert 'Processing search "spotify:album:123abc"' in result[0]

    # Finish search - message changes from "Processing search" to just "Processing"
    bundle.set_multi_input_request()
    result = bundle.print()
    assert len(result) == 1
    # After set_multi_input_request, the message changes
    assert 'Processing' in result[0]
    assert 'spotify:album:123abc' in result[0]

    # Add media requests
    req = fake_source_dict(fake_context)
    bundle.add_media_request(req)

    # Still shows processing message even before all_requests_added
    result = bundle.print()
    assert len(result) == 1
    assert 'Processing' in result[0]
    assert 'spotify:album:123abc' in result[0]

    # After all_requests_added, should print full bundle with table content
    bundle.all_requests_added()
    result = bundle.print()
    assert len(result) == 1
    assert 'Media request queued for download' in result[0]


def test_media_request_bundle_failure_reason_not_in_row(fake_context):  #pylint:disable=redefined-outer-name
    """Test that failure reasons are not included in the table row to keep messages short"""
    bundle = MultiMediaRequestBundle(
        fake_context['guild'].id,
        fake_context['channel'].id,
        fake_context['channel']
    )

    media_request = fake_source_dict(fake_context)
    bundle.set_initial_search('test search')
    bundle.set_multi_input_request()
    bundle.add_media_request(media_request)
    bundle.all_requests_added()

    # Mark as failed with a reason
    failure_reason = "Download attempt flagged as bot download, skipping"
    bundle.update_request_status(media_request, MediaRequestLifecycleStage.FAILED, failure_reason=failure_reason)

    # Get the printed output
    result = bundle.print()
    result_text = ' '.join(result)

    # Should show "failed download" message
    assert 'Media request failed download' in result_text
    # But should NOT include the failure reason in the row
    assert failure_reason not in result_text

    # Verify the failure reason is stored
    bundled_req = bundle.media_requests[0]
    assert bundled_req.failure_reason == failure_reason
    assert bundled_req.failure_reason_sent is False


def test_media_request_bundle_get_failure_summary(fake_context):  #pylint:disable=redefined-outer-name
    """Test that get_failure_summary returns error details"""
    bundle = MultiMediaRequestBundle(
        fake_context['guild'].id,
        fake_context['channel'].id,
        fake_context['channel']
    )

    # Add multiple requests
    req1 = fake_source_dict(fake_context)
    req2 = fake_source_dict(fake_context)
    req3 = fake_source_dict(fake_context)

    bundle.set_initial_search('test search')
    bundle.set_multi_input_request()
    bundle.add_media_request(req1)
    bundle.add_media_request(req2)
    bundle.add_media_request(req3)
    bundle.all_requests_added()

    # Mark two as failed with reasons
    bundle.update_request_status(req1, MediaRequestLifecycleStage.FAILED, failure_reason="Bot download flagged")
    bundle.update_request_status(req2, MediaRequestLifecycleStage.COMPLETED)
    bundle.update_request_status(req3, MediaRequestLifecycleStage.FAILED, failure_reason="Video unavailable")

    # Get failure summary
    summary = bundle.get_failure_summary()

    # Should contain both failure reasons
    assert summary is not None
    summary_text = '\n'.join(summary)
    assert 'Error Details for Failed Downloads' in summary_text
    assert 'Bot download flagged' in summary_text
    assert 'Video unavailable' in summary_text

    # Should be marked as sent
    assert bundle.media_requests[0].failure_reason_sent is True
    assert bundle.media_requests[2].failure_reason_sent is True


def test_media_request_bundle_get_failure_summary_no_duplicates(fake_context):  #pylint:disable=redefined-outer-name
    """Test that get_failure_summary only returns each failure once"""
    bundle = MultiMediaRequestBundle(
        fake_context['guild'].id,
        fake_context['channel'].id,
        fake_context['channel']
    )

    media_request = fake_source_dict(fake_context)
    bundle.set_initial_search('test search')
    bundle.set_multi_input_request()
    bundle.add_media_request(media_request)
    bundle.all_requests_added()

    # Mark as failed
    bundle.update_request_status(media_request, MediaRequestLifecycleStage.FAILED, failure_reason="Test error")

    # Get failure summary first time
    summary1 = bundle.get_failure_summary()
    assert summary1 is not None
    summary1_text = '\n'.join(summary1)
    assert 'Test error' in summary1_text

    # Get failure summary second time - should return None since already sent
    summary2 = bundle.get_failure_summary()
    assert summary2 is None


def test_media_request_bundle_get_failure_summary_none_when_no_failures(fake_context):  #pylint:disable=redefined-outer-name
    """Test that get_failure_summary returns None when there are no failures"""
    bundle = MultiMediaRequestBundle(
        fake_context['guild'].id,
        fake_context['channel'].id,
        fake_context['channel']
    )

    media_request = fake_source_dict(fake_context)
    bundle.set_initial_search('test search')
    bundle.set_multi_input_request()
    bundle.add_media_request(media_request)
    bundle.all_requests_added()

    # Mark as completed (not failed)
    bundle.update_request_status(media_request, MediaRequestLifecycleStage.COMPLETED)

    # Should return None
    summary = bundle.get_failure_summary()
    assert summary is None


def test_media_request_bundle_failure_summary_incremental(fake_context):  #pylint:disable=redefined-outer-name
    """Test that get_failure_summary can be called multiple times as failures accumulate"""
    bundle = MultiMediaRequestBundle(
        fake_context['guild'].id,
        fake_context['channel'].id,
        fake_context['channel']
    )

    req1 = fake_source_dict(fake_context)
    req2 = fake_source_dict(fake_context)

    bundle.set_initial_search('test search')
    bundle.set_multi_input_request()
    bundle.add_media_request(req1)
    bundle.add_media_request(req2)
    bundle.all_requests_added()

    # First failure
    bundle.update_request_status(req1, MediaRequestLifecycleStage.FAILED, failure_reason="Error 1")
    summary1 = bundle.get_failure_summary()
    assert summary1 is not None
    summary1_text = '\n'.join(summary1)
    assert 'Error 1' in summary1_text
    assert 'Error 2' not in summary1_text

    # Second failure
    bundle.update_request_status(req2, MediaRequestLifecycleStage.FAILED, failure_reason="Error 2")
    summary2 = bundle.get_failure_summary()
    assert summary2 is not None
    summary2_text = '\n'.join(summary2)
    # Should only contain the new error
    assert 'Error 1' not in summary2_text
    assert 'Error 2' in summary2_text


def test_media_request_bundle_get_failure_summary_with_none_reason(fake_context):  #pylint:disable=redefined-outer-name
    """Test that get_failure_summary ignores failed requests with None failure_reason"""
    bundle = MultiMediaRequestBundle(
        fake_context['guild'].id,
        fake_context['channel'].id,
        fake_context['channel']
    )

    req1 = fake_source_dict(fake_context)
    req2 = fake_source_dict(fake_context)

    bundle.set_initial_search('test search')
    bundle.set_multi_input_request()
    bundle.add_media_request(req1)
    bundle.add_media_request(req2)
    bundle.all_requests_added()

    # Mark first as failed with reason, second as failed without reason
    bundle.update_request_status(req1, MediaRequestLifecycleStage.FAILED, failure_reason="Real error")
    bundle.update_request_status(req2, MediaRequestLifecycleStage.FAILED, failure_reason=None)

    # Get failure summary - should only include req1
    summary = bundle.get_failure_summary()
    assert summary is not None
    summary_text = '\n'.join(summary)
    assert 'Real error' in summary_text
    # Should only have one failure in the output
    assert summary_text.count('Media Request') == 1


def test_media_request_bundle_get_failure_summary_with_empty_reason(fake_context):  #pylint:disable=redefined-outer-name
    """Test that get_failure_summary ignores failed requests with empty string failure_reason"""
    bundle = MultiMediaRequestBundle(
        fake_context['guild'].id,
        fake_context['channel'].id,
        fake_context['channel']
    )

    req1 = fake_source_dict(fake_context)
    req2 = fake_source_dict(fake_context)

    bundle.set_initial_search('test search')
    bundle.set_multi_input_request()
    bundle.add_media_request(req1)
    bundle.add_media_request(req2)
    bundle.all_requests_added()

    # Mark first as failed with reason, second as failed with empty string
    bundle.update_request_status(req1, MediaRequestLifecycleStage.FAILED, failure_reason="Real error")
    bundle.update_request_status(req2, MediaRequestLifecycleStage.FAILED, failure_reason="")

    # Get failure summary - should only include req1
    summary = bundle.get_failure_summary()
    assert summary is not None
    summary_text = '\n'.join(summary)
    assert 'Real error' in summary_text
    # Should only have one failure in the output
    assert summary_text.count('Media Request') == 1


def test_media_request_bundle_get_failure_summary_all_failures_without_reasons(fake_context):  #pylint:disable=redefined-outer-name
    """Test that get_failure_summary returns None when all failures lack reasons"""
    bundle = MultiMediaRequestBundle(
        fake_context['guild'].id,
        fake_context['channel'].id,
        fake_context['channel']
    )

    req1 = fake_source_dict(fake_context)
    req2 = fake_source_dict(fake_context)

    bundle.set_initial_search('test search')
    bundle.set_multi_input_request()
    bundle.add_media_request(req1)
    bundle.add_media_request(req2)
    bundle.all_requests_added()

    # Mark both as failed without reasons
    bundle.update_request_status(req1, MediaRequestLifecycleStage.FAILED, failure_reason=None)
    bundle.update_request_status(req2, MediaRequestLifecycleStage.FAILED, failure_reason="")

    # Get failure summary - should return None since no failures have reasons
    summary = bundle.get_failure_summary()
    assert summary is None


def test_media_request_bundle_get_failure_summary_empty_bundle(fake_context):  #pylint:disable=redefined-outer-name
    """Test that get_failure_summary returns None for an empty bundle"""
    bundle = MultiMediaRequestBundle(
        fake_context['guild'].id,
        fake_context['channel'].id,
        fake_context['channel']
    )

    bundle.set_initial_search('test search')
    bundle.set_multi_input_request()
    # Don't add any requests
    bundle.all_requests_added()

    # Get failure summary - should return None since there are no requests
    summary = bundle.get_failure_summary()
    assert summary is None
