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
    assert x.retry_information.retry_count == 0

@pytest.mark.asyncio
async def test_media_request_retry_count_increments(fake_context): #pylint:disable=redefined-outer-name
    """Test that retry_count can be incremented"""
    x = fake_source_dict(fake_context)
    assert x.retry_information.retry_count == 0

    x.retry_information.retry_count += 1
    assert x.retry_information.retry_count == 1

    x.retry_information.retry_count += 1
    assert x.retry_information.retry_count == 2

@pytest.mark.asyncio
async def test_media_request_bundle_single(fake_context): #pylint:disable=redefined-outer-name
    x = fake_source_dict(fake_context)
    b = MultiMediaRequestBundle(fake_context['guild'].id, fake_context['channel'].id, fake_context['channel'])
    b.set_initial_search(x.raw_search_string)
    b.add_media_request(x)
    b.all_requests_added()
    assert b.print()[0] == f'Media request queued for download: "{x.raw_search_string}"'

    x.lifecycle_stage = MediaRequestLifecycleStage.IN_PROGRESS
    assert b.print()[0] == f'Downloading and processing media request: "{x.raw_search_string}"'

    x.lifecycle_stage = MediaRequestLifecycleStage.COMPLETED
    assert not b.print()

@pytest.mark.asyncio
async def test_media_request_bundle(fake_context): #pylint:disable=redefined-outer-name
    multi_input_string = 'https://foo.example.com/playlist'
    x = fake_source_dict(fake_context)
    y = fake_source_dict(fake_context)
    z = fake_source_dict(fake_context)

    b = MultiMediaRequestBundle(fake_context['guild'].id, fake_context['channel'].id, fake_context['channel'])
    b.set_multi_input_request(multi_input_string)
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

    x.lifecycle_stage = MediaRequestLifecycleStage.IN_PROGRESS
    print_output = b.print()
    full_output = '\n'.join(print_output)
    assert 'Processing ' in full_output
    assert '0/3 media requests processed successfully, 0 failed' in full_output
    assert 'Downloading and processing media request:' in full_output

    x.lifecycle_stage = MediaRequestLifecycleStage.COMPLETED
    y.lifecycle_stage = MediaRequestLifecycleStage.IN_PROGRESS
    print_output = b.print()
    full_output = '\n'.join(print_output)
    assert '1/3 media requests processed successfully, 0 failed' in full_output

    y.lifecycle_stage = MediaRequestLifecycleStage.FAILED
    y.failure_reason = 'cats ate the chords'
    z.lifecycle_stage = MediaRequestLifecycleStage.IN_PROGRESS
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

    z.lifecycle_stage = MediaRequestLifecycleStage.COMPLETED
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
    b.set_multi_input_request('https://foo.example.com/playlist')
    b.add_media_request(x)
    b.add_media_request(y)
    b.add_media_request(z)
    b.all_requests_added()

    # First request starts processing
    x.lifecycle_stage = MediaRequestLifecycleStage.IN_PROGRESS
    print_output = b.print()
    full_output = '\n'.join(print_output)
    assert 'Downloading and processing media request:' in full_output

    # First request fails and will retry
    x.lifecycle_stage = MediaRequestLifecycleStage.RETRY
    print_output = b.print()
    full_output = '\n'.join(print_output)
    assert 'Failed, will retry:' in full_output
    assert x.search_string in full_output

    # Second request completes successfully
    y.lifecycle_stage = MediaRequestLifecycleStage.IN_PROGRESS
    y.lifecycle_stage = MediaRequestLifecycleStage.COMPLETED
    print_output = b.print()
    full_output = '\n'.join(print_output)
    assert '1/3 media requests processed successfully, 0 failed' in full_output

    # First request (after retry) completes successfully
    x.lifecycle_stage = MediaRequestLifecycleStage.IN_PROGRESS
    x.lifecycle_stage = MediaRequestLifecycleStage.COMPLETED

    # Third request completes
    z.lifecycle_stage = MediaRequestLifecycleStage.IN_PROGRESS
    z.lifecycle_stage = MediaRequestLifecycleStage.COMPLETED

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
    x.lifecycle_stage = MediaRequestLifecycleStage.IN_PROGRESS
    assert not b.print()

@pytest.mark.asyncio
async def test_media_request_bundle_shutdown_single_item(fake_context): #pylint:disable=redefined-outer-name
    """Test shutdown behavior with single item bundle"""
    x = fake_source_dict(fake_context)

    b = MultiMediaRequestBundle(fake_context['guild'].id, fake_context['channel'].id, fake_context['channel'])
    b.set_initial_search(x.raw_search_string)
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
    # Set the enqueued flag directly - no table rows exist, so all_requests_added() would fail
    bundle.all_requests_enqueued = True
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
    media_request.lifecycle_stage = MediaRequestLifecycleStage.COMPLETED
    media_request_bundle.update_request_status()

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
    bundled_requests = list(media_request_bundle.bundled_requests)

    bundled_requests[0].media_request.lifecycle_stage = MediaRequestLifecycleStage.COMPLETED
    bundled_requests[1].media_request.lifecycle_stage = MediaRequestLifecycleStage.FAILED
    media_request_bundle.update_request_status()

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
    media_request_bundle.add_media_request(media_request)
    media_request_bundle.all_requests_added()

    result = media_request_bundle.print()
    assert len(result) == 1
    assert 'Media request queued for download: "single test"' in result[0]


def test_media_request_bundle_print_multiple_items_with_status(media_request_bundle, fake_context):  #pylint:disable=redefined-outer-name
    """Test print method with multiple items showing top message and status"""
    media_request_bundle.set_multi_input_request('playlist test')

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

    media_request_bundle.set_multi_input_request('test playlist')
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
            media_request.lifecycle_stage = status
            media_request.failure_reason = "test failure"
        else:
            media_request.lifecycle_stage = status

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
    media_request_bundle.add_media_request(media_request)
    media_request_bundle.all_requests_added()

    # Mark as failed with reason
    media_request.lifecycle_stage = MediaRequestLifecycleStage.FAILED
    media_request.failure_reason = "Video too long"

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
    media_request_bundle.add_media_request(media_request)
    media_request_bundle.all_requests_added()

    # Set to BACKOFF status
    media_request.lifecycle_stage = MediaRequestLifecycleStage.BACKOFF

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

    media_request_bundle.set_multi_input_request('test playlist')
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

    for bundled_req, stage in zip(media_request_bundle.bundled_requests, lifecycle_stages):
        bundled_req.media_request.lifecycle_stage = stage

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
    media_request.lifecycle_stage = MediaRequestLifecycleStage.BACKOFF
    media_request_bundle.update_request_status()
    assert not media_request_bundle.finished

    # IN_PROGRESS status should not be considered finished
    media_request.lifecycle_stage = MediaRequestLifecycleStage.IN_PROGRESS
    media_request_bundle.update_request_status()
    assert not media_request_bundle.finished

    # COMPLETED status should be considered finished
    media_request.lifecycle_stage = MediaRequestLifecycleStage.COMPLETED
    media_request_bundle.update_request_status()
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
    """Test FAILED status shows 'Media request failed download' row, failure_reason via get_failure_summary()"""
    bundle = MultiMediaRequestBundle(fake_context['guild'].id, fake_context['channel'].id, fake_context['channel'])

    # Add request
    req = fake_source_dict(fake_context)
    bundle.set_initial_search(req.raw_search_string)
    bundle.add_media_request(req)
    bundle.all_requests_added()

    # Set FAILED with a failure_reason directly on the request
    req.lifecycle_stage = MediaRequestLifecycleStage.FAILED
    req.failure_reason = "Original failure"

    # The row shows "Media request failed download" (NOT the failure_reason inline)
    messages = bundle.print()
    assert len(messages) == 1
    assert "Media request failed download" in messages[0]
    assert "Original failure" not in messages[0]

    # The failure_reason is available via get_failure_summary()
    summary = bundle.get_failure_summary()
    assert summary is not None
    assert "Original failure" in '\n'.join(summary)


def test_bundle_empty_message_list(fake_context):  #pylint:disable=redefined-outer-name
    """Test bundle when all items are completed/discarded (empty message list)"""
    bundle = MultiMediaRequestBundle(123, 456, fake_context['channel'])

    # Add request that will be completed (shouldn't appear in messages)
    req = MediaRequest(123, 456, "user", 1, "search", "search", SearchType.SEARCH, download_file=True)
    bundle.set_initial_search("search")
    bundle.add_media_request(req)
    bundle.all_requests_added()
    req.lifecycle_stage = MediaRequestLifecycleStage.COMPLETED

    # Print should return empty list when no messages to display
    messages = bundle.print()
    assert not messages


def test_bundle_single_item_no_status_header(fake_context):  #pylint:disable=redefined-outer-name
    """Test that single-item bundles don't include status header"""
    bundle = MultiMediaRequestBundle(123, 456, fake_context['channel'])

    # Add single failed request
    req = MediaRequest(123, 456, "user", 1, "search", "search", SearchType.SEARCH, download_file=True)
    bundle.set_initial_search("search")
    bundle.add_media_request(req)
    bundle.all_requests_added()
    req.lifecycle_stage = MediaRequestLifecycleStage.FAILED
    req.failure_reason = "Test failure"

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
    bundle.set_multi_input_request("test-playlist")

    # Add multiple requests
    requests = []
    for _ in range(3):
        req = fake_source_dict(fake_context)
        bundle.add_media_request(req)
        requests.append(req)
    bundle.all_requests_added()

    requests[0].lifecycle_stage = MediaRequestLifecycleStage.COMPLETED
    requests[1].lifecycle_stage = MediaRequestLifecycleStage.FAILED
    requests[2].lifecycle_stage = MediaRequestLifecycleStage.QUEUED

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
    req1.lifecycle_stage = MediaRequestLifecycleStage.COMPLETED
    bundle.update_request_status()
    assert not bundle.finished_successfully

    # Fail second request - still not finished successfully (has failures)
    req2.lifecycle_stage = MediaRequestLifecycleStage.FAILED
    bundle.update_request_status()
    assert not bundle.finished_successfully

    # Discard third request - still not finished successfully (has failures)
    req3.lifecycle_stage = MediaRequestLifecycleStage.DISCARDED
    bundle.update_request_status()
    assert not bundle.finished_successfully

    # Test scenario where all are completed or discarded (no failures)
    bundle2 = MultiMediaRequestBundle(fake_context['guild'].id, fake_context['channel'].id, fake_context['channel'])
    req4 = fake_source_dict(fake_context)
    req5 = fake_source_dict(fake_context)
    bundle2.add_media_request(req4)
    bundle2.add_media_request(req5)

    req4.lifecycle_stage = MediaRequestLifecycleStage.COMPLETED
    req5.lifecycle_stage = MediaRequestLifecycleStage.DISCARDED
    bundle2.update_request_status()

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
    bundle.set_multi_input_request("test-playlist")

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
    req1.lifecycle_stage = MediaRequestLifecycleStage.COMPLETED
    req2.lifecycle_stage = MediaRequestLifecycleStage.COMPLETED

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
    messages = bundle.print()
    full_message = "\n".join(messages)
    assert "<https://example.com/playlist>" in full_message

    bundle2 = MultiMediaRequestBundle(fake_context['guild'].id, fake_context['channel'].id, fake_context['channel'])
    bundle2.set_multi_input_request('My Playlist')
    req1 = fake_source_dict(fake_context)
    req2 = fake_source_dict(fake_context)
    bundle2.add_media_request(req1)
    bundle2.add_media_request(req2)
    bundle2.all_requests_added()
    # Test non-URL doesn't get wrapped

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
    bundle.set_multi_input_request("test-playlist")

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
    requests[0].lifecycle_stage = MediaRequestLifecycleStage.COMPLETED
    requests[1].lifecycle_stage = MediaRequestLifecycleStage.COMPLETED

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
    requests[0].lifecycle_stage = MediaRequestLifecycleStage.COMPLETED
    requests[1].lifecycle_stage = MediaRequestLifecycleStage.COMPLETED

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
    bundle.set_multi_input_request("spotify:album:123abc")

    # During search phase (before set_multi_input_request), print should show processing message
    result = bundle.print()
    assert len(result) == 1
    assert 'Processing "spotify:album:123abc"' in result[0]

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
    bundle.add_media_request(media_request)
    bundle.all_requests_added()

    # Mark as failed with a reason
    failure_reason = "Download attempt flagged as bot download, skipping"
    media_request.lifecycle_stage = MediaRequestLifecycleStage.FAILED
    media_request.failure_reason = failure_reason

    # Get the printed output
    result = bundle.print()
    result_text = ' '.join(result)

    # Should show "failed download" message
    assert 'Media request failed download' in result_text
    # But should NOT include the failure reason in the row
    assert failure_reason not in result_text

    # Verify the failure reason is stored
    bundled_req = bundle.bundled_requests[0]
    assert bundled_req.media_request.failure_reason == failure_reason
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
    bundle.add_media_request(req1)
    bundle.add_media_request(req2)
    bundle.add_media_request(req3)
    bundle.all_requests_added()

    # Mark two as failed with reasons
    req1.lifecycle_stage = MediaRequestLifecycleStage.FAILED
    req1.failure_reason = "Bot download flagged"
    req2.lifecycle_stage = MediaRequestLifecycleStage.COMPLETED
    req3.lifecycle_stage = MediaRequestLifecycleStage.FAILED
    req3.failure_reason = "Video unavailable"

    # Get failure summary
    summary = bundle.get_failure_summary()

    # Should contain both failure reasons
    assert summary is not None
    summary_text = '\n'.join(summary)
    assert 'Error Details for Failed Downloads' in summary_text
    assert 'Bot download flagged' in summary_text
    assert 'Video unavailable' in summary_text

    # Should be marked as sent
    assert bundle.bundled_requests[0].failure_reason_sent is True
    assert bundle.bundled_requests[2].failure_reason_sent is True


def test_media_request_bundle_get_failure_summary_no_duplicates(fake_context):  #pylint:disable=redefined-outer-name
    """Test that get_failure_summary only returns each failure once"""
    bundle = MultiMediaRequestBundle(
        fake_context['guild'].id,
        fake_context['channel'].id,
        fake_context['channel']
    )

    media_request = fake_source_dict(fake_context)
    bundle.set_initial_search('test search')
    bundle.add_media_request(media_request)
    bundle.all_requests_added()

    # Mark as failed
    media_request.lifecycle_stage = MediaRequestLifecycleStage.FAILED
    media_request.failure_reason = "Test error"

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
    bundle.add_media_request(media_request)
    bundle.all_requests_added()

    # Mark as completed (not failed)
    media_request.lifecycle_stage = MediaRequestLifecycleStage.COMPLETED
    bundle.update_request_status()

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
    bundle.add_media_request(req1)
    bundle.add_media_request(req2)
    bundle.all_requests_added()

    # First failure
    req1.lifecycle_stage = MediaRequestLifecycleStage.FAILED
    req1.failure_reason = "Error 1"
    summary1 = bundle.get_failure_summary()
    assert summary1 is not None
    summary1_text = '\n'.join(summary1)
    assert 'Error 1' in summary1_text
    assert 'Error 2' not in summary1_text

    # Second failure
    req2.lifecycle_stage = MediaRequestLifecycleStage.FAILED
    req2.failure_reason = "Error 2"
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
    bundle.add_media_request(req1)
    bundle.add_media_request(req2)
    bundle.all_requests_added()

    # Mark first as failed with reason, second as failed without reason
    req1.lifecycle_stage = MediaRequestLifecycleStage.FAILED
    req1.failure_reason = "Real error"
    req2.lifecycle_stage = MediaRequestLifecycleStage.FAILED
    req2.failure_reason = None

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
    bundle.add_media_request(req1)
    bundle.add_media_request(req2)
    bundle.all_requests_added()

    # Mark first as failed with reason, second as failed with empty string
    req1.lifecycle_stage = MediaRequestLifecycleStage.FAILED
    req1.failure_reason = "Real error"
    req2.lifecycle_stage = MediaRequestLifecycleStage.FAILED
    req2.failure_reason = ""

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
    bundle.add_media_request(req1)
    bundle.add_media_request(req2)
    bundle.all_requests_added()

    # Mark both as failed without reasons
    req1.lifecycle_stage = MediaRequestLifecycleStage.FAILED
    req1.failure_reason = None
    req2.lifecycle_stage = MediaRequestLifecycleStage.FAILED
    req2.failure_reason = ""

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
    # Don't add any requests
    bundle.all_requests_added()

    # Get failure summary - should return None since there are no requests
    summary = bundle.get_failure_summary()
    assert summary is None


# Tests for get_retry_summary

def test_media_request_bundle_get_retry_summary_basic(fake_context):  #pylint:disable=redefined-outer-name
    """Test basic get_retry_summary functionality"""
    bundle = MultiMediaRequestBundle(
        fake_context['guild'].id,
        fake_context['channel'].id,
        fake_context['channel']
    )

    media_request = fake_source_dict(fake_context)
    bundle.set_initial_search('test search')
    bundle.add_media_request(media_request)
    bundle.all_requests_added()

    # Mark as retry with reason
    media_request.lifecycle_stage = MediaRequestLifecycleStage.RETRY
    media_request.retry_information.retry_reason = "Bot flagged download"
    media_request.retry_information.retry_count = 1

    # Get retry summary
    summary = bundle.get_retry_summary(max_retries=3)

    assert summary is not None
    assert len(summary) == 1
    assert 'Retrying' in summary[0]
    assert 'attempt 1/3' in summary[0]
    assert 'Bot flagged download' in summary[0]
    assert '```' in summary[0]  # Code block formatting


def test_media_request_bundle_get_retry_summary_with_backoff(fake_context):  #pylint:disable=redefined-outer-name
    """Test get_retry_summary includes backoff time"""
    bundle = MultiMediaRequestBundle(
        fake_context['guild'].id,
        fake_context['channel'].id,
        fake_context['channel']
    )

    media_request = fake_source_dict(fake_context)
    bundle.set_initial_search('test search')
    bundle.add_media_request(media_request)
    bundle.all_requests_added()

    # Mark as retry with backoff time (240 seconds = 4 minutes)
    media_request.lifecycle_stage = MediaRequestLifecycleStage.RETRY
    media_request.retry_information.retry_reason = "Bot flagged download"
    media_request.retry_information.retry_count = 1
    media_request.retry_information.retry_backoff_seconds = 240

    summary = bundle.get_retry_summary(max_retries=3)

    assert summary is not None
    assert 'retrying in ~4 minutes' in summary[0]


def test_media_request_bundle_get_retry_summary_backoff_seconds(fake_context):  #pylint:disable=redefined-outer-name
    """Test get_retry_summary shows seconds for short backoff"""
    bundle = MultiMediaRequestBundle(
        fake_context['guild'].id,
        fake_context['channel'].id,
        fake_context['channel']
    )

    media_request = fake_source_dict(fake_context)
    bundle.set_initial_search('test search')
    bundle.add_media_request(media_request)
    bundle.all_requests_added()

    # Mark as retry with short backoff (30 seconds)
    media_request.lifecycle_stage = MediaRequestLifecycleStage.RETRY
    media_request.retry_information.retry_reason = "Bot flagged download"
    media_request.retry_information.retry_count = 1
    media_request.retry_information.retry_backoff_seconds = 30

    summary = bundle.get_retry_summary(max_retries=3)

    assert summary is not None
    assert 'retrying in ~30 seconds' in summary[0]


def test_media_request_bundle_get_retry_summary_backoff_singular_minute(fake_context):  #pylint:disable=redefined-outer-name
    """Test get_retry_summary uses singular 'minute' for 1 minute"""
    bundle = MultiMediaRequestBundle(
        fake_context['guild'].id,
        fake_context['channel'].id,
        fake_context['channel']
    )

    media_request = fake_source_dict(fake_context)
    bundle.set_initial_search('test search')
    bundle.add_media_request(media_request)
    bundle.all_requests_added()

    # Mark as retry with 60 seconds = 1 minute
    media_request.lifecycle_stage = MediaRequestLifecycleStage.RETRY
    media_request.retry_information.retry_reason = "Bot flagged download"
    media_request.retry_information.retry_count = 1
    media_request.retry_information.retry_backoff_seconds = 60

    summary = bundle.get_retry_summary(max_retries=3)

    assert summary is not None
    assert 'retrying in ~1 minute' in summary[0]
    assert 'minutes' not in summary[0]


def test_media_request_bundle_get_retry_summary_no_duplicates(fake_context):  #pylint:disable=redefined-outer-name
    """Test that get_retry_summary only returns each retry once"""
    bundle = MultiMediaRequestBundle(
        fake_context['guild'].id,
        fake_context['channel'].id,
        fake_context['channel']
    )

    media_request = fake_source_dict(fake_context)
    bundle.set_initial_search('test search')
    bundle.add_media_request(media_request)
    bundle.all_requests_added()

    # Mark as retry
    media_request.lifecycle_stage = MediaRequestLifecycleStage.RETRY
    media_request.retry_information.retry_reason = "Test error"
    media_request.retry_information.retry_count = 1

    # Get retry summary first time
    summary1 = bundle.get_retry_summary(max_retries=3)
    assert summary1 is not None
    assert 'Test error' in summary1[0]

    # Get retry summary second time - should return None since already sent
    summary2 = bundle.get_retry_summary(max_retries=3)
    assert summary2 is None


def test_media_request_bundle_get_retry_summary_none_when_no_retries(fake_context):  #pylint:disable=redefined-outer-name
    """Test that get_retry_summary returns None when there are no retries"""
    bundle = MultiMediaRequestBundle(
        fake_context['guild'].id,
        fake_context['channel'].id,
        fake_context['channel']
    )

    media_request = fake_source_dict(fake_context)
    bundle.set_initial_search('test search')
    bundle.add_media_request(media_request)
    bundle.all_requests_added()

    # Mark as completed (not retry)
    media_request.lifecycle_stage = MediaRequestLifecycleStage.COMPLETED
    bundle.update_request_status()

    # Should return None
    summary = bundle.get_retry_summary(max_retries=3)
    assert summary is None


def test_media_request_bundle_get_retry_summary_multiple_retries(fake_context):  #pylint:disable=redefined-outer-name
    """Test get_retry_summary with multiple retrying requests"""
    bundle = MultiMediaRequestBundle(
        fake_context['guild'].id,
        fake_context['channel'].id,
        fake_context['channel']
    )

    req1 = fake_source_dict(fake_context)
    req2 = fake_source_dict(fake_context)
    req3 = fake_source_dict(fake_context)

    bundle.set_initial_search('test search')
    bundle.add_media_request(req1)
    bundle.add_media_request(req2)
    bundle.add_media_request(req3)
    bundle.all_requests_added()

    # Mark two as retry with different reasons
    req1.lifecycle_stage = MediaRequestLifecycleStage.RETRY
    req1.retry_information.retry_reason = "Error 1"
    req1.retry_information.retry_count = 1
    req2.lifecycle_stage = MediaRequestLifecycleStage.COMPLETED
    req3.lifecycle_stage = MediaRequestLifecycleStage.RETRY
    req3.retry_information.retry_reason = "Error 2"
    req3.retry_information.retry_count = 2

    # Get retry summary
    summary = bundle.get_retry_summary(max_retries=3)

    assert summary is not None
    assert len(summary) == 2
    # Check both errors are present in separate messages
    all_text = '\n'.join(summary)
    assert 'Error 1' in all_text
    assert 'Error 2' in all_text


def test_media_request_bundle_get_retry_summary_with_none_reason(fake_context):  #pylint:disable=redefined-outer-name
    """Test that get_retry_summary ignores retries with None retry_reason"""
    bundle = MultiMediaRequestBundle(
        fake_context['guild'].id,
        fake_context['channel'].id,
        fake_context['channel']
    )

    req1 = fake_source_dict(fake_context)
    req2 = fake_source_dict(fake_context)

    bundle.set_initial_search('test search')
    bundle.add_media_request(req1)
    bundle.add_media_request(req2)
    bundle.all_requests_added()

    # Mark first as retry with reason, second as retry without reason
    req1.lifecycle_stage = MediaRequestLifecycleStage.RETRY
    req1.retry_information.retry_reason = "Real error"
    req1.retry_information.retry_count = 1
    req2.lifecycle_stage = MediaRequestLifecycleStage.RETRY
    req2.retry_information.retry_reason = None
    req2.retry_information.retry_count = 1

    # Get retry summary - should only include req1
    summary = bundle.get_retry_summary(max_retries=3)
    assert summary is not None
    assert len(summary) == 1
    assert 'Real error' in summary[0]


def test_media_request_bundle_get_retry_summary_empty_bundle(fake_context):  #pylint:disable=redefined-outer-name
    """Test that get_retry_summary returns None for an empty bundle"""
    bundle = MultiMediaRequestBundle(
        fake_context['guild'].id,
        fake_context['channel'].id,
        fake_context['channel']
    )

    bundle.set_initial_search('test search')
    bundle.all_requests_added()

    # Get retry summary - should return None since there are no requests
    summary = bundle.get_retry_summary(max_retries=3)
    assert summary is None


def test_media_request_bundle_get_retry_summary_truncates_long_reason(fake_context):  #pylint:disable=redefined-outer-name
    """Test that get_retry_summary truncates very long error messages"""
    bundle = MultiMediaRequestBundle(
        fake_context['guild'].id,
        fake_context['channel'].id,
        fake_context['channel']
    )

    media_request = fake_source_dict(fake_context)
    bundle.set_initial_search('test search')
    bundle.add_media_request(media_request)
    bundle.all_requests_added()

    # Create a very long error message (longer than Discord's 2000 char limit)
    long_error = "A" * 3000

    media_request.lifecycle_stage = MediaRequestLifecycleStage.RETRY
    media_request.retry_information.retry_reason = long_error
    media_request.retry_information.retry_count = 1

    summary = bundle.get_retry_summary(max_retries=3)

    assert summary is not None
    # Message should be truncated to fit within Discord's limit
    assert len(summary[0]) <= DISCORD_MAX_MESSAGE_LENGTH
    # Should still have the code block formatting
    assert summary[0].startswith('Retrying')
    assert summary[0].endswith('```')


def test_media_request_bundle_get_retry_summary_incremental(fake_context):  #pylint:disable=redefined-outer-name
    """Test that get_retry_summary can be called multiple times as retries accumulate"""
    bundle = MultiMediaRequestBundle(
        fake_context['guild'].id,
        fake_context['channel'].id,
        fake_context['channel']
    )

    req1 = fake_source_dict(fake_context)
    req2 = fake_source_dict(fake_context)

    bundle.set_initial_search('test search')
    bundle.add_media_request(req1)
    bundle.add_media_request(req2)
    bundle.all_requests_added()

    # First retry
    req1.lifecycle_stage = MediaRequestLifecycleStage.RETRY
    req1.retry_information.retry_reason = "Error 1"
    req1.retry_information.retry_count = 1
    summary1 = bundle.get_retry_summary(max_retries=3)
    assert summary1 is not None
    assert len(summary1) == 1
    assert 'Error 1' in summary1[0]

    # Second retry (different request)
    req2.lifecycle_stage = MediaRequestLifecycleStage.RETRY
    req2.retry_information.retry_reason = "Error 2"
    req2.retry_information.retry_count = 1
    summary2 = bundle.get_retry_summary(max_retries=3)
    assert summary2 is not None
    assert len(summary2) == 1
    # Should only contain the new retry
    assert 'Error 1' not in summary2[0]
    assert 'Error 2' in summary2[0]
