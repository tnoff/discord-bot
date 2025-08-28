"""
Tests for MediaRequestBundle lifecycle and print functionality
"""
import pytest

from discord_bot.cogs.music_helpers.media_request import MultiMediaRequestBundle, MediaRequest
from discord_bot.cogs.music_helpers.common import SearchType, MediaRequestLifecycleStage
from tests.helpers import generate_fake_context


@pytest.fixture
def fake_context():  #pylint:disable=redefined-outer-name
    """Generate fake context for tests"""
    return generate_fake_context()


@pytest.fixture
def media_request_bundle(fake_context):  #pylint:disable=redefined-outer-name
    """Create a media request bundle for testing"""
    return MultiMediaRequestBundle(
        fake_context['guild'].id,
        fake_context['channel'].id,
        fake_context['channel'],
        items_per_message=3
    )


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
        SearchType.SEARCH
    )
    media_request_1.uuid = requests[0]['uuid']

    media_request_2 = MediaRequest(
        fake_context['guild'].id,
        fake_context['channel'].id,
        'test_user',
        123456,
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
        SearchType.SEARCH
    )
    media_request_bundle.add_media_request(media_request)

    result = media_request_bundle.print()
    assert len(result) == 1
    assert 'Media request queued for download: "single test"' in result[0]


def test_media_request_bundle_print_multiple_items_with_status(media_request_bundle, fake_context):  #pylint:disable=redefined-outer-name
    """Test print method with multiple items showing top message and status"""
    media_request_bundle.multi_input_string = 'playlist test'

    # Add multiple requests
    for i in range(3):
        media_request = MediaRequest(
            fake_context['guild'].id,
            fake_context['channel'].id,
            'test_user',
            123456,
            f'test {i}',
            SearchType.SEARCH
        )
        media_request_bundle.add_media_request(media_request)

    result = media_request_bundle.print()

    # Should have top message
    assert any('Downloading "playlist test"' in msg for msg in result)
    assert any('0/3 items downloaded successfully, 0 failed' in msg for msg in result)


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
        SearchType.DIRECT
    )
    media_request_bundle.add_media_request(media_request)

    result = media_request_bundle.print()
    result_text = ' '.join(result)

    # URL should be wrapped in angle brackets
    assert '<https://example.com/video>' in result_text
