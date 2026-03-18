import asyncio
from datetime import datetime, timezone, timedelta
from pathlib import Path
from tempfile import NamedTemporaryFile, TemporaryDirectory
from unittest.mock import patch

import pytest
from yt_dlp.utils import DownloadError

from discord_bot.cogs.music_helpers.download_client import (
    DownloadClient, VideoTooLong, VideoBanned, BotDownloadFlagged, RetryableException, RetryLimitExceeded,
    DownloadTerminalException, DownloadClientException, VideoAgeRestrictedException, match_generator
)
from discord_bot.exceptions import ExitEarlyException
from discord_bot.types.download import DownloadErrorType, DownloadResult, DownloadStatus as DlStatus
from discord_bot.utils.failure_queue import FailureQueue as DownloadFailureQueue, FailureStatus as DownloadStatus

from discord_bot.types.playlist_add_request import PlaylistAddRequest
from discord_bot.types.search import SearchResult
from discord_bot.cogs.music_helpers.common import SearchType
from tests.helpers import fake_source_dict, generate_fake_context

class MockYTDLP():
    def __init__(self, fake_file_path : Path = 'foo-bar.mp3'):
        self.fake_file_path = fake_file_path

    def extract_info(self, _search_string, download=True):
        data = {
            'entries': [
                {
                    'webpage_url': 'https://example.foo.com',
                    'title': 'Foo Title',
                    'uploader': 'Foo Uploader',
                    'duration': 1234,
                    'extractor': 'test-extractor',
                },
            ]
        }
        if download:
            data['entries'][0]['requested_downloads'] = [
                {
                    'filepath': self.fake_file_path,
                    'original_path': 'foo-bar-original.mp3',
                },
            ]
        return data

class MockYTDLPNoData():
    def __init__(self):
        pass

    def extract_info(self, _search_string, download=True): #pylint:disable=unused-argument
        return {
            'entries': []
        }

def yield_dlp_error(message):
    class MockYTDLPError():
        def __init__(self):
            pass

        def extract_info(self, _search_string, **_kwargs):
            raise DownloadError(message)
    return MockYTDLPError()

class MockYoutubeMusic():
    def __init__(self):
        pass

    def search(self, *_args, **_kwargs):
        return 'vid-1234'


@pytest.mark.asyncio(loop_scope="session")
async def test_prepare_source():
    loop = asyncio.get_running_loop()
    with TemporaryDirectory() as tmp_dir:
        with NamedTemporaryFile(delete=False) as tmp_file:
            fake_context = generate_fake_context()
            x = DownloadClient(MockYTDLP(fake_file_path=Path(tmp_file.name)), Path(tmp_dir))
            y = fake_source_dict(fake_context)
            result = await x.create_source(y, 3, loop)
            assert result.status.success
            assert result.ytdlp_data['webpage_url'] == 'https://example.foo.com'

@pytest.mark.asyncio(loop_scope="session")
async def test_prepare_source_s3_mode():
    '''In S3 mode, upload_file is called, local file deleted, result.file_name is S3 key'''
    loop = asyncio.get_running_loop()
    with TemporaryDirectory() as tmp_dir:
        with NamedTemporaryFile(delete=False, suffix='.mp3') as tmp_file:
            fake_context = generate_fake_context()
            x = DownloadClient(MockYTDLP(fake_file_path=Path(tmp_file.name)), Path(tmp_dir),
                               bucket_name='test-bucket')
            y = fake_source_dict(fake_context)
            with patch('discord_bot.cogs.music_helpers.download_client.upload_file', return_value=True) as upload_mock:
                result = await x.create_source(y, 3, loop)
            assert result.status.success
            upload_mock.assert_called_once()
            call_args = upload_mock.call_args[0]
            assert call_args[0] == 'test-bucket'
            assert str(call_args[2]).startswith('cache/')
            assert str(call_args[2]).endswith('.mp3')
            # local file gone, result carries the S3 key
            assert not call_args[1].exists()
            assert str(result.file_name).startswith('cache/')

@pytest.mark.asyncio(loop_scope="session")
async def test_prepare_source_empty_requested_downloads():
    """requested_downloads list is empty — should return FILE_NOT_FOUND, not crash."""
    loop = asyncio.get_running_loop()

    class MockYTDLPEmptyDownloads():
        def extract_info(self, _search_string, **_kwargs):
            return {'entries': [{'webpage_url': 'https://example.foo.com', 'title': 'T',
                                 'uploader': 'U', 'duration': 10, 'extractor': 'youtube',
                                 'requested_downloads': []}]}

    with TemporaryDirectory() as tmp_dir:
        fake_context = generate_fake_context()
        x = DownloadClient(MockYTDLPEmptyDownloads(), Path(tmp_dir))
        y = fake_source_dict(fake_context)
        result = await x.create_source(y, 3, loop)
        assert not result.status.success
        assert result.status.error_type == DownloadErrorType.FILE_NOT_FOUND


@pytest.mark.asyncio(loop_scope="session")
async def test_prepare_source_filepath_does_not_exist():
    """filepath returned by yt-dlp does not exist on disk — should return FILE_NOT_FOUND."""
    loop = asyncio.get_running_loop()

    class MockYTDLPMissingFile():
        def extract_info(self, _search_string, **_kwargs):
            return {'entries': [{'webpage_url': 'https://example.foo.com', 'title': 'T',
                                 'uploader': 'U', 'duration': 10, 'extractor': 'youtube',
                                 'requested_downloads': [{'filepath': '/nonexistent/no/such/file.mp3'}]}]}

    with TemporaryDirectory() as tmp_dir:
        fake_context = generate_fake_context()
        x = DownloadClient(MockYTDLPMissingFile(), Path(tmp_dir))
        y = fake_source_dict(fake_context)
        result = await x.create_source(y, 3, loop)
        assert not result.status.success
        assert result.status.error_type == DownloadErrorType.FILE_NOT_FOUND


@pytest.mark.asyncio(loop_scope="session")
async def test_prepare_source_copyfile_raises_file_not_found():
    """copyfile raises FileNotFoundError (file removed between check and copy) — should return FILE_NOT_FOUND."""
    loop = asyncio.get_running_loop()
    with TemporaryDirectory() as tmp_dir:
        with NamedTemporaryFile(delete=False) as tmp_file:
            fake_context = generate_fake_context()
            x = DownloadClient(MockYTDLP(fake_file_path=Path(tmp_file.name)), Path(tmp_dir))
            y = fake_source_dict(fake_context)
            with patch('discord_bot.cogs.music_helpers.download_client.copyfile', side_effect=FileNotFoundError('file gone')):
                result = await x.create_source(y, 3, loop)
    assert not result.status.success
    assert result.status.error_type == DownloadErrorType.FILE_NOT_FOUND
    assert 'File not found after download' in result.status.error_detail


@pytest.mark.asyncio(loop_scope="session")
async def test_prepare_source_no_download():
    loop = asyncio.get_running_loop()
    fake_context = generate_fake_context()
    x = DownloadClient(MockYTDLP(), None)
    y = PlaylistAddRequest(guild_id=fake_context['guild'].id, channel_id=fake_context['channel'].id,
                           requester_name=fake_context['author'].display_name, requester_id=fake_context['author'].id,
                           search_result=SearchResult(search_type=SearchType.DIRECT, raw_search_string='https://example.foo.com'),
                           playlist_id=1)
    result = await x.create_source(y, 3, loop)
    assert result.status.success
    assert result.ytdlp_data['webpage_url'] == 'https://example.foo.com'

@pytest.mark.asyncio(loop_scope="session")
async def test_prepare_source_errors():
    loop = asyncio.get_running_loop()
    fake_context = generate_fake_context()

    x = DownloadClient(yield_dlp_error('Sign in to confirm your age. This video may be inappropriate for some users'), None)
    y = fake_source_dict(fake_context)
    result = await x.create_source(y, 3, loop)
    assert not result.status.success
    assert result.status.error_type == DownloadErrorType.AGE_RESTRICTED
    assert 'Video is age restricted, cannot download' in result.status.user_message

    x = DownloadClient(yield_dlp_error("This video has been removed for violating YouTube's Terms of Service"), None)
    y = fake_source_dict(fake_context)
    result = await x.create_source(y, 3, loop)
    assert not result.status.success
    assert result.status.error_type == DownloadErrorType.TERMS_VIOLATION
    assert 'Video is unvailable due to violating terms of service, cannot download' in result.status.user_message

    x = DownloadClient(yield_dlp_error('Video unavailable'), None)
    y = fake_source_dict(fake_context)
    result = await x.create_source(y, 3, loop)
    assert not result.status.success
    assert result.status.error_type == DownloadErrorType.UNAVAILABLE
    assert 'Video is unavailable, cannot download' in result.status.user_message

    x = DownloadClient(yield_dlp_error('Private video'), None)
    y = fake_source_dict(fake_context)
    result = await x.create_source(y, 3, loop)
    assert not result.status.success
    assert result.status.error_type == DownloadErrorType.PRIVATE_VIDEO
    assert 'Video is private, cannot download' in result.status.user_message

    x = DownloadClient(yield_dlp_error("Sign in to confirm you're not a bot"), None)
    y = fake_source_dict(fake_context)
    result = await x.create_source(y, 3, loop)
    assert not result.status.success
    assert result.status.error_type == DownloadErrorType.BOT_FLAGGED
    # download_client no longer increments retry_count; music.py does
    assert result.media_request.download_retry_information.retry_count == 0

    x = DownloadClient(yield_dlp_error('Requested format is not available'), None)
    y = fake_source_dict(fake_context)
    result = await x.create_source(y, 3, loop)
    assert not result.status.success
    assert result.status.error_type == DownloadErrorType.INVALID_FORMAT
    assert 'Video is not available in requested format' in result.status.user_message

    x = DownloadClient(MockYTDLPNoData(), None)
    y = fake_source_dict(fake_context)
    result = await x.create_source(y, 3, loop)
    assert not result.status.success
    assert result.status.error_type == DownloadErrorType.NOT_FOUND
    assert 'No videos found' in result.status.user_message

def yield_metadata_check_error(exception):
    class MockYTDLPMetadataError():
        def extract_info(self, _search_string, **_kwargs):
            raise exception
    return MockYTDLPMetadataError()


@pytest.mark.asyncio(loop_scope="session")
async def test_prepare_source_video_too_long():
    loop = asyncio.get_running_loop()
    fake_context = generate_fake_context()
    x = DownloadClient(yield_metadata_check_error(VideoTooLong('Video Too Long', user_message='too long message')), None)
    y = fake_source_dict(fake_context)
    result = await x.create_source(y, 3, loop)
    assert not result.status.success
    assert result.status.error_type == DownloadErrorType.TOO_LONG
    assert result.status.user_message == 'too long message'


@pytest.mark.asyncio(loop_scope="session")
async def test_prepare_source_video_banned():
    loop = asyncio.get_running_loop()
    fake_context = generate_fake_context()
    x = DownloadClient(yield_metadata_check_error(VideoBanned('Video Banned', user_message='banned message')), None)
    y = fake_source_dict(fake_context)
    result = await x.create_source(y, 3, loop)
    assert not result.status.success
    assert result.status.error_type == DownloadErrorType.BANNED
    assert result.status.user_message == 'banned message'


def test_match_generator_video_too_long_improved_message():
    """Test improved error message for VideoTooLong exception via match_generator"""
    # Test the match_generator filter function directly
    filter_func = match_generator(max_video_length=3600, banned_videos_list=None)

    # Mock video info that exceeds max length
    video_info = {
        'webpage_url': 'https://example.com/long-video',
        'title': 'Very Long Video',
        'uploader': 'Test Uploader',
        'duration': 7200,  # 2 hours, longer than max
        'extractor': 'test-extractor',
    }

    with pytest.raises(VideoTooLong) as exc:
        filter_func(video_info, incomplete=False)

    # Check for improved error message format in user_message
    error_msg = exc.value.user_message
    assert 'duration 7200 seconds' in error_msg
    assert 'exceeds max duration of 3600 seconds' in error_msg

def test_match_generator_video_within_length_limit():
    """Test that videos within length limit pass through match_generator"""
    # Test the match_generator filter function directly
    filter_func = match_generator(max_video_length=3600, banned_videos_list=None)

    # Mock video info that is within length limit
    video_info = {
        'webpage_url': 'https://example.com/short-video',
        'title': 'Short Video',
        'uploader': 'Test Uploader',
        'duration': 1800,  # 30 minutes, within limit
        'extractor': 'test-extractor',
    }

    # Should not raise any exception
    filter_func(video_info, incomplete=False)

@pytest.mark.asyncio(loop_scope="session")
async def test_retryable_exception_on_timeout():
    """Test that RetryableException is returned for 'Read timed out.' errors"""
    loop = asyncio.get_running_loop()
    fake_context = generate_fake_context()

    x = DownloadClient(yield_dlp_error('Read timed out.'), None)
    y = fake_source_dict(fake_context)

    result = await x.create_source(y, 3, loop)

    assert not result.status.success
    assert result.status.error_type == DownloadErrorType.RETRYABLE
    assert result.media_request == y

@pytest.mark.asyncio(loop_scope="session")
async def test_retryable_exception_increments_retry_count():
    """Test that retry_count is NOT incremented by download_client (music.py does it)"""
    loop = asyncio.get_running_loop()
    fake_context = generate_fake_context()

    x = DownloadClient(yield_dlp_error('Read timed out.'), None)
    y = fake_source_dict(fake_context)

    assert y.download_retry_information.retry_count == 0

    result = await x.create_source(y, 3, loop)

    # download_client no longer increments; music.py does after inspecting result
    assert y.download_retry_information.retry_count == 0
    assert not result.status.success
    assert result.status.error_type == DownloadErrorType.RETRYABLE

@pytest.mark.asyncio(loop_scope="session")
async def test_all_unknown_errors_are_retryable():
    """Test that all unknown errors are now treated as RetryableException"""
    loop = asyncio.get_running_loop()
    fake_context = generate_fake_context()

    test_errors = [
        'Read timed out.',
        'tlsv1 alert protocol version',
        'Some other random error',
        'Connection refused',
    ]

    for error_message in test_errors:
        x = DownloadClient(yield_dlp_error(error_message), None)
        y = fake_source_dict(fake_context)

        result = await x.create_source(y, 3, loop)

        assert not result.status.success
        assert result.status.error_type == DownloadErrorType.RETRYABLE
        assert result.media_request == y
        # download_client no longer increments retry_count
        assert y.download_retry_information.retry_count == 0

# ========== DownloadFailureQueue Tests ==========

def test_failure_queue_basic_creation():
    """Test that queue can be created with valid parameters"""
    # Test default parameters
    queue1 = DownloadFailureQueue()
    assert queue1.size == 0
    assert queue1.max_age_seconds == 300

    # Test custom parameters
    queue2 = DownloadFailureQueue(max_size=50, max_age_seconds=600)
    assert queue2.size == 0
    assert queue2.max_age_seconds == 600


def test_failure_queue_old_item_cleanup():
    """Test that old items are automatically cleaned up"""
    queue = DownloadFailureQueue(max_size=10, max_age_seconds=60)

    # Add an old item directly to queue
    old_item = DownloadStatus(success=False, exception_type="OldException", exception_message="Old error")
    old_item.created_at = datetime.now(timezone.utc) - timedelta(seconds=120)
    queue.queue.put_nowait(old_item)

    # Add recent items
    for i in range(3):
        recent_item = DownloadStatus(success=False, exception_type="RecentException", exception_message=f"Recent error {i}")
        recent_item.created_at = datetime.now(timezone.utc) - timedelta(seconds=i * 10)
        queue.queue.put_nowait(recent_item)

    assert queue.size == 4

    # Add new item which triggers cleanup
    queue.add_item(DownloadStatus(success=False, exception_type="NewException", exception_message="New error"))

    # Old item should be cleaned, recent ones kept
    assert queue.size == 4

    # Verify old item is gone
    items = queue.queue.items()
    for item in items:
        age_seconds = (datetime.now(timezone.utc) - item.created_at).total_seconds()
        assert age_seconds < 90


def test_exception_hierarchy():
    """Test that BotDownloadFlagged is a RetryableException"""
    # Create a fake media_request
    class FakeMediaRequest:
        def __init__(self):
            self.retry_count = 0

    media_request = FakeMediaRequest()

    # BotDownloadFlagged should be instance of RetryableException
    bot_exception = BotDownloadFlagged("Test", media_request=media_request)
    assert isinstance(bot_exception, RetryableException)
    assert isinstance(bot_exception, Exception)

    # Terminal exceptions should NOT be RetryableException
    terminal_exception = VideoAgeRestrictedException("Test")
    assert isinstance(terminal_exception, DownloadTerminalException)
    assert not isinstance(terminal_exception, RetryableException)


def test_failure_queue_empty_handling():
    """Test handling of empty queue"""
    queue = DownloadFailureQueue()

    assert queue.size == 0


def test_failure_queue_size_tracking():
    """Test that queue size correctly tracks failures and successes"""
    queue = DownloadFailureQueue(max_size=10, max_age_seconds=300)

    # Add some failures
    for i in range(3):
        queue.add_item(DownloadStatus(success=False, exception_type="TestException", exception_message=f"Error {i}"))

    assert queue.size == 3

    # Add a success - should remove one item from queue
    queue.add_item(DownloadStatus(success=True))
    assert queue.size == 2

    # Add more failures
    for i in range(2):
        queue.add_item(DownloadStatus(success=False, exception_type="TestException", exception_message=f"Error {i}"))

    assert queue.size == 4

    # Add another success
    queue.add_item(DownloadStatus(success=True))
    assert queue.size == 3


def test_failure_queue_max_size():
    """Test that queue respects max size"""
    queue = DownloadFailureQueue(max_size=5)

    # Add more items than max
    for i in range(10):
        queue.add_item(DownloadStatus(success=False, exception_type="TestException", exception_message=f"Error {i}"))

    assert queue.size == 5

    # Verify latest items are kept
    items = queue.queue.items()
    messages = [item.exception_message for item in items]
    assert "Error 9" in messages
    assert "Error 0" not in messages


# ========== RetryLimitExceeded Tests ==========

@pytest.mark.asyncio(loop_scope="session")
async def test_retry_limit_exceeded_on_bot_flagged():
    """Test that RetryLimitExceeded is returned when retry count hits max on bot flagged error"""
    loop = asyncio.get_running_loop()
    fake_context = generate_fake_context()

    x = DownloadClient(yield_dlp_error("Sign in to confirm you're not a bot"), None)
    y = fake_source_dict(fake_context)

    # Set retry count to max_retries - 1, so next attempt hits the limit
    y.download_retry_information.retry_count = 2

    # With max_retries=3 and retry_count=2, 2+1 >= 3 is True
    result = await x.create_source(y, 3, loop)

    assert not result.status.success
    assert result.status.error_type == DownloadErrorType.RETRY_LIMIT_EXCEEDED


@pytest.mark.asyncio(loop_scope="session")
async def test_retry_limit_exceeded_on_unknown_error():
    """Test that RetryLimitExceeded is returned when retry count hits max on unknown error"""
    loop = asyncio.get_running_loop()
    fake_context = generate_fake_context()

    x = DownloadClient(yield_dlp_error('Some random unknown error'), None)
    y = fake_source_dict(fake_context)

    # Set retry count to max_retries - 1
    y.download_retry_information.retry_count = 2

    result = await x.create_source(y, 3, loop)

    assert not result.status.success
    assert result.status.error_type == DownloadErrorType.RETRY_LIMIT_EXCEEDED


@pytest.mark.asyncio(loop_scope="session")
async def test_retry_limit_exceeded_with_max_retries_one():
    """Test that RetryLimitExceeded is returned immediately when max_retries=1"""
    loop = asyncio.get_running_loop()
    fake_context = generate_fake_context()

    x = DownloadClient(yield_dlp_error('Read timed out.'), None)
    y = fake_source_dict(fake_context)

    # With max_retries=1 and retry_count=0, 0+1 >= 1 is True
    result = await x.create_source(y, 1, loop)
    assert not result.status.success
    assert result.status.error_type == DownloadErrorType.RETRY_LIMIT_EXCEEDED


def test_retry_limit_exceeded_exception_hierarchy():
    """Test that RetryLimitExceeded is a DownloadClientException"""

    exc = RetryLimitExceeded('Test message')
    assert isinstance(exc, DownloadClientException)
    assert isinstance(exc, Exception)
    # RetryLimitExceeded should NOT be a RetryableException (it's the terminal state)
    assert not isinstance(exc, RetryableException)


# ========== DownloadFailureQueue.get_status_summary Tests ==========

def test_failure_queue_status_summary_empty():
    """Test get_status_summary returns correct message for empty queue"""
    queue = DownloadFailureQueue()

    summary = queue.get_status_summary()
    assert summary == "0 failures in queue"


def test_failure_queue_status_summary_single_item():
    """Test get_status_summary with a single item"""
    queue = DownloadFailureQueue()

    queue.add_item(DownloadStatus(success=False, exception_type="TestException", exception_message="Test error"))

    summary = queue.get_status_summary()
    assert "1 failures in queue" in summary
    assert "oldest:" in summary


def test_failure_queue_status_summary_multiple_items():
    """Test get_status_summary with multiple items"""
    queue = DownloadFailureQueue()

    for i in range(5):
        queue.add_item(DownloadStatus(success=False, exception_type="TestException", exception_message=f"Error {i}"))

    summary = queue.get_status_summary()
    assert "5 failures in queue" in summary
    assert "oldest:" in summary


def test_failure_queue_status_summary_shows_seconds():
    """Test get_status_summary shows seconds for recent items"""
    queue = DownloadFailureQueue()

    # Add item with known age (30 seconds old)
    item = DownloadStatus(success=False, exception_type="TestException", exception_message="Test error")
    item.created_at = datetime.now(timezone.utc) - timedelta(seconds=30)
    queue.queue.put_nowait(item)

    summary = queue.get_status_summary()
    assert "1 failures in queue" in summary
    # Should show seconds since < 60 seconds
    assert "s ago" in summary
    assert "m " not in summary  # Should not have minutes component


def test_failure_queue_status_summary_shows_minutes():
    """Test get_status_summary shows minutes for older items"""
    queue = DownloadFailureQueue(max_age_seconds=600)  # Allow older items

    # Add item with known age (2 minutes 30 seconds old)
    item = DownloadStatus(success=False, exception_type="TestException", exception_message="Test error")
    item.created_at = datetime.now(timezone.utc) - timedelta(seconds=150)
    queue.queue.put_nowait(item)

    summary = queue.get_status_summary()
    assert "1 failures in queue" in summary
    # Should show minutes and seconds
    assert "2m 30s ago" in summary


def test_failure_queue_status_summary_oldest_item():
    """Test get_status_summary correctly identifies the oldest item"""
    queue = DownloadFailureQueue(max_age_seconds=600)

    # Add items with different ages
    old_item = DownloadStatus(success=False, exception_type="OldException", exception_message="Old error")
    old_item.created_at = datetime.now(timezone.utc) - timedelta(seconds=180)  # 3 minutes old
    queue.queue.put_nowait(old_item)

    recent_item = DownloadStatus(success=False, exception_type="RecentException", exception_message="Recent error")
    recent_item.created_at = datetime.now(timezone.utc) - timedelta(seconds=30)  # 30 seconds old
    queue.queue.put_nowait(recent_item)

    summary = queue.get_status_summary()
    assert "2 failures in queue" in summary
    # Should show age of oldest item (3 minutes)
    assert "3m" in summary


def test_failure_queue_status_summary_after_success_clears_item():
    """Test get_status_summary after a success removes an item"""
    queue = DownloadFailureQueue()

    # Add some failures
    for i in range(3):
        queue.add_item(DownloadStatus(success=False, exception_type="TestException", exception_message=f"Error {i}"))

    assert "3 failures in queue" in queue.get_status_summary()

    # Add a success (should remove one item)
    queue.add_item(DownloadStatus(success=True))

    assert "2 failures in queue" in queue.get_status_summary()


# ========== DownloadClient.update_tracking Tests ==========


def _make_result(success, error_type=None, extractor='youtube', error_detail=None, ytdlp_data=None):
    """Helper to build a DownloadResult for update_tracking tests."""
    if ytdlp_data is None and success:
        ytdlp_data = {'extractor': extractor}
    status = DlStatus(success=success, error_type=error_type, error_detail=error_detail)
    fake_context = generate_fake_context()
    media_request = fake_source_dict(fake_context)
    return DownloadResult(status=status, media_request=media_request, ytdlp_data=ytdlp_data, file_name=None)


def test_update_tracking_success_youtube_sets_timestamp():
    """Success with youtube extractor adds success item and sets backoff timestamp."""
    queue = DownloadFailureQueue(max_size=10)
    client = DownloadClient(None, None, failure_queue=queue, wait_period_minimum=30, wait_period_max_variance=10)
    result = _make_result(success=True, extractor='youtube')

    client.update_tracking(result)

    assert queue.size == 0  # success removes one item (queue was empty, stays 0)
    assert client._wait_timestamp is not None  # pylint: disable=protected-access


def test_update_tracking_success_non_youtube_no_timestamp():
    """Success with non-youtube extractor adds success item but does NOT set timestamp."""
    queue = DownloadFailureQueue(max_size=10)
    client = DownloadClient(None, None, failure_queue=queue, wait_period_minimum=30, wait_period_max_variance=10)
    result = _make_result(success=True, extractor='spotify')

    client.update_tracking(result)

    assert client._wait_timestamp is None  # pylint: disable=protected-access


def test_update_tracking_retryable_adds_failure_and_sets_timestamp():
    """RETRYABLE error adds failure item and sets backoff timestamp."""
    queue = DownloadFailureQueue(max_size=10)
    client = DownloadClient(None, None, failure_queue=queue, wait_period_minimum=30, wait_period_max_variance=10)
    result = _make_result(success=False, error_type=DownloadErrorType.RETRYABLE, error_detail='timeout')

    client.update_tracking(result)

    assert queue.size == 1
    assert client._wait_timestamp is not None  # pylint: disable=protected-access


def test_update_tracking_bot_flagged_adds_failure_and_sets_timestamp():
    """BOT_FLAGGED error adds failure item and sets backoff timestamp."""
    queue = DownloadFailureQueue(max_size=10)
    client = DownloadClient(None, None, failure_queue=queue, wait_period_minimum=30, wait_period_max_variance=10)
    result = _make_result(success=False, error_type=DownloadErrorType.BOT_FLAGGED, error_detail='bot check')

    client.update_tracking(result)

    assert queue.size == 1
    assert client._wait_timestamp is not None  # pylint: disable=protected-access


def test_update_tracking_retry_limit_exceeded_adds_failure_and_sets_timestamp():
    """RETRY_LIMIT_EXCEEDED adds failure item and sets backoff timestamp."""
    queue = DownloadFailureQueue(max_size=10)
    client = DownloadClient(None, None, failure_queue=queue, wait_period_minimum=30, wait_period_max_variance=10)
    result = _make_result(success=False, error_type=DownloadErrorType.RETRY_LIMIT_EXCEEDED, error_detail='too many')

    client.update_tracking(result)

    assert queue.size == 1
    assert client._wait_timestamp is not None  # pylint: disable=protected-access


def test_update_tracking_terminal_error_sets_timestamp_no_failure():
    """Terminal errors (AGE_RESTRICTED etc.) set timestamp but do not add failure item."""
    queue = DownloadFailureQueue(max_size=10)
    client = DownloadClient(None, None, failure_queue=queue, wait_period_minimum=30, wait_period_max_variance=10)
    result = _make_result(success=False, error_type=DownloadErrorType.AGE_RESTRICTED)

    client.update_tracking(result)

    assert queue.size == 0
    assert client._wait_timestamp is not None  # pylint: disable=protected-access


def test_update_tracking_no_failure_queue():
    """update_tracking works when failure_queue is None."""
    client = DownloadClient(None, None, failure_queue=None, wait_period_minimum=30, wait_period_max_variance=10)
    result = _make_result(success=False, error_type=DownloadErrorType.RETRYABLE)

    client.update_tracking(result)  # Should not raise
    assert client._wait_timestamp is not None  # pylint: disable=protected-access


def test_backoff_seconds_remaining_none_when_no_timestamp():
    """backoff_seconds_remaining is None when no timestamp has been set."""
    client = DownloadClient(None, None)
    assert client.backoff_seconds_remaining is None


def test_backoff_seconds_remaining_after_tracking():
    """backoff_seconds_remaining returns a non-negative int after update_tracking."""
    client = DownloadClient(None, None, wait_period_minimum=30, wait_period_max_variance=10)
    result = _make_result(success=True, extractor='youtube')

    client.update_tracking(result)

    remaining = client.backoff_seconds_remaining
    assert remaining is not None
    assert remaining >= 0


def test_failure_summary_no_queue():
    """failure_summary returns '0 failures in queue' when failure_queue is None."""
    client = DownloadClient(None, None)
    assert client.failure_summary == '0 failures in queue'


def test_failure_summary_with_queue():
    """failure_summary delegates to failure_queue.get_status_summary()."""
    queue = DownloadFailureQueue(max_size=10)
    client = DownloadClient(None, None, failure_queue=queue)
    assert client.failure_summary == '0 failures in queue'

    queue.add_item(DownloadStatus(success=False, exception_type='Err', exception_message='oops'))
    assert '1 failures in queue' in client.failure_summary


@pytest.mark.asyncio(loop_scope="session")
async def test_backoff_wait_returns_immediately_when_no_timestamp():
    """backoff_wait returns immediately when no timestamp is set."""
    shutdown = asyncio.Event()
    client = DownloadClient(None, None)
    await client.backoff_wait(shutdown)  # Should not raise or block


@pytest.mark.asyncio(loop_scope="session")
async def test_backoff_wait_raises_on_shutdown():
    """backoff_wait raises ExitEarlyException when shutdown_event is already set."""
    shutdown = asyncio.Event()
    shutdown.set()
    client = DownloadClient(None, None, wait_period_minimum=60, wait_period_max_variance=10)
    # Set a future timestamp so there's something to wait for
    client._wait_timestamp = datetime.now(timezone.utc).timestamp() + 120  # pylint: disable=protected-access

    with pytest.raises(ExitEarlyException):
        await client.backoff_wait(shutdown)


@pytest.mark.asyncio(loop_scope="session")
async def test_backoff_wait_returns_when_elapsed():
    """backoff_wait returns normally when backoff period has already elapsed."""
    shutdown = asyncio.Event()
    client = DownloadClient(None, None, wait_period_minimum=1, wait_period_max_variance=1)
    # Set timestamp in the past
    client._wait_timestamp = datetime.now(timezone.utc).timestamp() - 10  # pylint: disable=protected-access

    await client.backoff_wait(shutdown)  # Should return immediately, not raise
