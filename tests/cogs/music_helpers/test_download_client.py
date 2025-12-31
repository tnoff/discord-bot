import asyncio
from datetime import datetime, timezone, timedelta
from pathlib import Path
from tempfile import NamedTemporaryFile, TemporaryDirectory

import pytest
from yt_dlp.utils import DownloadError

from discord_bot.cogs.music_helpers.download_client import (
    DownloadClient, InvalidFormatException, VideoTooLong,
    RetryableException, BotDownloadFlagged, DownloadTerminalException, VideoAgeRestrictedException,
    DownloadFailureQueue, DownloadFailureMode, match_generator
)

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

        def extract_info(self, _search_string, **kwargs):
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
            result = await x.create_source(y, loop)
            assert result.webpage_url == 'https://example.foo.com'

@pytest.mark.asyncio(loop_scope="session")
async def test_prepare_source_no_download():
    loop = asyncio.get_running_loop()
    fake_context = generate_fake_context()
    x = DownloadClient(MockYTDLP(), None)
    y = fake_source_dict(fake_context, download_file=False)
    result = await x.create_source(y, loop)
    assert result.webpage_url == 'https://example.foo.com'

@pytest.mark.asyncio(loop_scope="session")
async def test_prepare_source_errors():
    loop = asyncio.get_running_loop()
    fake_context = generate_fake_context()

    x = DownloadClient(yield_dlp_error('Sign in to confirm your age. This video may be inappropriate for some users'), None)
    y = fake_source_dict(fake_context, download_file=False)
    with pytest.raises(DownloadTerminalException) as exc:
        await x.create_source(y, loop)
    assert 'Video is age restricted, cannot download' in str(exc.value.user_message)

    x = DownloadClient(yield_dlp_error("This video has been removed for violating YouTube's Terms of Service"), None)
    y = fake_source_dict(fake_context, download_file=False)
    with pytest.raises(DownloadTerminalException) as exc:
        await x.create_source(y, loop)
    assert 'Video is unvailable due to violating terms of service, cannot download' in str(exc.value.user_message)

    x = DownloadClient(yield_dlp_error('Video unavailable'), None)
    y = fake_source_dict(fake_context, download_file=False)
    with pytest.raises(DownloadTerminalException) as exc:
        await x.create_source(y, loop)
    assert 'Video is unavailable, cannot download' in str(exc.value.user_message)

    x = DownloadClient(yield_dlp_error('Private video'), None)
    y = fake_source_dict(fake_context, download_file=False)
    with pytest.raises(DownloadTerminalException) as exc:
        await x.create_source(y, loop)
    assert 'Video is private, cannot download' in str(exc.value.user_message)

    x = DownloadClient(yield_dlp_error("Sign in to confirm you're not a bot"), None)
    y = fake_source_dict(fake_context, download_file=False)
    with pytest.raises(RetryableException) as exc:
        await x.create_source(y, loop)
    # BotDownloadFlagged is now a RetryableException
    assert exc.value.media_request.retry_count == 1

    x = DownloadClient(yield_dlp_error('Requested format is not available'), None)
    y = fake_source_dict(fake_context, download_file=False)
    with pytest.raises(InvalidFormatException) as exc:
        await x.create_source(y, loop)
    assert 'Video format not available' in str(exc.value)

    x = DownloadClient(MockYTDLPNoData(), None)
    y = fake_source_dict(fake_context, download_file=False)
    with pytest.raises(DownloadTerminalException) as exc:
        await x.create_source(y, loop)
    assert 'No videos found' in str(exc.value)

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
    """Test that RetryableException is raised for 'Read timed out.' errors"""
    loop = asyncio.get_running_loop()
    fake_context = generate_fake_context()

    # Create a download client with a mock that raises a timeout error
    x = DownloadClient(yield_dlp_error('Read timed out.'), None)
    y = fake_source_dict(fake_context, download_file=False)

    # Should raise RetryableException
    with pytest.raises(RetryableException) as exc:
        await x.create_source(y, loop)

    # Verify the exception contains the media request
    assert exc.value.media_request == y
    assert 'Can retry media download' in str(exc.value)

@pytest.mark.asyncio(loop_scope="session")
async def test_retryable_exception_increments_retry_count():
    """Test that retry_count is incremented when RetryableException is raised"""
    loop = asyncio.get_running_loop()
    fake_context = generate_fake_context()

    x = DownloadClient(yield_dlp_error('Read timed out.'), None)
    y = fake_source_dict(fake_context, download_file=False)

    # Initial retry count should be 0
    assert y.retry_count == 0

    # Attempt download, should raise RetryableException and increment retry_count
    with pytest.raises(RetryableException):
        await x.create_source(y, loop)

    # Retry count should be incremented
    assert y.retry_count == 1

@pytest.mark.asyncio(loop_scope="session")
async def test_all_unknown_errors_are_retryable():
    """Test that all unknown errors are now treated as RetryableException"""
    loop = asyncio.get_running_loop()
    fake_context = generate_fake_context()

    # Test that random unknown errors are retryable
    test_errors = [
        'Read timed out.',
        'tlsv1 alert protocol version',
        'Some other random error',
        'Connection refused',
    ]

    for error_message in test_errors:
        x = DownloadClient(yield_dlp_error(error_message), None)
        y = fake_source_dict(fake_context, download_file=False)

        with pytest.raises(RetryableException) as exc:
            await x.create_source(y, loop)

        assert exc.value.media_request == y
        assert y.retry_count >= 1  # Should have incremented retry count

# ========== DownloadFailureQueue Tests ==========

def test_failure_queue_parameter_validation():
    """Test that invalid parameters raise ValueError"""
    # Test decay_tau_seconds <= 0
    try:
        DownloadFailureQueue(decay_tau_seconds=0)
        assert False, "Should raise ValueError for decay_tau_seconds=0"
    except ValueError as e:
        assert "decay_tau_seconds must be positive" in str(e)

    # Test negative decay_tau
    try:
        DownloadFailureQueue(decay_tau_seconds=-10)
        assert False, "Should raise ValueError for negative decay_tau_seconds"
    except ValueError as e:
        assert "decay_tau_seconds must be positive" in str(e)

    # Test negative max_age_seconds
    try:
        DownloadFailureQueue(max_age_seconds=-1)
        assert False, "Should raise ValueError for negative max_age_seconds"
    except ValueError as e:
        assert "max_age_seconds must be positive" in str(e)

    # Test max_backoff_factor < 1.0
    try:
        DownloadFailureQueue(max_backoff_factor=0.5)
        assert False, "Should raise ValueError for max_backoff_factor < 1.0"
    except ValueError as e:
        assert "max_backoff_factor must be >= 1.0" in str(e)

    # Test negative aggressiveness
    try:
        DownloadFailureQueue(aggressiveness=-1.0)
        assert False, "Should raise ValueError for negative aggressiveness"
    except ValueError as e:
        assert "aggressiveness must be positive" in str(e)


def test_failure_queue_old_item_cleanup():
    """Test that old items are automatically cleaned up"""
    queue = DownloadFailureQueue(max_size=10, max_age_seconds=60)

    # Add an old item directly to queue
    old_item = DownloadFailureMode("OldException", "Old error")
    old_item.created_at = datetime.now(timezone.utc) - timedelta(seconds=120)
    queue.queue.put_nowait(old_item)

    # Add recent items
    for i in range(3):
        recent_item = DownloadFailureMode("RecentException", f"Recent error {i}")
        recent_item.created_at = datetime.now(timezone.utc) - timedelta(seconds=i * 10)
        queue.queue.put_nowait(recent_item)

    assert queue.size == 4

    # Add new item which triggers cleanup
    queue.add_item(DownloadFailureMode("NewException", "New error"))

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

    # Empty queue should return base multiplier (1.0)
    multiplier = queue.get_backoff_multiplier()
    assert multiplier == 1.0


def test_failure_queue_backoff_scenarios():
    """Test exponential decay backoff multiplier scenarios"""
    # Scenario 1: Single recent failure
    queue1 = DownloadFailureQueue(
        max_age_seconds=300,
        decay_tau_seconds=75,
        max_backoff_factor=3.0,
        aggressiveness=1.0
    )
    queue1.add_item(DownloadFailureMode("TestException", "Error 1"))

    multiplier1 = queue1.get_backoff_multiplier()
    # Single recent failure should give moderate backoff
    # With score=1.0, k=1.0, max=3.0: factor = 1 + 2*(1-exp(-1)) ≈ 2.26
    assert 2.0 < multiplier1 < 2.5

    # Scenario 2: Multiple rapid failures (high score)
    queue2 = DownloadFailureQueue(
        max_age_seconds=300,
        decay_tau_seconds=75,
        max_backoff_factor=3.0,
        aggressiveness=1.0
    )
    for i in range(5):
        queue2.add_item(DownloadFailureMode("TestException", f"Error {i}"))

    multiplier2 = queue2.get_backoff_multiplier()
    # Many rapid failures should approach max backoff
    assert multiplier2 > 2.5
    assert multiplier2 <= 3.0  # Should not exceed max_backoff_factor

    # Scenario 3: Old failures (decayed score)
    queue3 = DownloadFailureQueue(
        max_age_seconds=300,
        decay_tau_seconds=75,
        max_backoff_factor=3.0,
        aggressiveness=1.0
    )
    # Add failures that are 225 seconds old (3 decay constants)
    for i in range(5):
        item = DownloadFailureMode("TestException", f"Error {i}")
        item.created_at = datetime.now(timezone.utc) - timedelta(seconds=225)
        queue3.queue.put_nowait(item)

    multiplier3 = queue3.get_backoff_multiplier()
    # Old failures should have decayed significantly (exp(-3) ≈ 0.05)
    # 5 failures * 0.05 = 0.25 score → minimal backoff
    assert 1.0 <= multiplier3 < 1.5

    # Scenario 4: Test aggressiveness parameter
    queue4_low = DownloadFailureQueue(
        max_age_seconds=300,
        decay_tau_seconds=75,
        max_backoff_factor=3.0,
        aggressiveness=0.5  # Less aggressive
    )
    queue4_high = DownloadFailureQueue(
        max_age_seconds=300,
        decay_tau_seconds=75,
        max_backoff_factor=3.0,
        aggressiveness=2.0  # More aggressive
    )

    # Add same failures to both
    for i in range(3):
        queue4_low.add_item(DownloadFailureMode("TestException", f"Error {i}"))
        queue4_high.add_item(DownloadFailureMode("TestException", f"Error {i}"))

    mult_low = queue4_low.get_backoff_multiplier()
    mult_high = queue4_high.get_backoff_multiplier()

    # Higher aggressiveness should result in higher multiplier for same score
    assert mult_high > mult_low


def test_failure_queue_max_size():
    """Test that queue respects max size"""
    queue = DownloadFailureQueue(max_size=5)

    # Add more items than max
    for i in range(10):
        queue.add_item(DownloadFailureMode("TestException", f"Error {i}"))

    assert queue.size == 5

    # Verify latest items are kept
    items = queue.queue.items()
    messages = [item.exception_message for item in items]
    assert "Error 9" in messages
    assert "Error 0" not in messages
