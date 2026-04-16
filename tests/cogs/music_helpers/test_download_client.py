import asyncio
from asyncio import QueueEmpty
from datetime import datetime, timezone, timedelta
import hashlib
from pathlib import Path
from tempfile import NamedTemporaryFile
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from yt_dlp.utils import DownloadError

from discord_bot.cogs.music_helpers.download_client import (
    DownloadClient, VideoTooLong, VideoBanned, BotDownloadFlagged, RetryableException, RetryLimitExceeded,
    DownloadTerminalException, DownloadClientException, VideoAgeRestrictedException, match_generator,
    DirectItemAvailableException,
)
from discord_bot.utils.audio import AudioProcessingError
from discord_bot.exceptions import ExitEarlyException
from discord_bot.types.download import DownloadErrorType, DownloadEvent, DownloadResult, DownloadStatus as DlStatus
from discord_bot.utils.failure_queue import FailureQueue as DownloadFailureQueue, FailureStatus as DownloadStatus

from discord_bot.types.playlist_add_request import PlaylistAddRequest
from discord_bot.types.search import SearchResult
from discord_bot.cogs.music_helpers.common import SearchType
from discord_bot.utils.queue import PutsBlocked
from tests.helpers import fake_source_dict, generate_fake_context

class MockYTDLP():
    '''Mock yt-dlp client that returns a fake successful download result.'''
    def __init__(self, fake_file_path : Path = 'foo-bar.mp3'):
        self.fake_file_path = fake_file_path

    def extract_info(self, _search_string, download=True):
        '''Return fake yt-dlp extract_info data.'''
        data = {
            'entries': [
                {
                    'webpage_url': 'https://example.foo.com',
                    'title': 'Foo Title',
                    'uploader': 'Foo Uploader',
                    'duration': 1234,
                    'extractor': 'test-extractor',
                    'id': 'vid123',
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
    '''Mock yt-dlp client that returns no entries.'''
    def __init__(self):
        pass

    def extract_info(self, _search_string, download=True): #pylint:disable=unused-argument
        '''Return empty entries list.'''
        return {
            'entries': []
        }

def yield_dlp_error(message):
    '''Return a mock yt-dlp client that raises DownloadError with the given message.'''
    class MockYTDLPError():
        '''Mock yt-dlp client that always raises a DownloadError.'''
        def __init__(self):
            pass

        def extract_info(self, _search_string, **_kwargs):
            '''Raise DownloadError unconditionally.'''
            raise DownloadError(message)
    return MockYTDLPError()

def make_download_client(mock_ytdl=None, **kwargs):
    '''Create a DownloadClient with an optional mock ytdl injected post-init.'''
    client = DownloadClient(None, Path('/tmp'), **kwargs)
    if mock_ytdl is not None:
        client.ytdl = mock_ytdl
    return client

class MockYoutubeMusic():
    '''Mock YouTube Music client.'''
    def __init__(self):
        pass

    def search(self, *_args, **_kwargs):
        '''Return a fake video ID.'''
        return 'vid-1234'


@pytest.mark.asyncio(loop_scope="session")
async def test_prepare_source():
    '''Successful download returns a result with the post-processed PCM file.'''
    with NamedTemporaryFile(delete=False) as tmp_file:
        fake_context = generate_fake_context()
        x = make_download_client(MockYTDLP(fake_file_path=Path(tmp_file.name)))
        y = fake_source_dict(fake_context)
        pcm_path = Path(tmp_file.name).with_suffix('.pcm')
        with patch('discord_bot.cogs.music_helpers.download_client.edit_audio_file', return_value=pcm_path):
            result = await x.create_source(y, 3)
        assert result.status.success
        assert result.ytdlp_data['webpage_url'] == 'https://example.foo.com'
        assert result.file_name == pcm_path
        assert result.post_process_timestamp is not None

@pytest.mark.asyncio(loop_scope="session")
async def test_prepare_source_s3_mode():
    '''In S3 mode, PCM conversion runs first on the local file, then the PCM is uploaded to S3'''
    with NamedTemporaryFile(delete=False, suffix='.mp3') as tmp_file:
        download_path = Path(tmp_file.name)
    # Create a real PCM file so __upload_s3 can unlink it
    pcm_path = download_path.with_suffix('.pcm')
    pcm_path.write_bytes(b'pcm data')
    fake_context = generate_fake_context()
    x = make_download_client(MockYTDLP(fake_file_path=download_path),
                             bucket_name='test-bucket')
    y = fake_source_dict(fake_context)
    expected_s3_key = f'cache/{pcm_path.name}'
    with patch('discord_bot.cogs.music_helpers.download_client.upload_file', return_value=True) as upload_mock:
        with patch('discord_bot.cogs.music_helpers.download_client.edit_audio_file', return_value=pcm_path) as edit_mock:
            result = await x.create_source(y, 3)
    assert result.status.success
    # edit_audio_file was called with the local download file, not an S3 key
    assert edit_mock.call_args[0][0] == download_path
    upload_mock.assert_called_once()
    call_args = upload_mock.call_args[0]
    assert call_args[0] == 'test-bucket'
    assert call_args[1] == pcm_path
    assert str(call_args[2]) == expected_s3_key
    # PCM file deleted after upload
    assert not pcm_path.exists()
    # result carries S3 key path
    assert result.file_name == Path(expected_s3_key)


@pytest.mark.asyncio(loop_scope="session")
async def test_prepare_source_s3_mode_audio_processing_error():
    '''In S3 mode, when AudioProcessingError occurs the original file is still uploaded to S3'''
    with NamedTemporaryFile(delete=False, suffix='.mp3') as tmp_file:
        download_path = Path(tmp_file.name)
    download_path.write_bytes(b'raw audio')
    fake_context = generate_fake_context()
    x = make_download_client(MockYTDLP(fake_file_path=download_path),
                             bucket_name='test-bucket')
    y = fake_source_dict(fake_context)
    expected_s3_key = f'cache/{download_path.name}'
    with patch('discord_bot.cogs.music_helpers.download_client.upload_file', return_value=True) as upload_mock:
        with patch('discord_bot.cogs.music_helpers.download_client.edit_audio_file',
                   side_effect=AudioProcessingError('bad codec')):
            result = await x.create_source(y, 3)
    assert not result.status.success
    assert result.status.error_type == DownloadErrorType.RETRYABLE
    # upload still runs after processing failure, using the original download file
    upload_mock.assert_called_once()
    call_args = upload_mock.call_args[0]
    assert call_args[0] == 'test-bucket'
    assert call_args[1] == download_path
    assert str(call_args[2]) == expected_s3_key

@pytest.mark.asyncio(loop_scope="session")
async def test_prepare_source_empty_requested_downloads():
    """requested_downloads list is empty — should return FILE_NOT_FOUND, not crash."""

    class MockYTDLPEmptyDownloads():
        '''Mock yt-dlp returning an entry with an empty requested_downloads list.'''
        def extract_info(self, _search_string, **_kwargs):
            '''Return entry with empty requested_downloads.'''
            return {'entries': [{'webpage_url': 'https://example.foo.com', 'title': 'T',
                                 'uploader': 'U', 'duration': 10, 'extractor': 'youtube',
                                 'requested_downloads': []}]}

    fake_context = generate_fake_context()
    x = make_download_client(MockYTDLPEmptyDownloads())
    y = fake_source_dict(fake_context)
    result = await x.create_source(y, 3)
    assert not result.status.success
    assert result.status.error_type == DownloadErrorType.FILE_NOT_FOUND


@pytest.mark.asyncio(loop_scope="session")
async def test_prepare_source_filepath_does_not_exist():
    """filepath returned by yt-dlp does not exist on disk — should return FILE_NOT_FOUND."""

    class MockYTDLPMissingFile():
        '''Mock yt-dlp returning a filepath that does not exist on disk.'''
        def extract_info(self, _search_string, **_kwargs):
            '''Return entry pointing to a nonexistent file.'''
            return {'entries': [{'webpage_url': 'https://example.foo.com', 'title': 'T',
                                 'uploader': 'U', 'duration': 10, 'extractor': 'youtube',
                                 'requested_downloads': [{'filepath': '/nonexistent/no/such/file.mp3'}]}]}

    fake_context = generate_fake_context()
    x = make_download_client(MockYTDLPMissingFile())
    y = fake_source_dict(fake_context)
    result = await x.create_source(y, 3)
    assert not result.status.success
    assert result.status.error_type == DownloadErrorType.FILE_NOT_FOUND


@pytest.mark.asyncio(loop_scope="session")
async def test_prepare_source_md5_match_no_warning(mocker):
    '''No warning when yt-dlp md5 matches the file on disk'''
    with NamedTemporaryFile(delete=False, suffix='.mp3') as tmp_file:
        file_path = Path(tmp_file.name)
    file_path.write_bytes(b'audio content')
    correct_md5 = hashlib.md5(b'audio content').hexdigest()

    class MockYTDLPWithMd5:
        '''Mock yt-dlp returning a matching md5 checksum.'''
        def extract_info(self, _search_string, **_kwargs):
            '''Return entry with correct md5.'''
            return {'entries': [{'webpage_url': 'https://example.foo.com', 'title': 'T',
                                 'uploader': 'U', 'duration': 10, 'extractor': 'youtube',
                                 'requested_downloads': [{'filepath': str(file_path), 'md5': correct_md5}]}]}

    fake_context = generate_fake_context()
    x = make_download_client(MockYTDLPWithMd5())
    mock_logger = mocker.patch.object(x, 'logger')
    y = fake_source_dict(fake_context)
    with patch('discord_bot.cogs.music_helpers.download_client.edit_audio_file',
               return_value=file_path.with_suffix('.pcm')):
        result = await x.create_source(y, 3)
    assert result.status.success
    mock_logger.warning.assert_not_called()


@pytest.mark.asyncio(loop_scope="session")
async def test_prepare_source_md5_mismatch_logs_warning(mocker):
    '''Warning logged when yt-dlp md5 does not match the file on disk; download still succeeds'''
    with NamedTemporaryFile(delete=False, suffix='.mp3') as tmp_file:
        file_path = Path(tmp_file.name)
    file_path.write_bytes(b'audio content')

    class MockYTDLPWithWrongMd5:
        '''Mock yt-dlp returning a mismatched md5 checksum.'''
        def extract_info(self, _search_string, **_kwargs):
            '''Return entry with incorrect md5.'''
            return {'entries': [{'webpage_url': 'https://example.foo.com', 'title': 'T',
                                 'uploader': 'U', 'duration': 10, 'extractor': 'youtube',
                                 'requested_downloads': [{'filepath': str(file_path), 'md5': 'deadbeef000000000000000000000000'}]}]}

    fake_context = generate_fake_context()
    x = make_download_client(MockYTDLPWithWrongMd5())
    mock_logger = mocker.patch.object(x, 'logger')
    y = fake_source_dict(fake_context)
    with patch('discord_bot.cogs.music_helpers.download_client.edit_audio_file',
               return_value=file_path.with_suffix('.pcm')):
        result = await x.create_source(y, 3)
    assert result.status.success
    mock_logger.warning.assert_called_once()
    args = mock_logger.warning.call_args[0]
    assert 'deadbeef000000000000000000000000' in args[1]


@pytest.mark.asyncio(loop_scope="session")
async def test_prepare_source_no_md5_no_warning(mocker):
    '''No warning when yt-dlp does not provide an md5 field (most sources)'''
    with NamedTemporaryFile(delete=False) as tmp_file:
        fake_context = generate_fake_context()
        x = make_download_client(MockYTDLP(fake_file_path=Path(tmp_file.name)))
        mock_logger = mocker.patch.object(x, 'logger')
        y = fake_source_dict(fake_context)
        pcm_path = Path(tmp_file.name).with_suffix('.pcm')
        with patch('discord_bot.cogs.music_helpers.download_client.edit_audio_file', return_value=pcm_path):
            result = await x.create_source(y, 3)
        assert result.status.success
        mock_logger.warning.assert_not_called()


@pytest.mark.asyncio(loop_scope="session")
async def test_prepare_source_no_download():
    '''PlaylistAddRequest skips file download and returns metadata only.'''
    fake_context = generate_fake_context()
    x = make_download_client(MockYTDLP())
    y = PlaylistAddRequest(guild_id=fake_context['guild'].id, channel_id=fake_context['channel'].id,
                           requester_name=fake_context['author'].display_name, requester_id=fake_context['author'].id,
                           search_result=SearchResult(search_type=SearchType.DIRECT, raw_search_string='https://example.foo.com'),
                           playlist_id=1)
    result = await x.create_source(y, 3)
    assert result.status.success
    assert result.ytdlp_data['webpage_url'] == 'https://example.foo.com'

@pytest.mark.asyncio(loop_scope="session")
async def test_prepare_source_errors():
    '''Various yt-dlp DownloadError messages map to the correct DownloadErrorType.'''
    fake_context = generate_fake_context()

    x = make_download_client(yield_dlp_error('Sign in to confirm your age. This video may be inappropriate for some users'))
    y = fake_source_dict(fake_context)
    result = await x.create_source(y, 3)
    assert not result.status.success
    assert result.status.error_type == DownloadErrorType.AGE_RESTRICTED
    assert 'Video is age restricted, cannot download' in result.status.user_message

    x = make_download_client(yield_dlp_error("This video has been removed for violating YouTube's Terms of Service"))
    y = fake_source_dict(fake_context)
    result = await x.create_source(y, 3)
    assert not result.status.success
    assert result.status.error_type == DownloadErrorType.TERMS_VIOLATION
    assert 'Video is unvailable due to violating terms of service, cannot download' in result.status.user_message

    x = make_download_client(yield_dlp_error('Video unavailable'))
    y = fake_source_dict(fake_context)
    result = await x.create_source(y, 3)
    assert not result.status.success
    assert result.status.error_type == DownloadErrorType.UNAVAILABLE
    assert 'Video is unavailable, cannot download' in result.status.user_message

    x = make_download_client(yield_dlp_error('Private video'))
    y = fake_source_dict(fake_context)
    result = await x.create_source(y, 3)
    assert not result.status.success
    assert result.status.error_type == DownloadErrorType.PRIVATE_VIDEO
    assert 'Video is private, cannot download' in result.status.user_message

    x = make_download_client(yield_dlp_error("Sign in to confirm you're not a bot"))
    y = fake_source_dict(fake_context)
    result = await x.create_source(y, 3)
    assert not result.status.success
    assert result.status.error_type == DownloadErrorType.BOT_FLAGGED
    # create_source does not increment retry_count; run() does
    assert result.media_request.download_retry_information.retry_count == 0

    x = make_download_client(yield_dlp_error('Requested format is not available'))
    y = fake_source_dict(fake_context)
    result = await x.create_source(y, 3)
    assert not result.status.success
    assert result.status.error_type == DownloadErrorType.INVALID_FORMAT
    assert 'Video is not available in requested format' in result.status.user_message

    x = make_download_client(MockYTDLPNoData())
    y = fake_source_dict(fake_context)
    result = await x.create_source(y, 3)
    assert not result.status.success
    assert result.status.error_type == DownloadErrorType.NOT_FOUND
    assert 'No videos found' in result.status.user_message

def yield_metadata_check_error(exception):
    '''Return a mock yt-dlp client that raises the given exception from extract_info.'''
    class MockYTDLPMetadataError():
        '''Mock yt-dlp client that raises a metadata check exception.'''
        def extract_info(self, _search_string, **_kwargs):
            '''Raise the configured exception.'''
            raise exception
    return MockYTDLPMetadataError()


@pytest.mark.asyncio(loop_scope="session")
async def test_prepare_source_video_too_long():
    fake_context = generate_fake_context()
    x = make_download_client(yield_metadata_check_error(VideoTooLong('Video Too Long', user_message='too long message')))
    y = fake_source_dict(fake_context)
    result = await x.create_source(y, 3)
    assert not result.status.success
    assert result.status.error_type == DownloadErrorType.TOO_LONG
    assert result.status.user_message == 'too long message'


@pytest.mark.asyncio(loop_scope="session")
async def test_prepare_source_video_banned():
    fake_context = generate_fake_context()
    x = make_download_client(yield_metadata_check_error(VideoBanned('Video Banned', user_message='banned message')))
    y = fake_source_dict(fake_context)
    result = await x.create_source(y, 3)
    assert not result.status.success
    assert result.status.error_type == DownloadErrorType.BANNED
    assert result.status.user_message == 'banned message'


@pytest.mark.asyncio(loop_scope="session")
async def test_prepare_source_audio_processing_error():
    """AudioProcessingError from edit_audio_file returns a RETRYABLE failure result"""
    with NamedTemporaryFile(delete=False) as tmp_file:
        fake_context = generate_fake_context()
        x = make_download_client(MockYTDLP(fake_file_path=Path(tmp_file.name)))
        y = fake_source_dict(fake_context)
        with patch('discord_bot.cogs.music_helpers.download_client.edit_audio_file',
                   side_effect=AudioProcessingError('bad codec')):
            result = await x.create_source(y, 3)
    assert not result.status.success
    assert result.status.error_type == DownloadErrorType.RETRYABLE
    assert 'Audio processing failed' in result.status.user_message


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
    fake_context = generate_fake_context()

    x = make_download_client(yield_dlp_error('Read timed out.'))
    y = fake_source_dict(fake_context)

    result = await x.create_source(y, 3)

    assert not result.status.success
    assert result.status.error_type == DownloadErrorType.RETRYABLE
    assert result.media_request == y

@pytest.mark.asyncio(loop_scope="session")
async def test_retryable_exception_increments_retry_count():
    """Test that retry_count is NOT incremented by download_client (music.py does it)"""
    fake_context = generate_fake_context()

    x = make_download_client(yield_dlp_error('Read timed out.'))
    y = fake_source_dict(fake_context)

    assert y.download_retry_information.retry_count == 0

    result = await x.create_source(y, 3)

    # create_source does not increment retry_count; run() does
    assert y.download_retry_information.retry_count == 0
    assert not result.status.success
    assert result.status.error_type == DownloadErrorType.RETRYABLE

@pytest.mark.asyncio(loop_scope="session")
async def test_all_unknown_errors_are_retryable():
    """Test that all unknown errors are now treated as RetryableException"""
    fake_context = generate_fake_context()

    test_errors = [
        'Read timed out.',
        'tlsv1 alert protocol version',
        'Some other random error',
        'Connection refused',
    ]

    for error_message in test_errors:
        x = make_download_client(yield_dlp_error(error_message))
        y = fake_source_dict(fake_context)

        result = await x.create_source(y, 3)

        assert not result.status.success
        assert result.status.error_type == DownloadErrorType.RETRYABLE
        assert result.media_request == y
        # create_source does not increment retry_count; run() does
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
        '''Minimal fake media request for exception hierarchy testing.'''
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
    fake_context = generate_fake_context()

    x = make_download_client(yield_dlp_error("Sign in to confirm you're not a bot"))
    y = fake_source_dict(fake_context)

    # Set retry count to max_retries - 1, so next attempt hits the limit
    y.download_retry_information.retry_count = 2

    # With max_retries=3 and retry_count=2, 2+1 >= 3 is True
    result = await x.create_source(y, 3)

    assert not result.status.success
    assert result.status.error_type == DownloadErrorType.RETRY_LIMIT_EXCEEDED


@pytest.mark.asyncio(loop_scope="session")
async def test_retry_limit_exceeded_on_unknown_error():
    """Test that RetryLimitExceeded is returned when retry count hits max on unknown error"""
    fake_context = generate_fake_context()

    x = make_download_client(yield_dlp_error('Some random unknown error'))
    y = fake_source_dict(fake_context)

    # Set retry count to max_retries - 1
    y.download_retry_information.retry_count = 2

    result = await x.create_source(y, 3)

    assert not result.status.success
    assert result.status.error_type == DownloadErrorType.RETRY_LIMIT_EXCEEDED


@pytest.mark.asyncio(loop_scope="session")
async def test_retry_limit_exceeded_with_max_retries_one():
    """Test that RetryLimitExceeded is returned immediately when max_retries=1"""
    fake_context = generate_fake_context()

    x = make_download_client(yield_dlp_error('Read timed out.'))
    y = fake_source_dict(fake_context)

    # With max_retries=1 and retry_count=0, 0+1 >= 1 is True
    result = await x.create_source(y, 1)
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


def _make_result(success, error_type=None, extractor='youtube', error_detail=None, ytdlp_data=None, is_direct_search=False):
    """Helper to build a DownloadResult for update_tracking tests."""
    if ytdlp_data is None and success:
        ytdlp_data = {'extractor': extractor}
    status = DlStatus(success=success, error_type=error_type, error_detail=error_detail)
    fake_context = generate_fake_context()
    media_request = fake_source_dict(fake_context, is_direct_search=is_direct_search)
    return DownloadResult(status=status, media_request=media_request, ytdlp_data=ytdlp_data, file_name=None)


def test_update_tracking_success_youtube_sets_timestamp():
    """Success with youtube extractor adds success item and sets backoff timestamp."""
    queue = DownloadFailureQueue(max_size=10)
    client = make_download_client(failure_queue=queue, wait_period_minimum=30, wait_period_max_variance=10)
    result = _make_result(success=True, extractor='youtube')

    client.update_tracking(result)

    assert queue.size == 0  # success removes one item (queue was empty, stays 0)
    assert client.wait_timestamp is not None

def test_update_tracking_success_non_youtube_no_timestamp():
    """Success with non-youtube extractor adds success item but does NOT set timestamp."""
    queue = DownloadFailureQueue(max_size=10)
    client = make_download_client(failure_queue=queue, wait_period_minimum=30, wait_period_max_variance=10)
    result = _make_result(success=True, extractor='spotify')

    client.update_tracking(result)

    assert client.wait_timestamp is None

def test_update_tracking_retryable_adds_failure_and_sets_timestamp():
    """RETRYABLE error adds failure item and sets backoff timestamp."""
    queue = DownloadFailureQueue(max_size=10)
    client = make_download_client(failure_queue=queue, wait_period_minimum=30, wait_period_max_variance=10)
    result = _make_result(success=False, error_type=DownloadErrorType.RETRYABLE, error_detail='timeout')

    client.update_tracking(result)

    assert queue.size == 1
    assert client.wait_timestamp is not None

def test_update_tracking_bot_flagged_adds_failure_and_sets_timestamp():
    """BOT_FLAGGED error adds failure item and sets backoff timestamp."""
    queue = DownloadFailureQueue(max_size=10)
    client = make_download_client(failure_queue=queue, wait_period_minimum=30, wait_period_max_variance=10)
    result = _make_result(success=False, error_type=DownloadErrorType.BOT_FLAGGED, error_detail='bot check')

    client.update_tracking(result)

    assert queue.size == 1
    assert client.wait_timestamp is not None

def test_update_tracking_retry_limit_exceeded_adds_failure_and_sets_timestamp():
    """RETRY_LIMIT_EXCEEDED adds failure item and sets backoff timestamp."""
    queue = DownloadFailureQueue(max_size=10)
    client = make_download_client(failure_queue=queue, wait_period_minimum=30, wait_period_max_variance=10)
    result = _make_result(success=False, error_type=DownloadErrorType.RETRY_LIMIT_EXCEEDED, error_detail='too many')

    client.update_tracking(result)

    assert queue.size == 1
    assert client.wait_timestamp is not None

def test_update_tracking_terminal_error_sets_timestamp_no_failure():
    """Terminal errors (AGE_RESTRICTED etc.) set timestamp but do not add failure item."""
    queue = DownloadFailureQueue(max_size=10)
    client = make_download_client(failure_queue=queue, wait_period_minimum=30, wait_period_max_variance=10)
    result = _make_result(success=False, error_type=DownloadErrorType.AGE_RESTRICTED)

    client.update_tracking(result)

    assert queue.size == 0
    assert client.wait_timestamp is not None

def test_update_tracking_no_failure_queue():
    """update_tracking works when failure_queue is None."""
    client = make_download_client(failure_queue=None, wait_period_minimum=30, wait_period_max_variance=10)
    result = _make_result(success=False, error_type=DownloadErrorType.RETRYABLE)

    client.update_tracking(result)  # Should not raise
    assert client.wait_timestamp is not None


def test_update_tracking_direct_retryable_does_not_set_timestamp():
    """RETRYABLE error from a DIRECT source does not set a backoff timestamp."""
    client = make_download_client(wait_period_minimum=30, wait_period_max_variance=10)
    result = _make_result(success=False, error_type=DownloadErrorType.RETRYABLE, is_direct_search=True)

    client.update_tracking(result)

    assert client.wait_timestamp is None


def test_update_tracking_direct_bot_flagged_does_not_set_timestamp():
    """BOT_FLAGGED error from a DIRECT source does not set a backoff timestamp."""
    client = make_download_client(wait_period_minimum=30, wait_period_max_variance=10)
    result = _make_result(success=False, error_type=DownloadErrorType.BOT_FLAGGED, is_direct_search=True)

    client.update_tracking(result)

    assert client.wait_timestamp is None


def test_update_tracking_direct_terminal_error_does_not_set_timestamp():
    """Terminal errors from a DIRECT source do not set a backoff timestamp."""
    client = make_download_client(wait_period_minimum=30, wait_period_max_variance=10)
    result = _make_result(success=False, error_type=DownloadErrorType.AGE_RESTRICTED, is_direct_search=True)

    client.update_tracking(result)

    assert client.wait_timestamp is None

def test_backoff_seconds_remaining_none_when_no_timestamp():
    """backoff_seconds_remaining is None when no timestamp has been set."""
    client = make_download_client()
    assert client.backoff_seconds_remaining is None


def test_backoff_seconds_remaining_after_tracking():
    """backoff_seconds_remaining returns a non-negative int after update_tracking."""
    client = make_download_client(wait_period_minimum=30, wait_period_max_variance=10)
    result = _make_result(success=True, extractor='youtube')

    client.update_tracking(result)

    remaining = client.backoff_seconds_remaining
    assert remaining is not None
    assert remaining >= 0


def test_failure_summary_no_queue():
    """failure_summary returns '0 failures in queue' when failure_queue is None."""
    client = make_download_client()
    assert client.failure_summary == '0 failures in queue'


def test_failure_summary_with_queue():
    """failure_summary delegates to failure_queue.get_status_summary()."""
    queue = DownloadFailureQueue(max_size=10)
    client = make_download_client(failure_queue=queue)
    assert client.failure_summary == '0 failures in queue'

    queue.add_item(DownloadStatus(success=False, exception_type='Err', exception_message='oops'))
    assert '1 failures in queue' in client.failure_summary


@pytest.mark.asyncio(loop_scope="session")
async def test_backoff_wait_returns_immediately_when_no_timestamp():
    """backoff_wait returns immediately when no timestamp is set."""
    shutdown = asyncio.Event()
    client = make_download_client()
    await client.backoff_wait(shutdown)  # Should not raise or block


@pytest.mark.asyncio(loop_scope="session")
async def test_backoff_wait_raises_on_shutdown():
    """backoff_wait raises ExitEarlyException when shutdown_event is already set."""
    shutdown = asyncio.Event()
    shutdown.set()
    client = make_download_client(wait_period_minimum=60, wait_period_max_variance=10)
    # Set a future timestamp so there's something to wait for
    client.wait_timestamp = datetime.now(timezone.utc).timestamp() + 120
    with pytest.raises(ExitEarlyException):
        await client.backoff_wait(shutdown)


@pytest.mark.asyncio(loop_scope="session")
async def test_backoff_wait_returns_when_elapsed():
    """backoff_wait returns normally when backoff period has already elapsed."""
    shutdown = asyncio.Event()
    client = make_download_client(wait_period_minimum=1, wait_period_max_variance=1)
    # Set timestamp in the past
    client.wait_timestamp = datetime.now(timezone.utc).timestamp() - 10
    await client.backoff_wait(shutdown)  # Should return immediately, not raise


# ========== Queue Interface Tests ==========

def test_submit_and_queue_size():
    '''submit enqueues a request; queue_size reflects it'''
    fake_context = generate_fake_context()
    client = make_download_client()
    mr = fake_source_dict(fake_context)
    assert client.queue_size(mr.guild_id) == 0
    client.submit(mr.guild_id, mr)
    assert client.queue_size(mr.guild_id) == 1


def test_submit_captures_span_context():
    '''submit sets span_context on the media request (None when no active span)'''
    fake_context = generate_fake_context()
    client = make_download_client()
    mr = fake_source_dict(fake_context)
    assert mr.span_context is None
    client.submit(mr.guild_id, mr)
    # No active OTEL span in tests → capture_span_context returns None, which is fine
    assert mr.span_context is None


def test_submit_does_not_overwrite_existing_span_context():
    '''submit preserves an already-set span_context on the media request'''
    fake_context = generate_fake_context()
    client = make_download_client()
    mr = fake_source_dict(fake_context)
    existing_ctx = {'trace_id': 1, 'span_id': 2, 'trace_flags': 1}
    mr.span_context = existing_ctx
    client.submit(mr.guild_id, mr)
    assert mr.span_context == existing_ctx


def test_submit_with_priority():
    '''submit with priority stores the request without error'''
    fake_context = generate_fake_context()
    client = make_download_client()
    mr = fake_source_dict(fake_context)
    client.submit(mr.guild_id, mr, priority=10)
    assert client.queue_size(mr.guild_id) == 1


def test_block_guild_prevents_submissions():
    '''block_guild blocks further put_nowait calls for that guild'''
    fake_context = generate_fake_context()
    client = make_download_client()
    mr = fake_source_dict(fake_context)
    client.submit(mr.guild_id, mr)
    client.block_guild(mr.guild_id)
    with pytest.raises(PutsBlocked):
        client.submit(mr.guild_id, fake_source_dict(fake_context))


def test_clear_guild_queue_returns_dropped():
    '''clear_guild_queue removes all requests and returns them'''
    fake_context = generate_fake_context()
    client = make_download_client()
    mr1 = fake_source_dict(fake_context)
    mr2 = fake_source_dict(fake_context)
    client.submit(mr1.guild_id, mr1)
    client.submit(mr1.guild_id, mr2)
    dropped = client.clear_guild_queue(mr1.guild_id)
    assert len(dropped) == 2
    assert client.queue_size(mr1.guild_id) == 0


def test_clear_guild_queue_with_predicate():
    '''preserve_predicate keeps matching items in the queue'''
    fake_context = generate_fake_context()
    client = make_download_client()

    mr = fake_source_dict(fake_context)
    par = PlaylistAddRequest(
        guild_id=fake_context['guild'].id,
        channel_id=fake_context['channel'].id,
        requester_name=fake_context['author'].display_name,
        requester_id=fake_context['author'].id,
        search_result=SearchResult(search_type=SearchType.DIRECT, raw_search_string='https://x.com/v'),
        playlist_id=1,
    )
    client.submit(mr.guild_id, mr)
    client.submit(par.guild_id, par)
    # Preserve PlaylistAddRequests (download_file=False)
    dropped = client.clear_guild_queue(mr.guild_id, preserve_predicate=lambda r: not r.download_file)
    assert len(dropped) == 1
    assert client.queue_size(mr.guild_id) == 1


def test_get_result_nowait_raises_when_empty():
    '''get_result_nowait raises QueueEmpty when no results are available'''
    client = make_download_client()
    with pytest.raises(QueueEmpty):
        client.get_result_nowait()


# ========== DownloadClient.run() Tests ==========

@pytest.mark.asyncio(loop_scope="session")
async def test_run_success_puts_result_on_result_queue():
    '''run() downloads and puts result on result queue; broker gets IN_PROGRESS (no backoff active)'''
    fake_context = generate_fake_context()
    mock_broker = MagicMock()
    with NamedTemporaryFile(delete=False) as tmp_file:
        client = make_download_client(MockYTDLP(fake_file_path=Path(tmp_file.name)), broker=mock_broker)
        mr = fake_source_dict(fake_context)
        client.submit(mr.guild_id, mr)
        shutdown = asyncio.Event()
        pcm_path = Path(tmp_file.name).with_suffix('.pcm')
        with patch('discord_bot.cogs.music_helpers.download_client.edit_audio_file', return_value=pcm_path):
            await client.run(shutdown)
    result = client.get_result_nowait()
    assert result.status.success
    assert result.file_name == pcm_path
    broker_events = [call.args[1].event for call in mock_broker.update_request_status.call_args_list]
    # No backoff active → no BACKOFF status emitted; only IN_PROGRESS
    assert DownloadEvent.BACKOFF not in broker_events
    assert DownloadEvent.IN_PROGRESS in broker_events


@pytest.mark.asyncio(loop_scope="session")
async def test_run_retryable_requeues_and_increments_retry_count():
    '''run() requeues retryable errors and increments retry_count'''
    fake_context = generate_fake_context()
    mock_broker = MagicMock()
    client = make_download_client(yield_dlp_error('Read timed out.'), broker=mock_broker)
    mr = fake_source_dict(fake_context)
    assert mr.download_retry_information.retry_count == 0
    client.submit(mr.guild_id, mr)
    shutdown = asyncio.Event()
    await client.run(shutdown)
    # Result queue should be empty — retryable goes back to input queue
    with pytest.raises(QueueEmpty):
        client.get_result_nowait()
    # retry_count incremented
    assert mr.download_retry_information.retry_count == 1
    # Input queue has the request again
    assert client.queue_size(mr.guild_id) == 1
    broker_events = [call.args[1].event for call in mock_broker.update_request_status.call_args_list]
    assert DownloadEvent.RETRY in broker_events


@pytest.mark.asyncio(loop_scope="session")
async def test_run_terminal_error_puts_result_on_result_queue():
    '''run() puts terminal failures on the result queue for music.py to handle'''
    fake_context = generate_fake_context()
    mock_broker = MagicMock()
    client = make_download_client(yield_dlp_error('Private video'), broker=mock_broker)
    mr = fake_source_dict(fake_context)
    client.submit(mr.guild_id, mr)
    shutdown = asyncio.Event()
    await client.run(shutdown)
    result = client.get_result_nowait()
    assert not result.status.success
    assert result.status.error_type == DownloadErrorType.PRIVATE_VIDEO


@pytest.mark.asyncio(loop_scope="session")
async def test_run_empty_queue_returns_immediately():
    '''run() returns immediately when input queue is empty'''
    client = make_download_client()
    shutdown = asyncio.Event()
    await client.run(shutdown)  # Should not raise or block
    with pytest.raises(QueueEmpty):
        client.get_result_nowait()


@pytest.mark.asyncio(loop_scope="session")
async def test_run_shutdown_during_backoff_does_not_lose_item():
    '''Shutdown during backoff leaves the item in the input queue (not discarded)'''
    fake_context = generate_fake_context()
    client = make_download_client(yield_dlp_error('Private video'))
    mr = fake_source_dict(fake_context)
    client.submit(mr.guild_id, mr)
    # Force an active backoff
    client.wait_timestamp = datetime.now(timezone.utc).timestamp() + 9999
    shutdown = asyncio.Event()
    shutdown.set()
    with pytest.raises(ExitEarlyException):
        await client.run(shutdown)
    # Item must still be in the input queue
    assert client.queue_size(mr.guild_id) == 1


@pytest.mark.asyncio(loop_scope="session")
async def test_run_without_broker_still_works():
    '''run() works correctly when no broker is configured'''
    fake_context = generate_fake_context()
    client = make_download_client(yield_dlp_error('Private video'))
    mr = fake_source_dict(fake_context)
    client.submit(mr.guild_id, mr)
    shutdown = asyncio.Event()
    await client.run(shutdown)
    result = client.get_result_nowait()
    assert not result.status.success


@pytest.mark.asyncio(loop_scope="session")
async def test_result_queue_depth():
    '''result_queue_depth returns total pending results across all guilds'''
    fake_context = generate_fake_context()
    client = make_download_client(yield_dlp_error('Private video'))
    assert client.result_queue_depth() == 0
    mr = fake_source_dict(fake_context)
    client.submit(mr.guild_id, mr)
    shutdown = asyncio.Event()
    await client.run(shutdown)
    assert client.result_queue_depth() == 1
    client.get_result_nowait()
    assert client.result_queue_depth() == 0


# ========== DIRECT item bypass tests ==========

def test_clear_guild_queue_clears_direct_event_when_no_direct_remain():
    '''Clearing all DIRECT items disarms _direct_available so backoff_wait is not spuriously interrupted'''
    fake_context = generate_fake_context()
    client = make_download_client()
    mr = fake_source_dict(fake_context, is_direct_search=True)
    client.submit(mr.guild_id, mr)
    assert client.has_direct_pending
    client.clear_guild_queue(mr.guild_id)
    assert not client.has_direct_pending


def test_clear_guild_queue_keeps_direct_event_when_other_guild_has_direct():
    '''_direct_available stays set if another guild still has DIRECT items after a clear'''
    fake_context_a = generate_fake_context()
    fake_context_b = generate_fake_context()
    client = make_download_client()
    mr_a = fake_source_dict(fake_context_a, is_direct_search=True)
    mr_b = fake_source_dict(fake_context_b, is_direct_search=True)
    client.submit(mr_a.guild_id, mr_a)
    client.submit(mr_b.guild_id, mr_b)
    # Clear only guild A's queue; guild B's DIRECT item still present
    client.clear_guild_queue(mr_a.guild_id)
    assert client.has_direct_pending


def test_submit_direct_routes_to_direct_queue():
    '''DIRECT items go to the direct queue; queue_size counts both queues'''
    fake_context = generate_fake_context()
    client = make_download_client()
    mr_search = fake_source_dict(fake_context)
    mr_direct = fake_source_dict(fake_context, is_direct_search=True)
    client.submit(mr_search.guild_id, mr_search)
    client.submit(mr_direct.guild_id, mr_direct)
    assert client.queue_size(mr_search.guild_id) == 2
    assert client.has_direct_pending


def test_submit_non_direct_does_not_set_direct_event():
    '''Submitting a non-DIRECT item does not set the direct_available event'''
    fake_context = generate_fake_context()
    client = make_download_client()
    mr = fake_source_dict(fake_context)
    client.submit(mr.guild_id, mr)
    assert not client.has_direct_pending


@pytest.mark.asyncio(loop_scope="session")
async def test_backoff_wait_raises_direct_item_available():
    '''backoff_wait raises DirectItemAvailableException when a DIRECT item is pending'''
    fake_context = generate_fake_context()
    shutdown = asyncio.Event()
    client = make_download_client(wait_period_minimum=60, wait_period_max_variance=10)
    client.wait_timestamp = datetime.now(timezone.utc).timestamp() + 120
    mr = fake_source_dict(fake_context, is_direct_search=True)
    client.submit(mr.guild_id, mr)
    with pytest.raises(DirectItemAvailableException):
        await client.backoff_wait(shutdown)


@pytest.mark.asyncio(loop_scope="session")
async def test_run_direct_item_bypasses_active_backoff():
    '''DIRECT item already queued is processed immediately even when backoff is active'''
    fake_context = generate_fake_context()
    with NamedTemporaryFile(delete=False) as tmp_file:
        client = make_download_client(MockYTDLP(fake_file_path=Path(tmp_file.name)))
        mr = fake_source_dict(fake_context, is_direct_search=True)
        client.submit(mr.guild_id, mr)
        client.wait_timestamp = datetime.now(timezone.utc).timestamp() + 9999
        shutdown = asyncio.Event()
        pcm_path = Path(tmp_file.name).with_suffix('.pcm')
        with patch('discord_bot.cogs.music_helpers.download_client.edit_audio_file', return_value=pcm_path):
            await client.run(shutdown)
    result = client.get_result_nowait()
    assert result.status.success


@pytest.mark.asyncio(loop_scope="session")
async def test_run_direct_item_interrupts_mid_wait():
    '''DIRECT item submitted while run() is mid-backoff-wait interrupts the wait'''
    fake_context = generate_fake_context()
    with NamedTemporaryFile(delete=False) as tmp_file:
        client = make_download_client(MockYTDLP(fake_file_path=Path(tmp_file.name)))
        client.wait_timestamp = datetime.now(timezone.utc).timestamp() + 9999
        shutdown = asyncio.Event()
        pcm_path = Path(tmp_file.name).with_suffix('.pcm')

        async def submit_after_delay():
            await asyncio.sleep(0.05)
            mr = fake_source_dict(fake_context, is_direct_search=True)
            client.submit(mr.guild_id, mr)

        with patch('discord_bot.cogs.music_helpers.download_client.edit_audio_file', return_value=pcm_path):
            await asyncio.gather(client.run(shutdown), submit_after_delay())

    result = client.get_result_nowait()
    assert result.status.success


@pytest.mark.asyncio(loop_scope="session")
async def test_run_non_direct_still_waits_backoff():
    '''Non-DIRECT items still wait out the backoff; run() returns without processing if shutdown fires'''
    fake_context = generate_fake_context()
    client = make_download_client(yield_dlp_error('Private video'))
    mr = fake_source_dict(fake_context)
    client.submit(mr.guild_id, mr)
    client.wait_timestamp = datetime.now(timezone.utc).timestamp() + 9999
    shutdown = asyncio.Event()
    shutdown.set()
    with pytest.raises(ExitEarlyException):
        await client.run(shutdown)
    assert client.queue_size(mr.guild_id) == 1


@pytest.mark.asyncio(loop_scope="session")
async def test_run_direct_retryable_requeues_to_direct_queue():
    '''Retryable DIRECT errors go back to the direct queue (not _input_queue)'''
    fake_context = generate_fake_context()
    client = make_download_client(yield_dlp_error('Read timed out.'))
    mr = fake_source_dict(fake_context, is_direct_search=True)
    client.submit(mr.guild_id, mr)
    shutdown = asyncio.Event()
    await client.run(shutdown)
    # Result queue should be empty
    with pytest.raises(QueueEmpty):
        client.get_result_nowait()
    # Item re-queued and direct event re-set
    assert client.queue_size(mr.guild_id) == 1
    assert client.has_direct_pending


@pytest.mark.asyncio(loop_scope="session")
async def test_run_three_direct_items_all_processed_with_backoff():
    '''Three DIRECT items pre-queued are all processed and event is clear after the last one'''
    fake_context = generate_fake_context()
    with NamedTemporaryFile(delete=False) as tmp_file:
        client = make_download_client(MockYTDLP(fake_file_path=Path(tmp_file.name)))
        mrs = [fake_source_dict(fake_context, is_direct_search=True) for _ in range(3)]
        for mr in mrs:
            client.submit(mr.guild_id, mr)
        client.wait_timestamp = datetime.now(timezone.utc).timestamp() + 9999
        shutdown = asyncio.Event()
        pcm_path = Path(tmp_file.name).with_suffix('.pcm')
        with patch('discord_bot.cogs.music_helpers.download_client.edit_audio_file', return_value=pcm_path):
            for _ in range(3):
                await client.run(shutdown)
    assert client.result_queue_depth() == 3
    assert not client.has_direct_pending


@pytest.mark.asyncio(loop_scope="session")
async def test_run_three_direct_items_arriving_mid_backoff():
    '''Three DIRECT items submitted one at a time mid-backoff are each processed in turn'''
    fake_context = generate_fake_context()
    with NamedTemporaryFile(delete=False) as tmp_file:
        client = make_download_client(MockYTDLP(fake_file_path=Path(tmp_file.name)))
        client.wait_timestamp = datetime.now(timezone.utc).timestamp() + 9999
        shutdown = asyncio.Event()
        pcm_path = Path(tmp_file.name).with_suffix('.pcm')

        async def submit_three_with_delays():
            for _ in range(3):
                await asyncio.sleep(0.05)
                mr = fake_source_dict(fake_context, is_direct_search=True)
                client.submit(mr.guild_id, mr)

        async def run_until_three_results():
            with patch('discord_bot.cogs.music_helpers.download_client.edit_audio_file', return_value=pcm_path):
                while client.result_queue_depth() < 3:
                    await client.run(shutdown)

        await asyncio.gather(submit_three_with_delays(), run_until_three_results())

    assert client.result_queue_depth() == 3
    assert not client.has_direct_pending


def test_get_input_nowait_returns_older_item_first():
    '''get_input_nowait picks the item with the older submission timestamp across both queues'''
    fake_context = generate_fake_context()
    client = make_download_client()
    mr_search = fake_source_dict(fake_context)
    mr_direct = fake_source_dict(fake_context, is_direct_search=True)
    # Submit search first, then direct — search should come out first
    client.submit(mr_search.guild_id, mr_search)
    client.submit(mr_direct.guild_id, mr_direct)
    first = client.get_input_nowait()
    assert first.uuid == mr_search.uuid


def test_get_input_nowait_direct_first_when_older():
    '''get_input_nowait picks DIRECT when it was submitted before the non-DIRECT item'''
    fake_context = generate_fake_context()
    client = make_download_client()
    mr_direct = fake_source_dict(fake_context, is_direct_search=True)
    mr_search = fake_source_dict(fake_context)
    client.submit(mr_direct.guild_id, mr_direct)
    client.submit(mr_search.guild_id, mr_search)
    first = client.get_input_nowait()
    assert first.uuid == mr_direct.uuid


def test_get_input_nowait_raises_when_both_empty():
    '''get_input_nowait raises QueueEmpty when both queues are empty'''
    client = make_download_client()
    with pytest.raises(QueueEmpty):
        client.get_input_nowait()



@pytest.mark.asyncio(loop_scope="session")
async def test_run_backoff_expires_empty_queue_returns_without_processing():
    '''When backoff expires naturally but both queues are empty, run() returns without error'''
    client = make_download_client()
    client.wait_timestamp = datetime.now(timezone.utc).timestamp() + 9999
    shutdown = asyncio.Event()
    with patch.object(client, 'backoff_wait', new=AsyncMock(return_value=None)):
        await client.run(shutdown)
    with pytest.raises(QueueEmpty):
        client.get_result_nowait()


@pytest.mark.asyncio(loop_scope="session")
async def test_run_no_backoff_preserves_submission_order():
    '''Without backoff, a DIRECT item submitted after a non-DIRECT item is processed second'''
    fake_context = generate_fake_context()
    with NamedTemporaryFile(delete=False) as tmp_file:
        client = make_download_client(MockYTDLP(fake_file_path=Path(tmp_file.name)))
        mr_search = fake_source_dict(fake_context)
        mr_direct = fake_source_dict(fake_context, is_direct_search=True)
        client.submit(mr_search.guild_id, mr_search)
        client.submit(mr_direct.guild_id, mr_direct)
        shutdown = asyncio.Event()
        pcm_path = Path(tmp_file.name).with_suffix('.pcm')
        with patch('discord_bot.cogs.music_helpers.download_client.edit_audio_file', return_value=pcm_path):
            await client.run(shutdown)
            await client.run(shutdown)
    first = client.get_result_nowait()
    second = client.get_result_nowait()
    assert first.media_request.uuid == mr_search.uuid
    assert second.media_request.uuid == mr_direct.uuid
