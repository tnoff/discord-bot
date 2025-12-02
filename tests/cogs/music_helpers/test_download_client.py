import asyncio
from pathlib import Path
from tempfile import NamedTemporaryFile, TemporaryDirectory

import pytest
from yt_dlp.utils import DownloadError

from discord_bot.cogs.music_helpers.download_client import DownloadClient, DownloadClientException, InvalidFormatException, VideoTooLong, match_generator

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
    with pytest.raises(DownloadClientException) as exc:
        await x.create_source(y, loop)
    assert 'Video is age restricted, cannot download' in str(exc.value.user_message)

    x = DownloadClient(yield_dlp_error("This video has been removed for violating YouTube's Terms of Service"), None)
    y = fake_source_dict(fake_context, download_file=False)
    with pytest.raises(DownloadClientException) as exc:
        await x.create_source(y, loop)
    assert 'Video is unvailable due to violating terms of service, cannot download' in str(exc.value.user_message)

    x = DownloadClient(yield_dlp_error('Video unavailable'), None)
    y = fake_source_dict(fake_context, download_file=False)
    with pytest.raises(DownloadClientException) as exc:
        await x.create_source(y, loop)
    assert 'Video is unavailable, cannot download' in str(exc.value.user_message)

    x = DownloadClient(yield_dlp_error('Private video'), None)
    y = fake_source_dict(fake_context, download_file=False)
    with pytest.raises(DownloadClientException) as exc:
        await x.create_source(y, loop)
    assert 'Video is private, cannot download' in str(exc.value.user_message)

    x = DownloadClient(yield_dlp_error("Sign in to confirm you're not a bot"), None)
    y = fake_source_dict(fake_context, download_file=False)
    with pytest.raises(DownloadClientException) as exc:
        await x.create_source(y, loop)
    assert 'Download attempt flagged as bot download, skipping' in str(exc.value.user_message)

    x = DownloadClient(yield_dlp_error('Requested format is not available'), None)
    y = fake_source_dict(fake_context, download_file=False)
    with pytest.raises(InvalidFormatException) as exc:
        await x.create_source(y, loop)
    assert 'Video format not available' in str(exc.value)

    x = DownloadClient(MockYTDLPNoData(), None)
    y = fake_source_dict(fake_context, download_file=False)
    with pytest.raises(DownloadClientException) as exc:
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
