import asyncio
from pathlib import Path
from tempfile import NamedTemporaryFile, TemporaryDirectory

import pytest
from yt_dlp.utils import DownloadError

from discord_bot.cogs.music_helpers.common import SearchType
from discord_bot.cogs.music_helpers.download_client import DownloadClient,  DownloadClientException
from discord_bot.cogs.music_helpers.source_dict import SourceDict

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


@pytest.mark.asyncio(scope="session")
async def test_prepare_source():
    loop = asyncio.get_running_loop()
    with TemporaryDirectory() as tmp_dir:
        with NamedTemporaryFile(delete=False) as tmp_file:
            x = DownloadClient(MockYTDLP(fake_file_path=Path(tmp_file.name)), Path(tmp_dir))
            y = SourceDict('1234', 'requester name', 'requester-id', 'foo bar', SearchType.SEARCH)
            result = await x.create_source(y, loop)
            assert result.webpage_url == 'https://example.foo.com'

@pytest.mark.asyncio(scope="session")
async def test_prepare_source_no_download():
    loop = asyncio.get_running_loop()
    x = DownloadClient(MockYTDLP(), None)
    y = SourceDict('1234', 'requester name', 'requester-id', 'foo bar', SearchType.SEARCH, download_file=False)
    result = await x.create_source(y, loop)
    assert result.webpage_url == 'https://example.foo.com'

@pytest.mark.asyncio(scope="session")
async def test_prepare_source_errors():
    loop = asyncio.get_running_loop()
    x = DownloadClient(yield_dlp_error('Sign in to confirm your age. This video may be inappropriate for some users'), None)
    y = SourceDict('1234', 'requester name', 'requester-id', 'foo bar', SearchType.SEARCH, download_file=False)
    with pytest.raises(DownloadClientException) as exc:
        await x.create_source(y, loop)
    assert 'Video Aged restricted' in str(exc.value)

    x = DownloadClient(yield_dlp_error('Video unavailable'), None)
    y = SourceDict('1234', 'requester name', 'requester-id', 'foo bar', SearchType.SEARCH, download_file=False)
    with pytest.raises(DownloadClientException) as exc:
        await x.create_source(y, loop)
    assert 'Video is unavailable' in str(exc.value)

    x = DownloadClient(yield_dlp_error('Private video'), None)
    y = SourceDict('1234', 'requester name', 'requester-id', 'foo bar', SearchType.SEARCH, download_file=False)
    with pytest.raises(DownloadClientException) as exc:
        await x.create_source(y, loop)
    assert 'Video is private' in str(exc.value)

    x = DownloadClient(yield_dlp_error("Sign in to confirm you're not a bot"), None)
    y = SourceDict('1234', 'requester name', 'requester-id', 'foo bar', SearchType.SEARCH, download_file=False)
    with pytest.raises(DownloadClientException) as exc:
        await x.create_source(y, loop)
    assert 'Bot flagged download' in str(exc.value)
