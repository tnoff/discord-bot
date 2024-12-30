import asyncio

from googleapiclient.errors import HttpError
import pytest
from spotipy.exceptions import SpotifyException
from yt_dlp.utils import DownloadError

from discord_bot.cogs.music_helpers.common import SearchType
from discord_bot.cogs.music_helpers.download_client import DownloadClient, InvalidSearchURL, ThirdPartyException, DownloadClientException
from discord_bot.cogs.music_helpers.message_queue import MessageQueue
from discord_bot.cogs.music_helpers.source_dict import SourceDict

from tests.helpers import FakeChannel

class MockSpotifyClient():
    def __init__(self):
        pass

    def album_get(self, _album_id):
        return [
            {
                'track_name': 'foo track',
                'track_artists': 'foo artists',
            }
        ]

    def playlist_get(self, _playlist_id):
        return [
            {
                'track_name': 'foo track',
                'track_artists': 'foo artists',
            }
        ]

    def track_get(self, _track_id):
        return [
            {
                'track_name': 'foo track',
                'track_artists': 'foo artists',
            }
        ]

class MockSpotifyRaise():
    def __init__(self):
        pass

    def album_get(self, _album_id):
        raise SpotifyException('foo exception', 404, 'foo exception')

class MockYoutubeClient():
    def __init__(self):
        pass

    def playlist_get(self, _playlist_id):
        return [
            'https://example.foo.com'
        ]

class MockResponse():
    def __init__(self):
        self.reason = 'cats unplugged servers'

class MockYoutubeRaise():
    def __init__(self):
        pass

    def playlist_get(self, _playlist_id):
        raise HttpError(MockResponse(), 'foo'.encode('utf-8'))

class MockYTDLP():
    def __init__(self):
        pass

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
                    'filepath': 'foo-bar.mp3',
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

@pytest.mark.asyncio
async def test_spotify_message_check():
    x = DownloadClient(None)
    with pytest.raises(InvalidSearchURL) as exc:
        await x.check_source('https://open.spotify.com/playlist/1111', '1234', 'foo bar requester', '2345', None, 5, MessageQueue(), FakeChannel())
    assert str(exc.value) == 'Missing spotify creds'
    assert exc.value.user_message == 'Spotify URLs invalid, no spotify credentials available to bot'

@pytest.mark.asyncio(scope="session")
async def test_spotify_throw_exception():
    loop = asyncio.get_running_loop()
    x = DownloadClient(None, spotify_client=MockSpotifyRaise())
    mq = MessageQueue()
    with pytest.raises(ThirdPartyException) as exc:
        await x.check_source('https://open.spotify.com/album/1111', '1234', 'foo bar requester', '2345', loop, 5, mq, FakeChannel())
    assert 'Issue fetching spotify info' in str(exc.value)
    typer, result = mq.get_next_message()
    assert not typer
    assert not result

@pytest.mark.asyncio(scope="session")
async def test_spotify_album_get():
    loop = asyncio.get_running_loop()
    x = DownloadClient(None, spotify_client=MockSpotifyClient())
    result = await x.check_source('https://open.spotify.com/album/1111', '1234', 'foo bar requester', '2345', loop, 5, MessageQueue(), FakeChannel())
    assert result[0].requester_id == '2345'
    assert result[0].search_string == 'foo track foo artists'
    assert result[0].search_type == SearchType.SPOTIFY

@pytest.mark.asyncio(scope="session")
async def test_spotify_album_get_shuffle():
    loop = asyncio.get_running_loop()
    x = DownloadClient(None, spotify_client=MockSpotifyClient())
    mq = MessageQueue()
    result = await x.check_source('https://open.spotify.com/album/1111 shuffle', '1234', 'foo bar requester', '2345', loop, 5, mq, FakeChannel())
    assert result[0].requester_id == '2345'
    assert result[0].search_string == 'foo track foo artists'
    assert result[0].search_type == SearchType.SPOTIFY
    typer, result = mq.get_next_message()
    assert not typer
    assert not result

@pytest.mark.asyncio(scope="session")
async def test_spotify_playlist_get():
    loop = asyncio.get_running_loop()
    x = DownloadClient(None, spotify_client=MockSpotifyClient())
    result = await x.check_source('https://open.spotify.com/playlist/1111', '1234', 'foo bar requester', '2345', loop, 5, MessageQueue(), FakeChannel())
    assert result[0].search_string == 'foo track foo artists'

@pytest.mark.asyncio(scope="session")
async def test_spotify_playlist_get_shuffle():
    loop = asyncio.get_running_loop()
    x = DownloadClient(None, spotify_client=MockSpotifyClient())
    result = await x.check_source('https://open.spotify.com/playlist/1111 shuffle', '1234', 'foo bar requester', '2345', loop, 5, MessageQueue(), FakeChannel())
    assert result[0].search_string == 'foo track foo artists'

@pytest.mark.asyncio(scope="session")
async def test_spotify_track_get():
    loop = asyncio.get_running_loop()
    x = DownloadClient(None, spotify_client=MockSpotifyClient())
    result = await x.check_source('https://open.spotify.com/track/1111', '1234', 'foo bar requester', '2345', loop, 5, MessageQueue(), FakeChannel())
    assert result[0].search_string == 'foo track foo artists'

@pytest.mark.asyncio(scope="session")
async def test_youtube_no_creds():
    loop = asyncio.get_running_loop()
    x = DownloadClient(None)
    with pytest.raises(InvalidSearchURL) as exc:
        await x.check_source('https://www.youtube.com/playlist?list=11111', '1234', 'foo bar requester', '2345', loop, 5, MessageQueue(), FakeChannel())
    assert 'Missing youtube creds' in str(exc.value)

@pytest.mark.asyncio(scope="session")
async def test_youtube_playlist():
    loop = asyncio.get_running_loop()
    x = DownloadClient(None, youtube_client=MockYoutubeClient())
    result = await x.check_source('https://www.youtube.com/playlist?list=11111', '1234', 'foo bar requester', '2345', loop, 5, MessageQueue(), FakeChannel())
    assert result[0].search_string == 'https://example.foo.com'
    assert result[0].search_type == SearchType.DIRECT

@pytest.mark.asyncio(scope="session")
async def test_youtube_playlist_shuffle():
    loop = asyncio.get_running_loop()
    x = DownloadClient(None, youtube_client=MockYoutubeClient())
    mq = MessageQueue()
    result = await x.check_source('https://www.youtube.com/playlist?list=11111 shuffle', '1234', 'foo bar requester', '2345', loop, 5, mq, FakeChannel())
    assert result[0].search_string == 'https://example.foo.com'
    assert result[0].search_type == SearchType.DIRECT
    assert mq.get_source_lifecycle() is None

@pytest.mark.asyncio(scope="session")
async def test_youtube_error():
    loop = asyncio.get_running_loop()
    x = DownloadClient(None, youtube_client=MockYoutubeRaise())
    with pytest.raises(ThirdPartyException) as exc:
        await x.check_source('https://www.youtube.com/playlist?list=11111', '1234', 'foo bar requester', '2345', loop, 5, MessageQueue(), FakeChannel())
    assert 'Issue fetching youtube info' in str(exc.value)

@pytest.mark.asyncio(scope="session")
async def test_youtube_short():
    loop = asyncio.get_running_loop()
    x = DownloadClient(None)
    result = await x.check_source('https://www.youtube.com/shorts/aaaaaaaaaaa?extra=foo', '1234', 'foo bar requester', '2345', loop, 5, MessageQueue(), FakeChannel())
    assert result[0].search_string == 'https://www.youtube.com/shorts/aaaaaaaaaaa'
    assert result[0].search_type == SearchType.DIRECT

@pytest.mark.asyncio(scope="session")
async def test_youtube_video():
    loop = asyncio.get_running_loop()
    x = DownloadClient(None)
    result = await x.check_source('https://www.youtube.com/watch?v=aaaaaaaaaaa?extra=foo', '1234', 'foo bar requester', '2345', loop, 5, MessageQueue(), FakeChannel())
    assert result[0].search_string == 'https://www.youtube.com/watch?v=aaaaaaaaaaa'
    assert result[0].search_type == SearchType.DIRECT

@pytest.mark.asyncio(scope="session")
async def test_fxtwitter():
    loop = asyncio.get_running_loop()
    x = DownloadClient(None)
    result = await x.check_source('https://fxtwitter.com/NicoleCahill_/status/1842208144073576615', '1234', 'foo bar requester', '2345', loop, 5, MessageQueue(), FakeChannel())
    assert result[0].search_string == 'https://x.com/NicoleCahill_/status/1842208144073576615'
    assert result[0].search_type == SearchType.DIRECT

@pytest.mark.asyncio(scope="session")
async def test_basic_search():
    loop = asyncio.get_running_loop()
    x = DownloadClient(None)
    result = await x.check_source('foo bar', '1234', 'foo bar requester', '2345', loop, 5, MessageQueue(), FakeChannel())
    assert result[0].search_string == 'foo bar'
    assert result[0].search_type == SearchType.SEARCH

@pytest.mark.asyncio(scope="session")
async def test_prepare_source():
    loop = asyncio.get_running_loop()
    x = DownloadClient(MockYTDLP())
    y = SourceDict('1234', 'requester name', 'requester-id', 'foo bar', SearchType.SEARCH)
    result = await x.create_source(y, loop)
    assert result.webpage_url == 'https://example.foo.com'

@pytest.mark.asyncio(scope="session")
async def test_prepare_source_no_download():
    loop = asyncio.get_running_loop()
    x = DownloadClient(MockYTDLP())
    y = SourceDict('1234', 'requester name', 'requester-id', 'foo bar', SearchType.SEARCH, download_file=False)
    result = await x.create_source(y, loop)
    assert result.webpage_url == 'https://example.foo.com'

@pytest.mark.asyncio(scope="session")
async def test_prepare_source_errors():
    loop = asyncio.get_running_loop()
    x = DownloadClient(yield_dlp_error('Sign in to confirm your age. This video may be inappropriate for some users'))
    y = SourceDict('1234', 'requester name', 'requester-id', 'foo bar', SearchType.SEARCH, download_file=False)
    with pytest.raises(DownloadClientException) as exc:
        await x.create_source(y, loop)
    assert 'Video Aged restricted' in str(exc.value)

    x = DownloadClient(yield_dlp_error('Video unavailable'))
    y = SourceDict('1234', 'requester name', 'requester-id', 'foo bar', SearchType.SEARCH, download_file=False)
    with pytest.raises(DownloadClientException) as exc:
        await x.create_source(y, loop)
    assert 'Video is unavailable' in str(exc.value)

    x = DownloadClient(yield_dlp_error('Private video'))
    y = SourceDict('1234', 'requester name', 'requester-id', 'foo bar', SearchType.SEARCH, download_file=False)
    with pytest.raises(DownloadClientException) as exc:
        await x.create_source(y, loop)
    assert 'Video is private' in str(exc.value)

class MockYoutubeMusic():
    def __init__(self):
        pass

    def search(self, *_args, **_kwargs):
        return 'vid-1234'

@pytest.mark.asyncio(scope="session")
async def test_basic_search_with_youtube_music():
    loop = asyncio.get_running_loop()
    x = DownloadClient(None, youtube_music_client=MockYoutubeMusic)
    result = await x.check_source('foo bar', '1234', 'foo bar requester', '2345', loop, 5, MessageQueue(), FakeChannel())
    assert result[0].search_string == 'https://www.youtube.com/watch?v=vid-1234'
    assert result[0].search_type == SearchType.SEARCH
    assert result[0].original_search_string == 'foo bar'

@pytest.mark.asyncio(scope="session")
async def test_basic_search_with_youtube_music_skips_direct():
    loop = asyncio.get_running_loop()
    x = DownloadClient(None, youtube_music_client=MockYoutubeMusic)
    result = await x.check_source('https://www.youtube.com/watch?v=aaaaaaaaaaa', '1234', 'foo bar requester', '2345', loop, 5, MessageQueue(), FakeChannel())
    assert result[0].search_string == 'https://www.youtube.com/watch?v=aaaaaaaaaaa'
    assert result[0].search_type == SearchType.DIRECT
    assert result[0].original_search_string == 'https://www.youtube.com/watch?v=aaaaaaaaaaa'
