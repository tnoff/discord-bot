import asyncio
from functools import partial
from pathlib import Path
from tempfile import NamedTemporaryFile, TemporaryDirectory

from googleapiclient.errors import HttpError
import pytest
from spotipy.exceptions import SpotifyException
from sqlalchemy import create_engine
from yt_dlp.utils import DownloadError

from discord_bot.database import BASE
from discord_bot.cogs.music_helpers.search_cache_client import SearchCacheClient
from discord_bot.cogs.music_helpers.common import SearchType
from discord_bot.cogs.music_helpers.download_client import DownloadClient, InvalidSearchURL, ThirdPartyException, DownloadClientException
from discord_bot.cogs.music_helpers.message_queue import MessageQueue
from discord_bot.cogs.music_helpers.source_dict import SourceDict
from discord_bot.cogs.music_helpers.source_download import SourceDownload

from tests.helpers import FakeChannel, mock_session

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
        raise SpotifyException(404, -1, 'foo exception')

class MockSpotifyRaiseUnauth():
    def __init__(self):
        pass

    def album_get(self, _album_id):
        raise SpotifyException(403, -1, 'foo exception')

class MockYoutubeClient():
    def __init__(self):
        pass

    def playlist_get(self, _playlist_id):
        return [
            'aaaaaaaaaaaaaa'
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

@pytest.mark.asyncio
async def test_spotify_message_check():
    x = DownloadClient(None, None, MessageQueue())
    with pytest.raises(InvalidSearchURL) as exc:
        await x.check_source('https://open.spotify.com/playlist/1111', '1234', 'foo bar requester', '2345', None, 5, FakeChannel())
    assert str(exc.value) == 'Missing spotify creds'
    assert exc.value.user_message == 'Spotify URLs invalid, no spotify credentials available to bot'

@pytest.mark.asyncio(scope="session")
async def test_spotify_throw_exception():
    loop = asyncio.get_running_loop()
    mq = MessageQueue()
    x = DownloadClient(None, None, mq, spotify_client=MockSpotifyRaise())
    with pytest.raises(ThirdPartyException) as exc:
        await x.check_source('https://open.spotify.com/album/1111', '1234', 'foo bar requester', '2345', loop, 5, FakeChannel())
    assert 'Issue fetching spotify info' in str(exc.value)
    assert 'If this is an official Spotify playlist' in str(exc.value.user_message)
    typer, result = mq.get_next_message()
    assert not typer
    assert not result

@pytest.mark.asyncio(scope="session")
async def test_spotify_throw_exception_403():
    loop = asyncio.get_running_loop()
    mq = MessageQueue()
    x = DownloadClient(None, None, mq, spotify_client=MockSpotifyRaiseUnauth())
    with pytest.raises(ThirdPartyException) as exc:
        await x.check_source('https://open.spotify.com/album/1111', '1234', 'foo bar requester', '2345', loop, 5, FakeChannel())
    assert 'Issue fetching spotify info' in str(exc.value)
    assert 'Issue gathering info from spotify url' in str(exc.value.user_message)
    typer, result = mq.get_next_message()
    assert not typer
    assert not result

@pytest.mark.asyncio(scope="session")
async def test_spotify_album_get():
    loop = asyncio.get_running_loop()
    x = DownloadClient(None, None, MessageQueue(), spotify_client=MockSpotifyClient())
    result = await x.check_source('https://open.spotify.com/album/1111', '1234', 'foo bar requester', '2345', loop, 5, FakeChannel())
    assert result[0].requester_id == '2345'
    assert result[0].search_string == 'foo track foo artists'
    assert result[0].search_type == SearchType.SPOTIFY

@pytest.mark.asyncio(scope="session")
async def test_spotify_album_with_cache():
    with NamedTemporaryFile(suffix='.sql') as temp_db:
        engine = create_engine(f'sqlite:///{temp_db.name}')
        BASE.metadata.create_all(engine)
        BASE.metadata.bind = engine
        loop = asyncio.get_running_loop()
        search_client = SearchCacheClient(partial(mock_session, engine), 10)
        source_dict = SourceDict('1234', 'foo bar requester1', 'foo-requester-1', 'foo track foo artists', SearchType.SPOTIFY)
        download = SourceDownload(None, {
            'webpage_url': 'https://youtube.com/watch=v?adafaonoasnfo'
        },
        source_dict)
        search_client.iterate(download)
        x = DownloadClient(None, None, MessageQueue(), spotify_client=MockSpotifyClient(), search_cache_client=search_client)
        result = await x.check_source('https://open.spotify.com/album/1111', '1234', 'foo bar requester', '2345', loop, 5, FakeChannel())
        assert result[0].requester_id == '2345'
        assert result[0].search_string == 'https://youtube.com/watch=v?adafaonoasnfo'
        assert result[0].original_search_string == 'foo track foo artists'
        assert result[0].search_type == SearchType.SPOTIFY

@pytest.mark.asyncio(scope="session")
async def test_spotify_album_with_cache_miss_and_youtube_fallback():
    with NamedTemporaryFile(suffix='.sql') as temp_db:
        engine = create_engine(f'sqlite:///{temp_db.name}')
        BASE.metadata.create_all(engine)
        BASE.metadata.bind = engine
        loop = asyncio.get_running_loop()
        search_client = SearchCacheClient(partial(mock_session, engine), 10)
        x = DownloadClient(None, None, MessageQueue(), spotify_client=MockSpotifyClient(), youtube_music_client=MockYoutubeMusic(), search_cache_client=search_client)
        result = await x.check_source('https://open.spotify.com/album/1111', '1234', 'foo bar requester', '2345', loop, 5, FakeChannel())
        assert result[0].requester_id == '2345'
        assert result[0].search_string == 'https://www.youtube.com/watch?v=vid-1234'
        assert result[0].original_search_string == 'foo track foo artists'
        assert result[0].search_type == SearchType.SPOTIFY

@pytest.mark.asyncio(scope="session")
async def test_spotify_album_get_shuffle():
    loop = asyncio.get_running_loop()
    mq = MessageQueue()
    x = DownloadClient(None, None, mq, spotify_client=MockSpotifyClient())
    result = await x.check_source('https://open.spotify.com/album/1111 shuffle', '1234', 'foo bar requester', '2345', loop, 5, FakeChannel())
    assert result[0].requester_id == '2345'
    assert result[0].search_string == 'foo track foo artists'
    assert result[0].search_type == SearchType.SPOTIFY
    typer, result = mq.get_next_message()
    assert not typer
    assert not result

@pytest.mark.asyncio(scope="session")
async def test_spotify_playlist_get():
    loop = asyncio.get_running_loop()
    x = DownloadClient(None, None, MessageQueue(), spotify_client=MockSpotifyClient())
    result = await x.check_source('https://open.spotify.com/playlist/1111', '1234', 'foo bar requester', '2345', loop, 5, FakeChannel())
    assert result[0].search_string == 'foo track foo artists'

@pytest.mark.asyncio(scope="session")
async def test_spotify_playlist_get_shuffle():
    loop = asyncio.get_running_loop()
    x = DownloadClient(None, None, MessageQueue(), spotify_client=MockSpotifyClient())
    result = await x.check_source('https://open.spotify.com/playlist/1111 shuffle', '1234', 'foo bar requester', '2345', loop, 5, FakeChannel())
    assert result[0].search_string == 'foo track foo artists'

@pytest.mark.asyncio(scope="session")
async def test_spotify_track_get():
    loop = asyncio.get_running_loop()
    x = DownloadClient(None, None, MessageQueue(), spotify_client=MockSpotifyClient())
    result = await x.check_source('https://open.spotify.com/track/1111', '1234', 'foo bar requester', '2345', loop, 5, FakeChannel())
    assert result[0].search_string == 'foo track foo artists'

@pytest.mark.asyncio(scope="session")
async def test_youtube_no_creds():
    loop = asyncio.get_running_loop()
    x = DownloadClient(None, None, MessageQueue())
    with pytest.raises(InvalidSearchURL) as exc:
        await x.check_source('https://www.youtube.com/playlist?list=11111', '1234', 'foo bar requester', '2345', loop, 5, FakeChannel())
    assert 'Missing youtube creds' in str(exc.value)

@pytest.mark.asyncio(scope="session")
async def test_youtube_playlist():
    loop = asyncio.get_running_loop()
    x = DownloadClient(None, None, MessageQueue(), youtube_client=MockYoutubeClient())
    result = await x.check_source('https://www.youtube.com/playlist?list=11111', '1234', 'foo bar requester', '2345', loop, 5, FakeChannel())
    assert result[0].search_string == 'https://www.youtube.com/watch?v=aaaaaaaaaaaaaa'
    assert result[0].search_type == SearchType.DIRECT

@pytest.mark.asyncio(scope="session")
async def test_youtube_playlist_shuffle():
    loop = asyncio.get_running_loop()
    mq = MessageQueue()
    x = DownloadClient(None, None, mq, youtube_client=MockYoutubeClient())
    result = await x.check_source('https://www.youtube.com/playlist?list=11111 shuffle', '1234', 'foo bar requester', '2345', loop, 5, FakeChannel())
    assert result[0].search_string == 'https://www.youtube.com/watch?v=aaaaaaaaaaaaaa'
    assert result[0].search_type == SearchType.DIRECT
    assert mq.get_source_lifecycle() is None

@pytest.mark.asyncio(scope="session")
async def test_youtube_error():
    loop = asyncio.get_running_loop()
    x = DownloadClient(None, None, MessageQueue(), youtube_client=MockYoutubeRaise())
    with pytest.raises(ThirdPartyException) as exc:
        await x.check_source('https://www.youtube.com/playlist?list=11111', '1234', 'foo bar requester', '2345', loop, 5, FakeChannel())
    assert 'Issue fetching youtube info' in str(exc.value)

@pytest.mark.asyncio(scope="session")
async def test_youtube_short():
    loop = asyncio.get_running_loop()
    x = DownloadClient(None, None, MessageQueue())
    result = await x.check_source('https://www.youtube.com/shorts/aaaaaaaaaaa?extra=foo', '1234', 'foo bar requester', '2345', loop, 5, FakeChannel())
    assert result[0].search_string == 'https://www.youtube.com/shorts/aaaaaaaaaaa'
    assert result[0].search_type == SearchType.DIRECT

@pytest.mark.asyncio(scope="session")
async def test_youtube_video():
    loop = asyncio.get_running_loop()
    x = DownloadClient(None, None, MessageQueue())
    result = await x.check_source('https://www.youtube.com/watch?v=aaaaaaaaaaa?extra=foo', '1234', 'foo bar requester', '2345', loop, 5, FakeChannel())
    assert result[0].search_string == 'https://www.youtube.com/watch?v=aaaaaaaaaaa'
    assert result[0].search_type == SearchType.DIRECT

@pytest.mark.asyncio(scope="session")
async def test_fxtwitter():
    loop = asyncio.get_running_loop()
    x = DownloadClient(None, None, MessageQueue())
    result = await x.check_source('https://fxtwitter.com/NicoleCahill_/status/1842208144073576615', '1234', 'foo bar requester', '2345', loop, 5, FakeChannel())
    assert result[0].search_string == 'https://x.com/NicoleCahill_/status/1842208144073576615'
    assert result[0].search_type == SearchType.DIRECT

@pytest.mark.asyncio(scope="session")
async def test_basic_search():
    loop = asyncio.get_running_loop()
    x = DownloadClient(None, None, MessageQueue())
    result = await x.check_source('foo bar', '1234', 'foo bar requester', '2345', loop, 5, FakeChannel())
    assert result[0].search_string == 'foo bar'
    assert result[0].search_type == SearchType.SEARCH

@pytest.mark.asyncio(scope="session")
async def test_prepare_source():
    loop = asyncio.get_running_loop()
    with TemporaryDirectory() as tmp_dir:
        with NamedTemporaryFile(delete=False) as tmp_file:
            x = DownloadClient(MockYTDLP(fake_file_path=Path(tmp_file.name)), Path(tmp_dir), MessageQueue())
            y = SourceDict('1234', 'requester name', 'requester-id', 'foo bar', SearchType.SEARCH)
            result = await x.create_source(y, loop)
            assert result.webpage_url == 'https://example.foo.com'

@pytest.mark.asyncio(scope="session")
async def test_prepare_source_no_download():
    loop = asyncio.get_running_loop()
    x = DownloadClient(MockYTDLP(), None, MessageQueue())
    y = SourceDict('1234', 'requester name', 'requester-id', 'foo bar', SearchType.SEARCH, download_file=False)
    result = await x.create_source(y, loop)
    assert result.webpage_url == 'https://example.foo.com'

@pytest.mark.asyncio(scope="session")
async def test_prepare_source_errors():
    loop = asyncio.get_running_loop()
    x = DownloadClient(yield_dlp_error('Sign in to confirm your age. This video may be inappropriate for some users'), None, MessageQueue())
    y = SourceDict('1234', 'requester name', 'requester-id', 'foo bar', SearchType.SEARCH, download_file=False)
    with pytest.raises(DownloadClientException) as exc:
        await x.create_source(y, loop)
    assert 'Video Aged restricted' in str(exc.value)

    x = DownloadClient(yield_dlp_error('Video unavailable'), None, MessageQueue())
    y = SourceDict('1234', 'requester name', 'requester-id', 'foo bar', SearchType.SEARCH, download_file=False)
    with pytest.raises(DownloadClientException) as exc:
        await x.create_source(y, loop)
    assert 'Video is unavailable' in str(exc.value)

    x = DownloadClient(yield_dlp_error('Private video'), None, MessageQueue())
    y = SourceDict('1234', 'requester name', 'requester-id', 'foo bar', SearchType.SEARCH, download_file=False)
    with pytest.raises(DownloadClientException) as exc:
        await x.create_source(y, loop)
    assert 'Video is private' in str(exc.value)

    x = DownloadClient(yield_dlp_error("Sign in to confirm you're not a bot"), None, MessageQueue())
    y = SourceDict('1234', 'requester name', 'requester-id', 'foo bar', SearchType.SEARCH, download_file=False)
    with pytest.raises(DownloadClientException) as exc:
        await x.create_source(y, loop)
    assert 'Bot flagged download' in str(exc.value)

@pytest.mark.asyncio(scope="session")
async def test_basic_search_with_youtube_music():
    loop = asyncio.get_running_loop()
    x = DownloadClient(None, None, MessageQueue(), youtube_music_client=MockYoutubeMusic)
    result = await x.check_source('foo bar', '1234', 'foo bar requester', '2345', loop, 5, FakeChannel())
    assert result[0].search_string == 'https://www.youtube.com/watch?v=vid-1234'
    assert result[0].search_type == SearchType.SEARCH
    assert result[0].original_search_string == 'foo bar'

@pytest.mark.asyncio(scope="session")
async def test_basic_search_with_youtube_music_skips_direct():
    loop = asyncio.get_running_loop()
    x = DownloadClient(None, None, MessageQueue(), youtube_music_client=MockYoutubeMusic)
    result = await x.check_source('https://www.youtube.com/watch?v=aaaaaaaaaaa', '1234', 'foo bar requester', '2345', loop, 5, FakeChannel())
    assert result[0].search_string == 'https://www.youtube.com/watch?v=aaaaaaaaaaa'
    assert result[0].search_type == SearchType.DIRECT
    assert result[0].original_search_string == 'https://www.youtube.com/watch?v=aaaaaaaaaaa'
