import asyncio

from googleapiclient.errors import HttpError
import pytest
from spotipy.exceptions import SpotifyException, SpotifyOauthError

from discord_bot.cogs.music_helpers.common import SearchType
from discord_bot.cogs.music_helpers.search_client import SearchClient, InvalidSearchURL, ThirdPartyException, SearchResult

from tests.helpers import fake_engine, fake_source_dict #pylint:disable=unused-import


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

class MockSpotifyOauth():
    def __init__(self):
        pass

    def album_get(self, _album_id):
        raise SpotifyOauthError(400, -1, 'foo exception')

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


class MockYoutubeMusic():
    def __init__(self):
        pass

    def search(self, *_args, **_kwargs):
        return 'vid-1234'

@pytest.mark.asyncio
async def test_spotify_message_check():
    x = SearchClient()
    with pytest.raises(InvalidSearchURL) as exc:
        await x.check_source('https://open.spotify.com/playlist/1111', asyncio.get_running_loop(), 5)
    assert str(exc.value) == 'Missing spotify creds'
    assert exc.value.user_message == 'Spotify URLs invalid, no spotify credentials available to bot'

@pytest.mark.asyncio(loop_scope="session")
async def test_spotify_throw_exception():
    loop = asyncio.get_running_loop()
    x = SearchClient(spotify_client=MockSpotifyRaise())
    with pytest.raises(ThirdPartyException) as exc:
        await x.check_source('https://open.spotify.com/album/1111', loop, 5)
    assert 'Issue fetching spotify info' in str(exc.value)
    assert 'If this is an official Spotify playlist' in str(exc.value.user_message)

@pytest.mark.asyncio(loop_scope="session")
async def test_spotify_throw_exception_403():
    loop = asyncio.get_running_loop()
    x = SearchClient(spotify_client=MockSpotifyRaiseUnauth())
    with pytest.raises(ThirdPartyException) as exc:
        await x.check_source('https://open.spotify.com/album/1111', loop, 5)
    assert 'Issue fetching spotify info' in str(exc.value)
    assert 'Issue gathering info from spotify url' in str(exc.value.user_message)

@pytest.mark.asyncio(loop_scope="session")
async def test_spotify_throw_oauth():
    loop = asyncio.get_running_loop()
    x = SearchClient(spotify_client=MockSpotifyRaiseUnauth())
    with pytest.raises(ThirdPartyException) as exc:
        await x.check_source('https://open.spotify.com/album/1111', loop, 5)
    assert 'Issue fetching spotify info' in str(exc.value)
    assert 'Issue gathering info from spotify url' in str(exc.value.user_message)

@pytest.mark.asyncio(loop_scope="session")
async def test_spotify_album_get():
    loop = asyncio.get_running_loop()
    x = SearchClient(spotify_client=MockSpotifyClient())
    result = await x.check_source('https://open.spotify.com/album/1111', loop, 5)
    assert result[0].raw_search_string == 'foo track foo artists'
    assert result[0].search_type == SearchType.SPOTIFY
    assert result[0].multi_search_input == 'https://open.spotify.com/album/1111'

@pytest.mark.asyncio(loop_scope="session")
async def test_spotify_album_with_cache_miss_and_youtube_fallback():
    # If no search cache is hit, make sure that youtube music returns the proper url
    loop = asyncio.get_running_loop()
    x = SearchClient(spotify_client=MockSpotifyClient(), youtube_music_client=MockYoutubeMusic())
    result = await x.check_source('https://open.spotify.com/album/1111', loop, 5)
    assert result[0].resolved_search_string == 'https://www.youtube.com/watch?v=vid-1234'
    assert result[0].raw_search_string == 'foo track foo artists'
    assert result[0].search_type == SearchType.SPOTIFY
    assert result[0].multi_search_input == 'https://open.spotify.com/album/1111'

@pytest.mark.asyncio(loop_scope="session")
async def test_spotify_album_get_shuffle():
    loop = asyncio.get_running_loop()
    x = SearchClient(spotify_client=MockSpotifyClient())
    result = await x.check_source('https://open.spotify.com/album/1111 shuffle', loop, 5)
    assert result[0].raw_search_string == 'foo track foo artists'
    assert result[0].search_type == SearchType.SPOTIFY
    assert result[0].multi_search_input == 'https://open.spotify.com/album/1111'

@pytest.mark.asyncio(loop_scope="session")
async def test_spotify_playlist_get():
    loop = asyncio.get_running_loop()
    x = SearchClient(spotify_client=MockSpotifyClient())
    result = await x.check_source('https://open.spotify.com/playlist/1111', loop, 5)
    assert result[0].raw_search_string == 'foo track foo artists'
    assert result[0].multi_search_input == 'https://open.spotify.com/playlist/1111'

@pytest.mark.asyncio(loop_scope="session")
async def test_spotify_playlist_get_shuffle():
    loop = asyncio.get_running_loop()
    x = SearchClient(spotify_client=MockSpotifyClient())
    result = await x.check_source('https://open.spotify.com/playlist/1111 shuffle', loop, 5)
    assert result[0].raw_search_string == 'foo track foo artists'
    assert result[0].multi_search_input == 'https://open.spotify.com/playlist/1111'

@pytest.mark.asyncio(loop_scope="session")
async def test_spotify_track_get():
    loop = asyncio.get_running_loop()
    x = SearchClient(spotify_client=MockSpotifyClient())
    result = await x.check_source('https://open.spotify.com/track/1111', loop, 5)
    assert result[0].raw_search_string == 'foo track foo artists'
    assert result[0].multi_search_input is None

@pytest.mark.asyncio(loop_scope="session")
async def test_youtube_no_creds():
    loop = asyncio.get_running_loop()
    x = SearchClient()
    with pytest.raises(InvalidSearchURL) as exc:
        await x.check_source('https://www.youtube.com/playlist?list=11111', loop, 5)
    assert 'Missing youtube creds' in str(exc.value)

@pytest.mark.asyncio(loop_scope="session")
async def test_youtube_playlist():
    loop = asyncio.get_running_loop()
    x = SearchClient(youtube_client=MockYoutubeClient())
    result = await x.check_source('https://www.youtube.com/playlist?list=11111', loop, 5)
    assert result[0].raw_search_string == 'https://www.youtube.com/watch?v=aaaaaaaaaaaaaa'
    assert result[0].search_type == SearchType.YOUTUBE_PLAYLIST
    assert result[0].multi_search_input == 'https://www.youtube.com/playlist?list=11111'

@pytest.mark.asyncio(loop_scope="session")
async def test_youtube_playlist_shuffle():
    loop = asyncio.get_running_loop()
    x = SearchClient(youtube_client=MockYoutubeClient())
    result = await x.check_source('https://www.youtube.com/playlist?list=11111 shuffle', loop, 5)
    assert result[0].raw_search_string == 'https://www.youtube.com/watch?v=aaaaaaaaaaaaaa'
    assert result[0].search_type == SearchType.YOUTUBE_PLAYLIST
    assert result[0].multi_search_input == 'https://www.youtube.com/playlist?list=11111'

@pytest.mark.asyncio(loop_scope="session")
async def test_youtube_error():
    loop = asyncio.get_running_loop()
    x = SearchClient(youtube_client=MockYoutubeRaise())
    with pytest.raises(ThirdPartyException) as exc:
        await x.check_source('https://www.youtube.com/playlist?list=11111', loop, 5)
    assert 'Issue fetching youtube info' in str(exc.value)

@pytest.mark.asyncio(loop_scope="session")
async def test_youtube_short():
    loop = asyncio.get_running_loop()
    x = SearchClient()
    result = await x.check_source('https://www.youtube.com/shorts/aaaaaaaaaaa?extra=foo', loop, 5)
    assert result[0].raw_search_string == 'https://www.youtube.com/shorts/aaaaaaaaaaa'
    assert result[0].search_type == SearchType.YOUTUBE
    assert result[0].multi_search_input is None

@pytest.mark.asyncio(loop_scope="session")
async def test_youtube_video():
    loop = asyncio.get_running_loop()
    x = SearchClient()
    result = await x.check_source('https://www.youtube.com/watch?v=aaaaaaaaaaa?extra=foo', loop, 5)
    assert result[0].raw_search_string == 'https://www.youtube.com/watch?v=aaaaaaaaaaa'
    assert result[0].search_type == SearchType.YOUTUBE
    assert result[0].multi_search_input is None

@pytest.mark.asyncio(loop_scope="session")
async def test_fxtwitter():
    loop = asyncio.get_running_loop()
    x = SearchClient()
    result = await x.check_source('https://fxtwitter.com/NicoleCahill_/status/1842208144073576615', loop, 5)
    assert result[0].raw_search_string == 'https://x.com/NicoleCahill_/status/1842208144073576615'
    assert result[0].search_type == SearchType.DIRECT
    assert result[0].multi_search_input is None

@pytest.mark.asyncio(loop_scope="session")
async def test_basic_search():
    loop = asyncio.get_running_loop()
    x = SearchClient()
    result = await x.check_source('foo bar', loop, 5)
    assert result[0].raw_search_string == 'foo bar'
    assert result[0].search_type == SearchType.SEARCH
    assert result[0].multi_search_input is None


@pytest.mark.asyncio(loop_scope="session")
async def test_basic_search_with_youtube_music():
    loop = asyncio.get_running_loop()
    x = SearchClient(youtube_music_client=MockYoutubeMusic())
    result = await x.check_source('foo bar', loop, 5)
    assert result[0].resolved_search_string == 'https://www.youtube.com/watch?v=vid-1234'
    assert result[0].search_type == SearchType.SEARCH
    assert result[0].raw_search_string == 'foo bar'
    assert result[0].multi_search_input is None

@pytest.mark.asyncio(loop_scope="session")
async def test_basic_search_with_youtube_music_skips_direct():
    loop = asyncio.get_running_loop()
    x = SearchClient(youtube_music_client=MockYoutubeMusic())
    result = await x.check_source('https://www.youtube.com/watch?v=aaaaaaaaaaa', loop, 5)
    assert result[0].raw_search_string == 'https://www.youtube.com/watch?v=aaaaaaaaaaa'
    assert result[0].search_type == SearchType.YOUTUBE
    assert result[0].multi_search_input is None


# Tests for SearchResult class functionality
def test_search_result_creation():
    """Test SearchResult object creation"""
    result = SearchResult(SearchType.SEARCH, 'test search', None)
    assert result.search_type == SearchType.SEARCH
    assert result.raw_search_string == 'test search'
    assert result.youtube_music_search_string is None
    assert result.multi_search_input is None


def test_search_result_with_multi_input():
    """Test SearchResult object creation with multi input"""
    result = SearchResult(SearchType.SPOTIFY, 'track search', 'https://open.spotify.com/album/123')
    assert result.search_type == SearchType.SPOTIFY
    assert result.raw_search_string == 'track search'
    assert result.youtube_music_search_string is None
    assert result.multi_search_input == 'https://open.spotify.com/album/123'


def test_search_result_add_youtube_music_result():
    """Test adding YouTube music result to SearchResult"""
    result = SearchResult(SearchType.SEARCH, 'foo bar', None)
    assert result.resolved_search_string == 'foo bar'

    result.add_youtube_music_result('https://www.youtube.com/watch?v=vid123')
    assert result.youtube_music_search_string == 'https://www.youtube.com/watch?v=vid123'
    assert result.resolved_search_string == 'https://www.youtube.com/watch?v=vid123'
    assert result.raw_search_string == 'foo bar'  # Original should remain unchanged


def test_search_result_resolved_search_string_fallback():
    """Test that resolved_search_string falls back to raw_search_string when no YouTube music result"""
    result = SearchResult(SearchType.DIRECT, 'https://example.com', None)
    assert result.resolved_search_string == 'https://example.com'
    assert result.youtube_music_search_string is None


def test_search_result_resolved_search_string_with_youtube_music():
    """Test that resolved_search_string prefers YouTube music result when available"""
    result = SearchResult(SearchType.SEARCH, 'original search', None)
    result.add_youtube_music_result('youtube result')
    assert result.resolved_search_string == 'youtube result'
    assert result.raw_search_string == 'original search'


@pytest.mark.asyncio
async def test_search_workflow_basic():
    """Test the complete search workflow for basic searches"""
    loop = asyncio.get_running_loop()
    x = SearchClient()
    results = await x.check_source('basic search', loop, 10)

    assert len(results) == 1
    assert results[0].search_type == SearchType.SEARCH
    assert results[0].raw_search_string == 'basic search'
    assert results[0].resolved_search_string == 'basic search'
    assert results[0].multi_search_input is None


@pytest.mark.asyncio
async def test_search_workflow_direct_url():
    """Test the search workflow for direct URLs"""
    loop = asyncio.get_running_loop()
    x = SearchClient()
    results = await x.check_source('https://example.com', loop, 10)

    assert len(results) == 1
    assert results[0].search_type == SearchType.DIRECT
    assert results[0].raw_search_string == 'https://example.com'
    assert results[0].resolved_search_string == 'https://example.com'


@pytest.mark.asyncio
async def test_search_workflow_with_youtube_music():
    """Test search workflow with YouTube Music integration"""
    loop = asyncio.get_running_loop()
    x = SearchClient(youtube_music_client=MockYoutubeMusic())
    results = await x.check_source('search term', loop, 5)

    assert len(results) == 1
    assert results[0].search_type == SearchType.SEARCH
    assert results[0].raw_search_string == 'search term'
    assert results[0].resolved_search_string == 'https://www.youtube.com/watch?v=vid-1234'
    assert results[0].youtube_music_search_string == 'https://www.youtube.com/watch?v=vid-1234'


@pytest.mark.asyncio
async def test_search_workflow_max_results_limit():
    """Test that max_results parameter properly limits results"""
    loop = asyncio.get_running_loop()
    x = SearchClient(spotify_client=MockSpotifyClient())

    # MockSpotifyClient returns only 1 result, so this tests the limit logic
    results = await x.check_source('https://open.spotify.com/album/1111', loop, 2)
    assert len(results) == 1  # Can't exceed what Spotify returns

    # Test with limit of 0 (should return empty)
    results = await x.check_source('https://open.spotify.com/album/1111', loop, 0)
    assert len(results) == 0
