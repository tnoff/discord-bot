
from googleapiclient.errors import HttpError
import pytest
from spotipy.exceptions import SpotifyException, SpotifyOauthError

from discord_bot.clients.media_search_client import InMemoryMediaSearchClient
from discord_bot.cogs.music_helpers.common import SearchType
from discord_bot.cogs.music_helpers.search_client import SearchClient, InvalidSearchURL, ThirdPartyException, check_youtube_video
from discord_bot.types.search import SearchResult, SearchCollection
from discord_bot.types.catalog import CatalogResponse, CatalogItem
from discord_bot.utils.integrations.common import YOUTUBE_VIDEO_PREFIX

from tests.helpers import fake_engine, fake_source_dict #pylint:disable=unused-import


class MockSpotifyClient():
    def __init__(self):
        pass

    def album_get(self, _album_id):
        return CatalogResponse(items=[CatalogItem(search_string='foo track foo artists', title='foo track')], collection_name='Mock Album Name')

    def playlist_get(self, _playlist_id):
        return CatalogResponse(items=[CatalogItem(search_string='foo track foo artists', title='foo track')], collection_name='Mock Playlist Name')

    def track_get(self, _track_id):
        return CatalogResponse(items=[CatalogItem(search_string='foo track foo artists', title='foo track')])

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
        return CatalogResponse(items=[CatalogItem(search_string=f'{YOUTUBE_VIDEO_PREFIX}aaaaaaaaaaaaaa', title='foo title')], collection_name='Mock YouTube Playlist')

class MockResponse():
    def __init__(self):
        self.reason = 'cats unplugged servers'

class MockYoutubeRaise():
    def __init__(self):
        pass

    def playlist_get(self, _playlist_id):
        raise HttpError(MockResponse(), 'foo'.encode('utf-8'))


@pytest.mark.asyncio
async def test_spotify_message_check():
    x = SearchClient(InMemoryMediaSearchClient())
    with pytest.raises(InvalidSearchURL) as exc:
        await x.check_source('https://open.spotify.com/playlist/1111', 5)
    assert str(exc.value) == 'Missing spotify creds'
    assert exc.value.user_message == 'Spotify URLs invalid, no spotify credentials available to bot'

@pytest.mark.asyncio(loop_scope="session")
async def test_spotify_throw_exception():
    x = SearchClient(InMemoryMediaSearchClient(spotify_client=MockSpotifyRaise()))
    with pytest.raises(ThirdPartyException) as exc:
        await x.check_source('https://open.spotify.com/album/1111', 5)
    assert 'Issue fetching spotify info' in str(exc.value)
    assert 'If this is an official Spotify playlist' in str(exc.value.user_message)

@pytest.mark.asyncio(loop_scope="session")
async def test_spotify_throw_exception_403():
    x = SearchClient(InMemoryMediaSearchClient(spotify_client=MockSpotifyRaiseUnauth()))
    with pytest.raises(ThirdPartyException) as exc:
        await x.check_source('https://open.spotify.com/album/1111', 5)
    assert 'Issue fetching spotify info' in str(exc.value)
    assert 'Issue gathering info from spotify url' in str(exc.value.user_message)

@pytest.mark.asyncio(loop_scope="session")
async def test_spotify_throw_oauth():
    x = SearchClient(InMemoryMediaSearchClient(spotify_client=MockSpotifyRaiseUnauth()))
    with pytest.raises(ThirdPartyException) as exc:
        await x.check_source('https://open.spotify.com/album/1111', 5)
    assert 'Issue fetching spotify info' in str(exc.value)
    assert 'Issue gathering info from spotify url' in str(exc.value.user_message)

@pytest.mark.asyncio(loop_scope="session")
async def test_spotify_album_get():
    x = SearchClient(InMemoryMediaSearchClient(spotify_client=MockSpotifyClient()))
    result = await x.check_source('https://open.spotify.com/album/1111', 5)
    assert result.search_results[0].raw_search_string == 'foo track foo artists'
    assert result.search_results[0].search_type == SearchType.SEARCH
    assert result.collection_name == 'Mock Album Name'

@pytest.mark.asyncio(loop_scope="session")
async def test_spotify_album_with_cache_miss_and_youtube_fallback():
    # YouTube music search is now handled separately in the music queue
    x = SearchClient(InMemoryMediaSearchClient(spotify_client=MockSpotifyClient()))
    result = await x.check_source('https://open.spotify.com/album/1111', 5)
    assert result.search_results[0].resolved_search_string == 'foo track foo artists'
    assert result.search_results[0].raw_search_string == 'foo track foo artists'
    assert result.search_results[0].search_type == SearchType.SEARCH
    assert result.collection_name == 'Mock Album Name'

@pytest.mark.asyncio(loop_scope="session")
async def test_spotify_album_get_shuffle():
    x = SearchClient(InMemoryMediaSearchClient(spotify_client=MockSpotifyClient()))
    result = await x.check_source('https://open.spotify.com/album/1111 shuffle', 5)
    assert result.search_results[0].raw_search_string == 'foo track foo artists'
    assert result.search_results[0].search_type == SearchType.SEARCH
    assert result.collection_name == 'Mock Album Name'

@pytest.mark.asyncio(loop_scope="session")
async def test_spotify_playlist_get():
    x = SearchClient(InMemoryMediaSearchClient(spotify_client=MockSpotifyClient()))
    result = await x.check_source('https://open.spotify.com/playlist/1111', 5)
    assert result.search_results[0].raw_search_string == 'foo track foo artists'
    assert result.collection_name == 'Mock Playlist Name'

@pytest.mark.asyncio(loop_scope="session")
async def test_spotify_playlist_get_shuffle():
    x = SearchClient(InMemoryMediaSearchClient(spotify_client=MockSpotifyClient()))
    result = await x.check_source('https://open.spotify.com/playlist/1111 shuffle', 5)
    assert result.search_results[0].raw_search_string == 'foo track foo artists'
    assert result.collection_name == 'Mock Playlist Name'

@pytest.mark.asyncio(loop_scope="session")
async def test_spotify_track_get():
    x = SearchClient(InMemoryMediaSearchClient(spotify_client=MockSpotifyClient()))
    result = await x.check_source('https://open.spotify.com/track/1111', 5)
    assert result.search_results[0].raw_search_string == 'foo track foo artists'
    assert result.collection_name == 'https://open.spotify.com/track/1111'

@pytest.mark.asyncio(loop_scope="session")
async def test_youtube_no_creds():
    x = SearchClient(InMemoryMediaSearchClient())
    with pytest.raises(InvalidSearchURL) as exc:
        await x.check_source('https://www.youtube.com/playlist?list=11111', 5)
    assert 'Missing youtube creds' in str(exc.value)

@pytest.mark.asyncio(loop_scope="session")
async def test_youtube_playlist():
    x = SearchClient(InMemoryMediaSearchClient(youtube_client=MockYoutubeClient()))
    result = await x.check_source('https://www.youtube.com/playlist?list=11111', 5)
    assert result.search_results[0].raw_search_string == 'https://www.youtube.com/watch?v=aaaaaaaaaaaaaa'
    assert result.search_results[0].search_type == SearchType.YOUTUBE
    assert result.collection_name == 'Mock YouTube Playlist'

@pytest.mark.asyncio(loop_scope="session")
async def test_youtube_playlist_shuffle():
    x = SearchClient(InMemoryMediaSearchClient(youtube_client=MockYoutubeClient()))
    result = await x.check_source('https://www.youtube.com/playlist?list=11111 shuffle', 5)
    assert result.search_results[0].raw_search_string == 'https://www.youtube.com/watch?v=aaaaaaaaaaaaaa'
    assert result.search_results[0].search_type == SearchType.YOUTUBE
    assert result.collection_name == 'Mock YouTube Playlist'

@pytest.mark.asyncio(loop_scope="session")
async def test_youtube_error():
    x = SearchClient(InMemoryMediaSearchClient(youtube_client=MockYoutubeRaise()))
    with pytest.raises(ThirdPartyException) as exc:
        await x.check_source('https://www.youtube.com/playlist?list=11111', 5)
    assert 'Issue fetching youtube info' in str(exc.value)

@pytest.mark.asyncio(loop_scope="session")
async def test_youtube_short():
    x = SearchClient(InMemoryMediaSearchClient())
    result = await x.check_source('https://www.youtube.com/shorts/aaaaaaaaaaa?extra=foo', 5)
    assert result.search_results[0].raw_search_string == 'https://www.youtube.com/shorts/aaaaaaaaaaa'
    assert result.search_results[0].search_type == SearchType.YOUTUBE
    assert result.collection_name is None

@pytest.mark.asyncio(loop_scope="session")
async def test_youtube_video():
    x = SearchClient(InMemoryMediaSearchClient())
    result = await x.check_source('https://www.youtube.com/watch?v=aaaaaaaaaaa?extra=foo', 5)
    assert result.search_results[0].raw_search_string == 'https://www.youtube.com/watch?v=aaaaaaaaaaa'
    assert result.search_results[0].search_type == SearchType.YOUTUBE
    assert result.collection_name is None

@pytest.mark.asyncio(loop_scope="session")
async def test_basic_search():
    x = SearchClient(InMemoryMediaSearchClient())
    result = await x.check_source('foo bar', 5)
    assert result.search_results[0].raw_search_string == 'foo bar'
    assert result.search_results[0].search_type == SearchType.SEARCH
    assert result.collection_name is None


@pytest.mark.asyncio(loop_scope="session")
async def test_basic_search_with_youtube_music():
    # YouTube music search is now handled separately in the music queue
    x = SearchClient(InMemoryMediaSearchClient())
    result = await x.check_source('foo bar', 5)
    assert result.search_results[0].resolved_search_string == 'foo bar'
    assert result.search_results[0].search_type == SearchType.SEARCH
    assert result.search_results[0].raw_search_string == 'foo bar'
    assert result.collection_name is None

@pytest.mark.asyncio(loop_scope="session")
async def test_basic_search_with_youtube_music_skips_direct():
    x = SearchClient(InMemoryMediaSearchClient())
    result = await x.check_source('https://www.youtube.com/watch?v=aaaaaaaaaaa', 5)
    assert result.search_results[0].raw_search_string == 'https://www.youtube.com/watch?v=aaaaaaaaaaa'
    assert result.search_results[0].search_type == SearchType.YOUTUBE
    assert result.collection_name is None


# Tests for SearchResult class functionality
def test_search_result_creation():
    """Test SearchResult object creation"""
    result = SearchResult(search_type=SearchType.SEARCH, raw_search_string='test search')
    assert result.search_type == SearchType.SEARCH
    assert result.raw_search_string == 'test search'
    assert result.youtube_music_search_string is None
    assert result.proper_name is None


def test_search_collection_creation():
    """Test SearchCollection separates collection name from individual results"""
    result = SearchResult(search_type=SearchType.SEARCH, raw_search_string='track search')
    collection = SearchCollection(search_results=[result], collection_name='My Album')
    assert collection.collection_name == 'My Album'
    assert collection.search_results[0].search_type == SearchType.SEARCH
    assert collection.search_results[0].raw_search_string == 'track search'


def test_search_result_add_youtube_music_result():
    """Test adding YouTube music result to SearchResult"""
    result = SearchResult(search_type=SearchType.SEARCH, raw_search_string='foo bar')
    assert result.resolved_search_string == 'foo bar'

    result.add_youtube_music_result('https://www.youtube.com/watch?v=vid123')
    assert result.youtube_music_search_string == 'https://www.youtube.com/watch?v=vid123'
    assert result.resolved_search_string == 'https://www.youtube.com/watch?v=vid123'
    assert result.raw_search_string == 'foo bar'  # Original should remain unchanged


def test_search_result_resolved_search_string_fallback():
    """Test that resolved_search_string falls back to raw_search_string when no YouTube music result"""
    result = SearchResult(search_type=SearchType.DIRECT, raw_search_string='https://example.com')
    assert result.resolved_search_string == 'https://example.com'
    assert result.youtube_music_search_string is None


def test_search_result_resolved_search_string_with_youtube_music():
    """Test that resolved_search_string prefers YouTube music result when available"""
    result = SearchResult(search_type=SearchType.SEARCH, raw_search_string='original search')
    result.add_youtube_music_result('youtube result')
    assert result.resolved_search_string == 'youtube result'
    assert result.raw_search_string == 'original search'


@pytest.mark.asyncio
async def test_search_workflow_basic():
    """Test the complete search workflow for basic searches"""
    x = SearchClient(InMemoryMediaSearchClient())
    results = await x.check_source('basic search', 10)

    assert len(results.search_results) == 1
    assert results.search_results[0].search_type == SearchType.SEARCH
    assert results.search_results[0].raw_search_string == 'basic search'
    assert results.search_results[0].resolved_search_string == 'basic search'
    assert results.collection_name is None


@pytest.mark.asyncio
async def test_search_workflow_direct_url():
    """Test the search workflow for direct URLs"""
    x = SearchClient(InMemoryMediaSearchClient())
    results = await x.check_source('https://example.com', 10)

    assert len(results.search_results) == 1
    assert results.search_results[0].search_type == SearchType.DIRECT
    assert results.search_results[0].raw_search_string == 'https://example.com'
    assert results.search_results[0].resolved_search_string == 'https://example.com'


@pytest.mark.asyncio
async def test_search_workflow_with_youtube_music():
    """Test search workflow - YouTube Music integration now handled separately in music queue"""
    x = SearchClient(InMemoryMediaSearchClient())
    results = await x.check_source('search term', 5)

    assert len(results.search_results) == 1
    assert results.search_results[0].search_type == SearchType.SEARCH
    assert results.search_results[0].raw_search_string == 'search term'
    assert results.search_results[0].resolved_search_string == 'search term'
    assert results.search_results[0].youtube_music_search_string is None


@pytest.mark.asyncio
async def test_search_workflow_max_results_limit():
    """Test that max_results parameter properly limits results"""
    x = SearchClient(InMemoryMediaSearchClient(spotify_client=MockSpotifyClient()))

    # MockSpotifyClient returns only 1 result, so this tests the limit logic
    results = await x.check_source('https://open.spotify.com/album/1111', 2)
    assert len(results.search_results) == 1  # Can't exceed what Spotify returns

    # Test with limit of 0 (should return empty)
    results = await x.check_source('https://open.spotify.com/album/1111', 0)
    assert len(results.search_results) == 0

def test_check_youtube_video_youtube_short():
    """Test check_youtube_video with YouTube Short URL"""
    youtube_short = "https://youtube.com/shorts/dQw4w9WgXcQ"
    assert check_youtube_video(youtube_short) is not None


def test_check_youtube_video_youtube_video():
    """Test check_youtube_video with regular YouTube video URL"""
    youtube_video = "https://www.youtube.com/watch?v=dQw4w9WgXcQ"
    assert check_youtube_video(youtube_video) is not None


def test_check_youtube_video_non_youtube():
    """Test check_youtube_video with non-YouTube URL"""
    non_youtube = "https://example.com/video"
    assert check_youtube_video(non_youtube) is None


def test_check_youtube_video_plain_text():
    """Test check_youtube_video with plain text search"""
    plain_text = "some search query"
    assert check_youtube_video(plain_text) is None


def test_check_youtube_video_boolean_logic():
    """Test check_youtube_video return value boolean logic"""
    # Test that YouTube URLs return truthy values
    youtube_video = "https://www.youtube.com/watch?v=dQw4w9WgXcQ"
    youtube_short = "https://youtube.com/shorts/dQw4w9WgXcQ"

    # Should be truthy (regex match objects)
    assert bool(check_youtube_video(youtube_video))
    assert bool(check_youtube_video(youtube_short))

    # Non-YouTube should be falsy (None)
    plain_text = "some search query"
    non_youtube_url = "https://example.com/video"

    assert not bool(check_youtube_video(plain_text))
    assert not bool(check_youtube_video(non_youtube_url))


# Import existing mock from test_search_client.py to test OAuth error handling
class MockSpotifyOauth:
    """Mock Spotify client that raises SpotifyOauthError - matches existing test pattern"""

    def __init__(self):
        pass

    def album_get(self, _album_id):
        raise SpotifyOauthError(400, -1, 'foo exception')

    def playlist_get(self, _playlist_id):
        raise SpotifyOauthError(400, -1, 'foo exception')

    def track_get(self, _track_id):
        raise SpotifyOauthError(400, -1, 'foo exception')


@pytest.mark.asyncio(loop_scope="session")
async def test_spotify_oauth_error_handling():
    """Test that SpotifyOauthError is properly handled and converted to ThirdPartyException"""

    # Create SearchClient with mock that raises SpotifyOauthError
    client = SearchClient(InMemoryMediaSearchClient(spotify_client=MockSpotifyOauth()))

    # Test with Spotify playlist URL that will trigger OAuth error
    spotify_playlist_url = "https://open.spotify.com/playlist/37i9dQZEVXbNG2KDcFcKOF"

    with pytest.raises(ThirdPartyException) as exc_info:
        await client.check_source(spotify_playlist_url, max_results=5)

    # Verify the error message matches expected format
    assert "Issue fetching spotify info" in str(exc_info.value)
    assert exc_info.value.user_message == "Issue gathering info from spotify, credentials seem invalid"


# ---------------------------------------------------------------------------
# Regex correctness: anchoring and character classes
# ---------------------------------------------------------------------------

@pytest.mark.asyncio(loop_scope="session")
async def test_youtube_video_mid_string_is_search():
    """A YouTube URL embedded inside a sentence must not be treated as YOUTUBE."""
    x = SearchClient(InMemoryMediaSearchClient())
    result = await x.check_source('listen to https://www.youtube.com/watch?v=dQw4w9WgXcQ later', 5)
    assert result.search_results[0].search_type == SearchType.SEARCH


@pytest.mark.asyncio(loop_scope="session")
async def test_youtu_be_mid_string_is_search():
    """A youtu.be short URL embedded in text must not be treated as YOUTUBE."""
    x = SearchClient(InMemoryMediaSearchClient())
    result = await x.check_source('check out https://youtu.be/dQw4w9WgXcQ please', 5)
    assert result.search_results[0].search_type == SearchType.SEARCH


@pytest.mark.asyncio(loop_scope="session")
async def test_https_url_mid_string_is_search():
    """An https:// URL embedded in a sentence must not be treated as DIRECT."""
    x = SearchClient(InMemoryMediaSearchClient())
    result = await x.check_source('see https://soundcloud.com/foo for details', 5)
    assert result.search_results[0].search_type == SearchType.SEARCH


@pytest.mark.asyncio(loop_scope="session")
async def test_youtube_video_invalid_id_falls_through_to_direct():
    """A YouTube URL with an invalid video ID does not match YOUTUBE — it falls through to DIRECT
    because the URL still starts with https://, so yt-dlp gets to try it."""
    x = SearchClient(InMemoryMediaSearchClient())
    result = await x.check_source('https://www.youtube.com/watch?v=not valid!', 5)
    assert result.search_results[0].search_type == SearchType.DIRECT


def test_check_youtube_video_mid_string_returns_none():
    """check_youtube_video must not match when the URL is embedded inside a sentence."""
    assert check_youtube_video('listen to https://www.youtube.com/watch?v=dQw4w9WgXcQ later') is None
    assert check_youtube_video('check https://youtu.be/dQw4w9WgXcQ out') is None


def test_check_youtube_video_invalid_id_returns_none():
    """check_youtube_video must not match when the video ID contains invalid characters."""
    assert check_youtube_video('https://www.youtube.com/watch?v=not valid!') is None


@pytest.mark.asyncio(loop_scope="session")
async def test_youtube_playlist_regex_no_dot_wildcard():
    """youtube.com in playlist URL must not match arbitrary characters in place of the dot.
    Falls through to DIRECT (starts with https://) rather than matching as a YouTube playlist."""
    x = SearchClient(InMemoryMediaSearchClient())
    # 'youtubeXcom' — dot replaced by a non-dot character; must not match as a playlist
    result = await x.check_source('https://www.youtubeXcom/playlist?list=PLabc123', 5)
    assert result.search_results[0].search_type == SearchType.DIRECT


# The "no id given" guard moved to InMemoryMediaSearchClient with the provider
# call it guards; see tests/clients/test_media_search_client.py.


@pytest.mark.asyncio(loop_scope="session")
async def test_spotify_non_404_message_interpolates_the_search_string():
    '''
    The non-404 Spotify message quotes the URL the user typed.

    On main it did not: the message was a plain string with a literal
    "{search}" in it -- no f prefix -- so every non-404 Spotify failure told the
    user the problem was with a url called {search}. The existing test asserted
    only the prefix, which is why a one-character bug survived in user-facing
    copy. Assert the interpolation, not the prefix.
    '''
    x = SearchClient(InMemoryMediaSearchClient(spotify_client=MockSpotifyRaiseUnauth()))
    with pytest.raises(ThirdPartyException) as exc:
        await x.check_source('https://open.spotify.com/album/1111', 5)
    assert exc.value.user_message == 'Issue gathering info from spotify url "https://open.spotify.com/album/1111"'
    assert '{search}' not in exc.value.user_message


@pytest.mark.asyncio(loop_scope="session")
async def test_youtube_playlist_message_interpolates_the_search_string():
    '''The YouTube message quoted its url correctly already -- lock it in alongside.'''
    x = SearchClient(InMemoryMediaSearchClient(youtube_client=MockYoutubeRaise()))
    with pytest.raises(ThirdPartyException) as exc:
        await x.check_source('https://www.youtube.com/playlist?list=11111', 5)
    assert exc.value.user_message == 'Issue gathering info from youtube url "https://www.youtube.com/playlist?list=11111"'
