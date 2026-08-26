'''
InMemoryMediaSearchClient — the provider seam the cog now expands sources through.

These cover the half that used to be private methods on SearchClient plus the
exception translation that used to be inline in the cog: every provider failure
becomes a MediaSearchError carrying (provider, reason, http_status), and no
spotipy or googleapiclient type escapes the client. That last part is what lets
the two heavy providers leave the bot image later, so it is asserted directly.
'''
from googleapiclient.errors import HttpError
import pytest
from spotipy.exceptions import SpotifyException, SpotifyOauthError

from discord_bot.clients.media_search_client import (
    InMemoryMediaSearchClient, build_media_search_client,
)
from discord_bot.exceptions import MediaSearchError
from discord_bot.types.catalog import CatalogResponse, CatalogItem


class MockSpotify():
    def __init__(self, exc=None):
        self.exc = exc
        self.calls = []

    def _maybe_raise(self):
        if self.exc:
            raise self.exc

    def playlist_get(self, playlist_id):
        self.calls.append(('playlist', playlist_id))
        self._maybe_raise()
        return CatalogResponse(items=[CatalogItem(search_string='a b')], collection_name='PL')

    def album_get(self, album_id):
        self.calls.append(('album', album_id))
        self._maybe_raise()
        return CatalogResponse(items=[CatalogItem(search_string='a b')], collection_name='AL')

    def track_get(self, track_id):
        self.calls.append(('track', track_id))
        self._maybe_raise()
        return CatalogResponse(items=[CatalogItem(search_string='a b')])


class MockResponse():
    def __init__(self):
        self.reason = 'cats unplugged servers'


class MockYoutube():
    def __init__(self, exc=None):
        self.exc = exc
        self.calls = []

    def playlist_get(self, playlist_id):
        self.calls.append(playlist_id)
        if self.exc:
            raise self.exc
        return CatalogResponse(items=[CatalogItem(search_string='v')], collection_name='YT')


@pytest.mark.asyncio
async def test_spotify_source_requires_an_id():
    with pytest.raises(ValueError, match='Playlist, album, or track id must be passed'):
        await InMemoryMediaSearchClient(spotify_client=MockSpotify()).spotify_source()


@pytest.mark.asyncio
@pytest.mark.parametrize('kwarg,expected_call', [
    ('playlist_id', 'playlist'),
    ('album_id', 'album'),
    ('track_id', 'track'),
])
async def test_spotify_source_routes_each_id_to_its_call(kwarg, expected_call):
    spotify = MockSpotify()
    result = await InMemoryMediaSearchClient(spotify_client=spotify).spotify_source(**{kwarg: 'abc'})
    assert spotify.calls == [(expected_call, 'abc')]
    assert result.items[0].search_string == 'a b'


@pytest.mark.asyncio
async def test_spotify_source_without_credentials():
    with pytest.raises(MediaSearchError) as exc:
        await InMemoryMediaSearchClient().spotify_source(playlist_id='abc')
    assert exc.value.provider == MediaSearchError.SPOTIFY
    assert exc.value.reason == MediaSearchError.MISSING_CREDENTIALS


@pytest.mark.asyncio
async def test_spotify_source_404_is_not_found():
    client = InMemoryMediaSearchClient(spotify_client=MockSpotify(SpotifyException(404, -1, 'nope')))
    with pytest.raises(MediaSearchError) as exc:
        await client.spotify_source(album_id='abc')
    assert exc.value.reason == MediaSearchError.NOT_FOUND
    assert exc.value.http_status == 404


@pytest.mark.asyncio
async def test_spotify_source_other_status_is_api_error():
    client = InMemoryMediaSearchClient(spotify_client=MockSpotify(SpotifyException(403, -1, 'nope')))
    with pytest.raises(MediaSearchError) as exc:
        await client.spotify_source(album_id='abc')
    assert exc.value.reason == MediaSearchError.API_ERROR
    assert exc.value.http_status == 403


@pytest.mark.asyncio
async def test_spotify_source_oauth_is_auth_error():
    client = InMemoryMediaSearchClient(spotify_client=MockSpotify(SpotifyOauthError('bad creds')))
    with pytest.raises(MediaSearchError) as exc:
        await client.spotify_source(album_id='abc')
    assert exc.value.reason == MediaSearchError.AUTH_ERROR
    assert exc.value.http_status is None


@pytest.mark.asyncio
async def test_youtube_source_returns_catalog():
    youtube = MockYoutube()
    result = await InMemoryMediaSearchClient(youtube_client=youtube).youtube_source('PL1')
    assert youtube.calls == ['PL1']
    assert result.collection_name == 'YT'


@pytest.mark.asyncio
async def test_youtube_source_without_credentials():
    with pytest.raises(MediaSearchError) as exc:
        await InMemoryMediaSearchClient().youtube_source('PL1')
    assert exc.value.provider == MediaSearchError.YOUTUBE
    assert exc.value.reason == MediaSearchError.MISSING_CREDENTIALS


@pytest.mark.asyncio
async def test_youtube_source_http_error_is_api_error():
    client = InMemoryMediaSearchClient(youtube_client=MockYoutube(HttpError(MockResponse(), b'foo')))
    with pytest.raises(MediaSearchError) as exc:
        await client.youtube_source('PL1')
    assert exc.value.provider == MediaSearchError.YOUTUBE
    assert exc.value.reason == MediaSearchError.API_ERROR


@pytest.mark.asyncio
async def test_no_provider_sdk_exception_escapes_the_client():
    '''
    The seam absorbs spotipy and googleapiclient rather than passing them through.

    This is the property that lets those two packages leave the bot image at the
    cutover: if a provider exception escaped here, whatever caught it upstream
    would have to import the SDK to name the type.
    '''
    for client, call in (
        (InMemoryMediaSearchClient(spotify_client=MockSpotify(SpotifyException(500, -1, 'x'))),
         lambda c: c.spotify_source(album_id='a')),
        (InMemoryMediaSearchClient(youtube_client=MockYoutube(HttpError(MockResponse(), b'x'))),
         lambda c: c.youtube_source('PL1')),
    ):
        with pytest.raises(MediaSearchError):
            await call(client)


def test_factory_builds_both_providers(mocker):
    '''Credentials present -> both provider clients constructed and injected.'''
    spotify = mocker.patch('discord_bot.clients.media_search_client.SpotifyClient')
    youtube = mocker.patch('discord_bot.clients.media_search_client.YoutubeClient')
    client = build_media_search_client(spotify_client_id='cid',
                                       spotify_client_secret='secret',
                                       youtube_api_key='ytkey')
    spotify.assert_called_once_with('cid', 'secret')
    youtube.assert_called_once_with('ytkey')
    assert client.spotify_client is spotify.return_value
    assert client.youtube_client is youtube.return_value


@pytest.mark.parametrize('kwargs', [
    {},
    {'spotify_client_id': 'cid'},
    {'spotify_client_secret': 'secret'},
])
def test_factory_needs_both_spotify_halves(kwargs, mocker):
    '''
    Half a Spotify credential is no credential.

    Constructing a SpotifyClient from an id with no secret would fail later, at
    the first request, as an auth error the user sees -- rather than here, as an
    absent provider whose route answers MISSING_CREDENTIALS.
    '''
    spotify = mocker.patch('discord_bot.clients.media_search_client.SpotifyClient')
    client = build_media_search_client(**kwargs)
    spotify.assert_not_called()
    assert client.spotify_client is None


def test_factory_without_a_youtube_key(mocker):
    '''No key -> no client, and the route answers MISSING_CREDENTIALS instead.'''
    youtube = mocker.patch('discord_bot.clients.media_search_client.YoutubeClient')
    client = build_media_search_client(spotify_client_id='cid', spotify_client_secret='s')
    youtube.assert_not_called()
    assert client.youtube_client is None
