'''Tests for HttpMediaSearchClient — exercised against a real MediaSearchHttpServer.

Both halves go through aiohttp's TestServer + TestClient rather than a mocked
client, because the risk in this seam is the wire, not the logic: a serialisation
mismatch between the two sides passes every unit test and fails in prod. That is
how the HttpBrokerClient guild_path bug shipped.

The provider work behind the server is a small fake; InMemoryMediaSearchClient's
own translation of spotipy/googleapiclient failures is covered in
tests/clients/test_media_search_client.py.
'''
# NOTE: test names here deliberately avoid being exactly 40 characters long.
# trufflehog's Lob detector matches `test_` + 35 chars, so a 40-char test
# function name is reported as a VERIFIED secret and fails pr-check:secrets.
import pytest
from aiohttp.test_utils import TestClient, TestServer

from discord_bot.clients.http_media_search_client import HttpMediaSearchClient
from discord_bot.exceptions import MediaSearchError
from discord_bot.servers.media_search_server import MediaSearchHttpServer
from discord_bot.types.catalog import CatalogResponse, CatalogItem

from tests.cli._image_deps import measure


class _FakeProviders:
    '''MediaSearchClient stand-in that records calls and returns canned answers.'''

    def __init__(self, *, catalog=None, error=None):
        self.catalog = catalog or CatalogResponse(
            items=[CatalogItem(search_string='a track an artist', title='a track')],
            collection_name='A Collection')
        self.error = error
        self.spotify_calls = []
        self.youtube_calls = []

    async def spotify_source(self, playlist_id=None, album_id=None, track_id=None):
        self.spotify_calls.append({'playlist_id': playlist_id, 'album_id': album_id,
                                   'track_id': track_id})
        if self.error:
            raise self.error
        return self.catalog

    async def youtube_source(self, playlist_id):
        self.youtube_calls.append(playlist_id)
        if self.error:
            raise self.error
        return self.catalog


def _server(providers) -> MediaSearchHttpServer:
    return MediaSearchHttpServer(providers)


@pytest.mark.asyncio
@pytest.mark.parametrize('kwarg', ['playlist_id', 'album_id', 'track_id'])
async def test_spotify_round_trip_carries_the_id(kwarg):
    '''Each id kind reaches the server as itself, and only itself.'''
    providers = _FakeProviders()
    async with TestClient(TestServer(_server(providers).build_app())) as tc:
        client = HttpMediaSearchClient(str(tc.make_url('')), session=tc.session)
        result = await client.spotify_source(**{kwarg: 'the-id'})
    assert providers.spotify_calls == [{'playlist_id': None, 'album_id': None,
                                        'track_id': None, **{kwarg: 'the-id'}}]
    assert result.collection_name == 'A Collection'
    assert result.items[0].search_string == 'a track an artist'
    assert result.items[0].title == 'a track'


@pytest.mark.asyncio
async def test_youtube_round_trip_carries_the_id():
    providers = _FakeProviders()
    async with TestClient(TestServer(_server(providers).build_app())) as tc:
        client = HttpMediaSearchClient(str(tc.make_url('')), session=tc.session)
        result = await client.youtube_source('PL-the-id')
    assert providers.youtube_calls == ['PL-the-id']
    assert result.collection_name == 'A Collection'


@pytest.mark.asyncio
async def test_empty_catalog_survives_the_round_trip():
    '''An empty expansion is a real answer, not an error, and must not become one.'''
    providers = _FakeProviders(catalog=CatalogResponse(items=[], collection_name=None))
    async with TestClient(TestServer(_server(providers).build_app())) as tc:
        client = HttpMediaSearchClient(str(tc.make_url('')), session=tc.session)
        result = await client.spotify_source(album_id='abc')
    assert result.items == []
    assert result.collection_name is None


@pytest.mark.asyncio
@pytest.mark.parametrize('provider,reason,status', [
    (MediaSearchError.SPOTIFY, MediaSearchError.NOT_FOUND, 404),
    (MediaSearchError.SPOTIFY, MediaSearchError.API_ERROR, 403),
    (MediaSearchError.SPOTIFY, MediaSearchError.AUTH_ERROR, None),
    (MediaSearchError.SPOTIFY, MediaSearchError.MISSING_CREDENTIALS, None),
])
async def test_spotify_failure_rebuilds_the_error(provider, reason, status):
    '''
    Every field the cog branches on survives the wire.

    The cog picks its user-facing message off provider + reason, and quotes the
    Spotify 404 differently from every other status, so a field lost in transit
    silently degrades user-visible copy rather than failing anything.
    '''
    providers = _FakeProviders(error=MediaSearchError(provider, reason, 'boom',
                                                      http_status=status))
    async with TestClient(TestServer(_server(providers).build_app())) as tc:
        client = HttpMediaSearchClient(str(tc.make_url('')), session=tc.session)
        with pytest.raises(MediaSearchError) as exc:
            await client.spotify_source(album_id='abc')
    assert exc.value.provider == provider
    assert exc.value.reason == reason
    assert exc.value.http_status == status


@pytest.mark.asyncio
async def test_youtube_failure_rebuilds_the_error():
    providers = _FakeProviders(error=MediaSearchError(
        MediaSearchError.YOUTUBE, MediaSearchError.MISSING_CREDENTIALS, 'boom'))
    async with TestClient(TestServer(_server(providers).build_app())) as tc:
        client = HttpMediaSearchClient(str(tc.make_url('')), session=tc.session)
        with pytest.raises(MediaSearchError) as exc:
            await client.youtube_source('PL1')
    assert exc.value.provider == MediaSearchError.YOUTUBE
    assert exc.value.reason == MediaSearchError.MISSING_CREDENTIALS


@pytest.mark.asyncio
async def test_provider_failure_is_not_an_http_failure():
    '''
    A provider saying no comes back 200, so the retry wrapper does not re-run it.

    Asserted at the transport rather than through the client: the point is the
    status code on the wire, and the client deliberately hides it.
    '''
    providers = _FakeProviders(error=MediaSearchError(
        MediaSearchError.SPOTIFY, MediaSearchError.NOT_FOUND, 'boom', http_status=404))
    async with TestClient(TestServer(_server(providers).build_app())) as tc:
        resp = await tc.post('/search/spotify', json={'album_id': 'abc'})
        assert resp.status == 200
        body = await resp.json()
    assert body['catalog'] is None
    assert body['error']['reason'] == MediaSearchError.NOT_FOUND
    assert body['error']['http_status'] == 404


@pytest.mark.asyncio
async def test_spotify_rejects_more_than_one_id():
    '''Two ids is a caller bug, not a precedence question for the pod to guess.'''
    providers = _FakeProviders()
    async with TestClient(TestServer(_server(providers).build_app())) as tc:
        resp = await tc.post('/search/spotify', json={'album_id': 'a', 'track_id': 'b'})
    assert resp.status == 422
    assert not providers.spotify_calls


@pytest.mark.asyncio
@pytest.mark.parametrize('route,body', [
    ('/search/spotify', {}),
    ('/search/youtube', {}),
    ('/search/youtube', {'playlist_id': ''}),
])
async def test_missing_ids_are_rejected(route, body):
    providers = _FakeProviders()
    async with TestClient(TestServer(_server(providers).build_app())) as tc:
        resp = await tc.post(route, json=body)
    assert resp.status == 422


@pytest.mark.asyncio
async def test_malformed_json_is_rejected():
    providers = _FakeProviders()
    async with TestClient(TestServer(_server(providers).build_app())) as tc:
        resp = await tc.post('/search/spotify', data='not json',
                             headers={'Content-Type': 'application/json'})
    assert resp.status == 422


@pytest.mark.asyncio
async def test_client_rejects_no_id_without_a_round_trip():
    '''The no-id guard answers locally; the in-process client raises the same.'''
    providers = _FakeProviders()
    async with TestClient(TestServer(_server(providers).build_app())) as tc:
        client = HttpMediaSearchClient(str(tc.make_url('')), session=tc.session)
        with pytest.raises(ValueError, match='Playlist, album, or track id must be passed'):
            await client.spotify_source()
    assert not providers.spotify_calls


@pytest.mark.asyncio
async def test_draining_server_refuses_new_requests():
    '''The shared drain middleware is wired here too, not just on queue servers.'''
    providers = _FakeProviders()
    server = _server(providers)
    async with TestClient(TestServer(server.build_app())) as tc:
        server.start_draining()
        resp = await tc.post('/search/youtube', json={'playlist_id': 'PL1'})
    assert resp.status == 503
    assert not providers.youtube_calls


def test_http_client_pulls_in_no_provider_sdk():
    '''
    Importing this module must not pull spotipy or googleapiclient into a process.

    This is the reason the class lives here rather than beside
    InMemoryMediaSearchClient, which imports both at module scope to catch their
    exceptions. Measured in a clean interpreter, the same way the per-image
    boundaries are, because an import chain is not something to verify by reading:
    the ytmusicapi leak that CrashLooped a pod was invisible in the source too.
    '''
    imported = set(measure('discord_bot.clients.http_media_search_client')['packages'])
    assert 'spotipy' not in imported
    assert 'googleapiclient' not in imported


def test_wire_types_pull_in_no_provider_sdk():
    '''Same guarantee for the types both sides serialise through.'''
    imported = set(measure('discord_bot.types.media_search')['packages'])
    assert 'spotipy' not in imported
    assert 'googleapiclient' not in imported
