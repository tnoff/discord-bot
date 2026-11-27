'''MediaSearchHttpServer, tested over HTTP with no client: raw requests in, raw answers out.

The wire shapes it must honour are the contract in `testing/contracts/media_search.py`; the same table drives
the commands pod's client tests, so this is the server's half of the seam. tests/routes/ also runs every
declared reply of this surface against the real handler; what is here is the behaviour around that: which
request bodies are accepted and which are refused, what reaches the provider, and how a provider failure is
reported.
'''
# NOTE: test names here deliberately avoid being exactly 40 characters long.
# trufflehog's Lob detector matches `test_` + 35 chars, so a 40-char test
# function name is reported as a VERIFIED secret and fails pr-check:secrets.
import pytest
from aiohttp.test_utils import TestClient, TestServer
from pydantic import ValidationError

from discord_core.exceptions import MediaSearchError
from discord_core.routes import media_search as routes
from discord_core.types.catalog import CatalogResponse
from discord_core.types.media_search import MediaSearchResponse

from discord_search.servers.media_search_server import MediaSearchHttpServer

from testing.contracts.media_search import CONTRACTS
from testing.fakes.media_search_providers import FakeProviders


def _served(providers):
    server = MediaSearchHttpServer(providers)
    return server, TestClient(TestServer(server.build_app()))


@pytest.mark.asyncio
@pytest.mark.parametrize('kwarg', ['playlist_id', 'album_id', 'track_id'])
async def test_spotify_hands_the_provider_that_id_and_only_that_id(kwarg):
    '''Each id kind reaches the provider as itself, and the answer is the catalog, serialised.'''
    providers = FakeProviders()
    _, client = _served(providers)
    async with client as tc:
        resp = await tc.post(routes.SPOTIFY.template, json={kwarg: 'the-id'})
        body = await resp.json()
    assert resp.status == 200
    assert providers.spotify_calls == [{'playlist_id': None, 'album_id': None, 'track_id': None,
                                        **{kwarg: 'the-id'}}]
    assert body == MediaSearchResponse(catalog=providers.catalog).model_dump(mode='json')


@pytest.mark.asyncio
async def test_youtube_hands_the_provider_the_playlist_id():
    '''The playlist id reaches the provider and the catalog comes back.'''
    providers = FakeProviders()
    _, client = _served(providers)
    async with client as tc:
        resp = await tc.post(routes.YOUTUBE.template, json={'playlist_id': 'PL-the-id'})
        body = await resp.json()
    assert resp.status == 200
    assert providers.youtube_calls == ['PL-the-id']
    assert body['catalog']['collection_name'] == 'A Collection'


@pytest.mark.asyncio
async def test_empty_catalog_is_served_as_a_catalog():
    '''An empty expansion is an answer: a catalog with no items, and no error.'''
    providers = FakeProviders(catalog=CatalogResponse(items=[], collection_name=None))
    _, client = _served(providers)
    async with client as tc:
        body = await (await tc.post(routes.SPOTIFY.template, json={'album_id': 'abc'})).json()
    assert body == {'catalog': {'items': [], 'collection_name': None}, 'error': None}


@pytest.mark.asyncio
@pytest.mark.parametrize('route,body', [
    (routes.SPOTIFY, {'album_id': 'abc'}),
    (routes.YOUTUBE, {'playlist_id': 'abc'}),
])
@pytest.mark.parametrize('provider,reason,status', [
    (MediaSearchError.SPOTIFY, MediaSearchError.NOT_FOUND, 404),
    (MediaSearchError.SPOTIFY, MediaSearchError.API_ERROR, 403),
    (MediaSearchError.YOUTUBE, MediaSearchError.AUTH_ERROR, None),
    (MediaSearchError.YOUTUBE, MediaSearchError.MISSING_CREDENTIALS, None),
])
async def test_provider_failure_is_a_200_that_carries_every_field(route, body, provider, reason, status):
    '''
    A provider saying no comes back 200, so the client's retry wrapper does not re-run it, and the error body
    keeps provider, reason and the provider's own status: the cog branches on all three.
    '''
    providers = FakeProviders(error=MediaSearchError(provider, reason, 'boom', http_status=status))
    _, client = _served(providers)
    async with client as tc:
        resp = await tc.post(route.template, json=body)
        payload = await resp.json()
    assert resp.status == 200
    assert payload['catalog'] is None
    assert payload['error'] == {'provider': provider, 'reason': reason, 'http_status': status}


@pytest.mark.asyncio
@pytest.mark.parametrize('route,body', [
    (routes.SPOTIFY, {}),
    (routes.SPOTIFY, {'album_id': 'a', 'track_id': 'b'}),
    (routes.SPOTIFY, {'playlist_id': 'a', 'album_id': 'b', 'track_id': 'c'}),
    (routes.SPOTIFY, {'playlist_id': None}),
    (routes.YOUTUBE, {}),
    (routes.YOUTUBE, {'playlist_id': ''}),
    (routes.YOUTUBE, {'playlist_id': None}),
])
async def test_a_body_that_names_no_single_id_is_refused(route, body):
    '''Two ids is a caller bug, not a precedence question for the pod to guess; none is not a request.'''
    providers = FakeProviders()
    _, client = _served(providers)
    async with client as tc:
        resp = await tc.post(route.template, json=body)
    assert resp.status == 422
    assert not providers.spotify_calls
    assert not providers.youtube_calls


@pytest.mark.asyncio
@pytest.mark.parametrize('route', [routes.SPOTIFY, routes.YOUTUBE])
async def test_malformed_json_is_refused(route):
    '''A body that is not JSON never reaches a provider.'''
    providers = FakeProviders()
    _, client = _served(providers)
    async with client as tc:
        resp = await tc.post(route.template, data='not json', headers={'Content-Type': 'application/json'})
    assert resp.status == 422
    assert not providers.spotify_calls
    assert not providers.youtube_calls


@pytest.mark.asyncio
@pytest.mark.parametrize('route,body', [
    (routes.SPOTIFY, {}),
    (routes.SPOTIFY, {'playlist_id': 'a'}),
    (routes.SPOTIFY, {'album_id': 'a', 'track_id': 'b'}),
    (routes.SPOTIFY, {'playlist_id': 'a', 'album_id': 'b', 'track_id': 'c'}),
    (routes.YOUTUBE, {}),
    (routes.YOUTUBE, {'playlist_id': 'a'}),
    (routes.YOUTUBE, {'playlist_id': 'a', 'extra': 1}),
    (routes.YOUTUBE, {'playlist_id': ''}),
    (routes.SPOTIFY, {'playlist_id': ''}),
])
async def test_the_server_accepts_exactly_what_the_contract_accepts(route, body):
    '''The request model the commands pod's client is held to is the one this server enforces.'''
    model = CONTRACTS[route].request
    try:
        model.model_validate(body)
        accepted = True
    except ValidationError:
        accepted = False
    _, client = _served(FakeProviders())
    async with client as tc:
        resp = await tc.post(route.template, json=body)
    assert (resp.status == 200) is accepted


@pytest.mark.asyncio
async def test_draining_server_refuses_new_requests():
    '''The shared drain middleware is wired here too, not just on queue servers.'''
    providers = FakeProviders()
    server, client = _served(providers)
    async with client as tc:
        server.start_draining()
        resp = await tc.post(routes.YOUTUBE.template, json={'playlist_id': 'PL1'})
    assert resp.status == 503
    assert not providers.youtube_calls
