'''HttpMediaSearchClient, tested against the media-search contract and nothing else.

The client is served by a `ContractStub` built from `testing/contracts/media_search.py`, the same description
the search pod's real server is proved against. So this checks the client's half of the seam: the bodies it
sends are ones the contract accepts (the stub records a violation, and the test fails, if not), and it parses
every reply the contract declares. It does not need the search pod, and that is the point.

That the pod does the right thing with a body it accepts is the pod's own tests
(discord_search/tests/servers/test_media_search_server.py) plus the round trips in
tests/integration/commands_search/test_media_search_round_trip.py.
'''
# NOTE: test names here deliberately avoid being exactly 40 characters long.
# trufflehog's Lob detector matches `test_` + 35 chars, so a 40-char test
# function name is reported as a VERIFIED secret and fails pr-check:secrets.
import pytest

from discord_core.exceptions import MediaSearchError
from discord_core.routes import media_search as routes
from discord_core.types.catalog import CatalogItem, CatalogResponse
from discord_core.types.media_search import MediaSearchErrorBody, MediaSearchResponse

from discord_commands.clients.http_media_search_client import HttpMediaSearchClient

from ci._image_deps import measure
from testing.contracts.media_search import CONTRACTS
from testing.contracts.stub import ContractStub, serving

CATALOG = CatalogResponse(items=[CatalogItem(search_string='a track an artist', title='a track')],
                          collection_name='A Collection')


def _client(tc) -> HttpMediaSearchClient:
    return HttpMediaSearchClient(str(tc.make_url('')), session=tc.session)


def _failure(provider, reason, status=None) -> MediaSearchResponse:
    return MediaSearchResponse(error=MediaSearchErrorBody.from_exception(
        MediaSearchError(provider, reason, 'boom', http_status=status)))


@pytest.mark.asyncio
@pytest.mark.parametrize('kwarg', ['playlist_id', 'album_id', 'track_id'])
async def test_spotify_sends_only_the_id_it_was_given(kwarg):
    '''Each id kind goes out as itself, alone, and the declared catalog comes back whole.'''
    stub = ContractStub(CONTRACTS)
    stub.script(routes.SPOTIFY, MediaSearchResponse(catalog=CATALOG))
    async with serving(stub) as tc:
        result = await _client(tc).spotify_source(**{kwarg: 'the-id'})
    assert stub.calls_to(routes.SPOTIFY) == [{kwarg: 'the-id'}]
    assert result == CATALOG


@pytest.mark.asyncio
async def test_youtube_sends_the_playlist_id():
    '''The playlist id goes out under the name the contract gives it.'''
    stub = ContractStub(CONTRACTS)
    stub.script(routes.YOUTUBE, MediaSearchResponse(catalog=CATALOG))
    async with serving(stub) as tc:
        result = await _client(tc).youtube_source('PL-the-id')
    assert stub.calls_to(routes.YOUTUBE) == [{'playlist_id': 'PL-the-id'}]
    assert result == CATALOG


@pytest.mark.asyncio
async def test_empty_catalog_is_an_answer_not_an_error():
    '''An empty expansion is a real reply, and must not become a failure.'''
    stub = ContractStub(CONTRACTS)
    stub.script(routes.SPOTIFY, MediaSearchResponse(catalog=CatalogResponse(items=[], collection_name=None)))
    async with serving(stub) as tc:
        result = await _client(tc).spotify_source(album_id='abc')
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
    A declared error reply becomes the MediaSearchError the in-process client would have raised.

    The cog picks its user-facing message off provider + reason, and quotes the Spotify 404 differently from
    every other status, so a field lost in the rebuild silently degrades user-visible copy rather than failing
    anything.
    '''
    stub = ContractStub(CONTRACTS)
    stub.script(routes.SPOTIFY, _failure(provider, reason, status))
    async with serving(stub) as tc:
        with pytest.raises(MediaSearchError) as exc:
            await _client(tc).spotify_source(album_id='abc')
    assert (exc.value.provider, exc.value.reason, exc.value.http_status) == (provider, reason, status)


@pytest.mark.asyncio
async def test_youtube_failure_rebuilds_the_error():
    '''The same rebuild on the other route.'''
    stub = ContractStub(CONTRACTS)
    stub.script(routes.YOUTUBE, _failure(MediaSearchError.YOUTUBE, MediaSearchError.MISSING_CREDENTIALS))
    async with serving(stub) as tc:
        with pytest.raises(MediaSearchError) as exc:
            await _client(tc).youtube_source('PL1')
    assert exc.value.provider == MediaSearchError.YOUTUBE
    assert exc.value.reason == MediaSearchError.MISSING_CREDENTIALS


@pytest.mark.asyncio
async def test_client_rejects_no_id_without_a_round_trip():
    '''The no-id guard answers locally; the in-process client raises the same.'''
    stub = ContractStub(CONTRACTS)
    async with serving(stub) as tc:
        with pytest.raises(ValueError, match='Playlist, album, or track id must be passed'):
            await _client(tc).spotify_source()
    assert not stub.calls


@pytest.mark.asyncio
async def test_the_stub_refuses_a_reply_the_contract_does_not_declare():
    '''The guard the other tests lean on: a test cannot make the pod say something it never says.'''
    stub = ContractStub(CONTRACTS)
    with pytest.raises(AssertionError, match='declares no 404 reply'):
        stub.script(routes.SPOTIFY, status=404)
    with pytest.raises(AssertionError, match='is none of'):
        stub.script(routes.SPOTIFY, {'unexpected': True})


@pytest.mark.asyncio
async def test_the_stub_flags_a_body_the_contract_refuses():
    '''...and a client that sends one fails the test even if it ignores the answer.'''
    stub = ContractStub(CONTRACTS)
    stub.script(routes.SPOTIFY, MediaSearchResponse(catalog=CATALOG))
    with pytest.raises(AssertionError, match='broke the contract'):
        async with serving(stub) as tc:
            await tc.post('/search/spotify', json={'album_id': 'a', 'track_id': 'b'})


def test_http_client_pulls_in_no_provider_sdk():
    '''
    Importing this module must not pull spotipy or googleapiclient into a process.

    This is the reason the class lives here rather than beside LocalMediaSearchClient, which imports both at
    module scope to catch their exceptions. Measured in a clean interpreter, the same way the per-image
    boundaries are, because an import chain is not something to verify by reading: the ytmusicapi leak that
    CrashLooped a pod was invisible in the source too.
    '''
    imported = set(measure('discord_commands.clients.http_media_search_client')['packages'])
    assert 'spotipy' not in imported
    assert 'googleapiclient' not in imported
