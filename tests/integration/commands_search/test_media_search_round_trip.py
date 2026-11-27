'''The media-search seam end to end: the commands pod's real client against the search pod's real server.

Deliberately thin. Each pod is tested against the seam's contract on its own: the client against a stub that
serves the contract (discord_commands/tests/clients/test_http_media_search_client.py), the server over raw
HTTP (discord_search/tests/servers/test_media_search_server.py), and the handlers against every declared reply
(tests/routes/). What none of those can show is that a value means the same thing on both sides of the wire,
since each only looks at its own end: that the id a caller passes is the id the provider receives, and that an
error survives the crossing field for field. One round trip each is enough to pin that.
'''
# NOTE: test names here deliberately avoid being exactly 40 characters long.
# trufflehog's Lob detector matches `test_` + 35 chars, so a 40-char test
# function name is reported as a VERIFIED secret and fails pr-check:secrets.
import pytest
from aiohttp.test_utils import TestClient, TestServer

from discord_core.exceptions import MediaSearchError

from discord_commands.clients.http_media_search_client import HttpMediaSearchClient
from discord_search.servers.media_search_server import MediaSearchHttpServer

from testing.fakes.media_search_providers import FakeProviders


@pytest.mark.asyncio
@pytest.mark.parametrize('kwarg', ['playlist_id', 'album_id', 'track_id'])
async def test_spotify_id_arrives_as_the_same_kind_of_id(kwarg):
    '''The id a caller passes reaches the provider under the same name, and the catalog comes back whole.'''
    providers = FakeProviders()
    async with TestClient(TestServer(MediaSearchHttpServer(providers).build_app())) as tc:
        result = await HttpMediaSearchClient(str(tc.make_url('')), session=tc.session).spotify_source(
            **{kwarg: 'the-id'})
    assert providers.spotify_calls == [{'playlist_id': None, 'album_id': None, 'track_id': None,
                                        **{kwarg: 'the-id'}}]
    assert result == providers.catalog


@pytest.mark.asyncio
async def test_youtube_playlist_id_arrives_intact():
    '''Same on the other route.'''
    providers = FakeProviders()
    async with TestClient(TestServer(MediaSearchHttpServer(providers).build_app())) as tc:
        result = await HttpMediaSearchClient(str(tc.make_url('')), session=tc.session).youtube_source('PL-the-id')
    assert providers.youtube_calls == ['PL-the-id']
    assert result == providers.catalog


@pytest.mark.asyncio
async def test_provider_error_survives_the_crossing_field_for_field():
    '''Raised by the provider, serialised by the pod, rebuilt by the client: provider, reason and status intact.'''
    providers = FakeProviders(error=MediaSearchError(
        MediaSearchError.SPOTIFY, MediaSearchError.NOT_FOUND, 'boom', http_status=404))
    async with TestClient(TestServer(MediaSearchHttpServer(providers).build_app())) as tc:
        with pytest.raises(MediaSearchError) as exc:
            await HttpMediaSearchClient(str(tc.make_url('')), session=tc.session).spotify_source(album_id='abc')
    assert (exc.value.provider, exc.value.reason, exc.value.http_status) == (
        MediaSearchError.SPOTIFY, MediaSearchError.NOT_FOUND, 404)
