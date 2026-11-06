'''
HttpCacheCorruptMixin against a stub broker.

Same approach as test_http_gateway_intents: the stub serves the route from its registry, records
what the client sent and answers with a canned body. The real server arrives with the change that
moves the pods' pins.
'''
import aiohttp
import pytest

from discord_core.clients.http_client_base import SeamResponseInvalid
from discord_core.routes import cache_corrupt as corrupt_routes
from discord_core.routes import broker as broker_routes
from discord_core.types.media_request import MediaRequest
from discord_core.cogs.music_helpers.common import SearchType
from discord_core.types.search import SearchResult

from discord_core.tests.clients.stub_broker import StubBrokerBase, connected

KEY = 'cache/youtube.abc.sha256-0123456789abcdef0123456789abcdef.pcm'


class StubBroker(StubBrokerBase):
    '''A broker that serves the report-corrupt route and remembers every call.'''
    routes = corrupt_routes.ALL


def _request() -> MediaRequest:
    return MediaRequest(
        guild_id=1, channel_id=2, requester_name='tester', requester_id=3,
        search_result=SearchResult(search_type=SearchType.DIRECT, raw_search_string='https://youtu.be/abc'),
    )


def test_the_registry_is_one_route_outside_the_broker_seam_for_now():
    '''The route ships ahead of its server: the broker registry, and so the peer route check, leave it out.'''
    assert corrupt_routes.ALL == (corrupt_routes.REPORT_CORRUPT_CACHE_OBJECT,)
    assert corrupt_routes.REPORT_CORRUPT_CACHE_OBJECT not in broker_routes.ALL


@pytest.mark.asyncio
async def test_report_sends_the_key_and_the_request_and_returns_the_answer():
    stub = StubBroker()
    stub.answer(corrupt_routes.REPORT_CORRUPT_CACHE_OBJECT, {'evicted': True})
    request = _request()
    async with connected(stub) as client:
        assert await client.report_corrupt_cache_object(request, KEY) is True
    body = stub.last(corrupt_routes.REPORT_CORRUPT_CACHE_OBJECT)['body']
    assert body['key'] == KEY
    assert body['media_request'] == request.model_dump(mode='json')


@pytest.mark.asyncio
async def test_a_healthy_object_is_reported_as_not_evicted():
    stub = StubBroker()
    stub.answer(corrupt_routes.REPORT_CORRUPT_CACHE_OBJECT, {'evicted': False})
    async with connected(stub) as client:
        assert await client.report_corrupt_cache_object(_request(), KEY) is False


@pytest.mark.asyncio
async def test_an_empty_answer_is_not_evicted():
    stub = StubBroker()
    stub.answer(corrupt_routes.REPORT_CORRUPT_CACHE_OBJECT, None, status=200)
    async with connected(stub) as client:
        assert await client.report_corrupt_cache_object(_request(), KEY) is False


@pytest.mark.asyncio
async def test_a_malformed_answer_names_the_peer():
    stub = StubBroker()
    stub.answer(corrupt_routes.REPORT_CORRUPT_CACHE_OBJECT, {'evicted': 'maybe-not-a-bool'})
    async with connected(stub) as client:
        with pytest.raises(SeamResponseInvalid):
            await client.report_corrupt_cache_object(_request(), KEY)


@pytest.mark.asyncio
async def test_a_broker_without_the_route_fails_loudly():
    '''A 404 is not swallowed: the caller decides what a report that never arrived means.'''
    stub = StubBroker()
    stub.answer(corrupt_routes.REPORT_CORRUPT_CACHE_OBJECT, {'error': 'nope'}, status=404)
    async with connected(stub) as client:
        with pytest.raises(aiohttp.ClientResponseError):
            await client.report_corrupt_cache_object(_request(), KEY)
