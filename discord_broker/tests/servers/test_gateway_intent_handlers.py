'''Tests for the gateway-intent route handlers: bad requests and an unconfigured server.'''
import pytest
import pytest_asyncio
from aiohttp.test_utils import TestClient, TestServer

from discord_core.routes import gateway_intents as intent_routes

from tests.fakes.asyncio_broker import AsyncioBroker
from tests.fakes.asyncio_queues import make_broker_http_server


async def _client(server):
    client = TestClient(TestServer(server.build_app()))
    await client.start_server()
    return client


@pytest_asyncio.fixture(name='client')
async def client_fixture():
    client = await _client(make_broker_http_server(AsyncioBroker()))
    yield client
    await client.close()


def _path(route, **parts):
    path = route.template
    for name, value in parts.items():
        path = path.replace('{' + name + '}', value)
    return path


@pytest.mark.asyncio
async def test_a_server_with_no_registry_serves_none_of_the_routes():
    '''No GatewayIntentRegistry, no routes: the registry has them, the router does not.'''
    server = make_broker_http_server(AsyncioBroker(), gateway_intents=None)
    assert not server.gateway_intent_handlers()
    client = await _client(server)
    try:
        for route in intent_routes.ALL:
            resp = await client.request(route.method, _path(route, guild_id='5', gateway_id='g', intent_id='i'),
                                        json={})
            assert resp.status == 404, route
    finally:
        await client.close()


@pytest.mark.asyncio
async def test_every_gateway_intent_route_has_a_handler():
    server = make_broker_http_server(AsyncioBroker())
    assert set(server.gateway_intent_handlers()) == set(intent_routes.ALL)


@pytest.mark.asyncio
@pytest.mark.parametrize('body', [
    {},
    {'kind': 'explode'},
    {'kind': 'join', 'params': 'nope'},
    {'kind': 'join', 'params': [1]},
])
async def test_submit_rejects_a_bad_body(client, body):
    resp = await client.post(_path(intent_routes.SUBMIT_INTENT, guild_id='5'), json=body)
    assert resp.status == 422


@pytest.mark.asyncio
async def test_submit_rejects_a_guild_that_is_not_a_number(client):
    resp = await client.post(_path(intent_routes.SUBMIT_INTENT, guild_id='abc'), json={'kind': 'wake'})
    assert resp.status == 422


@pytest.mark.asyncio
async def test_submit_without_params_stores_an_empty_dict(client):
    resp = await client.post(_path(intent_routes.SUBMIT_INTENT, guild_id='5'), json={'kind': 'wake'})
    assert resp.status == 200
    claimed = await client.post(_path(intent_routes.CLAIM_INTENTS, gateway_id='g'))
    assert (await claimed.json())['intents'][0]['params'] == {}


@pytest.mark.asyncio
async def test_claiming_nothing_is_204_with_no_body(client):
    resp = await client.post(_path(intent_routes.CLAIM_INTENTS, gateway_id='g'))
    assert resp.status == 204
    assert await resp.read() == b''


@pytest.mark.asyncio
@pytest.mark.parametrize('body', [{}, {'error': 'x'}])
async def test_complete_rejects_a_body_without_ok(client, body):
    resp = await client.post(_path(intent_routes.COMPLETE_INTENT, intent_id='5-abc'), json=body)
    assert resp.status == 422


@pytest.mark.asyncio
async def test_completing_an_unknown_intent_is_a_200_that_says_so(client):
    resp = await client.post(_path(intent_routes.COMPLETE_INTENT, intent_id='5-gone'), json={'ok': True})
    assert resp.status == 200
    assert await resp.json() == {'recorded': False}


@pytest.mark.asyncio
async def test_the_result_of_an_unknown_intent_is_a_200_not_done(client):
    resp = await client.get(_path(intent_routes.GET_INTENT_RESULT, intent_id='5-gone'))
    assert resp.status == 200
    assert await resp.json() == {'done': False, 'ok': None, 'error': None}
