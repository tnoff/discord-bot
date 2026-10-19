'''
HttpGatewayIntentsMixin against a stub broker.

Same approach as test_http_guild_queue: the stub registers a handler for every gateway-intent
route from the shared registry, records what the client sent, and answers with canned bodies. That
checks both directions of the contract. The real server is exercised in discord_broker's tests.
'''
import ast
import contextlib
import inspect

import aiohttp
import pytest
from aiohttp import web
from aiohttp.test_utils import TestServer

from discord_core.clients import http_gateway_intents
from discord_core.clients.http_broker_client import HttpBrokerClient
from discord_core.clients.http_client_base import SeamResponseInvalid
from discord_core.interfaces.gateway_intent_client import GatewayIntentClient
from discord_core.routes import gateway_intents as intent_routes
from discord_core.types.gateway_intents import GatewayIntent, IntentKind, IntentResult

GUILD = 42
GATEWAY = 'gw-1'

INTENT_ROUTES = intent_routes.ALL


class StubBroker:
    '''A broker that serves the gateway-intent routes and remembers every call.'''

    def __init__(self):
        self.calls = []
        self.answers = {}

    def answer(self, route, body=None, status=200):
        '''Set what a route answers. body None sends an empty response.'''
        self.answers[route] = (status, body)

    def _handler(self, route):
        async def handle(request: web.Request) -> web.StreamResponse:
            body = await request.json() if request.can_read_body else None
            self.calls.append({'route': route, 'match': dict(request.match_info), 'body': body})
            status, answer = self.answers[route]
            if answer is None:
                return web.Response(status=status)
            return web.json_response(answer, status=status)
        return handle

    def build_app(self) -> web.Application:
        app = web.Application()
        for route in INTENT_ROUTES:
            app.router.add_route(route.method, route.template, self._handler(route))
        return app

    def last(self, route):
        return [call for call in self.calls if call['route'] == route][-1]


@contextlib.asynccontextmanager
async def _connected(stub: StubBroker):
    async with TestServer(stub.build_app()) as server:
        async with aiohttp.ClientSession() as session:
            yield HttpBrokerClient(str(server.make_url('')), session=session)


# ---------------------------------------------------------------------------
# shape of the contract
# ---------------------------------------------------------------------------

def test_the_mixin_implements_the_whole_protocol():
    '''Every GatewayIntentClient method exists on HttpBrokerClient with the same parameters.'''
    for name, member in inspect.getmembers(GatewayIntentClient, inspect.isfunction):
        if name.startswith('_'):
            continue
        implemented = getattr(HttpBrokerClient, name)
        assert list(inspect.signature(implemented).parameters) == list(inspect.signature(member).parameters), name


def test_the_mixin_calls_every_route_in_the_registry():
    '''Every route declared in routes/gateway_intents.py is referenced by the mixin's source.'''
    tree = ast.parse(inspect.getsource(http_gateway_intents))
    referenced = {
        node.attr
        for node in ast.walk(tree)
        if isinstance(node, ast.Attribute)
        and isinstance(node.value, ast.Name) and node.value.id == 'intent_routes'
    }
    declared = {name for name, value in vars(intent_routes).items() if value in INTENT_ROUTES}
    assert referenced == declared


def test_the_registry_is_well_formed():
    '''Four distinct routes: one per guild, one per gateway, two per intent.'''
    assert len(INTENT_ROUTES) == 4
    assert len(set(INTENT_ROUTES)) == 4


# ---------------------------------------------------------------------------
# submit
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_submit_sends_the_kind_and_params():
    stub = StubBroker()
    stub.answer(intent_routes.SUBMIT_INTENT, {'intent_id': 'i-1'})
    async with _connected(stub) as client:
        intent_id = await client.submit_intent(GUILD, IntentKind.JOIN,
                                               {'voice_channel_id': 5, 'text_channel_id': 6})
    assert intent_id == 'i-1'
    call = stub.last(intent_routes.SUBMIT_INTENT)
    assert call['match'] == {'guild_id': str(GUILD)}
    assert call['body'] == {'kind': 'join', 'params': {'voice_channel_id': 5, 'text_channel_id': 6}}


@pytest.mark.asyncio
async def test_submit_without_params_sends_an_empty_dict():
    stub = StubBroker()
    stub.answer(intent_routes.SUBMIT_INTENT, {'intent_id': 'i-2'})
    async with _connected(stub) as client:
        await client.submit_intent(GUILD, IntentKind.WAKE)
    assert stub.last(intent_routes.SUBMIT_INTENT)['body'] == {'kind': 'wake', 'params': {}}


@pytest.mark.asyncio
async def test_a_broker_without_the_route_fails_loudly():
    '''A 404 is not swallowed: an intent that never reached the broker means a command did nothing.'''
    stub = StubBroker()
    stub.answer(intent_routes.SUBMIT_INTENT, {'error': 'nope'}, status=404)
    async with _connected(stub) as client:
        with pytest.raises(aiohttp.ClientResponseError):
            await client.submit_intent(GUILD, IntentKind.STOP, {'text_channel_id': 6})


# ---------------------------------------------------------------------------
# claim
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_claim_with_nothing_pending_is_empty():
    stub = StubBroker()
    stub.answer(intent_routes.CLAIM_INTENTS, None, status=204)
    async with _connected(stub) as client:
        assert await client.claim_intents(GATEWAY) == []
    assert stub.last(intent_routes.CLAIM_INTENTS)['match'] == {'gateway_id': GATEWAY}


@pytest.mark.asyncio
async def test_claim_returns_the_intents_in_order():
    stub = StubBroker()
    stub.answer(intent_routes.CLAIM_INTENTS, {'intents': [
        {'id': 'a', 'guild_id': GUILD, 'kind': 'join', 'params': {'voice_channel_id': 5, 'text_channel_id': 6}},
        {'id': 'b', 'guild_id': GUILD, 'kind': 'wake'},
    ]})
    async with _connected(stub) as client:
        claimed = await client.claim_intents(GATEWAY)
    assert claimed == [
        GatewayIntent(id='a', guild_id=GUILD, kind=IntentKind.JOIN,
                      params={'voice_channel_id': 5, 'text_channel_id': 6}),
        GatewayIntent(id='b', guild_id=GUILD, kind=IntentKind.WAKE, params={}),
    ]


@pytest.mark.asyncio
async def test_claim_with_an_unknown_kind_names_the_peer():
    stub = StubBroker()
    stub.answer(intent_routes.CLAIM_INTENTS, {'intents': [{'id': 'a', 'guild_id': GUILD, 'kind': 'explode'}]})
    async with _connected(stub) as client:
        with pytest.raises(SeamResponseInvalid):
            await client.claim_intents(GATEWAY)


# ---------------------------------------------------------------------------
# complete and result
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_complete_sends_the_outcome():
    stub = StubBroker()
    stub.answer(intent_routes.COMPLETE_INTENT, {'recorded': True})
    async with _connected(stub) as client:
        assert await client.complete_intent('i-1', False, 'Bot cannot join channel') is True
    call = stub.last(intent_routes.COMPLETE_INTENT)
    assert call['match'] == {'intent_id': 'i-1'}
    assert call['body'] == {'ok': False, 'error': 'Bot cannot join channel'}


@pytest.mark.asyncio
async def test_complete_after_expiry_is_not_recorded():
    stub = StubBroker()
    stub.answer(intent_routes.COMPLETE_INTENT, {'recorded': False})
    async with _connected(stub) as client:
        assert await client.complete_intent('gone', True) is False
    assert stub.last(intent_routes.COMPLETE_INTENT)['body'] == {'ok': True, 'error': None}


@pytest.mark.asyncio
async def test_result_not_ready_is_an_answer_not_an_error():
    stub = StubBroker()
    stub.answer(intent_routes.GET_INTENT_RESULT, {'done': False})
    async with _connected(stub) as client:
        assert await client.get_intent_result('i-1') == IntentResult(done=False)
    assert stub.last(intent_routes.GET_INTENT_RESULT)['match'] == {'intent_id': 'i-1'}


@pytest.mark.asyncio
async def test_result_carries_the_error_text():
    stub = StubBroker()
    stub.answer(intent_routes.GET_INTENT_RESULT, {'done': True, 'ok': False, 'error': 'Bot cannot join channel'})
    async with _connected(stub) as client:
        assert await client.get_intent_result('i-1') == IntentResult(
            done=True, ok=False, error='Bot cannot join channel')
