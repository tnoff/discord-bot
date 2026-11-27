'''
AsyncioBrokerClient's gateway intents must answer exactly as HttpBrokerClient does.

The gateway's tests drive the inbox through AsyncioBrokerClient (the real GatewayIntentRegistry,
minus the HTTP hop), while production reaches it through HttpBrokerClient and the broker's
server.  Any difference between those two paths is a way for the gateway suite to pass while prod
does something else, so this runs ONE scenario through both and requires the same observations.

Observations use kinds and params, never ids: each stack mints its own.
'''
import contextlib
import inspect

import aiohttp
import pytest
from aiohttp.test_utils import TestServer

from discord_core.clients.http_broker_client import HttpBrokerClient
from discord_core.interfaces.gateway_intent_client import GatewayIntentClient
from discord_core.types.gateway_intents import IntentKind

from tests.fakes.asyncio_broker import AsyncioBroker
from tests.fakes.asyncio_broker_client import AsyncioBrokerClient
from tests.fakes.asyncio_queues import make_broker_http_server, make_gateway_intents

GUILD = 77
OTHER = 78


@contextlib.asynccontextmanager
async def _asyncio():
    yield AsyncioBrokerClient(AsyncioBroker(), gateway_intents=make_gateway_intents())


@contextlib.asynccontextmanager
async def _over_http():
    server = make_broker_http_server(AsyncioBroker())
    async with TestServer(server.build_app()) as running, aiohttp.ClientSession() as session:
        yield HttpBrokerClient(str(running.make_url('')), session=session)


STACKS = [pytest.param(_asyncio, id='asyncio'), pytest.param(_over_http, id='over-http')]


async def _scenario(client) -> list:
    '''Drive the inbox through every call; return what each one said.'''
    seen = []

    def view(claimed):
        return [(item.guild_id, item.kind, item.params) for item in claimed]

    join = await client.submit_intent(GUILD, IntentKind.JOIN, {'voice_channel_id': 1, 'text_channel_id': 2})
    wake = await client.submit_intent(GUILD, IntentKind.WAKE)
    seen.append(('wake-coalesced', await client.submit_intent(GUILD, IntentKind.WAKE) == wake))
    await client.submit_intent(OTHER, IntentKind.STOP, {'text_channel_id': 3})
    seen.append(('result-pending', await client.get_intent_result(join)))
    seen.append(('claim', view(await client.claim_intents('gw-1'))))
    seen.append(('claim-again', view(await client.claim_intents('gw-1'))))
    seen.append(('complete', await client.complete_intent(join, False, 'no voice')))
    seen.append(('result', await client.get_intent_result(join)))
    seen.append(('complete-unknown', await client.complete_intent('77-gone', True)))
    seen.append(('result-unknown', await client.get_intent_result('77-gone')))
    seen.append(('wake-after-claim-is-new', await client.submit_intent(GUILD, IntentKind.WAKE) != wake))
    seen.append(('claim-new-wake', view(await client.claim_intents('gw-2'))))
    return seen


@pytest.mark.asyncio
async def test_both_paths_give_identical_answers():
    observed = []
    for stack in (_asyncio, _over_http):
        async with stack() as client:
            observed.append(await _scenario(client))
    assert observed[0] == observed[1]


@pytest.mark.asyncio
@pytest.mark.parametrize('stack', STACKS)
async def test_the_scenario_ends_where_it_should(stack):
    '''Guards the comparison above against passing because both sides are equally wrong.'''
    async with stack() as client:
        seen = dict(await _scenario(client))
    assert seen['wake-coalesced'] is True
    assert [kind for _, kind, _ in seen['claim']] == [IntentKind.JOIN, IntentKind.WAKE, IntentKind.STOP]
    assert seen['claim-again'] == []
    assert seen['complete'] is True and seen['complete-unknown'] is False
    assert seen['result'].ok is False and seen['result'].error == 'no voice'
    assert seen['wake-after-claim-is-new'] is True
    assert [kind for _, kind, _ in seen['claim-new-wake']] == [IntentKind.WAKE]


def test_the_asyncio_client_implements_the_whole_protocol():
    for name, member in inspect.getmembers(GatewayIntentClient, inspect.isfunction):
        if name.startswith('_'):
            continue
        implemented = getattr(AsyncioBrokerClient, name)
        assert list(inspect.signature(implemented).parameters) == list(inspect.signature(member).parameters), name
