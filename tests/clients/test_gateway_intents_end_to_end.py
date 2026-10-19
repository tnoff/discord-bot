'''
The gateway-intent contract, end to end: the real HttpBrokerClient against the real BrokerHttpServer.

discord_core's own tests check the client against a stub broker, and discord_broker's check the
server with raw requests.  Neither can notice the two disagreeing, so this does: the asking pod's
calls and the gateway's calls, over HTTP, with only Redis replaced (by fakeredis).
'''
import contextlib

import aiohttp
import pytest
from aiohttp.test_utils import TestServer

from discord_core.clients.http_broker_client import HttpBrokerClient
from discord_core.types.gateway_intents import GatewayIntent, IntentKind, IntentResult

from tests.fakes.asyncio_broker import AsyncioBroker
from tests.fakes.asyncio_queues import make_broker_http_server

GUILD = 31
GATEWAY = 'gw-1'
JOIN = {'voice_channel_id': 5, 'text_channel_id': 6}


@contextlib.asynccontextmanager
async def _stack():
    '''One client standing for both ends: the asking pod and the gateway.'''
    server = make_broker_http_server(AsyncioBroker())
    async with TestServer(server.build_app()) as running, aiohttp.ClientSession() as session:
        yield HttpBrokerClient(str(running.make_url('')), session=session)


@pytest.mark.asyncio
async def test_a_join_goes_from_the_commands_pod_to_the_gateway_and_back():
    async with _stack() as client:
        intent_id = await client.submit_intent(GUILD, IntentKind.JOIN, JOIN)
        assert await client.get_intent_result(intent_id) == IntentResult(done=False)

        assert await client.claim_intents(GATEWAY) == [
            GatewayIntent(id=intent_id, guild_id=GUILD, kind=IntentKind.JOIN, params=JOIN)]
        assert await client.claim_intents(GATEWAY) == []

        assert await client.complete_intent(intent_id, False, 'Bot cannot join channel X') is True
        assert await client.get_intent_result(intent_id) == IntentResult(
            done=True, ok=False, error='Bot cannot join channel X')


@pytest.mark.asyncio
async def test_fire_and_forget_kinds_are_claimed_in_order_and_a_wake_is_coalesced():
    async with _stack() as client:
        await client.submit_intent(GUILD, IntentKind.STOP, {'text_channel_id': 6})
        wake = await client.submit_intent(GUILD, IntentKind.WAKE)
        assert await client.submit_intent(GUILD, IntentKind.WAKE) == wake
        await client.submit_intent(GUILD, IntentKind.MOVE_MESSAGES, {'text_channel_id': 9})

        claimed = await client.claim_intents(GATEWAY)
        assert [item.kind for item in claimed] == [IntentKind.STOP, IntentKind.WAKE, IntentKind.MOVE_MESSAGES]
        for item in claimed:
            assert await client.complete_intent(item.id, True) is True
        assert await client.claim_intents(GATEWAY) == []


@pytest.mark.asyncio
async def test_completing_an_unknown_intent_is_reported_not_raised():
    async with _stack() as client:
        assert await client.complete_intent('31-gone', True) is False
