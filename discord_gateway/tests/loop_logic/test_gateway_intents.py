import asyncio
from logging import getLogger
from unittest.mock import AsyncMock, MagicMock

import pytest

from discord_core.types.gateway_intents import GatewayIntent, IntentKind

from discord_core.interfaces.player_gateway import EnsureResult
from discord_gateway.loop_logic.gateway_intents import INTERNAL_ERROR, GatewayIntentsLoop


def intent(intent_id, kind, guild_id=11, **params):
    return GatewayIntent(id=intent_id, guild_id=guild_id, kind=kind, params=params)


def make_loop(*batches):
    '''A loop whose broker hands out the given batches of intents, one per claim.'''
    broker = MagicMock()
    broker.claim_intents = AsyncMock(side_effect=list(batches) + [[]] * 5)
    broker.complete_intent = AsyncMock(return_value=True)
    gateway = MagicMock()
    gateway.ensure = AsyncMock(return_value=EnsureResult(ok=True))
    gateway.stop = AsyncMock()
    gateway.move_messages = AsyncMock()
    gateway.wake = AsyncMock()
    return GatewayIntentsLoop(getLogger('test'), broker, gateway, 'gw-1'), broker, gateway


async def settle(loop):
    '''Let every guild's worker run to the end.'''
    while loop._workers:  # pylint: disable=protected-access
        await asyncio.gather(*loop._workers.values())  # pylint: disable=protected-access


@pytest.mark.asyncio
async def test_poll_with_nothing_claimed_does_nothing():
    loop, broker, _ = make_loop()
    assert await loop.poll() == 0
    broker.claim_intents.assert_awaited_once_with('gw-1')
    broker.complete_intent.assert_not_called()


@pytest.mark.asyncio
async def test_join_ensures_the_player_and_reports_the_result():
    loop, broker, gateway = make_loop([intent('a', IntentKind.JOIN, voice_channel_id='55', text_channel_id=66)])
    assert await loop.poll() == 1
    await settle(loop)
    gateway.ensure.assert_awaited_once_with(11, 55, 66)
    broker.complete_intent.assert_awaited_once_with('a', True, None)


@pytest.mark.asyncio
async def test_a_join_that_fails_carries_the_text_for_the_channel():
    loop, broker, gateway = make_loop([intent('a', IntentKind.JOIN, voice_channel_id=55, text_channel_id=66)])
    gateway.ensure.return_value = EnsureResult(ok=False, error='Bot cannot join channel music')
    await loop.poll()
    await settle(loop)
    broker.complete_intent.assert_awaited_once_with('a', False, 'Bot cannot join channel music')


@pytest.mark.asyncio
async def test_stop_move_and_wake_reach_the_player_and_are_completed():
    loop, broker, gateway = make_loop([
        intent('a', IntentKind.STOP, text_channel_id=66),
        intent('b', IntentKind.MOVE_MESSAGES, text_channel_id=77),
        intent('c', IntentKind.WAKE),
    ])
    await loop.poll()
    await settle(loop)
    gateway.stop.assert_awaited_once_with(11, 66)
    gateway.move_messages.assert_awaited_once_with(11, 77)
    gateway.wake.assert_awaited_once_with(11)
    assert [call.args for call in broker.complete_intent.await_args_list] == [
        ('a', True, None), ('b', True, None), ('c', True, None)]


@pytest.mark.asyncio
async def test_a_guilds_intents_run_in_order_and_do_not_block_another_guild():
    release = asyncio.Event()
    order = []

    async def slow_ensure(guild_id, *_):
        order.append(('join-start', guild_id))
        await release.wait()
        order.append(('join-end', guild_id))
        return EnsureResult(ok=True)

    loop, _, gateway = make_loop([
        intent('a', IntentKind.JOIN, guild_id=1, voice_channel_id=5, text_channel_id=6),
        intent('b', IntentKind.WAKE, guild_id=1),
        intent('c', IntentKind.WAKE, guild_id=2),
    ])
    gateway.ensure.side_effect = slow_ensure
    gateway.wake.side_effect = lambda guild_id: order.append(('wake', guild_id))
    await loop.poll()
    await asyncio.sleep(0)
    await asyncio.sleep(0)
    # Guild 2 got through while guild 1 was still joining; guild 1's wake waits behind its join.
    assert order == [('join-start', 1), ('wake', 2)]
    release.set()
    await settle(loop)
    assert order == [('join-start', 1), ('wake', 2), ('join-end', 1), ('wake', 1)]


@pytest.mark.asyncio
async def test_an_intent_still_running_is_not_run_again_when_redelivered():
    release = asyncio.Event()

    async def slow_ensure(*_):
        await release.wait()
        return EnsureResult(ok=True)

    joining = intent('a', IntentKind.JOIN, voice_channel_id=5, text_channel_id=6)
    loop, broker, gateway = make_loop([joining], [joining])
    gateway.ensure.side_effect = slow_ensure
    assert await loop.poll() == 1
    assert await loop.poll() == 0
    release.set()
    await settle(loop)
    gateway.ensure.assert_awaited_once()
    broker.complete_intent.assert_awaited_once()


@pytest.mark.asyncio
async def test_an_intent_that_raises_is_completed_as_failed_and_the_next_still_runs():
    loop, broker, gateway = make_loop([
        intent('a', IntentKind.STOP, text_channel_id=66),
        intent('b', IntentKind.WAKE),
    ])
    gateway.stop.side_effect = RuntimeError('boom')
    await loop.poll()
    await settle(loop)
    assert [call.args for call in broker.complete_intent.await_args_list] == [
        ('a', False, INTERNAL_ERROR), ('b', True, None)]


@pytest.mark.asyncio
async def test_close_cancels_unfinished_work():
    started = asyncio.Event()

    async def hang(*_):
        started.set()
        await asyncio.Event().wait()

    loop, broker, gateway = make_loop([intent('a', IntentKind.JOIN, voice_channel_id=5, text_channel_id=6)])
    gateway.ensure.side_effect = hang
    await loop.poll()
    await started.wait()
    await loop.close()
    broker.complete_intent.assert_not_called()
    assert not loop._workers and not loop._in_flight  # pylint: disable=protected-access
