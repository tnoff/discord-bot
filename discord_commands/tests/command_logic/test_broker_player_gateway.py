from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest

from discord_core.types.gateway_intents import IntentKind, IntentResult

from discord_commands.command_logic.broker_player_gateway import BrokerPlayerGateway, JOIN_TIMEOUT_MESSAGE


def make_gateway(queue=None, results=(), join_timeout=5.0):
    '''A gateway over a mock broker. ``results`` are the intent results get_intent_result answers with, in order.'''
    broker = AsyncMock()
    broker.get_guild_queue.return_value = queue or SimpleNamespace(text_channel_id=None, closed=False, playing=None)
    broker.submit_intent.return_value = 'intent-1'
    broker.get_intent_result.side_effect = list(results)
    download_client = AsyncMock()
    download_client.queue_size.return_value = 2
    queuer = MagicMock()
    queuer.enqueue_download = AsyncMock(return_value=True)
    gateway = BrokerPlayerGateway(broker, download_client, queuer, join_timeout=join_timeout, poll_interval=0)
    return gateway, broker, queuer


def open_queue(playing=None):
    return SimpleNamespace(text_channel_id=77, closed=False, playing=playing)


@pytest.mark.asyncio
async def test_status_of_a_guild_with_no_open_queue():
    gateway, _, _ = make_gateway()
    assert (await gateway.status(11)).exists is False
    assert await gateway.has_live_player(11) is False


@pytest.mark.asyncio
async def test_status_of_a_closed_queue():
    gateway, _, _ = make_gateway(SimpleNamespace(text_channel_id=77, closed=True, playing=None))
    assert (await gateway.status(11)).exists is False
    assert await gateway.has_live_player(11) is False


@pytest.mark.asyncio
@pytest.mark.parametrize('playing', [None, SimpleNamespace(uuid='u')])
async def test_status_of_an_open_queue(playing):
    gateway, _, _ = make_gateway(open_queue(playing))
    status = await gateway.status(11)
    assert (status.exists, status.voice_connected, status.is_playing, status.pending_downloads, status.text_channel_id) == \
        (True, True, playing is not None, 2, 77)
    assert await gateway.has_live_player(11) is True


@pytest.mark.asyncio
async def test_ensure_waits_for_the_join_result():
    gateway, broker, _ = make_gateway(results=[IntentResult(done=False), IntentResult(done=True, ok=True)])
    result = await gateway.ensure(11, 55, 22)
    assert (result.ok, result.error) == (True, None)
    broker.submit_intent.assert_awaited_once_with(11, IntentKind.JOIN, {'voice_channel_id': 55, 'text_channel_id': 22})
    assert broker.get_intent_result.await_count == 2


@pytest.mark.asyncio
async def test_ensure_carries_the_gateways_error_back():
    gateway, _, _ = make_gateway(results=[IntentResult(done=True, ok=False, error='Bot cannot join channel music')])
    result = await gateway.ensure(11, 55, 22)
    assert (result.ok, result.error) == (False, 'Bot cannot join channel music')


@pytest.mark.asyncio
async def test_ensure_gives_up_on_a_join_nobody_claims():
    gateway, broker, _ = make_gateway(results=[IntentResult(done=False)] * 3, join_timeout=0)
    result = await gateway.ensure(11, 55, 22)
    assert (result.ok, result.error) == (False, JOIN_TIMEOUT_MESSAGE)
    broker.get_intent_result.assert_awaited_once_with('intent-1')


@pytest.mark.asyncio
async def test_stop_move_messages_and_wake_are_asked_not_awaited():
    gateway, broker, _ = make_gateway()
    await gateway.stop(11, 22)
    await gateway.move_messages(11, 23)
    await gateway.wake(11)
    assert [call.args for call in broker.submit_intent.await_args_list] == [
        (11, IntentKind.STOP, {'text_channel_id': 22}),
        (11, IntentKind.MOVE_MESSAGES, {'text_channel_id': 23}),
        (11, IntentKind.WAKE),
    ]
    broker.get_intent_result.assert_not_called()


@pytest.mark.asyncio
async def test_add_cached_queues_the_track_and_wakes_the_player():
    gateway, broker, queuer = make_gateway(open_queue())
    download = MagicMock()
    assert await gateway.add_cached(11, download) is True
    on_queued = queuer.enqueue_download.await_args.args[1]
    queuer.enqueue_download.assert_awaited_once_with(download, on_queued)
    broker.submit_intent.assert_not_called()
    await on_queued()
    broker.submit_intent.assert_awaited_once_with(11, IntentKind.WAKE)


@pytest.mark.asyncio
async def test_add_cached_without_a_player_does_nothing():
    gateway, _, queuer = make_gateway()
    assert await gateway.add_cached(11, MagicMock()) is False
    queuer.enqueue_download.assert_not_called()
