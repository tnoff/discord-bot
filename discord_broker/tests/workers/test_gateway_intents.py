'''Tests for GatewayIntentRegistry, the Redis side of the gateway-intent inbox.'''
import json
from datetime import datetime, timedelta, timezone

import fakeredis.aioredis
import pytest
from freezegun import freeze_time

from discord_core.clients.redis_client import RedisManager

from discord_broker.workers.gateway_intents import (
    GINTENT_KEY_PREFIX,
    GINTENTS_KEY,
    GINTENT_WAKE_KEY_PREFIX,
    INTENT_LEASE_SECONDS,
    GatewayIntentRegistry,
)

GUILD = 7
OTHER_GUILD = 8
GATEWAY = 'gw-1'
JOIN = {'voice_channel_id': 5, 'text_channel_id': 6}
START = datetime(2026, 10, 19, 12, 0, tzinfo=timezone.utc)


def _make() -> tuple[GatewayIntentRegistry, fakeredis.aioredis.FakeRedis]:
    client = fakeredis.aioredis.FakeRedis(decode_responses=True)
    return GatewayIntentRegistry(RedisManager.from_client(client)), client


@pytest.mark.asyncio
async def test_a_submitted_intent_is_claimed_with_its_params():
    registry, _ = _make()
    intent_id = await registry.submit(GUILD, 'join', JOIN)
    assert await registry.claim(GATEWAY) == [
        {'id': intent_id, 'guild_id': GUILD, 'kind': 'join', 'params': JOIN}]


@pytest.mark.asyncio
async def test_claiming_nothing_is_empty():
    registry, _ = _make()
    assert await registry.claim(GATEWAY) == []


@pytest.mark.asyncio
async def test_intents_come_out_in_the_order_they_went_in_across_guilds():
    registry, _ = _make()
    first = await registry.submit(GUILD, 'join', JOIN)
    second = await registry.submit(OTHER_GUILD, 'stop', {'text_channel_id': 9})
    third = await registry.submit(GUILD, 'stop', {'text_channel_id': 6})
    assert [item['id'] for item in await registry.claim(GATEWAY)] == [first, second, third]


@pytest.mark.asyncio
async def test_a_claimed_intent_is_not_handed_out_again_while_leased():
    registry, _ = _make()
    await registry.submit(GUILD, 'join', JOIN)
    assert len(await registry.claim(GATEWAY)) == 1
    assert await registry.claim(GATEWAY) == []
    assert await registry.claim('gw-2') == []


@pytest.mark.asyncio
async def test_an_intent_is_handed_out_again_when_the_lease_lapses():
    '''A gateway that dies after claiming loses nothing: the intent comes back.'''
    registry, _ = _make()
    with freeze_time(START) as clock:
        intent_id = await registry.submit(GUILD, 'stop', {'text_channel_id': 6})
        assert len(await registry.claim(GATEWAY)) == 1
        clock.tick(timedelta(seconds=INTENT_LEASE_SECONDS - 1))
        assert await registry.claim('gw-2') == []
        clock.tick(timedelta(seconds=2))
        again = await registry.claim('gw-2')
    assert [item['id'] for item in again] == [intent_id]


@pytest.mark.asyncio
async def test_an_expired_intent_is_dropped_from_the_order_when_a_claim_walks_it():
    registry, client = _make()
    intent_id = await registry.submit(GUILD, 'stop', {'text_channel_id': 6})
    await client.delete(f'{GINTENT_KEY_PREFIX}{intent_id}')
    assert await registry.claim(GATEWAY) == []
    assert await client.zcard(GINTENTS_KEY) == 0


@pytest.mark.asyncio
async def test_a_second_wake_for_a_guild_is_not_stored_twice():
    registry, client = _make()
    first = await registry.submit(GUILD, 'wake', {})
    assert await registry.submit(GUILD, 'wake', {}) == first
    assert await client.zcard(GINTENTS_KEY) == 1


@pytest.mark.asyncio
async def test_wakes_for_different_guilds_are_separate():
    registry, _ = _make()
    assert await registry.submit(GUILD, 'wake', {}) != await registry.submit(OTHER_GUILD, 'wake', {})


@pytest.mark.asyncio
async def test_a_wake_after_the_pending_one_was_claimed_is_a_new_intent():
    '''The marker means "a wake is still waiting"; once it is taken, the next wake must be seen.'''
    registry, client = _make()
    first = await registry.submit(GUILD, 'wake', {})
    await registry.claim(GATEWAY)
    assert await client.get(f'{GINTENT_WAKE_KEY_PREFIX}{GUILD}') is None
    second = await registry.submit(GUILD, 'wake', {})
    assert second != first
    assert [item['id'] for item in await registry.claim(GATEWAY)] == [second]


@pytest.mark.asyncio
async def test_other_kinds_are_never_coalesced():
    registry, _ = _make()
    one = await registry.submit(GUILD, 'stop', {'text_channel_id': 6})
    two = await registry.submit(GUILD, 'stop', {'text_channel_id': 6})
    assert one != two


@pytest.mark.asyncio
async def test_an_unfinished_intent_reads_as_not_done():
    registry, _ = _make()
    intent_id = await registry.submit(GUILD, 'join', JOIN)
    assert await registry.result(intent_id) == {'done': False, 'ok': None, 'error': None}
    await registry.claim(GATEWAY)
    assert await registry.result(intent_id) == {'done': False, 'ok': None, 'error': None}


@pytest.mark.asyncio
async def test_an_unknown_intent_reads_as_not_done():
    registry, _ = _make()
    assert await registry.result('nope') == {'done': False, 'ok': None, 'error': None}


@pytest.mark.asyncio
async def test_completing_records_the_outcome_and_ends_the_claim():
    registry, _ = _make()
    intent_id = await registry.submit(GUILD, 'join', JOIN)
    await registry.claim(GATEWAY)
    assert await registry.complete(intent_id, False, 'Bot cannot join channel X') is True
    assert await registry.result(intent_id) == {'done': True, 'ok': False, 'error': 'Bot cannot join channel X'}
    assert await registry.claim(GATEWAY) == []


@pytest.mark.asyncio
async def test_a_success_has_no_error_text():
    registry, _ = _make()
    intent_id = await registry.submit(GUILD, 'join', JOIN)
    await registry.complete(intent_id, True, None)
    assert await registry.result(intent_id) == {'done': True, 'ok': True, 'error': None}


@pytest.mark.asyncio
async def test_completing_an_expired_intent_is_not_recorded():
    registry, client = _make()
    intent_id = await registry.submit(GUILD, 'join', JOIN)
    await client.delete(f'{GINTENT_KEY_PREFIX}{intent_id}')
    assert await registry.complete(intent_id, True, None) is False


@pytest.mark.asyncio
async def test_a_completed_intent_is_not_redelivered_after_its_lease_lapses():
    registry, _ = _make()
    with freeze_time(START) as clock:
        intent_id = await registry.submit(GUILD, 'stop', {'text_channel_id': 6})
        await registry.claim(GATEWAY)
        await registry.complete(intent_id, True, None)
        clock.tick(timedelta(seconds=INTENT_LEASE_SECONDS + 1))
        assert await registry.claim('gw-2') == []


@pytest.mark.asyncio
async def test_params_survive_the_round_trip_as_json():
    registry, client = _make()
    intent_id = await registry.submit(GUILD, 'move_messages', {'text_channel_id': 123456789012345678})
    assert json.loads(await client.hget(f'{GINTENT_KEY_PREFIX}{intent_id}', 'params')) == {
        'text_channel_id': 123456789012345678}
    assert (await registry.claim(GATEWAY))[0]['params'] == {'text_channel_id': 123456789012345678}
