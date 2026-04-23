'''Tests for RedisBrokerRegistry.'''
import asyncio

import fakeredis.aioredis
import pytest

from discord_bot.clients.redis_client import RedisManager
from discord_bot.workers.broker_registry import (
    ENTRY_KEY_PREFIX,
    LOCK_KEY_PREFIX,
    RedisBrokerRegistry,
)


def _registry() -> RedisBrokerRegistry:
    '''Return a RedisBrokerRegistry backed by a fresh FakeRedis client.'''
    client = fakeredis.aioredis.FakeRedis(decode_responses=True)
    return RedisBrokerRegistry(RedisManager.from_client(client))


# ---------------------------------------------------------------------------
# get_entry / set_entry / delete_entry
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_get_entry_returns_none_when_missing():
    '''get_entry returns None for an unknown UUID.'''
    reg = _registry()
    assert await reg.get_entry('no-such-uuid') is None


@pytest.mark.asyncio
async def test_set_and_get_entry_roundtrip():
    '''set_entry then get_entry returns the stored data.'''
    reg = _registry()
    data = {'zone': 'in_flight', 'checked_out_by': None, 'download': None, 'request': {}}
    await reg.set_entry('uuid-1', data)
    result = await reg.get_entry('uuid-1')
    assert result == data


@pytest.mark.asyncio
async def test_delete_entry_removes_entry():
    '''delete_entry removes the entry so get_entry returns None.'''
    reg = _registry()
    await reg.set_entry('uuid-1', {'zone': 'in_flight'})
    await reg.delete_entry('uuid-1')
    assert await reg.get_entry('uuid-1') is None


@pytest.mark.asyncio
async def test_delete_entry_missing_is_noop():
    '''delete_entry on a non-existent key does not raise.'''
    reg = _registry()
    await reg.delete_entry('no-such-uuid')  # should not raise


# ---------------------------------------------------------------------------
# all_entries
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_all_entries_empty():
    '''all_entries returns an empty list when no entries exist.'''
    reg = _registry()
    assert await reg.all_entries() == []


@pytest.mark.asyncio
async def test_all_entries_returns_stored_entries():
    '''all_entries returns all entries regardless of UUID.'''
    reg = _registry()
    a = {'zone': 'in_flight', 'uuid': 'a'}
    b = {'zone': 'available', 'uuid': 'b'}
    await reg.set_entry('uuid-a', a)
    await reg.set_entry('uuid-b', b)
    results = await reg.all_entries()
    assert len(results) == 2
    uuids = {r['uuid'] for r in results}
    assert uuids == {'a', 'b'}


# ---------------------------------------------------------------------------
# atomic_checkout
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_atomic_checkout_succeeds_for_available_entry():
    '''atomic_checkout returns True when zone is "available".'''
    reg = _registry()
    await reg.set_entry('uuid-1', {
        'zone': 'available',
        'checked_out_by': None,
        'download': {'file_path': '/tmp/file.mp4'},
        'request': {},
    })
    result = await reg.atomic_checkout('uuid-1', guild_id=42)
    assert result is True
    data = await reg.get_entry('uuid-1')
    assert data['zone'] == 'checked_out'
    assert data['checked_out_by'] == 42


@pytest.mark.asyncio
async def test_atomic_checkout_fails_for_in_flight_entry():
    '''atomic_checkout returns False when zone is "in_flight".'''
    reg = _registry()
    await reg.set_entry('uuid-1', {'zone': 'in_flight', 'checked_out_by': None})
    assert await reg.atomic_checkout('uuid-1', guild_id=1) is False


@pytest.mark.asyncio
async def test_atomic_checkout_fails_for_already_checked_out():
    '''atomic_checkout returns False when zone is already "checked_out".'''
    reg = _registry()
    await reg.set_entry('uuid-1', {'zone': 'checked_out', 'checked_out_by': 99})
    assert await reg.atomic_checkout('uuid-1', guild_id=1) is False


@pytest.mark.asyncio
async def test_atomic_checkout_fails_for_missing_entry():
    '''atomic_checkout returns False when the entry does not exist.'''
    reg = _registry()
    assert await reg.atomic_checkout('no-such-uuid', guild_id=1) is False


@pytest.mark.asyncio
async def test_atomic_checkout_only_one_succeeds_under_contention():
    '''
    Two concurrent atomic_checkout calls on the same AVAILABLE entry: exactly one succeeds.
    '''
    reg = _registry()
    await reg.set_entry('uuid-1', {
        'zone': 'available',
        'checked_out_by': None,
        'download': {'file_path': '/tmp/file.mp4'},
        'request': {},
    })

    results = await asyncio.gather(
        reg.atomic_checkout('uuid-1', guild_id=1),
        reg.atomic_checkout('uuid-1', guild_id=2),
    )
    # Exactly one should succeed
    assert results.count(True) == 1
    assert results.count(False) == 1


@pytest.mark.asyncio
async def test_all_entries_skips_invalid_json():
    '''all_entries silently skips entries whose value is not valid JSON.'''
    client = fakeredis.aioredis.FakeRedis(decode_responses=True)
    reg = RedisBrokerRegistry(RedisManager.from_client(client))
    await client.set(f'{ENTRY_KEY_PREFIX}valid-uuid', '{"zone": "available"}')
    await client.set(f'{ENTRY_KEY_PREFIX}corrupt-uuid', 'not{{valid}json')
    results = await reg.all_entries()
    assert len(results) == 1
    assert results[0]['zone'] == 'available'


@pytest.mark.asyncio
async def test_atomic_checkout_returns_false_when_lock_contested():
    '''atomic_checkout returns False immediately when another caller holds the lock.'''
    client = fakeredis.aioredis.FakeRedis(decode_responses=True)
    reg = RedisBrokerRegistry(RedisManager.from_client(client))
    await reg.set_entry('uuid-1', {'zone': 'available', 'checked_out_by': None})
    # Pre-occupy the lock to simulate contention
    await client.set(f'{LOCK_KEY_PREFIX}uuid-1', '1')
    assert await reg.atomic_checkout('uuid-1', guild_id=42) is False
    data = await reg.get_entry('uuid-1')
    assert data['zone'] == 'available'


@pytest.mark.asyncio
async def test_set_entry_uses_ttl():
    '''set_entry stores entry with a TTL so Redis will expire it.'''
    client = fakeredis.aioredis.FakeRedis(decode_responses=True)
    reg = RedisBrokerRegistry(RedisManager.from_client(client))
    await reg.set_entry('uuid-ttl', {'zone': 'in_flight'})
    ttl = await client.ttl(f'{ENTRY_KEY_PREFIX}uuid-ttl')
    assert ttl > 0
