'''Tests for the shared Redis per-guild queue primitives.'''
import asyncio

import fakeredis.aioredis
import pytest

from discord_core.tests.workers.queue_support import (
    KEY_PREFIX, RedisQueueStub, make_playlist_add, make_request)
from discord_core.types.queue import PutsBlocked
from discord_core.workers import redis_guild_queue
from discord_core.workers.redis_guild_queue import (
    GUILD_BLOCK_TTL_SECONDS, build_status_snapshot, collect_queue_sizes, redis_pop_lock)


@pytest.fixture(name='stub')
def _stub(redis_client):
    '''A queue worker stub over the shared fake Redis.'''
    return RedisQueueStub(redis_client)


@pytest.mark.asyncio
async def test_block_guild_sets_expiring_flag(stub, redis_client):
    '''Blocking writes a flag key that expires on its own.'''
    assert await stub.block_guild(7) is True
    assert await redis_client.get(f'{KEY_PREFIX}7:blocked') == '1'
    assert 0 < await redis_client.ttl(f'{KEY_PREFIX}7:blocked') <= GUILD_BLOCK_TTL_SECONDS
    assert await stub.guild_is_blocked(7) is True
    assert await stub.guild_is_blocked(8) is False


@pytest.mark.asyncio
async def test_block_flag_lapses_after_expiry(stub, redis_client):
    '''Once the flag key is gone the guild accepts submits again.'''
    await stub.block_guild(7)
    await redis_client.delete(f'{KEY_PREFIX}7:blocked')
    assert await stub.guild_is_blocked(7) is False


@pytest.mark.asyncio
async def test_submit_rejected_while_blocked(stub):
    '''A blocked guild raises PutsBlocked and nothing reaches the base submit.'''
    await stub.block_guild(7)
    with pytest.raises(PutsBlocked, match='Puts blocked for guild 7'):
        await stub.submit(7, make_request())
    assert not stub.submitted


@pytest.mark.asyncio
async def test_submit_delegates_when_not_blocked(stub):
    '''An unblocked guild reaches the base submit, priority intact; other guilds are unaffected.'''
    await stub.block_guild(8)
    request = make_request(guild_id=7)
    await stub.submit(7, request, priority=3)
    assert stub.submitted == [(7, request.uuid, 3)]


@pytest.mark.asyncio
async def test_clear_stale_blocks_removes_only_unexpiring(stub, redis_client):
    '''Only block keys with no TTL are swept; live blocks and other keys survive.'''
    await redis_client.set(f'{KEY_PREFIX}1:blocked', '1')
    await redis_client.set(f'{KEY_PREFIX}2:blocked', '1')
    await stub.block_guild(3)
    await redis_client.set(f'{KEY_PREFIX}4', 'not a block')
    assert await stub.clear_stale_guild_blocks() == 2
    assert await redis_client.exists(f'{KEY_PREFIX}1:blocked') == 0
    assert await redis_client.exists(f'{KEY_PREFIX}2:blocked') == 0
    assert await stub.guild_is_blocked(3) is True
    assert await redis_client.get(f'{KEY_PREFIX}4') == 'not a block'
    assert await stub.clear_stale_guild_blocks() == 0


@pytest.mark.asyncio
async def test_pop_lock_acquires_and_releases(redis_client):
    '''The lock key holds a token inside the section and is deleted on exit.'''
    async with redis_pop_lock(redis_client, 'lock:a'):
        token = await redis_client.get('lock:a')
        assert token
        assert 0 < await redis_client.ttl('lock:a') <= redis_guild_queue.POP_LOCK_TTL_SECONDS
    assert await redis_client.exists('lock:a') == 0


@pytest.mark.asyncio
async def test_pop_lock_waits_for_holder(redis_client, monkeypatch):
    '''A second taker polls until the first holder's lock disappears.'''
    monkeypatch.setattr(redis_guild_queue, 'POP_LOCK_POLL_INTERVAL_SECONDS', 0.01)
    order = []

    async def first():
        async with redis_pop_lock(redis_client, 'lock:b'):
            order.append('first-in')
            await asyncio.sleep(0.05)
            order.append('first-out')

    async def second():
        await asyncio.sleep(0.01)
        async with redis_pop_lock(redis_client, 'lock:b'):
            order.append('second-in')

    await asyncio.gather(first(), second())
    assert order == ['first-in', 'first-out', 'second-in']


@pytest.mark.asyncio
async def test_pop_lock_gives_up_and_keeps_foreign_lock(redis_client, monkeypatch):
    '''After the wait window the section runs without the lock and leaves the holder's key alone.'''
    monkeypatch.setattr(redis_guild_queue, 'POP_LOCK_WAIT_SECONDS', 0.05)
    monkeypatch.setattr(redis_guild_queue, 'POP_LOCK_POLL_INTERVAL_SECONDS', 0.01)
    await redis_client.set('lock:c', 'someone-else')
    ran = False
    async with redis_pop_lock(redis_client, 'lock:c'):
        ran = True
    assert ran
    assert await redis_client.get('lock:c') == 'someone-else'


@pytest.mark.asyncio
async def test_pop_lock_does_not_delete_successor(redis_client):
    '''If the lock changed hands mid-section, exit leaves the new holder's token.'''
    async with redis_pop_lock(redis_client, 'lock:d'):
        await redis_client.set('lock:d', 'successor')
    assert await redis_client.get('lock:d') == 'successor'


@pytest.mark.asyncio
async def test_clear_drops_everything_without_predicate(stub, redis_client):
    '''With no predicate every entry is returned and its payload deleted.'''
    first, second = make_request(text='a'), make_request(text='b')
    await stub.enqueue(first, 1.0)
    await stub.enqueue(second, 2.0)
    dropped = await stub.clear_guild_queue(7)
    assert [mr.uuid for mr in dropped] == [first.uuid, second.uuid]
    assert await stub.queue_size(7) == 0
    assert await redis_client.exists(stub.request_key(first.uuid)) == 0


@pytest.mark.asyncio
async def test_clear_honours_preserve_predicate(stub, redis_client):
    '''Entries the predicate keeps stay queued with their payload; the rest drop.'''
    keep, drop = make_playlist_add(), make_request()
    await stub.enqueue(keep, 1.0)
    await stub.enqueue(drop, 2.0)
    dropped = await stub.clear_guild_queue(7, preserve_predicate=lambda mr: not mr.download_file)
    assert [mr.uuid for mr in dropped] == [drop.uuid]
    assert await redis_client.zrange(stub.queue_key(7), 0, -1) == [keep.uuid]
    assert await redis_client.exists(stub.request_key(keep.uuid)) == 1


@pytest.mark.asyncio
async def test_clear_silently_prunes_expired_payloads(stub, redis_client):
    '''A ZSET entry whose payload TTL'd away is removed and not reported as dropped.'''
    live, ghost = make_request(text='live'), make_request(text='ghost')
    await stub.enqueue(live, 2.0)
    await redis_client.zadd(stub.queue_key(7), {ghost.uuid: 1.0})
    dropped = await stub.clear_guild_queue(7)
    assert [mr.uuid for mr in dropped] == [live.uuid]
    assert await stub.queue_size(7) == 0


@pytest.mark.asyncio
async def test_collect_queue_sizes_keys_are_strings():
    '''Guild ids of either type come back as string keys with their counts.'''
    async def size(guild_id):
        return guild_id * 2
    assert await collect_queue_sizes([1, '2'], size) == {'1': 2, '2': 4}
    assert await collect_queue_sizes([], size) == {}


def test_status_snapshot_shape():
    '''The snapshot carries all four fields as given.'''
    assert build_status_snapshot('rate limited', 3, 42, {'1': 5}) == {
        'failure_summary': 'rate limited', 'failure_count': 3,
        'backoff_seconds_remaining': 42, 'queue_sizes': {'1': 5}}


@pytest.mark.parametrize('backoff', [0, None])
def test_status_snapshot_zero_backoff_is_absent(backoff):
    '''A zero or missing backoff is reported as None, not 0.'''
    snapshot = build_status_snapshot('', 0, backoff, {})
    assert snapshot['backoff_seconds_remaining'] is None


@pytest.mark.asyncio
async def test_stub_snapshot_counts_per_guild(stub):
    '''Sizes in the stub's snapshot reflect what is queued per guild.'''
    await stub.enqueue(make_request(guild_id=1), 1.0)
    await stub.enqueue(make_request(guild_id=1, text='x'), 2.0)
    await stub.enqueue(make_request(guild_id=2), 1.0)
    await stub.block_guild(2)
    assert (await stub.status_snapshot())['queue_sizes'] == {'1': 2, '2': 1}


def test_fixture_is_fakeredis(redis_client):
    '''The shared fixture hands out a fakeredis client, never a real server.'''
    assert isinstance(redis_client, fakeredis.aioredis.FakeRedis)
