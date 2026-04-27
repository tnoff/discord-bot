'''Tests for RedisBundleStore and RedisWorkQueue.'''
import pytest
import fakeredis.aioredis

from discord_bot.clients.redis_client import RedisManager
from discord_bot.workers.redis_queues import (
    load_bundle,
    save_bundle,
    RedisBundleStore,
    RedisWorkQueue,
)


def _manager():
    '''Return a RedisManager backed by a fresh FakeRedis client.'''
    client = fakeredis.aioredis.FakeRedis(decode_responses=True)
    return RedisManager.from_client(client)


# ---------------------------------------------------------------------------
# load_bundle free function
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_load_bundle_returns_dict_when_present():
    '''load_bundle returns the stored dict when the key exists.'''
    client = fakeredis.aioredis.FakeRedis(decode_responses=True)
    bundle = {'guild_id': 1, 'channel_id': 2, 'sticky_messages': False, 'message_contexts': []}
    await save_bundle(client, 'k', bundle)
    result = await load_bundle(client, 'k')
    assert result == bundle


@pytest.mark.asyncio
async def test_load_bundle_returns_none_when_missing():
    '''load_bundle returns None when the key does not exist.'''
    client = fakeredis.aioredis.FakeRedis(decode_responses=True)
    assert await load_bundle(client, 'no-such-key') is None


# ---------------------------------------------------------------------------
# RedisBundleStore
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_redis_bundle_store_save_and_load():
    '''save then load returns the stored bundle dict.'''
    store = RedisBundleStore(_manager())
    bundle = {'guild_id': 1, 'channel_id': 10, 'sticky_messages': True, 'message_contexts': []}
    await store.save('k', bundle)
    assert await store.load('k') == bundle


@pytest.mark.asyncio
async def test_redis_bundle_store_load_missing_returns_none():
    '''load returns None for an unknown key.'''
    store = RedisBundleStore(_manager())
    assert await store.load('missing') is None


@pytest.mark.asyncio
async def test_redis_bundle_store_delete():
    '''delete removes the bundle so load returns None.'''
    store = RedisBundleStore(_manager())
    await store.save('k', {'x': 1})
    await store.delete('k')
    assert await store.load('k') is None


@pytest.mark.asyncio
async def test_redis_bundle_store_load_all():
    '''load_all returns all saved bundles.'''
    store = RedisBundleStore(_manager())
    b1 = {'guild_id': 1, 'channel_id': 1, 'sticky_messages': False, 'message_contexts': []}
    b2 = {'guild_id': 2, 'channel_id': 2, 'sticky_messages': False, 'message_contexts': []}
    await store.save('a', b1)
    await store.save('b', b2)
    result = await store.load_all()
    assert result == {'a': b1, 'b': b2}


# ---------------------------------------------------------------------------
# RedisWorkQueue
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_redis_work_queue_enqueue_and_dequeue():
    '''enqueue then dequeue returns the member and payload.'''
    q = RedisWorkQueue(_manager(), shard_id=0, process_id='test-pod')
    await q.enqueue('send:1', {'content': 'hello'}, priority=1)
    result = await q.dequeue(timeout=0.5)
    assert result is not None
    member, payload = result
    assert member == 'send:1'
    assert payload['content'] == 'hello'


@pytest.mark.asyncio
async def test_redis_work_queue_enqueue_unique_deduplicates():
    '''enqueue_unique skips a duplicate member already in the queue.'''
    q = RedisWorkQueue(_manager(), shard_id=0, process_id='test-pod')
    await q.enqueue_unique('mutable:k', {'v': 1}, priority=0)
    await q.enqueue_unique('mutable:k', {'v': 2}, priority=0)
    result = await q.dequeue(timeout=0.5)
    assert result is not None
    assert result[0] == 'mutable:k'
    assert await q.dequeue(timeout=0.1) is None


@pytest.mark.asyncio
async def test_redis_work_queue_acquire_and_release_lock():
    '''acquire_lock returns True; release_lock releases it so it can be acquired again.'''
    q = RedisWorkQueue(_manager(), shard_id=0, process_id='test-pod')
    assert await q.acquire_lock('bundle-key') is True
    await q.release_lock('bundle-key')
    assert await q.acquire_lock('bundle-key') is True
    await q.release_lock('bundle-key')


@pytest.mark.asyncio
async def test_redis_work_queue_store_and_get_result():
    '''store_result then get_result returns the stored dict.'''
    q = RedisWorkQueue(_manager(), shard_id=0, process_id='test-pod')
    await q.store_result('req-42', {'data': 'ok'})
    assert await q.get_result('req-42') == {'data': 'ok'}


@pytest.mark.asyncio
async def test_redis_work_queue_get_result_missing_returns_none():
    '''get_result returns None for an unknown request_id.'''
    q = RedisWorkQueue(_manager(), shard_id=0, process_id='test-pod')
    assert await q.get_result('no-such-id') is None


@pytest.mark.asyncio
async def test_redis_work_queue_lazy_queue_creation():
    '''_get_queue creates the inner RedisDispatchQueue lazily on first call.'''
    q = RedisWorkQueue(_manager(), shard_id=0, process_id='test-pod')
    assert q._queue is None  # pylint: disable=protected-access
    q._get_queue()  # pylint: disable=protected-access
    assert q._queue is not None  # pylint: disable=protected-access
