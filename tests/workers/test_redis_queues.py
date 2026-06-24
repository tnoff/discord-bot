'''Tests for RedisBundleStore, RedisWorkQueue, and RedisDownloadResultQueue.'''
import pytest
import fakeredis.aioredis

from discord_bot.clients.redis_client import RedisManager
from discord_bot.types.download import DownloadResult, DownloadStatus
from discord_bot.workers.redis_queues import (
    load_bundle,
    save_bundle,
    RedisBundleStore,
    RedisDownloadResultQueue,
    RedisWorkQueue,
)
from tests.helpers import fake_source_dict, generate_fake_context


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


# ---------------------------------------------------------------------------
# RedisDownloadResultQueue
# ---------------------------------------------------------------------------


def _download_result() -> DownloadResult:
    mr = fake_source_dict(generate_fake_context())
    return DownloadResult(
        status=DownloadStatus(success=True),
        media_request=mr,
        ytdlp_data={'id': 'x', 'title': 't', 'webpage_url': 'http://e/v'},
        file_name=None,
    )


@pytest.mark.asyncio
async def test_redis_download_result_queue_round_trip():
    '''put then get_nowait returns a structurally-identical DownloadResult.'''
    q = RedisDownloadResultQueue(_manager())
    r = _download_result()
    await q.put(r)
    popped = await q.get_nowait()
    assert popped is not None
    assert str(popped.media_request.uuid) == str(r.media_request.uuid)
    assert popped.ytdlp_data == r.ytdlp_data


@pytest.mark.asyncio
async def test_redis_download_result_queue_empty_returns_none():
    '''get_nowait returns None when the Redis list is empty.'''
    q = RedisDownloadResultQueue(_manager())
    assert await q.get_nowait() is None


@pytest.mark.asyncio
async def test_redis_download_result_queue_fifo_order():
    '''Queue is FIFO — first put pops first.'''
    manager = _manager()
    q = RedisDownloadResultQueue(manager)
    r1 = _download_result()
    r2 = _download_result()
    await q.put(r1)
    await q.put(r2)
    assert str((await q.get_nowait()).media_request.uuid) == str(r1.media_request.uuid)
    assert str((await q.get_nowait()).media_request.uuid) == str(r2.media_request.uuid)
    assert await q.get_nowait() is None


@pytest.mark.asyncio
async def test_redis_download_result_queue_shared_across_instances():
    '''Two RedisDownloadResultQueue instances pointing at the same manager
    share the same backing list — proves multi-pod broker handoff works.'''
    manager = _manager()
    pod_a = RedisDownloadResultQueue(manager)
    pod_b = RedisDownloadResultQueue(manager)
    r = _download_result()
    await pod_a.put(r)
    popped = await pod_b.get_nowait()
    assert popped is not None
    assert str(popped.media_request.uuid) == str(r.media_request.uuid)
