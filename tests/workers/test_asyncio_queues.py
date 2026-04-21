'''Tests for AsyncioBundleStore and AsyncioWorkQueue.'''
import pytest

from discord_bot.workers.asyncio_queues import AsyncioBundleStore, AsyncioWorkQueue


# ---------------------------------------------------------------------------
# AsyncioBundleStore
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_bundle_store_save_and_load():
    '''save then load returns the stored dict.'''
    store = AsyncioBundleStore()
    await store.save('k', {'a': 1})
    assert await store.load('k') == {'a': 1}


@pytest.mark.asyncio
async def test_bundle_store_load_missing_returns_none():
    '''load returns None for an unknown key.'''
    store = AsyncioBundleStore()
    assert await store.load('missing') is None


@pytest.mark.asyncio
async def test_bundle_store_delete():
    '''delete removes the key so load returns None.'''
    store = AsyncioBundleStore()
    await store.save('k', {'x': 1})
    await store.delete('k')
    assert await store.load('k') is None


@pytest.mark.asyncio
async def test_bundle_store_delete_missing_is_noop():
    '''delete on a missing key does not raise.'''
    store = AsyncioBundleStore()
    await store.delete('no-such-key')


@pytest.mark.asyncio
async def test_bundle_store_load_all_returns_all():
    '''load_all returns a snapshot of every saved bundle.'''
    store = AsyncioBundleStore()
    await store.save('a', {'n': 1})
    await store.save('b', {'n': 2})
    result = await store.load_all()
    assert result == {'a': {'n': 1}, 'b': {'n': 2}}


@pytest.mark.asyncio
async def test_bundle_store_load_all_outer_dict_is_copy():
    '''Adding a key to the load_all snapshot does not affect the store.'''
    store = AsyncioBundleStore()
    await store.save('k', {'v': 0})
    snapshot = await store.load_all()
    snapshot['new_key'] = {'v': 1}
    assert 'new_key' not in await store.load_all()


# ---------------------------------------------------------------------------
# AsyncioWorkQueue
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_work_queue_enqueue_dequeue_roundtrip():
    '''enqueue then dequeue returns the same member and payload.'''
    q = AsyncioWorkQueue()
    await q.enqueue('send:1', {'content': 'hi'}, priority=1)
    result = await q.dequeue(timeout=0.1)
    assert result == ('send:1', {'content': 'hi'})


@pytest.mark.asyncio
async def test_work_queue_enqueue_unique_deduplicates():
    '''enqueue_unique ignores a second enqueue for the same member.'''
    q = AsyncioWorkQueue()
    await q.enqueue_unique('mutable:k', {'v': 1}, priority=0)
    await q.enqueue_unique('mutable:k', {'v': 2}, priority=0)
    result = await q.dequeue(timeout=0.1)
    assert result is not None
    assert result[0] == 'mutable:k'
    # Second dequeue should find nothing
    assert await q.dequeue(timeout=0.05) is None


@pytest.mark.asyncio
async def test_work_queue_dequeue_timeout_returns_none():
    '''dequeue returns None when the queue is empty and timeout expires.'''
    q = AsyncioWorkQueue()
    assert await q.dequeue(timeout=0.05) is None


@pytest.mark.asyncio
async def test_work_queue_priority_ordering():
    '''Items with lower priority value are dequeued first.'''
    q = AsyncioWorkQueue()
    await q.enqueue('low', {}, priority=2)
    await q.enqueue('high', {}, priority=0)
    first, _ = await q.dequeue(timeout=0.1)
    assert first == 'high'


@pytest.mark.asyncio
async def test_work_queue_acquire_release_lock_noop():
    '''acquire_lock always returns True; release_lock is a no-op.'''
    q = AsyncioWorkQueue()
    assert await q.acquire_lock('k') is True
    await q.release_lock('k')


@pytest.mark.asyncio
async def test_work_queue_store_and_get_result():
    '''store_result then get_result returns the stored dict.'''
    q = AsyncioWorkQueue()
    await q.store_result('req-1', {'data': 42})
    assert await q.get_result('req-1') == {'data': 42}


@pytest.mark.asyncio
async def test_work_queue_get_result_missing_returns_none():
    '''get_result returns None for an unknown request_id.'''
    q = AsyncioWorkQueue()
    assert await q.get_result('no-such-id') is None
