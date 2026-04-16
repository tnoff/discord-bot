'''Tests for RedisDispatchQueue — the Redis sorted-set work queue.'''
import json
from unittest.mock import AsyncMock

import pytest

from discord_bot.utils.dispatch_queue import RedisDispatchQueue, dispatch_request_id

_QUEUE_KEY = 'discord_bot:dispatch:queue:0'


# ---------------------------------------------------------------------------
# dispatch_request_id (module-level helper)
# ---------------------------------------------------------------------------

def test_dispatch_request_id_is_stable():
    '''Same params always produce the same ID.'''
    assert dispatch_request_id({'a': 1, 'b': 2}) == dispatch_request_id({'b': 2, 'a': 1})


def test_dispatch_request_id_differs_for_different_params():
    '''Different params produce different IDs.'''
    assert dispatch_request_id({'a': 1}) != dispatch_request_id({'a': 2})


# ---------------------------------------------------------------------------
# Static key helpers (lines 52, 57, 62)
# ---------------------------------------------------------------------------

def test_payload_key():
    '''payload_key returns the expected namespaced key.'''
    assert RedisDispatchQueue.payload_key('send:uuid-1') == 'discord_bot:dispatch:payload:send:uuid-1'


def test_result_key():
    '''result_key returns the expected namespaced key.'''
    assert RedisDispatchQueue.result_key('req-abc') == 'discord_bot:dispatch:result:req-abc'


def test_lock_key():
    '''lock_key returns the expected namespaced key.'''
    assert RedisDispatchQueue.lock_key('bundle-k') == 'discord_bot:dispatch:executing:bundle-k'


# ---------------------------------------------------------------------------
# _score (line 66)
# ---------------------------------------------------------------------------

def test_score_higher_urgency_yields_lower_score(dispatch_queue):
    '''priority=0 (HIGH) yields a lower score than priority=1 (NORMAL).'''
    assert dispatch_queue._score(0) < dispatch_queue._score(1)  # pylint: disable=protected-access


# ---------------------------------------------------------------------------
# enqueue_unique (lines 80-83)
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_enqueue_unique_stores_payload_and_adds_member(dispatch_queue, redis_client):
    '''enqueue_unique writes the payload and adds the member to the sorted set.'''
    await dispatch_queue.enqueue_unique('mutable:k1', {'key': 'k1'}, priority=0)
    raw = await redis_client.get(RedisDispatchQueue.payload_key('mutable:k1'))
    assert json.loads(raw) == {'key': 'k1'}
    count = await redis_client.zcard(_QUEUE_KEY)
    assert count == 1


@pytest.mark.asyncio
async def test_enqueue_unique_nx_preserves_existing_position(dispatch_queue, redis_client):
    '''A second enqueue_unique for the same member does not change its queue position (NX).'''
    await dispatch_queue.enqueue_unique('mutable:dup', {'v': 1}, priority=0)
    score1 = await redis_client.zscore(_QUEUE_KEY, 'mutable:dup')
    await dispatch_queue.enqueue_unique('mutable:dup', {'v': 2}, priority=1)
    score2 = await redis_client.zscore(_QUEUE_KEY, 'mutable:dup')
    assert score1 == score2


# ---------------------------------------------------------------------------
# enqueue (lines 90-93)
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_enqueue_stores_payload_and_adds_member(dispatch_queue, redis_client):
    '''enqueue writes the payload and always adds a new sorted-set entry.'''
    await dispatch_queue.enqueue('send:uuid-1', {'content': 'hi'}, priority=0)
    raw = await redis_client.get(RedisDispatchQueue.payload_key('send:uuid-1'))
    assert json.loads(raw) == {'content': 'hi'}
    count = await redis_client.zcard(_QUEUE_KEY)
    assert count == 1


# ---------------------------------------------------------------------------
# dequeue (lines 106-119)
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_dequeue_returns_member_and_payload(dispatch_queue):
    '''dequeue returns (member, payload) for an enqueued item.'''
    await dispatch_queue.enqueue('send:uuid-2', {'content': 'hello'}, priority=0)
    result = await dispatch_queue.dequeue(timeout=1.0)
    assert result is not None
    member, payload = result
    assert member == 'send:uuid-2'
    assert payload == {'content': 'hello'}


@pytest.mark.asyncio
async def test_dequeue_removes_item_from_queue(dispatch_queue, redis_client):
    '''dequeue removes the item from the sorted set.'''
    await dispatch_queue.enqueue('send:uuid-3', {'x': 1}, priority=0)
    await dispatch_queue.dequeue(timeout=1.0)
    count = await redis_client.zcard(_QUEUE_KEY)
    assert count == 0


@pytest.mark.asyncio
async def test_dequeue_returns_none_when_empty(dispatch_queue, mocker):
    '''dequeue returns None when the queue is empty (bzpopmin times out).'''
    mocker.patch.object(dispatch_queue._redis, 'bzpopmin',  # pylint: disable=protected-access
                        new=AsyncMock(return_value=None))
    result = await dispatch_queue.dequeue()
    assert result is None


@pytest.mark.asyncio
async def test_dequeue_returns_none_when_payload_expired(dispatch_queue, redis_client):
    '''dequeue returns None when the payload key has expired after bzpopmin.'''
    await dispatch_queue.enqueue('orphan:1', {'data': 'gone'}, priority=0)
    # Simulate payload expiry by deleting it before dequeue runs
    await redis_client.delete(RedisDispatchQueue.payload_key('orphan:1'))
    result = await dispatch_queue.dequeue(timeout=1.0)
    assert result is None


# ---------------------------------------------------------------------------
# acquire_lock / release_lock (lines 130, 133, 137)
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_acquire_lock_returns_true_when_free(dispatch_queue):
    '''acquire_lock returns True for an uncontested lock.'''
    assert await dispatch_queue.acquire_lock('bundle-k') is True


@pytest.mark.asyncio
async def test_acquire_lock_returns_false_when_held(dispatch_queue):
    '''acquire_lock returns False when another pod holds the lock.'''
    await dispatch_queue.acquire_lock('bundle-k')
    assert await dispatch_queue.acquire_lock('bundle-k') is False


@pytest.mark.asyncio
async def test_release_lock_allows_reacquisition(dispatch_queue):
    '''release_lock deletes the lock so it can be re-acquired.'''
    await dispatch_queue.acquire_lock('bundle-k')
    await dispatch_queue.release_lock('bundle-k')
    assert await dispatch_queue.acquire_lock('bundle-k') is True


# ---------------------------------------------------------------------------
# store_result / get_result (lines 145, 151-152)
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_store_and_get_result_roundtrip(dispatch_queue):
    '''store_result and get_result round-trip a result dict.'''
    payload = {'guild_id': 1, 'channel_id': 2, 'messages': []}
    await dispatch_queue.store_result('req-1', payload)
    result = await dispatch_queue.get_result('req-1')
    assert result == payload


@pytest.mark.asyncio
async def test_get_result_returns_none_when_missing(dispatch_queue):
    '''get_result returns None for an unknown request_id.'''
    result = await dispatch_queue.get_result('nonexistent-req')
    assert result is None
