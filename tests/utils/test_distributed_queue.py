from asyncio import QueueEmpty
from datetime import datetime, timezone

import pytest

from discord_bot.utils.distributed_queue import DistributedQueue, DistributedQueueItem
from discord_bot.utils.queue import PutsBlocked

def test_block():
    x = DistributedQueue(10)
    x.put_nowait('123', 5)
    x.block('123')
    with pytest.raises(PutsBlocked) as exc:
        x.put_nowait('123', 4)
    assert 'Puts Blocked on Queue' in str(exc.value)

    result = x.block('234')
    assert result is False

def test_get():
    x = DistributedQueue(10)
    x.put_nowait('123', 5)
    result = x.get_nowait()
    assert result == 5

    x.put_nowait('123', 10)
    x.put_nowait('234', 15)
    x.put_nowait('123', 20)

    assert x.size('123') == 2

    result = x.get_nowait()
    assert result == 10
    result = x.get_nowait()
    assert result == 15
    result = x.get_nowait()
    assert result == 20

    with pytest.raises(QueueEmpty) as exc:
        x.get_nowait()
    assert 'No items in queue' in str(exc.value)

    assert not x.queues
    assert x.size('123') == 0

def test_clear():
    x = DistributedQueue(10)
    assert not x.clear_queue('123')

    x.put_nowait('123', 5)
    x.put_nowait('123', 10)
    results = x.clear_queue('123')
    assert results == [5, 10]

    # Assert guild was removed
    assert '123' not in x.queues

def test_get_with_priority():
    x = DistributedQueue(10)
    x.put_nowait('guild-123', 5)
    x.put_nowait('guild-234', 10)
    x.put_nowait('guild-345', 15, priority=150)
    x.put_nowait('guild-456', 20, priority=200)

    result = x.get_nowait()
    assert result == 20
    result = x.get_nowait()
    assert result == 15
    result = x.get_nowait()
    assert result == 5
    result = x.get_nowait()
    assert result == 10


def test_get_skips_empty_internal_queue():
    """get_nowait skips guild entries whose inner queue is empty (line 87)"""
    x = DistributedQueue(10)
    # Manually insert an empty queue entry — this state can't arise through the
    # public API but the code defends against it
    x.queues['empty-guild'] = DistributedQueueItem(datetime.now(timezone.utc), 10, 100)
    x.put_nowait('real-guild', 'value')
    result = x.get_nowait()
    assert result == 'value'


def test_get_skips_lower_priority_guild():
    """get_nowait ignores a guild whose priority is lower than the current best (line 96)"""
    x = DistributedQueue(10)
    # high-priority guild inserted first so it is iterated first
    x.put_nowait('high', 'first', priority=200)
    x.put_nowait('low', 'second', priority=100)
    # The low-priority guild must be skipped via the line-96 continue
    result = x.get_nowait()
    assert result == 'first'
    result = x.get_nowait()
    assert result == 'second'


def test_clear_with_preserve_predicate_partial():
    """clear_queue with predicate keeps matching items and returns the rest"""
    x = DistributedQueue(10)
    x.put_nowait('guild1', 1)
    x.put_nowait('guild1', 2)
    x.put_nowait('guild1', 3)
    dropped = x.clear_queue('guild1', preserve_predicate=lambda item: item % 2 == 0)
    assert dropped == [1, 3]
    assert x.size('guild1') == 1
    assert x.get_nowait() == 2


def test_clear_with_preserve_predicate_all_dropped():
    """clear_queue removes the guild entry when no items are kept"""
    x = DistributedQueue(10)
    x.put_nowait('guild1', 1)
    x.put_nowait('guild1', 2)
    dropped = x.clear_queue('guild1', preserve_predicate=lambda item: False)
    assert dropped == [1, 2]
    assert 'guild1' not in x.queues


def test_clear_with_preserve_predicate_all_kept():
    """clear_queue returns empty list and preserves the queue when all items match"""
    x = DistributedQueue(10)
    x.put_nowait('guild1', 1)
    x.put_nowait('guild1', 2)
    dropped = x.clear_queue('guild1', preserve_predicate=lambda item: True)
    assert not dropped
    assert x.size('guild1') == 2
