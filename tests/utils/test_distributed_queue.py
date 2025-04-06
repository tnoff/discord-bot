from asyncio import QueueEmpty

import pytest

from discord_bot.utils.distributed_queue import DistributedQueue
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

    assert x.get_queue_size('123') == 2

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
    assert x.get_queue_size('123') == 0

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
