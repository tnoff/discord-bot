"""Unit tests for InProcessDownloadQueue."""
from asyncio import QueueEmpty, QueueFull

import pytest

from discord_bot.utils.download_queue import InProcessDownloadQueue
from discord_bot.utils.queue import PutsBlocked


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _queue(max_size=10) -> InProcessDownloadQueue:
    return InProcessDownloadQueue(max_size)


# ---------------------------------------------------------------------------
# put_nowait / get_nowait round-trip
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_put_get_single_guild():
    q = _queue()
    await q.put_nowait(1, 'a')
    result = await q.get_nowait()
    assert result == 'a'


@pytest.mark.asyncio
async def test_put_get_fifo_within_guild():
    q = _queue()
    for i in range(3):
        await q.put_nowait(1, i)
    for i in range(3):
        assert await q.get_nowait() == i


@pytest.mark.asyncio
async def test_get_nowait_empty_raises():
    q = _queue()
    with pytest.raises(QueueEmpty):
        await q.get_nowait()


@pytest.mark.asyncio
async def test_priority_ordering():
    """Higher priority guild items are returned before lower priority ones."""
    q = _queue()
    # guild 1 has default priority (100), guild 2 gets priority 200
    await q.put_nowait(1, 'low', priority=100)
    await q.put_nowait(2, 'high', priority=200)
    first = await q.get_nowait()
    assert first == 'high'
    second = await q.get_nowait()
    assert second == 'low'


# ---------------------------------------------------------------------------
# size
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_size_tracks_items():
    q = _queue()
    assert q.size(1) == 0
    await q.put_nowait(1, 'x')
    assert q.size(1) == 1
    await q.put_nowait(1, 'y')
    assert q.size(1) == 2
    await q.get_nowait()
    assert q.size(1) == 1


@pytest.mark.asyncio
async def test_size_unknown_guild_returns_zero():
    q = _queue()
    assert q.size(999) == 0


# ---------------------------------------------------------------------------
# block
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_block_prevents_put():
    q = _queue()
    await q.put_nowait(1, 'before')
    await q.block(1)
    with pytest.raises(PutsBlocked):
        await q.put_nowait(1, 'after')


@pytest.mark.asyncio
async def test_block_unknown_guild_returns_false():
    q = _queue()
    result = await q.block(999)
    assert result is False


@pytest.mark.asyncio
async def test_block_existing_guild_returns_true():
    q = _queue()
    await q.put_nowait(1, 'x')
    result = await q.block(1)
    assert result is True


@pytest.mark.asyncio
async def test_get_nowait_still_works_after_block():
    """Blocking only prevents puts; existing items can still be consumed."""
    q = _queue()
    await q.put_nowait(1, 'item')
    await q.block(1)
    assert await q.get_nowait() == 'item'


# ---------------------------------------------------------------------------
# clear_queue
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_clear_queue_unknown_guild_returns_empty():
    q = _queue()
    assert await q.clear_queue(999) == []


@pytest.mark.asyncio
async def test_clear_queue_returns_all_items():
    q = _queue()
    for i in range(3):
        await q.put_nowait(1, i)
    dropped = await q.clear_queue(1)
    assert sorted(dropped) == [0, 1, 2]
    assert q.size(1) == 0


@pytest.mark.asyncio
async def test_clear_queue_removes_guild_entry():
    q = _queue()
    await q.put_nowait(1, 'x')
    await q.clear_queue(1)
    assert 1 not in q.queues


@pytest.mark.asyncio
async def test_clear_queue_preserve_predicate_keeps_matching():
    q = _queue()
    await q.put_nowait(1, 'keep_me')
    await q.put_nowait(1, 'drop_me')
    dropped = await q.clear_queue(1, preserve_predicate=lambda x: x == 'keep_me')
    assert dropped == ['drop_me']
    # Kept item is still in the queue
    assert await q.get_nowait() == 'keep_me'


@pytest.mark.asyncio
async def test_clear_queue_preserve_predicate_all_kept():
    q = _queue()
    for i in range(3):
        await q.put_nowait(1, i)
    dropped = await q.clear_queue(1, preserve_predicate=lambda _: True)
    assert dropped == []
    assert q.size(1) == 3


@pytest.mark.asyncio
async def test_clear_queue_preserve_predicate_all_dropped():
    q = _queue()
    for i in range(3):
        await q.put_nowait(1, i)
    dropped = await q.clear_queue(1, preserve_predicate=lambda _: False)
    assert len(dropped) == 3
    assert q.size(1) == 0


@pytest.mark.asyncio
async def test_clear_queue_does_not_affect_other_guilds():
    q = _queue()
    await q.put_nowait(1, 'guild1')
    await q.put_nowait(2, 'guild2')
    await q.clear_queue(1)
    assert q.size(2) == 1
    assert await q.get_nowait() == 'guild2'


# ---------------------------------------------------------------------------
# capacity (QueueFull)
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_put_raises_queue_full_at_capacity():
    q = _queue(max_size=2)
    await q.put_nowait(1, 'a')
    await q.put_nowait(1, 'b')
    with pytest.raises(QueueFull):
        await q.put_nowait(1, 'c')


# ---------------------------------------------------------------------------
# queues property (test-inspection API)
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_queues_property_reflects_state():
    q = _queue()
    assert 1 not in q.queues
    await q.put_nowait(1, 'x')
    assert 1 in q.queues


@pytest.mark.asyncio
async def test_queues_property_empty_after_drain():
    q = _queue()
    await q.put_nowait(1, 'x')
    await q.get_nowait()
    assert 1 not in q.queues
