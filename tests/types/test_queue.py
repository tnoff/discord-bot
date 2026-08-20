from asyncio import QueueFull

import pytest

from discord_bot.types.queue import (Queue, PutsBlocked, submit_rejection_status,
                                      SUBMIT_REJECTION_BY_STATUS)

@pytest.mark.asyncio
async def test_block():
    x = Queue()
    x.block()
    with pytest.raises(PutsBlocked) as exc:
        x.put_nowait(5)
    assert 'Puts Blocked on Queue' in str(exc.value)

    with pytest.raises(PutsBlocked) as exc:
        await x.put(5)
    assert 'Puts Blocked on Queue' in str(exc.value)

@pytest.mark.asyncio
async def test_size_and_clear():
    x = Queue()
    await x.put(5)
    await x.put(10)

    assert x.size() == 2

    x.clear()
    assert x.size() == 0

@pytest.mark.asyncio
async def test_bump_item():
    x = Queue()
    await x.put(5)
    await x.put(10)
    await x.put(15)

    x.bump_item(2)

    result = await x.get()
    assert result == 10

@pytest.mark.asyncio
async def test_bump_item_non_exist():
    x = Queue()
    await x.put(5)
    await x.put(10)
    await x.put(15)

    result = x.bump_item(5)
    assert result is None

@pytest.mark.asyncio
async def test_shuffle():
    x = Queue()
    await x.put(5)
    await x.put(10)
    await x.put(15)

    assert x.shuffle() is True

    items = x.items()
    assert 5 in items
    assert 10 in items
    assert 15 in items
    assert len(items) == 3


def test_submit_rejection_status_maps_the_contract():
    '''Both queue-contract exceptions get a 4xx, and the reverse map round-trips.
    4xx matters: async_retry_broker_command retries 5xx, and a refusal is
    deterministic, so a 5xx would burn three retries for the same answer.'''
    for exception in (PutsBlocked('blocked'), QueueFull()):
        status = submit_rejection_status(exception)
        assert 400 <= status < 500
        assert SUBMIT_REJECTION_BY_STATUS[status] is type(exception)


def test_submit_rejection_status_ignores_other_errors():
    '''Anything outside the queue contract is a genuine fault and must keep
    travelling as a 500, not get quietly downgraded to a refusal.'''
    assert submit_rejection_status(RuntimeError('boom')) is None
