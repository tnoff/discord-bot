import pytest

from discord_bot.utils.queue import Queue, PutsBlocked

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

    x.unblock()

    await x.put(5)
    result = await x.get()
    assert result == 5

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
