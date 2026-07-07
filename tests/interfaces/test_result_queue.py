'''Tests for the DownloadResultQueue ABC.

DownloadResultQueue ships unwired in this MR; the concrete impls
(AsyncioDownloadResultQueue / RedisDownloadResultQueue) land later.  These
tests just lock the abstract surface — it cannot be instantiated directly and
a trivial concrete subclass round-trips.
'''
import pytest

from discord_bot.interfaces.result_queue import DownloadResultQueue


def test_download_result_queue_is_abstract():
    '''The ABC cannot be instantiated without overriding put / get_nowait.'''
    with pytest.raises(TypeError):
        DownloadResultQueue()  # pylint: disable=abstract-class-instantiated


@pytest.mark.asyncio
async def test_concrete_subclass_round_trips():
    '''A minimal concrete impl satisfies the interface and round-trips items.'''
    class _MemQueue(DownloadResultQueue):
        def __init__(self):
            self._items = []

        async def put(self, result):
            self._items.append(result)

        async def get_nowait(self):
            return self._items.pop(0) if self._items else None

        async def depth(self):
            return len(self._items)

    queue = _MemQueue()
    assert await queue.get_nowait() is None
    assert await queue.depth() == 0
    sentinel = object()
    await queue.put(sentinel)
    assert await queue.depth() == 1
    assert await queue.get_nowait() is sentinel
    assert await queue.get_nowait() is None
