'''
The tests the download-result queues share.

RedisDownloadResultQueue (what production runs) and AsyncioDownloadResultQueue (its test double)
are one contract, so the cases that do not depend on the backing store are stated once. A test
module subclasses ``DownloadResultQueueSuite`` and supplies ``make_queue``.
'''
import pytest

from tests.helpers import minimal_download_result


class DownloadResultQueueSuite:
    '''Cases every download-result queue must pass. Not collected itself.'''

    def make_queue(self):
        '''A fresh, empty queue.'''
        raise NotImplementedError('make_queue')

    @pytest.mark.asyncio
    async def test_get_nowait_on_an_empty_queue_returns_none(self):
        '''get_nowait returns None when there is nothing queued.'''
        assert await self.make_queue().get_nowait() is None

    @pytest.mark.asyncio
    async def test_depth_drops_as_items_are_popped(self):
        '''depth() counts what is queued and drops as items are popped.'''
        q = self.make_queue()
        assert await q.depth() == 0
        await q.put(minimal_download_result())
        await q.put(minimal_download_result())
        assert await q.depth() == 2
        await q.get_nowait()
        assert await q.depth() == 1
