'''
In-process asyncio implementations of BundleStore and WorkQueue.

Used when no Redis is configured (single-process / local-asyncio mode).
Locking methods are no-ops: single-process deployments have no cross-pod
contention so acquire_lock always succeeds and release_lock does nothing.
'''
import asyncio
import itertools

from discord_bot.interfaces.dispatch_protocols import BundleStore, WorkQueue
from discord_bot.interfaces.result_queue import DownloadResultQueue, SearchResultQueue


class AsyncioBundleStore(BundleStore):
    '''
    In-memory BundleStore backed by a plain dict.

    No persistence across process restarts; suitable for single-process deployments.
    '''

    def __init__(self):
        self._store: dict[str, dict] = {}

    async def save(self, key: str, bundle_dict: dict) -> None:
        self._store[key] = bundle_dict

    async def delete(self, key: str) -> None:
        self._store.pop(key, None)

    async def load(self, key: str) -> dict | None:
        return self._store.get(key)

    async def load_all(self) -> dict[str, dict]:
        return dict(self._store)


class AsyncioWorkQueue(WorkQueue):
    '''
    In-process WorkQueue backed by asyncio.PriorityQueue.

    Locking is a no-op: only one process and one event loop, so there is no
    cross-pod contention to guard against.  Results are stored in a plain dict
    for in-process fetch result delivery.
    '''

    def __init__(self):
        self._queue: asyncio.PriorityQueue = asyncio.PriorityQueue()
        self._seq = itertools.count()
        self._dedup: set[str] = set()
        # Latest payload per unique member, mirroring the Redis payload key so the
        # newest content wins on coalesced updates. Non-unique enqueue() items carry
        # their payload inline in the queue tuple and never appear here.
        self._payloads: dict[str, dict] = {}
        self._results: dict[str, dict] = {}

    async def enqueue(self, member: str, payload: dict, priority: int) -> None:
        await self._queue.put((priority, next(self._seq), member, payload))

    async def enqueue_unique(self, member: str, payload: dict, priority: int,
                             overwrite: bool = True) -> None:
        # Keep the newest payload (overwrite) unless this is a lock-retry re-enqueue
        # carrying a possibly-stale payload (overwrite=False), which must not clobber
        # a newer update that arrived after the original dequeue.
        if overwrite or member not in self._payloads:
            self._payloads[member] = payload
        if member not in self._dedup:
            self._dedup.add(member)
            # Payload is also stored inline as a fallback; _payloads holds the
            # authoritative latest value resolved at dequeue time.
            await self._queue.put((priority, next(self._seq), member, payload))

    async def dequeue(self, timeout: float = 1.0) -> tuple[str, dict] | None:
        try:
            _priority, _seq, member, payload = await asyncio.wait_for(
                self._queue.get(), timeout=timeout
            )
            self._dedup.discard(member)
            # Unique members store the live payload separately so coalesced updates
            # resolve to the latest content; fall back to the inline tuple payload.
            payload = self._payloads.pop(member, payload)
            return member, payload
        except asyncio.TimeoutError:
            return None

    async def acquire_lock(self, _bundle_key: str) -> bool:
        '''Single-process: no cross-pod contention; always succeeds.'''
        return True

    async def release_lock(self, _bundle_key: str) -> None:
        '''Single-process: no-op.'''

    async def store_result(self, request_id: str, result: dict) -> None:
        self._results[request_id] = result

    async def get_result(self, request_id: str) -> dict | None:
        return self._results.get(request_id)


class _AsyncioResultQueue:
    '''Shared asyncio.Queue backing for the bot-ready result queues.

    The download and search result queues are both FIFO asyncio.Queues that
    differ only in element type, so the put / get_nowait / depth / raw_queue
    plumbing lives here once and the two concrete queues just pin the ABC and
    element type.  Used in single-process deployments; ``raw_queue`` exposes the
    underlying asyncio.Queue so the cog's metric callback can read ``qsize()``
    synchronously.
    '''

    def __init__(self, queue: asyncio.Queue | None = None):
        self._queue = queue if queue is not None else asyncio.Queue()

    @property
    def raw_queue(self) -> asyncio.Queue:
        '''The wrapped asyncio.Queue — only meaningful in single-process.'''
        return self._queue

    async def put(self, item) -> None:
        '''Append an item to the back of the queue.'''
        self._queue.put_nowait(item)

    async def get_nowait(self):
        '''Pop the oldest item, or None if the queue is empty.'''
        try:
            return self._queue.get_nowait()
        except asyncio.QueueEmpty:
            return None

    async def depth(self) -> int:
        '''Return the number of items currently waiting in the queue.'''
        return self._queue.qsize()


class AsyncioDownloadResultQueue(_AsyncioResultQueue, DownloadResultQueue):
    '''In-memory DownloadResultQueue backed by asyncio.Queue (single-process).'''


class AsyncioSearchResultQueue(_AsyncioResultQueue, SearchResultQueue):
    '''In-memory SearchResultQueue backed by asyncio.Queue (single-process).'''
