'''
In-process asyncio implementations of BundleStore and WorkQueue.

Used when no Redis is configured (single-process / local-asyncio mode).
Locking methods are no-ops: single-process deployments have no cross-pod
contention so acquire_lock always succeeds and release_lock does nothing.
'''
import asyncio
import itertools

from discord_bot.interfaces.dispatch_protocols import BundleStore, WorkQueue


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
        self._results: dict[str, dict] = {}

    async def enqueue(self, member: str, payload: dict, priority: int) -> None:
        await self._queue.put((priority, next(self._seq), member, payload))

    async def enqueue_unique(self, member: str, payload: dict, priority: int) -> None:
        if member not in self._dedup:
            self._dedup.add(member)
            await self._queue.put((priority, next(self._seq), member, payload))

    async def dequeue(self, timeout: float = 1.0) -> tuple[str, dict] | None:
        try:
            _priority, _seq, member, payload = await asyncio.wait_for(
                self._queue.get(), timeout=timeout
            )
            self._dedup.discard(member)
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
