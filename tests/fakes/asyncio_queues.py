'''
In-memory asyncio implementations of BundleStore, WorkQueue and the result
queues. Test fakes: production pods always use the Redis-backed versions.
Locking methods are no-ops, so acquire_lock always succeeds and release_lock
does nothing.
'''
import asyncio
import itertools

import fakeredis.aioredis

from discord_core.clients.redis_client import RedisManager
from discord_core.interfaces.dispatch_protocols import BundleStore, WorkQueue
from discord_core.interfaces.result_queue import DownloadResultQueue, SearchResultQueue

from discord_broker.servers.broker_server import BrokerHttpServer
from discord_broker.workers.broker_registry import RedisBrokerRegistry
from discord_broker.workers.guild_queue import GuildQueueBroker
from discord_broker.workers.guild_queue_registry import GuildQueueRegistry
from discord_broker.workers.redis_broker import RedisBroker


class AsyncioBundleStore(BundleStore):
    '''
    In-memory BundleStore backed by a plain dict.

    No persistence across process restarts; for tests.
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
    In-memory WorkQueue backed by asyncio.PriorityQueue.

    Locking is a no-op: one process and one event loop, so there is no
    cross-pod contention to guard against.  Results are stored in a plain dict
    for fetch result delivery.
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
        '''No cross-pod contention in a test; always succeeds.'''
        return True

    async def release_lock(self, _bundle_key: str) -> None:
        '''No-op: nothing else holds the lock.'''

    async def store_result(self, request_id: str, result: dict) -> None:
        self._results[request_id] = result

    async def get_result(self, request_id: str) -> dict | None:
        return self._results.get(request_id)


class _AsyncioResultQueue:
    '''Shared asyncio.Queue backing for the bot-ready result queues.

    The download and search result queues are both FIFO asyncio.Queues that
    differ only in element type, so the put / get_nowait / depth / raw_queue
    plumbing lives here once and the two concrete queues just pin the ABC and
    element type.  ``raw_queue`` exposes the
    underlying asyncio.Queue so a test can read ``qsize()`` synchronously.
    '''

    def __init__(self, queue: asyncio.Queue | None = None):
        self._queue = queue if queue is not None else asyncio.Queue()

    @property
    def raw_queue(self) -> asyncio.Queue:
        '''The wrapped asyncio.Queue.'''
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
    '''In-memory DownloadResultQueue backed by asyncio.Queue.'''


class AsyncioSearchResultQueue(_AsyncioResultQueue, SearchResultQueue):
    '''In-memory SearchResultQueue backed by asyncio.Queue.'''


def make_broker_http_server(broker, **kwargs):
    '''BrokerHttpServer over in-memory result queues unless the test passes its own.'''
    kwargs.setdefault('result_queue', AsyncioDownloadResultQueue())
    kwargs.setdefault('search_result_queue', AsyncioSearchResultQueue())
    if 'guild_queue' not in kwargs:
        kwargs['guild_queue'] = make_guild_queue_broker()
    return BrokerHttpServer(broker, **kwargs)


def make_guild_queue_for(broker, dispatcher=None):
    '''A GuildQueueBroker over `broker` (any engine with get_entry/get_entries/checkout/release/
    remove), with its queue state on a fresh fakeredis.

    This is how the in-process test stack gets the real queue next to the AsyncioBroker double
    that holds the media entries.
    '''
    manager = RedisManager.from_client(fakeredis.aioredis.FakeRedis(decode_responses=True))
    return GuildQueueBroker(broker, GuildQueueRegistry(manager), dispatcher)


def make_guild_queue_broker(bucket_name: str = 'test-bucket', dispatcher=None):
    '''A GuildQueueBroker over a RedisBroker on fakeredis -- the real engine, no Redis server.

    Unlike the AsyncioBroker double the rest of this module wraps, there is no asyncio stand-in
    for the guild queue: its behavior is Lua scripts, which only a Redis (or fakeredis) runs, so
    a double would test a different implementation.  Tests that need the media half of the
    broker too can reach it as `.broker`.
    '''
    manager = RedisManager.from_client(fakeredis.aioredis.FakeRedis(decode_responses=True))
    broker = RedisBroker(RedisBrokerRegistry(manager), bucket_name=bucket_name)
    guild_queue = GuildQueueBroker(broker, GuildQueueRegistry(manager), dispatcher)
    guild_queue.broker = broker
    return guild_queue
