'''
Redis-backed implementations of BundleStore and WorkQueue.

These require the redis optional dependency and a running RedisManager.
The inner RedisDispatchQueue is created lazily on first use so that
RedisWorkQueue can be constructed before redis_manager.start() is called.
'''
import json

import redis.asyncio as aioredis

from discord_bot.clients.redis_client import RedisManager
from discord_bot.interfaces.dispatch_protocols import BundleStore, WorkQueue
from discord_bot.utils.dispatch_queue import RedisDispatchQueue

BUNDLE_KEY_PREFIX = 'discord_bot:bundle:'
BUNDLE_TTL_SECONDS = 86400  # 1 day — fallback expiry for orphaned bundles


async def save_bundle(client: aioredis.Redis, key: str, bundle_dict: dict) -> None:
    '''Persist *bundle_dict* under *key* in Redis with a 1-day TTL.'''
    await client.set(f'{BUNDLE_KEY_PREFIX}{key}', json.dumps(bundle_dict), ex=BUNDLE_TTL_SECONDS)


async def delete_bundle(client: aioredis.Redis, key: str) -> None:
    '''Remove the bundle stored under *key* from Redis.'''
    await client.delete(f'{BUNDLE_KEY_PREFIX}{key}')


async def load_bundle(client: aioredis.Redis, key: str) -> dict | None:
    '''Return the bundle dict stored under *key*, or None if not found.'''
    raw = await client.get(f'{BUNDLE_KEY_PREFIX}{key}')
    return json.loads(raw) if raw else None


async def load_all_bundles(client: aioredis.Redis) -> dict[str, dict]:
    '''Return all persisted bundles keyed by their bundle key (prefix stripped).'''
    keys = [k async for k in client.scan_iter(f'{BUNDLE_KEY_PREFIX}*')]
    if not keys:
        return {}
    values = await client.mget(*keys)
    return {
        key[len(BUNDLE_KEY_PREFIX):]: json.loads(raw)
        for key, raw in zip(keys, values)
        if raw
    }


class RedisBundleStore(BundleStore):
    '''BundleStore backed by Redis via the shared RedisManager.'''

    def __init__(self, manager: RedisManager):
        self._manager = manager

    async def save(self, key: str, bundle_dict: dict) -> None:
        await save_bundle(self._manager.client, key, bundle_dict)

    async def delete(self, key: str) -> None:
        await delete_bundle(self._manager.client, key)

    async def load(self, key: str) -> dict | None:
        return await load_bundle(self._manager.client, key)

    async def load_all(self) -> dict[str, dict]:
        return await load_all_bundles(self._manager.client)


class RedisWorkQueue(WorkQueue):
    '''WorkQueue backed by RedisDispatchQueue.'''

    def __init__(self, manager: RedisManager, shard_id: int, process_id: str):
        self._manager = manager
        self._shard_id = shard_id
        self._process_id = process_id
        self._queue: RedisDispatchQueue | None = None

    def _get_queue(self) -> RedisDispatchQueue:
        if self._queue is None:
            self._queue = RedisDispatchQueue(self._manager.client, self._shard_id, self._process_id)
        return self._queue

    async def enqueue(self, member: str, payload: dict, priority: int) -> None:
        await self._get_queue().enqueue(member, payload, priority)

    async def enqueue_unique(self, member: str, payload: dict, priority: int) -> None:
        await self._get_queue().enqueue_unique(member, payload, priority)

    async def dequeue(self, timeout: float = 1.0) -> tuple[str, dict] | None:
        return await self._get_queue().dequeue(timeout)

    async def acquire_lock(self, bundle_key: str) -> bool:
        return await self._get_queue().acquire_lock(bundle_key)

    async def release_lock(self, bundle_key: str) -> None:
        await self._get_queue().release_lock(bundle_key)

    async def store_result(self, request_id: str, result: dict) -> None:
        await self._get_queue().store_result(request_id, result)

    async def get_result(self, request_id: str) -> dict | None:
        return await self._get_queue().get_result(request_id)
