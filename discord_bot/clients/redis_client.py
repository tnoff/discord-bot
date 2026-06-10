import json

import redis.asyncio as aioredis

BUNDLE_KEY_PREFIX = 'discord_bot:bundle:'
BUNDLE_TTL_SECONDS = 86400  # 1 day — fallback expiry for orphaned bundles


def get_redis_client(url: str) -> aioredis.Redis:
    '''Return an async Redis client connected to *url*.'''
    return aioredis.from_url(url, decode_responses=True)


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
