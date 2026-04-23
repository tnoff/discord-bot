'''
Redis-backed registry for MediaBroker state.

Stores BrokerEntry data as JSON in Redis so that multiple broker pods can
share the same state. Uses SET NX locks for atomic checkout transitions.

Key schema:
    discord_bot:broker:entry:{uuid}  →  JSON blob with zone, request, download
    discord_bot:broker:lock:{uuid}   →  ephemeral SET NX lock (10s TTL)
'''
import json
import logging

import redis.asyncio as aioredis

from discord_bot.clients.redis_client import RedisManager

logger = logging.getLogger(__name__)

ENTRY_KEY_PREFIX = 'discord_bot:broker:entry:'
LOCK_KEY_PREFIX = 'discord_bot:broker:lock:'
ENTRY_TTL_SECONDS = 86400  # 24h — stale cleanup if broker restarts without releasing
LOCK_TTL_SECONDS = 10


class RedisBrokerRegistry:
    '''
    Async CRUD for broker entries stored as JSON in Redis.

    Each entry is keyed by media request UUID. Checkout transitions are
    protected by a short-lived SET NX lock so multiple broker pods cannot
    check out the same item simultaneously.
    '''

    def __init__(self, manager: RedisManager):
        self._manager = manager

    @property
    def _client(self) -> aioredis.Redis:
        return self._manager.client

    async def get_entry(self, uuid: str) -> dict | None:
        '''Return the stored entry dict for uuid, or None if not present.'''
        raw = await self._client.get(f'{ENTRY_KEY_PREFIX}{uuid}')
        return json.loads(raw) if raw else None

    async def set_entry(self, uuid: str, data: dict) -> None:
        '''Upsert an entry with a 24h TTL.'''
        await self._client.set(
            f'{ENTRY_KEY_PREFIX}{uuid}', json.dumps(data), ex=ENTRY_TTL_SECONDS
        )

    async def delete_entry(self, uuid: str) -> None:
        '''Remove an entry from Redis.'''
        await self._client.delete(f'{ENTRY_KEY_PREFIX}{uuid}')

    async def all_entries(self) -> list[dict]:
        '''
        Return all current broker entries.

        Used for eviction scans (can_evict_base, get_checked_out_by). Result
        is a point-in-time snapshot; entries may change between calls.
        '''
        keys = [k async for k in self._client.scan_iter(f'{ENTRY_KEY_PREFIX}*')]
        if not keys:
            return []
        values = await self._client.mget(*keys)
        result = []
        for raw in values:
            if raw:
                try:
                    result.append(json.loads(raw))
                except Exception:
                    pass
        return result

    async def atomic_checkout(self, uuid: str, guild_id: int) -> bool:
        '''
        Atomically transition an AVAILABLE entry to CHECKED_OUT.

        Acquires a short-lived SET NX lock, verifies zone == "available",
        then writes the updated entry. Returns True if the checkout succeeded,
        False if the entry is absent, not AVAILABLE, or the lock is contested.
        '''
        lock_key = f'{LOCK_KEY_PREFIX}{uuid}'
        entry_key = f'{ENTRY_KEY_PREFIX}{uuid}'

        acquired = await self._client.set(lock_key, '1', nx=True, ex=LOCK_TTL_SECONDS)
        if not acquired:
            logger.warning('atomic_checkout: could not acquire lock for %s', uuid)
            return False
        try:
            raw = await self._client.get(entry_key)
            if raw is None:
                return False
            data = json.loads(raw)
            if data.get('zone') != 'available':
                return False
            data['zone'] = 'checked_out'
            data['checked_out_by'] = guild_id
            await self._client.set(entry_key, json.dumps(data), ex=ENTRY_TTL_SECONDS)
            return True
        finally:
            await self._client.delete(lock_key)
