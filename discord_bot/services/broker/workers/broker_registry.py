'''
Redis-backed registry for MediaBroker state.

Stores BrokerEntry data as JSON in Redis so that multiple broker pods can
share the same state. Uses SET NX locks for atomic checkout transitions
and a separate per-bundle lock for bundle read-modify-write serialization.

Key schema:
    discord_bot:broker:entry:{uuid}        →  JSON blob with zone, request, download
    discord_bot:broker:lock:{uuid}         →  ephemeral SET NX checkout lock (10s TTL)
    discord_bot:broker:bundle:{uuid}       →  JSON BundleState (counters, requests, banner)
    discord_bot:broker:bundlelock:{uuid}   →  ephemeral SET NX bundle-mutation lock (10s TTL)
'''
import asyncio
import contextlib
import json
import logging
import uuid as uuid_module

import redis.asyncio as aioredis

from discord_bot.clients.redis_client import RedisManager

logger = logging.getLogger(__name__)

ENTRY_KEY_PREFIX = 'discord_bot:broker:entry:'
LOCK_KEY_PREFIX = 'discord_bot:broker:lock:'
BUNDLE_KEY_PREFIX = 'discord_bot:broker:bundle:'
BUNDLE_LOCK_KEY_PREFIX = 'discord_bot:broker:bundlelock:'
ENTRY_TTL_SECONDS = 86400  # 24h — stale cleanup if broker restarts without releasing
# in_flight entries are transient (a request still downloading). Each lifecycle
# status update rewrites the entry, refreshing this TTL — so it acts as an
# inactivity timeout: a request that stalls without ever reaching a terminal
# event (e.g. its download result never routes back) eventually expires instead
# of squatting in the registry indefinitely. Held at the full 24h for now — a
# deep download queue can legitimately keep a request in_flight for a long time
# before it's serviced, so we don't want to evict a still-valid entry. The
# terminal DISCARDED/FAILED deletion is what actually stops the leak; this is a
# backstop that can be tuned down once we have a feel for real queue depth.
IN_FLIGHT_TTL_SECONDS = 86400  # 24h
LOCK_TTL_SECONDS = 10
BUNDLE_TTL_SECONDS = 86400  # 24h — bundles share the entry TTL
SESSION_KEY_PREFIX = 'discord_bot:broker:session:'
# Player sessions share the entry TTL for a concrete reason: a session replays its
# queue through the normal enqueue path, which resolves each request against the
# media cache.  Outliving the entries it references would not break a resume (an
# evicted request simply re-downloads), but there is no value in holding a session
# past the point where every request in it has gone cold.
SESSION_TTL_SECONDS = 86400  # 24h
BUNDLE_LOCK_TTL_SECONDS = 10
BUNDLE_LOCK_POLL_INTERVAL_SECONDS = 0.05
BUNDLE_LOCK_WAIT_SECONDS = 5.0


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
        '''Upsert an entry. in_flight entries get a short TTL (inactivity
        timeout); anything past in_flight (available / checked_out) gets the
        full 24h.'''
        ttl = IN_FLIGHT_TTL_SECONDS if data.get('zone') == 'in_flight' else ENTRY_TTL_SECONDS
        await self._client.set(
            f'{ENTRY_KEY_PREFIX}{uuid}', json.dumps(data), ex=ttl
        )

    async def delete_entry(self, uuid: str) -> None:
        '''Remove an entry from Redis.'''
        await self._client.delete(f'{ENTRY_KEY_PREFIX}{uuid}')

    async def _scan_prefix(self, prefix: str, label: str) -> list[dict]:
        '''SCAN every key with prefix and return their JSON-decoded values.

        Skips entries whose JSON is unparseable; the warning identifies which
        kind of record (entry / bundle) so logs are searchable.
        '''
        keys = [k async for k in self._client.scan_iter(f'{prefix}*')]
        if not keys:
            return []
        values = await self._client.mget(*keys)
        result = []
        for key, raw in zip(keys, values):
            if raw:
                try:
                    result.append(json.loads(raw))
                except json.JSONDecodeError as e:
                    logger.warning('Skipping corrupt %s %s: %s', label, key, e)
        return result

    async def all_entries(self) -> list[dict]:
        '''
        Return all current broker entries.

        Used for eviction scans (can_evict_base, get_checked_out_by). Result
        is a point-in-time snapshot; entries may change between calls.
        '''
        return await self._scan_prefix(ENTRY_KEY_PREFIX, 'broker entry')

    async def get_bundle(self, uuid: str) -> dict | None:
        '''Return the stored bundle dict for uuid, or None if not present.'''
        raw = await self._client.get(f'{BUNDLE_KEY_PREFIX}{uuid}')
        return json.loads(raw) if raw else None

    async def set_bundle(self, uuid: str, data: dict) -> None:
        '''Upsert a bundle with a 24h TTL.'''
        await self._client.set(
            f'{BUNDLE_KEY_PREFIX}{uuid}', json.dumps(data), ex=BUNDLE_TTL_SECONDS
        )

    async def delete_bundle(self, uuid: str) -> None:
        '''Remove a bundle from Redis.'''
        await self._client.delete(f'{BUNDLE_KEY_PREFIX}{uuid}')

    async def all_bundles(self) -> list[dict]:
        '''Return every stored bundle dict — point-in-time snapshot.

        Used for guild-scoped lookups (list bundles for a guild on cleanup).
        '''
        return await self._scan_prefix(BUNDLE_KEY_PREFIX, 'bundle')

    async def get_session(self, guild_id: int) -> dict | None:
        '''Return the stored player session for guild_id, or None if not present.'''
        raw = await self._client.get(f'{SESSION_KEY_PREFIX}{guild_id}')
        return json.loads(raw) if raw else None

    async def set_session(self, guild_id: int, data: dict) -> None:
        '''Upsert a player session with a 24h TTL.'''
        await self._client.set(
            f'{SESSION_KEY_PREFIX}{guild_id}', json.dumps(data), ex=SESSION_TTL_SECONDS
        )

    async def delete_session(self, guild_id: int) -> None:
        '''Remove a player session from Redis.'''
        await self._client.delete(f'{SESSION_KEY_PREFIX}{guild_id}')

    async def all_sessions(self) -> list[dict]:
        '''Return every stored player session dict — point-in-time snapshot.

        Used once per startup to find guilds with playback to resume.
        '''
        return await self._scan_prefix(SESSION_KEY_PREFIX, 'player session')

    @contextlib.asynccontextmanager
    async def bundle_lock(self, bundle_uuid: str):
        '''Async context manager serialising bundle read-modify-write.

        Without this guard, register_request and the download worker's
        IN_PROGRESS / COMPLETED pushes load the same bundle snapshot,
        both modify it, both write — last writer wins and whichever
        request was added in the losing branch is silently dropped.

        Uses SET NX EX with a short TTL so a crashed lock holder doesn't
        deadlock the bundle.  Token-tagged release prevents a slow holder
        from deleting someone else's lock if its TTL expired.
        '''
        lock_key = f'{BUNDLE_LOCK_KEY_PREFIX}{bundle_uuid}'
        token = uuid_module.uuid4().hex
        deadline = asyncio.get_running_loop().time() + BUNDLE_LOCK_WAIT_SECONDS
        while True:
            acquired = await self._client.set(
                lock_key, token, nx=True, ex=BUNDLE_LOCK_TTL_SECONDS,
            )
            if acquired:
                break
            if asyncio.get_running_loop().time() >= deadline:
                logger.warning('bundle_lock: timed out waiting for %s', bundle_uuid)
                # Fall through and proceed without the lock — better to risk
                # a torn write than to drop the operation entirely.
                token = None
                break
            await asyncio.sleep(BUNDLE_LOCK_POLL_INTERVAL_SECONDS)
        try:
            yield
        finally:
            if token is not None:
                # Only delete if we still own the lock — skip if our TTL
                # expired and another holder took it.
                current = await self._client.get(lock_key)
                if current == token:
                    await self._client.delete(lock_key)

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
