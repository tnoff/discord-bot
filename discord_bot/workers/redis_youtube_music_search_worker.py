'''
Redis-backed YouTube-Music search engine for HA.

RedisYoutubeMusicSearchWorker is the multi-pod YoutubeMusicSearchWorkerBase impl:
it supplies the same per-guild input-queue surface as
AsyncioYoutubeMusicSearchWorker but backs it with Redis instead of an in-process
DistributedQueue, so several search pods can share one work queue and one 429
backoff window.  All the ytmusicapi resolution logic lives on
YoutubeMusicSearchWorkerBase; this class only supplies the Redis queue surface +
the shared backoff / failure state.

Simpler than RedisDownloadWorker: search has a single per-guild queue (no DIRECT
fast-path) and a single global 429 backoff window.  The search pod is not
VPN-bound, so there is no per-egress bucketing — one shared ``wait_until`` key is
observed by every pod.

Redis schema (all keys under ``discord_bot:ytmusic_search:``):
    request:{uuid}      STRING  JSON MediaRequest (TTL fallback)
    guild:{gid}         ZSET    request_uuid -> priority*1e9 + submitted_ts
    guild:{gid}:blocked STRING  '1' when blocked
    guilds              ZSET    guild_id -> last_popped_ts (round-robin)
    wait_until          STRING  epoch ts; shared cross-pod 429 backoff window
    failures            ZSET    failure_uuid -> ts (ZCARD ~ backoff exponent)

Guilds round-robin by ``last_popped_ts`` under a short-lived token-tagged SET NX
pop-lock so no guild starves another and no two pods pop the same request.  The
pinned fakeredis test stack has no Lua, so SET NX is the atomicity primitive
rather than an EVAL script — mirrors RedisBrokerRegistry / RedisDownloadWorker.
'''
import asyncio
import json
import uuid as uuid_module
from asyncio import QueueEmpty
from datetime import datetime, timezone
from random import randint, seed
from time import time
from typing import Callable

from discord_bot.clients.redis_client import RedisManager
from discord_bot.interfaces.youtube_music_search_protocols import YoutubeMusicSearchWorkerBase
from discord_bot.types.media_request import MediaRequest
from discord_bot.types.playlist_add_request import parse_media_request
from discord_bot.exceptions import YoutubeMusicRetryException
from discord_bot.workers.redis_guild_queue import (
    build_status_snapshot, collect_queue_sizes, drain_guild_zset, redis_pop_lock,
)

REQUEST_KEY_PREFIX = 'discord_bot:ytmusic_search:request:'
GUILD_QUEUE_PREFIX = 'discord_bot:ytmusic_search:guild:'
GUILD_BLOCKED_SUFFIX = ':blocked'
GUILDS_KEY = 'discord_bot:ytmusic_search:guilds'
WAIT_UNTIL_KEY = 'discord_bot:ytmusic_search:wait_until'
FAILURES_KEY = 'discord_bot:ytmusic_search:failures'
POP_LOCK_KEY = 'discord_bot:ytmusic_search:poplock'

REQUEST_TTL_SECONDS = 86400  # 24h fallback so abandoned items eventually expire
WAIT_TTL_SECONDS_MULTIPLIER = 4  # wait_until expires after wait_period * 4 * multiplier
FAILURE_TTL_SECONDS = 600  # 10 min — matches FailureQueue.max_age_seconds default

_DEFAULT_FAILURE_SUMMARY = '0 failures in queue'


class RedisYoutubeMusicSearchWorker(YoutubeMusicSearchWorkerBase):
    '''
    Multi-pod YouTube-Music search engine backed by Redis ZSET queues.

    Supplies the YoutubeMusicSearchWorkerBase queue surface with Redis: a single
    per-guild ZSET popped round-robin under a short-lived SET NX pop-lock, plus a
    shared cross-pod 429 backoff window and a shared failure ZSET.  The sync
    ``failure_summary`` / ``backoff_seconds_remaining`` reads work off a per-pod
    cache the async hooks refresh from Redis; cross-pod correctness is enforced by
    the pop-lock, not the cache.
    '''
    def __init__(self, *args, redis_manager: RedisManager, **kwargs):
        '''
        Forward the resolution / backoff kwargs to YoutubeMusicSearchWorkerBase,
        then wire the Redis client.

        redis_manager : RedisManager whose .client is the shared aioredis handle.
        '''
        super().__init__(*args, **kwargs)
        self._manager = redis_manager
        # Per-pod caches for the sync properties, refreshed by the async hooks.
        self._failure_summary_cache = _DEFAULT_FAILURE_SUMMARY
        self._failure_count_cache = 0

    # ------------------------------------------------------------------
    # Key / score helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _request_key(request_uuid: str) -> str:
        return f'{REQUEST_KEY_PREFIX}{request_uuid}'

    @staticmethod
    def _guild_queue_key(guild_id: int) -> str:
        return f'{GUILD_QUEUE_PREFIX}{guild_id}'

    @staticmethod
    def _guild_blocked_key(guild_id: int) -> str:
        return f'{GUILD_QUEUE_PREFIX}{guild_id}{GUILD_BLOCKED_SUFFIX}'

    @staticmethod
    def _now_seconds() -> float:
        return datetime.now(timezone.utc).timestamp()

    def _build_score(self, priority: int | None) -> float:
        '''Compose a sortable score: priority bucket + submitted_ts (lower wins).'''
        prio = priority if priority is not None else 100
        return float(prio) * 1_000_000_000 + self._now_seconds()

    # ------------------------------------------------------------------
    # Lock-serialised atomic pop
    # ------------------------------------------------------------------

    async def _round_robin_pop(self) -> tuple[int, str, str | None] | None:
        '''
        Pick the guild with the oldest last_popped_ts, ZPOPMIN one request, rotate
        that guild to the back (or drop it when emptied), and GET+DEL the payload.
        Returns (guild_id, request_uuid, raw) or None.  Caller holds _pop_lock.
        '''
        client = self._manager.client
        while True:
            guilds = await client.zrange(GUILDS_KEY, 0, 0)
            if not guilds:
                return None
            guild_id = guilds[0]
            guild_queue = self._guild_queue_key(int(guild_id))
            popped = await client.zpopmin(guild_queue, 1)
            if not popped:
                await client.zrem(GUILDS_KEY, guild_id)
                continue
            request_uuid = popped[0][0]
            if await client.zcard(guild_queue) == 0:
                await client.zrem(GUILDS_KEY, guild_id)
            else:
                await client.zadd(GUILDS_KEY, {guild_id: self._now_seconds()})
            request_key = self._request_key(request_uuid)
            raw = await client.get(request_key)
            await client.delete(request_key)
            return int(guild_id), request_uuid, raw

    # ------------------------------------------------------------------
    # Queue interface — backed by Redis
    # ------------------------------------------------------------------

    async def _enqueue(self, guild_id: int, media_request: MediaRequest,
                       priority: int | None = None) -> None:
        '''Persist the request and ZADD it onto the guild's queue + round-robin tracker.'''
        request_uuid = str(media_request.uuid)
        client = self._manager.client
        await client.set(self._request_key(request_uuid),
                         media_request.model_dump_json(), ex=REQUEST_TTL_SECONDS)
        await client.zadd(self._guild_queue_key(guild_id),
                          {request_uuid: self._build_score(priority)})
        # ZADD NX so an already-listed guild keeps its round-robin position.
        await client.zadd(GUILDS_KEY, {str(guild_id): self._now_seconds()}, nx=True)

    async def get_input_nowait(self) -> MediaRequest:
        '''Round-robin pop the next request under the pop-lock, raising QueueEmpty
        if nothing is queued (or the popped payload has already TTL'd away).'''
        async with redis_pop_lock(self._manager.client, POP_LOCK_KEY):
            result = await self._round_robin_pop()
        if result is None:
            raise QueueEmpty('No search requests in queue')
        _, _, raw = result
        if not raw:
            raise QueueEmpty('Search request payload missing')
        return parse_media_request(json.loads(raw))

    async def block_guild(self, guild_id: int) -> bool:
        '''Mark the guild blocked (used during shutdown/cleanup).'''
        await self._manager.client.set(self._guild_blocked_key(guild_id), '1')
        return True

    async def clear_guild_queue(self, guild_id: int,
                                preserve_predicate: Callable[[MediaRequest], bool] | None = None,
                                ) -> list[MediaRequest]:
        '''Clear the guild's queue, returning the dropped requests.'''
        client = self._manager.client
        queue_key = self._guild_queue_key(guild_id)
        dropped = await drain_guild_zset(client, queue_key, self._request_key, preserve_predicate)
        # Drop the guild from the round-robin tracker if its queue is now empty.
        if await client.zcard(queue_key) == 0:
            await client.zrem(GUILDS_KEY, str(guild_id))
        return dropped

    async def queue_size(self, guild_id: int) -> int:
        '''Pending request count for a guild, or 0 if none.'''
        return await self._manager.client.zcard(self._guild_queue_key(guild_id)) or 0

    # ------------------------------------------------------------------
    # Shared backoff / failure state
    # ------------------------------------------------------------------

    async def _record_search_success(self) -> None:
        '''Pop one failure (oldest) on success, then refresh the failure cache.'''
        await self._manager.client.zpopmin(FAILURES_KEY)
        await self._refresh_failure_summary()

    async def _record_search_failure(self, error: YoutubeMusicRetryException) -> None:
        '''
        Record a 429 on the shared failure ZSET, extend the shared backoff window
        scaled by the resulting count, and refresh the per-pod caches.

        Overrides the base's local-FailureQueue bookkeeping so the failure count
        and backoff window are shared across every search pod.
        '''
        count = await self._record_failure()
        await self._extend_wait_until(backoff_multiplier=2 ** count)
        await self._refresh_failure_summary()
        self.logger.info(
            f'Youtube music search rate limited ({error}); shared failure count {count}')

    async def _record_failure(self) -> int:
        '''Add a failure at now, trim expired ones, return the resulting count.'''
        client = self._manager.client
        cutoff = self._now_seconds() - FAILURE_TTL_SECONDS
        await client.zremrangebyscore(FAILURES_KEY, 0, cutoff)
        await client.zadd(FAILURES_KEY, {uuid_module.uuid4().hex: self._now_seconds()})
        return await client.zcard(FAILURES_KEY)

    async def _extend_wait_until(self, backoff_multiplier: int = 1) -> None:
        '''Max-extend the shared 429 backoff window with jitter, caching the result.'''
        new_ts = self._now_seconds() + self._wait_period_minimum * backoff_multiplier
        seed(time())
        # bandit B311: backoff jitter, not security-sensitive
        new_ts += randint(1000, self._wait_period_max_variance * 1000) / 1000  # nosec B311
        client = self._manager.client
        current_raw = await client.get(WAIT_UNTIL_KEY)
        current = float(current_raw) if current_raw else 0.0
        if new_ts <= current:
            self._wait_timestamp = current or None
            return
        await client.set(
            WAIT_UNTIL_KEY, str(new_ts),
            ex=self._wait_period_minimum * WAIT_TTL_SECONDS_MULTIPLIER * backoff_multiplier,
        )
        self._wait_timestamp = new_ts

    async def _refresh_wait_timestamp(self) -> None:
        '''Refresh the local _wait_timestamp cache from the shared Redis window.'''
        raw = await self._manager.client.get(WAIT_UNTIL_KEY)
        wait_until = float(raw) if raw else 0.0
        self._wait_timestamp = wait_until if wait_until > self._now_seconds() else None

    async def _refresh_failure_summary(self) -> None:
        '''Recompute the cached failure_summary + count from the shared ZSET.'''
        count = await self._manager.client.zcard(FAILURES_KEY) or 0
        self._failure_count_cache = count
        self._failure_summary_cache = (
            f'{count} failures in queue' if count else _DEFAULT_FAILURE_SUMMARY
        )

    async def backoff_wait(self, shutdown_event: asyncio.Event,
                           max_wait_seconds: float | None = None) -> None:
        '''Refresh the shared backoff window from Redis, then wait it out.

        A pod that did not itself hit the 429 still backs off: the window lives in
        Redis, not this pod's memory, so refresh the cache before delegating to the
        base sleep loop.  Re-reading per slice is also what lets a pod pick up a
        window another pod extended (or cleared) mid-wait.'''
        await self._refresh_wait_timestamp()
        await super().backoff_wait(shutdown_event, max_wait_seconds=max_wait_seconds)

    @property
    def failure_summary(self) -> str:
        '''Cached human-readable summary of the shared failure queue.'''
        return self._failure_summary_cache

    async def status_snapshot(self) -> dict:
        '''
        Live status for the future search HTTP server's status endpoint.

        Refreshes the shared failure summary + backoff window from Redis, then
        collects each active guild's pending count.  Bounded by the number of
        guilds with queued work, not by total request count.  The search server
        (running in the worker pod) will serve this to the bot pod's polling
        client, which can't read Redis itself.
        '''
        await self._refresh_failure_summary()
        await self._refresh_wait_timestamp()
        backoff = self.backoff_seconds_remaining
        guild_ids = await self._manager.client.zrange(GUILDS_KEY, 0, -1)
        queue_sizes = await collect_queue_sizes(guild_ids, self.queue_size)
        return build_status_snapshot(
            self._failure_summary_cache, self._failure_count_cache, backoff, queue_sizes)
