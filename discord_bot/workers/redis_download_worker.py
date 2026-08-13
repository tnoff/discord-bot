'''
Redis-backed download engine for HA.

RedisDownloadWorker is the multi-pod DownloadWorkerBase impl: it supplies the
same queue surface as AsyncioDownloadWorker but backs it with Redis instead of
in-process DistributedQueues, so several downloader pods can share one work
queue.  All the yt-dlp / create_source / consumer-loop logic lives on
DownloadWorkerBase; this class only supplies the queue surface + the shared
backoff/failure state.

Redis schema (all keys under ``discord_bot:download:``):
    request:{uuid}              STRING  JSON MediaRequest (TTL fallback)
    guild:{gid}:youtube         ZSET    request_uuid -> priority*1e9 + submitted_ts
    guild:{gid}:direct          ZSET    same, DIRECT items
    guild:{gid}:blocked         STRING  '1' when blocked
    guilds:youtube              ZSET    guild_id -> last_popped_ts (round-robin)
    guilds:direct               ZSET    same, DIRECT pool
    youtube_wait_until:{egress}  STRING  epoch ts; shared per egress bucket
    failures:youtube:{egress}    ZSET    failure_uuid -> ts (ZCARD ~ backoff exponent)
    failures:direct              ZSET    same, informational only

The YouTube pool is subject to a per-egress backoff window (``youtube_wait_until``)
so pods sharing an egress IP never hammer YouTube past its rate limit; the DIRECT
pool has no backoff and drains in parallel.  Both round-robin across guilds by
``last_popped_ts`` under a short-lived SET NX pop-lock so no guild starves another
and no two pods pop the same request.
'''
import asyncio
import json
import random
import uuid as uuid_module
from asyncio import QueueEmpty, sleep
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from time import time
from typing import Callable, List

from discord_bot.clients.redis_client import RedisManager
from discord_bot.cogs.music_helpers.common import SearchType
from discord_bot.exceptions import ExitEarlyException
from discord_bot.interfaces.download_protocols import (
    DownloadWorkerBase, DirectItemAvailableException,
)
from discord_bot.types.download import DownloadErrorType, DownloadResult
from discord_bot.types.media_request import MediaRequest
from discord_bot.types.playlist_add_request import parse_media_request
from discord_bot.workers.redis_guild_queue import (
    build_status_snapshot, collect_queue_sizes, drain_guild_zset, redis_pop_lock,
)

REQUEST_KEY_PREFIX = 'discord_bot:download:request:'
GUILD_QUEUE_PREFIX = 'discord_bot:download:guild:'
GUILD_YOUTUBE_SUFFIX = ':youtube'
GUILD_DIRECT_SUFFIX = ':direct'
GUILD_BLOCKED_SUFFIX = ':blocked'
GUILDS_YOUTUBE_KEY = 'discord_bot:download:guilds:youtube'
GUILDS_DIRECT_KEY = 'discord_bot:download:guilds:direct'
# Per-egress-bucket prefixes: pods behind distinct egress IPs keep independent
# YouTube backoff + failure state; the ':default' suffix is the single-bucket schema.
YOUTUBE_WAIT_UNTIL_KEY_PREFIX = 'discord_bot:download:youtube_wait_until'
FAILURES_YOUTUBE_KEY_PREFIX = 'discord_bot:download:failures:youtube'
FAILURES_DIRECT_KEY = 'discord_bot:download:failures:direct'

DEFAULT_YOUTUBE_EGRESS_KEY = 'default'

REQUEST_TTL_SECONDS = 86400  # 24h fallback so abandoned items eventually expire
FAILURE_TTL_SECONDS = 600  # 10 min — matches FailureQueue.max_age_seconds default
BACKOFF_POLL_SECONDS = 0.1  # granularity of backoff_wait's shutdown/direct-interrupt polling


def youtube_wait_until_key(egress_key: str) -> str:
    '''Per-egress YouTube wait-until Redis key.'''
    return f'{YOUTUBE_WAIT_UNTIL_KEY_PREFIX}:{egress_key}'


def youtube_failures_key(egress_key: str) -> str:
    '''Per-egress YouTube failure-ZSET Redis key.'''
    return f'{FAILURES_YOUTUBE_KEY_PREFIX}:{egress_key}'


# Per-pool pop lock.  The round-robin pop (pick oldest guild -> ZPOPMIN -> rotate)
# and the YouTube check-wait-then-claim are multi-step read-modify-writes; the
# shared redis_pop_lock (a token-tagged SET NX lock, mirroring RedisBrokerRegistry)
# serialises them across pods.  (fakeredis on the pinned test stack has no Lua, so
# this is the atomicity primitive rather than an EVAL script.)
POP_LOCK_KEY_PREFIX = 'discord_bot:download:poplock:'


class RedisDownloadWorker(DownloadWorkerBase):
    '''
    Multi-pod download engine backed by Redis ZSET queues.

    Supplies the DownloadWorkerBase queue surface with Redis: per-guild ZSETs
    popped round-robin under a short-lived SET NX pop-lock, a shared per-egress
    YouTube backoff window, and shared failure ZSETs.  The sync
    backoff_seconds_remaining / failure_summary properties read a per-pod cache the
    async hooks refresh from Redis; cross-pod correctness is enforced by the pop-lock
    + per-egress claim, not the cache.
    '''
    def __init__(self, *args, redis_manager: RedisManager,
                 youtube_egress_key: str = DEFAULT_YOUTUBE_EGRESS_KEY, **kwargs):
        '''
        Forward all yt-dlp / backoff / broker kwargs to DownloadWorkerBase, then
        wire the Redis client + per-egress keys.

        redis_manager : RedisManager whose .client is the shared aioredis handle
        youtube_egress_key : bucket for the shared YouTube wait/failure keys
        '''
        super().__init__(*args, **kwargs)
        self._manager = redis_manager
        self._youtube_egress_key = youtube_egress_key
        self._youtube_wait_until_key = youtube_wait_until_key(youtube_egress_key)
        self._youtube_failures_key = youtube_failures_key(youtube_egress_key)
        # Per-pod cache for the sync properties, refreshed by the async hooks.
        self._failure_summary_cache = '0 failures in queue'
        self._failure_count_cache = 0
        self._direct_pending_cache = False
        # Cold-start floor: a freshly-(re)started pod waits wait_period_minimum
        # before its first YouTube pop even if the shared stamp was GC'd.
        self._startup_wait_until = self._now_seconds() + self._wait_period_minimum
        self._wait_timestamp = self._startup_wait_until

    # ------------------------------------------------------------------
    # Key / score helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _request_key(request_uuid: str) -> str:
        return f'{REQUEST_KEY_PREFIX}{request_uuid}'

    @staticmethod
    def _guild_queue_key(guild_id: int, *, direct: bool) -> str:
        suffix = GUILD_DIRECT_SUFFIX if direct else GUILD_YOUTUBE_SUFFIX
        return f'{GUILD_QUEUE_PREFIX}{guild_id}{suffix}'

    @staticmethod
    def _guild_blocked_key(guild_id: int) -> str:
        return f'{GUILD_QUEUE_PREFIX}{guild_id}{GUILD_BLOCKED_SUFFIX}'

    @staticmethod
    def _guilds_zset_key(*, direct: bool) -> str:
        return GUILDS_DIRECT_KEY if direct else GUILDS_YOUTUBE_KEY

    def _youtube_wait_key_for(self, exit_name: str | None) -> str:
        '''Per-exit YouTube wait-until key (pool mode); the pod default bucket when
        exit_name is None (fixed http-proxy mode).'''
        return youtube_wait_until_key(exit_name) if exit_name else self._youtube_wait_until_key

    def _youtube_failures_key_for(self, exit_name: str | None) -> str:
        '''Per-exit YouTube failure-ZSET key (pool mode); the pod default bucket when
        exit_name is None (fixed http-proxy mode).'''
        return youtube_failures_key(exit_name) if exit_name else self._youtube_failures_key

    def _failures_key(self, *, direct: bool, exit_name: str | None = None) -> str:
        '''Per-exit failure-ZSET key for YouTube; single global key for direct.'''
        return FAILURES_DIRECT_KEY if direct else self._youtube_failures_key_for(exit_name)

    @staticmethod
    def _is_direct(media_request: MediaRequest) -> bool:
        return media_request.search_result.search_type == SearchType.DIRECT

    def _build_score(self, priority: int | None, queued_at: float) -> float:
        '''Compose a sortable score: priority bucket + submission ts (lower wins).

        queued_at is the request's stable queue_order (set once at first enqueue),
        not now(), so a re-enqueued request keeps its original queue position rather
        than being pushed to the back on every retry / contention bounce.'''
        prio = priority if priority is not None else 100
        return float(prio) * 1_000_000_000 + queued_at

    @staticmethod
    def _now_seconds() -> float:
        return datetime.now(timezone.utc).timestamp()

    # ------------------------------------------------------------------
    # Lock-serialised atomic pops
    # ------------------------------------------------------------------

    @asynccontextmanager
    async def _pop_lock(self, *, direct: bool):
        '''
        Hold the shared SET NX pop-lock over one pool's critical section.

        Per-pool key so the DIRECT and per-egress YouTube pops don't serialise
        against each other; the token-tagging + fall-through live in redis_pop_lock.
        '''
        pool = 'direct' if direct else f'youtube:{self._youtube_egress_key}'
        async with redis_pop_lock(self._manager.client, f'{POP_LOCK_KEY_PREFIX}{pool}'):
            yield

    async def _round_robin_pop(self, *, direct: bool) -> tuple[int, str, str | None] | None:
        '''
        Pick the guild with the oldest last_popped_ts, ZPOPMIN one request, rotate
        that guild to the back (or drop it when emptied), and GET+DEL the payload.
        Returns (guild_id, request_uuid, raw) or None.  Caller holds _pop_lock.
        '''
        client = self._manager.client
        guilds_zset = self._guilds_zset_key(direct=direct)
        while True:
            guilds = await client.zrange(guilds_zset, 0, 0)
            if not guilds:
                return None
            guild_id = guilds[0]
            guild_queue = self._guild_queue_key(int(guild_id), direct=direct)
            popped = await client.zpopmin(guild_queue, 1)
            if not popped:
                await client.zrem(guilds_zset, guild_id)
                continue
            request_uuid = popped[0][0]
            if await client.zcard(guild_queue) == 0:
                await client.zrem(guilds_zset, guild_id)
            else:
                await client.zadd(guilds_zset, {guild_id: self._now_seconds()})
            request_key = self._request_key(request_uuid)
            raw = await client.get(request_key)
            await client.delete(request_key)
            return int(guild_id), request_uuid, raw

    async def _pool_is_empty(self, *, direct: bool) -> bool:
        '''
        Lock-free "is there anything queued at all" check for one pool.

        The consumer loop polls every _IDLE_POLL_BACKOFF_SECONDS whether or not work
        exists, and an idle poll that goes straight into _pop_lock costs a SET NX +
        GET + DEL lock cycle per pool just to have _round_robin_pop discover an empty
        ZSET.  With a driver per egress exit that was the pod's dominant redis
        traffic (and ~98% of its trace spans).  Checking the round-robin ZSET first
        collapses an idle poll to one ZCARD per pool and takes no lock.

        Only ever skips work that _round_robin_pop would also have skipped: it reads
        the same ZSET that ZRANGE reads, so an empty count means an empty ZRANGE.  A
        request landing right after the check simply waits for the next poll, and the
        count is not trusted in the other direction — a non-empty ZSET still takes the
        lock and re-checks under it, because a guild can sit in the ZSET with an
        already-drained queue.
        '''
        return not await self._manager.client.zcard(self._guilds_zset_key(direct=direct))

    async def _atomic_pop_direct(self) -> tuple[int, str, str | None] | None:
        '''Round-robin pop one DIRECT request under the direct-pool lock.'''
        if await self._pool_is_empty(direct=True):
            return None
        async with self._pop_lock(direct=True):
            return await self._round_robin_pop(direct=True)

    async def _atomic_pop_youtube(self) -> tuple:
        '''
        Under the YouTube-pool lock, round-robin pop one request.

        Pool mode: the pop is not gated on a pod-global window — YouTube rate
        limiting is enforced per exit when the download reserves one, so pops don't
        serialise pod-wide and concurrent drivers can each pop + reserve a distinct
        exit.  Fixed http-proxy mode keeps the single shared window: bail with
        ('wait', ts) while it backs off, else pop and re-claim it.

        An empty pool returns None before the lock, so a backing-off fixed-mode pod
        with nothing queued reports "empty" rather than ('wait', ts).  Both raise
        QueueEmpty in _merged_get_nowait; the 'wait' signal only carries information
        when there is actually something queued behind the window.
        '''
        if await self._pool_is_empty(direct=False):
            return None
        async with self._pop_lock(direct=False):
            if self._egress.is_pool:
                return await self._round_robin_pop(direct=False)
            now = self._now_seconds()
            raw_until = await self._manager.client.get(self._youtube_wait_until_key)
            wait_until = float(raw_until) if raw_until else 0.0
            if now < wait_until:
                return ('wait', str(wait_until))
            result = await self._round_robin_pop(direct=False)
            if result is None:
                return None
            await self._claim_youtube_window(now)
            return result

    async def _claim_youtube_window(self, now: float) -> None:
        '''Max-extend youtube_wait_until by one wait_period to claim the egress
        (fixed http-proxy mode; pool mode claims per-exit windows at reserve time).'''
        new_wait = now + self._wait_period_minimum
        client = self._manager.client
        current_raw = await client.get(self._youtube_wait_until_key)
        current = float(current_raw) if current_raw else 0.0
        if new_wait > current:
            await client.set(
                self._youtube_wait_until_key, str(new_wait),
                ex=max(1, int(self._wait_period_minimum) + 1),
            )

    @staticmethod
    def _parse_raw(raw: str | None) -> MediaRequest:
        '''Deserialize a popped request payload, raising QueueEmpty if it's gone.'''
        if not raw:
            raise QueueEmpty('Request payload missing')
        return parse_media_request(json.loads(raw))

    # ------------------------------------------------------------------
    # Queue interface — backed by Redis
    # ------------------------------------------------------------------

    @property
    def has_direct_pending(self) -> bool:
        '''Cached view of whether a DIRECT item is waiting (refreshed by the hooks).'''
        return self._direct_pending_cache

    async def _enqueue_request(self, guild_id: int, media_request: MediaRequest,
                               priority: int | None = None) -> None:
        '''Persist the request and ZADD it onto the guild's pool + round-robin tracker.'''
        request_uuid = str(media_request.uuid)
        direct = self._is_direct(media_request)
        # Stamp the FIFO ordering key once, on first enqueue; a re-enqueued request
        # already carries it, so it keeps its original queue position.
        if media_request.queue_order is None:
            media_request.queue_order = self._now_seconds()
        client = self._manager.client
        await client.set(self._request_key(request_uuid),
                         media_request.model_dump_json(), ex=REQUEST_TTL_SECONDS)
        await client.zadd(self._guild_queue_key(guild_id, direct=direct),
                          {request_uuid: self._build_score(priority, media_request.queue_order)})
        # ZADD NX so an already-listed guild keeps its round-robin position.
        await client.zadd(self._guilds_zset_key(direct=direct),
                          {str(guild_id): self._now_seconds()}, nx=True)
        if direct:
            self._direct_pending_cache = True

    async def block_guild(self, guild_id: int) -> bool:
        '''Mark the guild blocked (used during shutdown/cleanup).'''
        await self._manager.client.set(self._guild_blocked_key(guild_id), '1')
        return True

    async def clear_guild_queue(self, guild_id: int,
                                preserve_predicate: Callable[[MediaRequest], bool] | None = None,
                                ) -> list[MediaRequest]:
        '''Clear both pools for a guild, returning the dropped requests.'''
        client = self._manager.client
        dropped: List[MediaRequest] = []
        for direct in (False, True):
            queue_key = self._guild_queue_key(guild_id, direct=direct)
            dropped.extend(
                await drain_guild_zset(client, queue_key, self._request_key, preserve_predicate))
            # Drop the guild from the round-robin tracker if its queue is now empty.
            if await client.zcard(queue_key) == 0:
                await client.zrem(self._guilds_zset_key(direct=direct), str(guild_id))
        return dropped

    async def queue_size(self, guild_id: int) -> int:
        '''Total pending requests for a guild across both pools.'''
        client = self._manager.client
        youtube = await client.zcard(self._guild_queue_key(guild_id, direct=False)) or 0
        direct = await client.zcard(self._guild_queue_key(guild_id, direct=True)) or 0
        return youtube + direct

    async def _dequeue_direct(self) -> MediaRequest:
        '''Dequeue the next DIRECT item, raising QueueEmpty if none available.'''
        result = await self._atomic_pop_direct()
        if result is None:
            self._direct_pending_cache = False
            raise QueueEmpty('No direct items in queue')
        _, _, raw = result
        return self._parse_raw(raw)

    async def _merged_get_nowait(self) -> MediaRequest:
        '''
        Pop the next item, DIRECT first (bypasses backoff), then YouTube with an
        atomic per-egress claim; raise QueueEmpty if nothing is servable.
        '''
        direct = await self._atomic_pop_direct()
        if direct is not None:
            _, _, raw = direct
            return self._parse_raw(raw)
        self._direct_pending_cache = False
        youtube = await self._atomic_pop_youtube()
        if youtube is None:
            raise QueueEmpty('No items in queue')
        if youtube[0] == 'wait':
            # Bucket is backing off — refresh the cached timestamp and yield.
            self._wait_timestamp = float(youtube[1])
            raise QueueEmpty('YouTube backoff active')
        _, _, raw = youtube
        return self._parse_raw(raw)

    async def backoff_wait(self, shutdown_event: asyncio.Event) -> None:
        '''
        Wait out the shared YouTube backoff, waking early on shutdown or a DIRECT
        arrival.  Raises ExitEarlyException on shutdown, DirectItemAvailableException
        when a DIRECT item appears mid-wait.
        '''
        if shutdown_event.is_set():
            raise ExitEarlyException('Exiting bot wait loop')
        deadline = self._now_seconds() + await self._effective_backoff_remaining()
        while self._now_seconds() < deadline:
            if shutdown_event.is_set():
                raise ExitEarlyException('Exiting bot wait loop')
            if await self._direct_pending_in_redis():
                self._direct_pending_cache = True
                raise DirectItemAvailableException()
            await sleep(min(BACKOFF_POLL_SECONDS, deadline - self._now_seconds()))

    async def _direct_pending_in_redis(self) -> bool:
        '''True when at least one guild has a DIRECT item queued.'''
        pending = bool(await self._manager.client.zcard(self._guilds_zset_key(direct=True)))
        self._direct_pending_cache = pending
        return pending

    # ------------------------------------------------------------------
    # Shared backoff / failure state
    # ------------------------------------------------------------------

    async def _effective_backoff_remaining(self) -> int:
        '''Seconds until the pod can next pop a YouTube item.

        Pool mode: the soonest any exit's per-exit window frees — 0 while any exit
        is free (so run() pops immediately), only positive when every exit is
        busy/backed off.  Fixed http-proxy mode: the single shared window, floored
        at the pod startup wait.'''
        if self._egress.is_pool:
            return await self._soonest_exit_free_seconds()
        raw = await self._manager.client.get(self._youtube_wait_until_key)
        redis_until = float(raw) if raw else 0.0
        effective = max(redis_until, self._startup_wait_until)
        self._wait_timestamp = effective or None
        if not effective:
            return 0
        return max(0, int(effective - self._now_seconds()))

    async def _soonest_exit_free_seconds(self) -> int:
        '''Min remaining seconds across every exit's per-exit YouTube window; 0 as
        soon as any exit is free.  Feeds run()'s backoff gate in pool mode so the
        pod waits only when the whole pool is busy, then wakes when the first
        exit frees.'''
        now = self._now_seconds()
        soonest: float | None = None
        for exit_name in self._egress.exit_names:
            raw = await self._manager.client.get(youtube_wait_until_key(exit_name))
            remaining = (float(raw) if raw else 0.0) - now
            if remaining <= 0:
                self._wait_timestamp = None
                return 0
            soonest = remaining if soonest is None else min(soonest, remaining)
        self._wait_timestamp = (now + soonest) if soonest else None
        return int(soonest) if soonest else 0

    async def _reserve_youtube_exit(self, exit_name: str) -> bool:
        '''
        Atomically reserve an exit's per-exit YouTube window via SET NX: the first
        task/pod to claim a free window wins the exit; concurrent leases (in-pod or
        cross-pod) see the key present and rotate to another exit.  Seeds the window
        at now+wait_period for spacing (max-extended from completion, or backed off
        exponentially on failure, by _update_youtube_tracking).  The key's TTL tracks
        the window duration so key-existence == exit-busy.  Overrides the base stub.
        '''
        new_wait = self._now_seconds() + self._wait_period_minimum
        reserved = await self._manager.client.set(
            youtube_wait_until_key(exit_name), str(new_wait),
            nx=True, ex=max(1, int(self._wait_period_minimum) + 1),
        )
        return bool(reserved)

    async def _record_success(self, *, direct: bool, exit_name: str | None = None) -> None:
        '''Pop one failure (oldest) on success, mirroring FailureQueue.add_item.'''
        await self._manager.client.zpopmin(self._failures_key(direct=direct, exit_name=exit_name))

    async def _record_failure(self, *, direct: bool, exit_name: str | None = None) -> int:
        '''Add a failure at now, trim expired ones, return the resulting count.'''
        client = self._manager.client
        key = self._failures_key(direct=direct, exit_name=exit_name)
        cutoff = self._now_seconds() - FAILURE_TTL_SECONDS
        await client.zremrangebyscore(key, 0, cutoff)
        await client.zadd(key, {uuid_module.uuid4().hex: self._now_seconds()})
        return await client.zcard(key)

    async def _extend_wait_until(self, multiplier: int = 1, exit_name: str | None = None) -> None:
        '''Max-extend a YouTube backoff window with jitter.  Keyed by the leased
        exit in pool mode (exit_name), else the pod default bucket.  The key's TTL
        tracks the window duration so key-existence == window-active, which the
        per-exit SET NX reserve relies on to tell a busy exit from a free one.'''
        now = self._now_seconds()
        new_ts = now + self._wait_period_minimum * multiplier
        random.seed(time())
        # bandit B311: backoff jitter, not security-sensitive
        new_ts += random.randint(1000, self._wait_period_max_variance * 1000) / 1000  # nosec B311
        client = self._manager.client
        wait_key = self._youtube_wait_key_for(exit_name)
        current_raw = await client.get(wait_key)
        current = float(current_raw) if current_raw else 0.0
        if new_ts <= current:
            return
        await client.set(wait_key, str(new_ts), ex=max(1, int(new_ts - now) + 1))

    async def _update_youtube_tracking(self, result: DownloadResult, exit_name: str | None = None) -> None:
        '''Advance the per-exit YouTube failure ZSET + backoff window from a result.

        exit_name selects the per-exit bucket (pool mode); None falls back to the
        shared ':default' bucket (fixed http-proxy mode), keeping the pre-pool
        single-window behaviour intact.'''
        error_type = result.status.error_type
        if result.status.success:
            await self._record_success(direct=False, exit_name=exit_name)
            extractor = (result.ytdlp_data or {}).get('extractor')
            if extractor is None or extractor == 'youtube':
                await self._extend_wait_until(exit_name=exit_name)
            return
        if error_type in {DownloadErrorType.RETRY_LIMIT_EXCEEDED,
                          DownloadErrorType.RETRYABLE,
                          DownloadErrorType.BOT_FLAGGED}:
            # Prod path: RedisDownloadWorker.update_tracking overrides the base and
            # never calls super, so the by-exit failure log must fire here too.
            self._log_exit_failure(error_type, exit_name)
            count = await self._record_failure(direct=False, exit_name=exit_name)
            await self._extend_wait_until(multiplier=2 ** count, exit_name=exit_name)
            return
        await self._extend_wait_until(exit_name=exit_name)

    async def _update_direct_tracking(self, result: DownloadResult) -> None:
        '''Track DIRECT successes/failures separately; never touches backoff.'''
        if result.status.success:
            await self._record_success(direct=True)
            return
        await self._record_failure(direct=True)

    async def _refresh_failure_summary(self) -> None:
        '''Recompute the cached failure_summary + count from the shared YouTube ZSET.'''
        count = await self._manager.client.zcard(self._youtube_failures_key) or 0
        self._failure_count_cache = count
        self._failure_summary_cache = (
            f'{count} failures in queue' if count else '0 failures in queue'
        )

    @property
    def failure_summary(self) -> str:
        '''Cached human-readable summary of the shared failure queue.'''
        return self._failure_summary_cache

    async def update_tracking(self, result: DownloadResult, exit_name: str | None = None) -> int | None:
        '''
        Persist shared backoff/failure state to Redis, then refresh the per-pod
        caches the sync properties read.  Cross-pod correctness is enforced by the
        SET NX pop-lock + per-egress claim; this cache only feeds logging + the next
        run() decision.

        exit_name is the exit this download leased (pool mode); it attributes the
        failure log to the real exit and keys the per-exit backoff window.  None on
        the fixed http-proxy path, where the shared ':default' bucket is used.
        '''
        if self._is_direct(result.media_request):
            await self._update_direct_tracking(result)
        else:
            await self._update_youtube_tracking(result, exit_name)
        await self._refresh_failure_summary()
        await self._effective_backoff_remaining()
        return self.backoff_seconds_remaining

    async def status_snapshot(self) -> dict:
        '''
        Live status for the download HTTP server's GET /downloads/status endpoint.

        Refreshes the shared failure summary + YouTube backoff window from Redis,
        then collects each active guild's pending count across both round-robin
        pools.  Bounded by the number of guilds with queued work, not by total
        request count.  The download server (running in the worker pod) serves this
        to the bot pod's HttpDownloadClient poller, which can't read Redis itself.
        '''
        await self._refresh_failure_summary()
        backoff = await self._effective_backoff_remaining()
        client = self._manager.client
        queue_sizes: dict[str, int] = {}
        for direct in (False, True):
            guild_ids = await client.zrange(self._guilds_zset_key(direct=direct), 0, -1)
            queue_sizes.update(await collect_queue_sizes(guild_ids, self.queue_size))
        return build_status_snapshot(
            self._failure_summary_cache, self._failure_count_cache, backoff, queue_sizes)
