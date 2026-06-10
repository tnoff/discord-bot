'''
Redis-backed work queue for MessageDispatcher in HTTP multi-pod mode.

A single sorted set per shard is shared across all dispatcher pods.
Score encodes priority + arrival time so any pod pops the highest-urgency item.
'''
import hashlib
import json
import time

import redis.asyncio as aioredis

_PREFIX = 'discord_bot:dispatch'


def dispatch_request_id(params: dict) -> str:
    '''Stable SHA-256 hex digest of *params* — same params always yield the same ID.'''
    canonical = json.dumps(params, sort_keys=True, default=str)
    return hashlib.sha256(canonical.encode()).hexdigest()


_PAYLOAD_TTL = 86400  # 1 day — fallback expiry for orphaned payloads
_RESULT_TTL = 86400   # 1 day — fallback expiry for orphaned fetch results
_LOCK_TTL = 30        # seconds — execution lock for mutable bundle updates


class RedisDispatchQueue:
    '''
    Shared Redis sorted-set work queue for cross-pod MessageDispatcher dispatch.

    Score formula: priority * 10^12 + unix_timestamp_ms
    Lower score = higher urgency (priority HIGH=0 > NORMAL=1 > LOW=2).

    Fire-and-forget items (sends, deletes) use a UUID member so each is unique.
    Mutable bundle items use ``mutable:{bundle_key}`` so rapid updates collapse
    to one queue entry (ZADD NX) while the payload key always holds the latest
    content.
    '''

    def __init__(self, redis_client: aioredis.Redis, shard_id: int, pod_id: str):
        self._redis = redis_client
        self._queue_key = f'{_PREFIX}:queue:{shard_id}'
        self._pod_id = pod_id

    # ------------------------------------------------------------------
    # Key helpers
    # ------------------------------------------------------------------

    @staticmethod
    def payload_key(member: str) -> str:
        '''Redis key for the JSON payload associated with a work item member.'''
        return f'{_PREFIX}:payload:{member}'

    @staticmethod
    def result_key(request_id: str) -> str:
        '''Redis key for the stored result of a completed fetch request.'''
        return f'{_PREFIX}:result:{request_id}'

    @staticmethod
    def lock_key(bundle_key: str) -> str:
        '''Redis key for the per-bundle execution lock.'''
        return f'{_PREFIX}:executing:{bundle_key}'

    def _score(self, priority: int) -> float:
        '''Lower score = processed sooner. Ties broken by millisecond timestamp.'''
        return priority * 10 ** 12 + int(time.time() * 1000)

    # ------------------------------------------------------------------
    # Enqueueing
    # ------------------------------------------------------------------

    async def enqueue_unique(self, member: str, payload: dict, priority: int) -> None:
        '''
        Store payload and ZADD NX — payload always overwritten, queue entry
        only added if the member is not already present.

        Used for mutable bundle updates: rapid-fire calls collapse to one entry
        (same semantics as the in-memory sentinel dedup).
        '''
        pipe = self._redis.pipeline()
        pipe.set(self.payload_key(member), json.dumps(payload), ex=_PAYLOAD_TTL)
        pipe.zadd(self._queue_key, {member: self._score(priority)}, nx=True)
        await pipe.execute()

    async def enqueue(self, member: str, payload: dict, priority: int) -> None:
        '''
        Store payload and ZADD — always adds a new entry.
        Used for unique items (sends, deletes, fetch requests).
        '''
        pipe = self._redis.pipeline()
        pipe.set(self.payload_key(member), json.dumps(payload), ex=_PAYLOAD_TTL)
        pipe.zadd(self._queue_key, {member: self._score(priority)})
        await pipe.execute()

    # ------------------------------------------------------------------
    # Dequeueing
    # ------------------------------------------------------------------

    async def dequeue(self, timeout: float = 1.0) -> tuple[str, dict] | None:
        '''
        Blocking pop of the lowest-score (highest-priority) item.
        Returns ``(member, payload)`` or ``None`` on timeout.

        BZPOPMIN is atomic so two pods cannot receive the same item.
        '''
        result = await self._redis.bzpopmin(self._queue_key, timeout=timeout)
        if result is None:
            return None
        # result: (queue_key, member, score)
        member = result[1]
        pipe = self._redis.pipeline()
        pipe.get(self.payload_key(member))
        pipe.delete(self.payload_key(member))
        values = await pipe.execute()
        raw = values[0]
        if raw is None:
            # Payload expired or consumed by a concurrent pod (unlikely but safe)
            return None
        return member, json.loads(raw)

    # ------------------------------------------------------------------
    # Execution lock (mutable bundles)
    # ------------------------------------------------------------------

    async def acquire_lock(self, bundle_key: str) -> bool:
        '''
        Try to acquire the per-bundle execution lock.
        Returns True if acquired; False if another pod holds it.
        '''
        result = await self._redis.set(
            self.lock_key(bundle_key), self._pod_id, nx=True, ex=_LOCK_TTL
        )
        return bool(result)

    async def release_lock(self, bundle_key: str) -> None:
        '''Release the per-bundle execution lock.'''
        await self._redis.delete(self.lock_key(bundle_key))

    # ------------------------------------------------------------------
    # Fetch results
    # ------------------------------------------------------------------

    async def store_result(self, request_id: str, result: dict) -> None:
        '''Persist a fetch result in Redis so any pod can serve the poll.'''
        await self._redis.set(
            self.result_key(request_id), json.dumps(result), ex=_RESULT_TTL
        )

    async def get_result(self, request_id: str) -> dict | None:
        '''Return the stored result dict, or None if not yet ready.'''
        raw = await self._redis.get(self.result_key(request_id))
        return json.loads(raw) if raw else None
