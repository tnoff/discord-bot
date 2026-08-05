'''
Shared Redis per-guild-queue primitives for the HA worker engines.

RedisDownloadWorker and RedisYoutubeMusicSearchWorker both back a per-guild ZSET
work queue with the same three mechanics: a token-tagged SET NX pop-lock, a
"drain one guild's ZSET" clear loop, and a status-snapshot shape served to a
bot-pod poller over HTTP.  The download worker adds a DIRECT fast-path and
per-egress bucketing on top; the search worker uses the single-pool subset.  The
shared bits live here once so the two workers stay duplicate-code (R0801) clean.

The pinned fakeredis test stack has no Lua, so the pop-lock is a token-tagged
SET NX rather than an EVAL script — mirrors RedisBrokerRegistry.bundle_lock.
'''
import asyncio
import json
import uuid as uuid_module
from contextlib import asynccontextmanager
from typing import Awaitable, Callable

from discord_bot.types.media_request import MediaRequest
from discord_bot.types.playlist_add_request import parse_media_request

POP_LOCK_TTL_SECONDS = 10
POP_LOCK_POLL_INTERVAL_SECONDS = 0.05
POP_LOCK_WAIT_SECONDS = 5.0


@asynccontextmanager
async def redis_pop_lock(client, lock_key: str):
    '''
    Hold a short-lived token-tagged SET NX lock over a pop critical section.

    Token-tagged so a slow holder whose TTL expired can't delete a successor's
    lock; falls through (token=None) after POP_LOCK_WAIT_SECONDS rather than
    deadlocking.  The timing constants are read at call time so tests can
    monkeypatch them on this module.
    '''
    token = uuid_module.uuid4().hex
    deadline = asyncio.get_running_loop().time() + POP_LOCK_WAIT_SECONDS
    while True:
        if await client.set(lock_key, token, nx=True, ex=POP_LOCK_TTL_SECONDS):
            break
        if asyncio.get_running_loop().time() >= deadline:
            token = None
            break
        await asyncio.sleep(POP_LOCK_POLL_INTERVAL_SECONDS)
    try:
        yield
    finally:
        if token is not None and await client.get(lock_key) == token:
            await client.delete(lock_key)


async def drain_guild_zset(client, queue_key: str,
                           request_key: Callable[[str], str],
                           preserve_predicate: Callable[[MediaRequest], bool] | None,
                           ) -> list[MediaRequest]:
    '''
    Drain one guild's queue ZSET, deleting each popped request payload, and return
    the dropped MediaRequests.

    Entries whose payload has already TTL'd away are removed silently (nothing to
    return); entries the *preserve_predicate* keeps are left in place.  The caller
    owns pruning the round-robin guild tracker afterwards.
    '''
    dropped: list[MediaRequest] = []
    uuids = await client.zrange(queue_key, 0, -1)
    for request_uuid in uuids:
        raw = await client.get(request_key(request_uuid))
        if raw is None:
            await client.zrem(queue_key, request_uuid)
            continue
        media_request = parse_media_request(json.loads(raw))
        if preserve_predicate is not None and preserve_predicate(media_request):
            continue
        await client.zrem(queue_key, request_uuid)
        await client.delete(request_key(request_uuid))
        dropped.append(media_request)
    return dropped


async def collect_queue_sizes(guild_ids: list,
                              queue_size: Callable[[int], Awaitable[int]],
                              ) -> dict[str, int]:
    '''Map each guild id to its pending count via *queue_size*.'''
    sizes: dict[str, int] = {}
    for guild_id in guild_ids:
        sizes[str(guild_id)] = await queue_size(int(guild_id))
    return sizes


def build_status_snapshot(failure_summary: str, failure_count: int,
                          backoff_seconds: int | None, queue_sizes: dict[str, int],
                          ) -> dict:
    '''Assemble the worker status dict a bot-pod poller reads over HTTP.'''
    return {
        'failure_summary': failure_summary,
        'failure_count': failure_count,
        'backoff_seconds_remaining': backoff_seconds or None,
        'queue_sizes': queue_sizes,
    }
