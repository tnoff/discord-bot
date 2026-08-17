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
from discord_bot.types.queue import PutsBlocked

GUILD_BLOCKED_SUFFIX = ':blocked'

# Why a TTL instead of an explicit unblock call.
#
# This mirrors DistributedQueue.block, which blocks a per-guild *queue object* --
# and that object is dropped whenever the guild's queue drains (get_nowait) or is
# cleared (clear_queue), so the next put_nowait recreates a fresh, unblocked
# queue.  The in-process block is therefore inherently transient, which is why
# there is no unblock method anywhere to mirror.
#
# A Redis key with no expiry would not reproduce that behaviour, it would invert
# it: the guild would stay blocked for the life of the Redis data and never
# accept another request.  The block only has to outlive the teardown it guards
# -- clear the guild's queue, then stop a racing submit from refilling a queue
# that is going away -- so it expires shortly after on its own.
GUILD_BLOCK_TTL_SECONDS = 60

# Redis TTL sentinel: the key exists but has no expiry set.  (-2 is "no such
# key".)  Only a -1 block key can be a legacy write -- see clear_stale_guild_blocks.
NO_EXPIRY_TTL = -1


class RedisGuildBlockMixin:
    '''
    block_guild plus submit-side enforcement for a Redis-backed worker.

    Mix in BEFORE the worker base so this submit runs first and cooperatively
    delegates to the base implementation:

        class RedisDownloadWorker(RedisGuildBlockMixin, DownloadWorkerBase):
            GUILD_KEY_PREFIX = GUILD_QUEUE_PREFIX

    The host class must expose ``_manager`` (a RedisManager) and set
    GUILD_KEY_PREFIX.

    Only submit is gated, not the internal enqueue paths.  "Block new
    submissions" means exactly that: the worker's own retry, no-exit-available
    and deferred-promotion re-queues are work already accepted, they run on the
    consumer loop with no PutsBlocked handler above them, and failing them would
    take the loop down rather than shed the request.
    '''

    GUILD_KEY_PREFIX: str = ''

    def _guild_blocked_key(self, guild_id: int) -> str:
        '''Redis key holding the block flag for one guild.'''
        return f'{self.GUILD_KEY_PREFIX}{guild_id}{GUILD_BLOCKED_SUFFIX}'

    async def block_guild(self, guild_id: int) -> bool:
        '''Block new submissions for a guild for the teardown window.'''
        await self._manager.client.set(self._guild_blocked_key(guild_id), '1',
                                       ex=GUILD_BLOCK_TTL_SECONDS)
        return True

    async def guild_is_blocked(self, guild_id: int) -> bool:
        '''True while the guild is inside its teardown block window.'''
        return bool(await self._manager.client.get(self._guild_blocked_key(guild_id)))

    async def clear_stale_guild_blocks(self) -> int:
        '''
        Drop block keys left behind with no expiry, returning how many went.

        Builds before GUILD_BLOCK_TTL_SECONDS landed wrote this key with a plain
        SET and no expiry, and nothing ever read it back, so the keys piled up
        unnoticed -- there is no unblock path anywhere to clear them with.  Adding
        the submit-side gate then turned every one of those leftovers into a
        permanent block on that guild: prod lost !play for every guild carrying
        one, and a guild only recovered by chance, when a teardown happened to
        overwrite its key with a TTL'd one.

        Keys with a TTL are left alone, which is what makes this safe to run
        unconditionally on every pod start.  Every write this class makes carries
        an expiry, so a -1 key cannot be a live block -- and a teardown racing
        this sweep keeps its block instead of having it swept out from under it.
        The same reasoning covers a restored Redis snapshot, where the leftovers
        come back with the rest of the data.
        '''
        client = self._manager.client
        pattern = f'{self.GUILD_KEY_PREFIX}*{GUILD_BLOCKED_SUFFIX}'
        stale = [key async for key in client.scan_iter(pattern)
                 if await client.ttl(key) == NO_EXPIRY_TTL]
        for key in stale:
            await client.delete(key)
        return len(stale)

    async def submit(self, guild_id: int, media_request: MediaRequest,
                     priority: int | None = None) -> None:
        '''Reject the submission if the guild is blocked, else enqueue as normal.'''
        if await self.guild_is_blocked(guild_id):
            raise PutsBlocked(f'Puts blocked for guild {guild_id}')
        await super().submit(guild_id, media_request, priority=priority)


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
