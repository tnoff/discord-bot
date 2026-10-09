'''
Redis-backed per-guild player state: the ordered play queue, the now-playing record, the
history list, and the markers the gateway watches (queue version, pending skip).

The queue holds request uuids only.  What a uuid IS lives in the broker entry (see
broker_registry.py); this module says which tracks a guild's player holds and in what order.
Every mutation is one Lua script, so concurrent callers (the pod running a command, the
gateway's playback loop) only ever see whole operations.

Key schema:
    discord_bot:broker:gqueue:{guild}      →  LIST of request uuids, head (index 0) plays next
    discord_bot:broker:gclaim:{guild}      →  uuid popped from gqueue and not yet confirmed as playing
    discord_bot:broker:gplaying:{guild}    →  HASH uuid / started_at / gateway_id, short TTL kept alive by a heartbeat
    discord_bot:broker:ghistory:{guild}    →  LIST of JSON blobs for tracks that played to the end (oldest first)
    discord_bot:broker:gversion:{guild}    →  INT bumped by every queue mutation
    discord_bot:broker:gskip:{guild}       →  uuid of the playing track a skip was requested for (short TTL)
    discord_bot:broker:gclosed:{guild}     →  flag, set while the guild's player is shut down
'''
import json
import random
import time
from dataclasses import dataclass, field

from discord_core.clients.redis_client import RedisManager

GQUEUE_KEY_PREFIX = 'discord_bot:broker:gqueue:'
GCLAIM_KEY_PREFIX = 'discord_bot:broker:gclaim:'
GPLAYING_KEY_PREFIX = 'discord_bot:broker:gplaying:'
GHISTORY_KEY_PREFIX = 'discord_bot:broker:ghistory:'
GVERSION_KEY_PREFIX = 'discord_bot:broker:gversion:'
GSKIP_KEY_PREFIX = 'discord_bot:broker:gskip:'
GCLOSED_KEY_PREFIX = 'discord_bot:broker:gclosed:'
# Guild queue keys share the entry TTL: a queue outliving the entries it points at
# is useless, and a guild nobody has touched in a day should not squat in Redis.
# Every write refreshes it.
GUILD_QUEUE_TTL_SECONDS = 86400
# The now-playing hash is liveness, not state: the gateway refreshes it while a track
# plays, so a gateway that dies stops looking like it is playing within this window.
PLAYING_TTL_SECONDS = 15
# A skip marker only means something for the track it names. It expires on its own so
# a skip aimed at a gateway that never saw it cannot linger.
SKIP_TTL_SECONDS = 30
SHUFFLE_MAX_ATTEMPTS = 5

# Results of GuildQueueRegistry.enqueue, also the strings the Lua script returns.
ENQUEUE_OK = 'ok'
ENQUEUE_CLOSED = 'closed'
ENQUEUE_FULL = 'full'
ENQUEUE_DUPLICATE = 'duplicate'

# Results of request_skip.
SKIP_OK = 'ok'
SKIP_NO_PLAYER = 'no_player'
SKIP_NOT_CURRENT = 'not_current'

# Every script below is one atomic step on the Redis server. The guild queue is
# mutated from more than one place (the pod running a command, the gateway's
# playback loop), so a read-then-write in Python would race; these do not.
# KEYS and ARGV are positional -- each script's header says what they are.

# KEYS: queue, version, closed          ARGV: uuid, max_size (0 = unbounded), ttl
_ENQUEUE_LUA = """
if redis.call('EXISTS', KEYS[3]) == 1 then return 'closed' end
if redis.call('LPOS', KEYS[1], ARGV[1]) then return 'duplicate' end
if tonumber(ARGV[2]) > 0 and redis.call('LLEN', KEYS[1]) >= tonumber(ARGV[2]) then return 'full' end
redis.call('RPUSH', KEYS[1], ARGV[1])
redis.call('EXPIRE', KEYS[1], ARGV[3])
redis.call('INCR', KEYS[2])
redis.call('EXPIRE', KEYS[2], ARGV[3])
return 'ok'
"""

# KEYS: queue, version                  ARGV: uuid, ttl
_REMOVE_LUA = """
local removed = redis.call('LREM', KEYS[1], 1, ARGV[1])
if removed > 0 then
    redis.call('INCR', KEYS[2])
    redis.call('EXPIRE', KEYS[2], ARGV[2])
end
return removed
"""

# KEYS: queue, version                  ARGV: uuid, ttl
_BUMP_LUA = """
local removed = redis.call('LREM', KEYS[1], 1, ARGV[1])
if removed > 0 then
    redis.call('LPUSH', KEYS[1], ARGV[1])
    redis.call('EXPIRE', KEYS[1], ARGV[2])
    redis.call('INCR', KEYS[2])
    redis.call('EXPIRE', KEYS[2], ARGV[2])
end
return removed
"""

# Redis has no shuffle and a script's own PRNG is not worth trusting, so the caller
# picks the permutation and the script applies it -- but only if the list is still the
# length the permutation was drawn for. -1 means it changed; the caller redraws.
# KEYS: queue, version                  ARGV: expected_len, ttl, permutation (1-based) ...
_SHUFFLE_LUA = """
local n = redis.call('LLEN', KEYS[1])
if n ~= tonumber(ARGV[1]) then return -1 end
if n < 2 then return 0 end
local items = redis.call('LRANGE', KEYS[1], 0, -1)
redis.call('DEL', KEYS[1])
for i = 3, n + 2 do
    redis.call('RPUSH', KEYS[1], items[tonumber(ARGV[i])])
end
redis.call('EXPIRE', KEYS[1], ARGV[2])
redis.call('INCR', KEYS[2])
redis.call('EXPIRE', KEYS[2], ARGV[2])
return 1
"""

# KEYS: queue, version                  ARGV: ttl
_CLEAR_LUA = """
local items = redis.call('LRANGE', KEYS[1], 0, -1)
if #items > 0 then
    redis.call('DEL', KEYS[1])
    redis.call('INCR', KEYS[2])
    redis.call('EXPIRE', KEYS[2], ARGV[1])
end
return items
"""

# Hand out the next track. The popped uuid is parked in the claim key until the caller
# confirms it, so a caller that dies between pop and play loses nothing: the next claim
# returns the same uuid again instead of the one behind it.
# KEYS: queue, claim, version           ARGV: ttl
_CLAIM_LUA = """
local held = redis.call('GET', KEYS[2])
if held then return held end
local uuid = redis.call('LPOP', KEYS[1])
if not uuid then return false end
redis.call('SET', KEYS[2], uuid, 'EX', ARGV[1])
redis.call('INCR', KEYS[3])
redis.call('EXPIRE', KEYS[3], ARGV[1])
return uuid
"""

# KEYS: claim, playing                  ARGV: uuid, started_at, gateway_id, playing_ttl
_CONFIRM_CLAIM_LUA = """
if redis.call('GET', KEYS[1]) ~= ARGV[1] then return 0 end
redis.call('DEL', KEYS[1])
redis.call('DEL', KEYS[2])
redis.call('HSET', KEYS[2], 'uuid', ARGV[1], 'started_at', ARGV[2], 'gateway_id', ARGV[3])
redis.call('EXPIRE', KEYS[2], ARGV[4])
return 1
"""

# KEYS: playing                         ARGV: uuid, ttl
_HEARTBEAT_LUA = """
if redis.call('HGET', KEYS[1], 'uuid') ~= ARGV[1] then return 0 end
redis.call('EXPIRE', KEYS[1], ARGV[2])
return 1
"""

# KEYS: playing, skip                   ARGV: expected uuid, ttl
_REQUEST_SKIP_LUA = """
local current = redis.call('HGET', KEYS[1], 'uuid')
if not current then return 'no_player' end
if current ~= ARGV[1] then return 'not_current' end
redis.call('SET', KEYS[2], current, 'EX', ARGV[2])
return 'ok'
"""

# End of a track. Clears the now-playing and skip markers for this uuid (and only this
# uuid: a late finish for a track that is no longer current must not erase its successor),
# then records it in history unless it was skipped.
# KEYS: playing, skip, history, version ARGV: uuid, record_history (1/0), history_json, history_cap, ttl
_FINISH_LUA = """
if redis.call('HGET', KEYS[1], 'uuid') == ARGV[1] then redis.call('DEL', KEYS[1]) end
if redis.call('GET', KEYS[2]) == ARGV[1] then redis.call('DEL', KEYS[2]) end
if ARGV[2] == '1' then
    redis.call('RPUSH', KEYS[3], ARGV[3])
    redis.call('LTRIM', KEYS[3], -tonumber(ARGV[4]), -1)
    redis.call('EXPIRE', KEYS[3], ARGV[5])
end
redis.call('INCR', KEYS[4])
redis.call('EXPIRE', KEYS[4], ARGV[5])
return 1
"""

# Shut a guild's player down: refuse further enqueues and hand back everything that was
# still held so the caller can release it.
# KEYS: queue, claim, playing, history, skip, closed, version   ARGV: ttl
# Returns a flat list: the unconfirmed claim uuid ('' if none), the playing uuid ('' if
# none), then the queued uuids in play order.
_CLOSE_LUA = """
local queued = redis.call('LRANGE', KEYS[1], 0, -1)
local claimed = redis.call('GET', KEYS[2]) or ''
local playing = redis.call('HGET', KEYS[3], 'uuid') or ''
redis.call('DEL', KEYS[1], KEYS[2], KEYS[3], KEYS[4], KEYS[5])
redis.call('SET', KEYS[6], '1', 'EX', ARGV[1])
redis.call('INCR', KEYS[7])
redis.call('EXPIRE', KEYS[7], ARGV[1])
return {claimed, playing, unpack(queued)}
"""




@dataclass
class GuildQueueState:
    '''
    One consistent read of a guild's player state.

    version moves on every queue mutation, so a caller holding an older version knows its
    view is stale without comparing contents.  playing is the now-playing hash
    (uuid / started_at / gateway_id) or None when nothing is, or the gateway stopped
    refreshing it.
    '''
    version: int = 0
    queue: list[str] = field(default_factory=list)
    playing: dict | None = None
    skip_for: str | None = None
    closed: bool = False


class GuildQueueRegistry:
    '''
    Async access to the guild queue keys.

    Methods return plain uuids and dicts; turning those into entries is GuildQueueBroker's job.
    '''

    def __init__(self, manager: RedisManager):
        self._manager = manager

    @property
    def _client(self):
        return self._manager.client

    # ------------------------------------------------------------------
    # Guild queue
    #
    # Ordered per-guild play state. Entries (above) stay the source of truth for what a
    # track IS; these keys only say which tracks a guild's player holds and in what order.
    # Every mutation is one Lua script, so concurrent callers see whole operations only.
    # ------------------------------------------------------------------

    def _guild_keys(self, guild_id: int) -> dict[str, str]:
        return {
            'queue': f'{GQUEUE_KEY_PREFIX}{guild_id}',
            'claim': f'{GCLAIM_KEY_PREFIX}{guild_id}',
            'playing': f'{GPLAYING_KEY_PREFIX}{guild_id}',
            'history': f'{GHISTORY_KEY_PREFIX}{guild_id}',
            'version': f'{GVERSION_KEY_PREFIX}{guild_id}',
            'skip': f'{GSKIP_KEY_PREFIX}{guild_id}',
            'closed': f'{GCLOSED_KEY_PREFIX}{guild_id}',
        }

    async def queue_enqueue(self, guild_id: int, uuid: str, max_size: int = 0) -> str:
        '''
        Append uuid to the guild's queue.

        Returns ENQUEUE_OK, ENQUEUE_CLOSED (the guild's player is shut down),
        ENQUEUE_FULL (max_size reached; 0 means unbounded) or ENQUEUE_DUPLICATE (uuid is
        already queued).  Closed and full mirror the PutsBlocked and QueueFull rejections
        the in-process queue raised.
        '''
        keys = self._guild_keys(guild_id)
        return await self._client.eval(
            _ENQUEUE_LUA, 3, keys['queue'], keys['version'], keys['closed'],
            uuid, max_size, GUILD_QUEUE_TTL_SECONDS)

    async def queue_remove(self, guild_id: int, uuid: str) -> bool:
        '''Remove uuid from the queue. False if it was not queued (already played, or removed).'''
        keys = self._guild_keys(guild_id)
        removed = await self._client.eval(
            _REMOVE_LUA, 2, keys['queue'], keys['version'], uuid, GUILD_QUEUE_TTL_SECONDS)
        return removed > 0

    async def queue_bump(self, guild_id: int, uuid: str) -> bool:
        '''Move uuid to the head of the queue. False if it was not queued.'''
        keys = self._guild_keys(guild_id)
        bumped = await self._client.eval(
            _BUMP_LUA, 2, keys['queue'], keys['version'], uuid, GUILD_QUEUE_TTL_SECONDS)
        return bumped > 0

    async def queue_shuffle(self, guild_id: int, rng: random.Random | None = None) -> bool:
        '''
        Shuffle the queue in place.

        The permutation is drawn here and applied atomically.  If the queue changes length
        between drawing and applying, draw again.  Returns False only if the queue kept
        changing under every attempt; an empty or single-item queue is a trivially successful
        shuffle, like the in-process one.
        '''
        keys = self._guild_keys(guild_id)
        rng = rng or random.Random()  # nosec B311 - shuffle order, not a secret
        for _ in range(SHUFFLE_MAX_ATTEMPTS):
            length = await self._client.llen(keys['queue'])
            permutation = rng.sample(range(1, length + 1), length)
            result = await self._client.eval(
                _SHUFFLE_LUA, 2, keys['queue'], keys['version'],
                length, GUILD_QUEUE_TTL_SECONDS, *permutation)
            if result != -1:
                return True
        return False

    async def queue_clear(self, guild_id: int) -> list[str]:
        '''Empty the queue and return the uuids that were in it, in play order.'''
        keys = self._guild_keys(guild_id)
        return await self._client.eval(
            _CLEAR_LUA, 2, keys['queue'], keys['version'], GUILD_QUEUE_TTL_SECONDS)

    async def queue_claim_next(self, guild_id: int) -> str | None:
        '''
        Take the next uuid off the queue, or None if it is empty.

        The uuid stays parked in a claim until confirm_claim or drop_claim, and a second call
        before then returns the same uuid, not the one behind it.  That is what makes a crash
        between popping a track and starting it recoverable.
        '''
        keys = self._guild_keys(guild_id)
        return await self._client.eval(
            _CLAIM_LUA, 3, keys['queue'], keys['claim'], keys['version'], GUILD_QUEUE_TTL_SECONDS)

    async def drop_claim(self, guild_id: int) -> None:
        '''Forget an unconfirmed claim, for a track that turned out to be unplayable.'''
        await self._client.delete(self._guild_keys(guild_id)['claim'])

    async def confirm_claim(self, guild_id: int, uuid: str, gateway_id: str,
                            started_at: float | None = None) -> bool:
        '''
        Turn the claim on uuid into the guild's now-playing record.

        False if the claim is not on uuid (it was dropped, or the guild was closed meanwhile).
        '''
        keys = self._guild_keys(guild_id)
        confirmed = await self._client.eval(
            _CONFIRM_CLAIM_LUA, 2, keys['claim'], keys['playing'],
            uuid, started_at if started_at is not None else time.time(), gateway_id,
            PLAYING_TTL_SECONDS)
        return bool(confirmed)

    async def playing_heartbeat(self, guild_id: int, uuid: str) -> bool:
        '''Keep the now-playing record alive. False if uuid is no longer the playing track.'''
        keys = self._guild_keys(guild_id)
        alive = await self._client.eval(
            _HEARTBEAT_LUA, 1, keys['playing'], uuid, PLAYING_TTL_SECONDS)
        return bool(alive)

    async def request_skip(self, guild_id: int, expect_uuid: str) -> str:
        '''
        Ask the gateway to skip the playing track, provided it is still expect_uuid.

        Returns SKIP_OK, SKIP_NO_PLAYER or SKIP_NOT_CURRENT.  Naming the track is what stops
        a skip that arrives late from skipping the track after it.
        '''
        keys = self._guild_keys(guild_id)
        return await self._client.eval(
            _REQUEST_SKIP_LUA, 2, keys['playing'], keys['skip'], expect_uuid, SKIP_TTL_SECONDS)

    async def finish_track(self, guild_id: int, uuid: str, skipped: bool,
                           history_item: dict | None, history_cap: int) -> None:
        '''
        Close out a track: clear its now-playing and skip markers, and append history_item to
        the guild's history (kept to the last history_cap) unless it was skipped.
        '''
        keys = self._guild_keys(guild_id)
        record = not skipped and history_item is not None and history_cap > 0
        await self._client.eval(
            _FINISH_LUA, 4, keys['playing'], keys['skip'], keys['history'], keys['version'],
            uuid, 1 if record else 0, json.dumps(history_item) if record else '',
            history_cap, GUILD_QUEUE_TTL_SECONDS)

    async def queue_state(self, guild_id: int) -> GuildQueueState:
        '''Read queue, version, now-playing, pending skip and closed flag as one snapshot.'''
        keys = self._guild_keys(guild_id)
        async with self._client.pipeline(transaction=True) as pipe:
            pipe.lrange(keys['queue'], 0, -1)
            pipe.get(keys['version'])
            pipe.hgetall(keys['playing'])
            pipe.get(keys['skip'])
            pipe.exists(keys['closed'])
            queue, version, playing, skip_for, closed = await pipe.execute()
        return GuildQueueState(
            version=int(version) if version else 0,
            queue=queue,
            playing=playing or None,
            skip_for=skip_for,
            closed=bool(closed),
        )

    async def queue_poll(self, guild_id: int) -> tuple[int, str | None]:
        '''Cheap change check for a poller: (version, uuid a skip is pending for).'''
        keys = self._guild_keys(guild_id)
        version, skip_for = await self._client.mget(keys['version'], keys['skip'])
        return (int(version) if version else 0, skip_for)

    async def history_items(self, guild_id: int) -> list[dict]:
        '''The guild's history, oldest first.'''
        raw = await self._client.lrange(self._guild_keys(guild_id)['history'], 0, -1)
        return [json.loads(item) for item in raw]

    async def queue_close(self, guild_id: int) -> tuple[list[str], str | None, str | None]:
        '''
        Shut down a guild's player state: refuse further enqueues and clear everything it held.

        Returns (queued uuids in play order, unconfirmed claim uuid, playing uuid) so the caller
        can release the entries behind them.  Stays closed until queue_open.
        '''
        keys = self._guild_keys(guild_id)
        result = await self._client.eval(
            _CLOSE_LUA, 7, keys['queue'], keys['claim'], keys['playing'], keys['history'],
            keys['skip'], keys['closed'], keys['version'], GUILD_QUEUE_TTL_SECONDS)
        claimed, playing, *queued = result
        return queued, claimed or None, playing or None

    async def queue_open(self, guild_id: int) -> None:
        '''Reopen a closed guild so a new player can enqueue again.'''
        await self._client.delete(self._guild_keys(guild_id)['closed'])
