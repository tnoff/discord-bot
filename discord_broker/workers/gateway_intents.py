'''
Redis-backed gateway intents: requests for a gateway to do something only a gateway can.

Another pod (the commands pod, #1030 Phase 2) cannot call a gateway, so it writes an intent here
and the gateway claims it with a short poll.  The contract is discord_core/routes/
gateway_intents.py; the kinds and their params are in discord_core/types/gateway_intents.py.

An intent lives through three states:  pending (submitted), claimed (leased to a gateway), done
(the gateway recorded an outcome).  Every kind is idempotent, which is what makes the lease safe:
a gateway that dies after claiming loses nothing, the intent is handed out again when the lease
lapses, and if two gateways race on one intent only one wins the lease.

Key schema:
    discord_bot:broker:gintent_seq              ->  INT, bumped by every submit; the order intents are handed out in
    discord_bot:broker:gintents                 ->  ZSET of intent ids not yet done, scored by that sequence
    discord_bot:broker:gintent:{id}             ->  HASH guild_id / kind / params / lease_until / gateway_id / state /
                                                    ok / error; expires, so an intent nobody claimed in time vanishes
    discord_bot:broker:gintentwake:{guild}      ->  id of the guild's pending wake, so a second wake is not stored twice

An intent id is "{guild_id}-{random hex}".  The guild is in it so every key an operation touches
can be named up front, which a Lua script must do.

The ZSET is cleaned up lazily: an id whose hash has expired is dropped the next time a claim walks
it, and a gateway polls many times a second, so it never grows.
'''
import json
import time
from uuid import uuid4

from discord_core.clients.redis_client import RedisManager

GINTENT_SEQ_KEY = 'discord_bot:broker:gintent_seq'
GINTENTS_KEY = 'discord_bot:broker:gintents'
GINTENT_KEY_PREFIX = 'discord_bot:broker:gintent:'
GINTENT_WAKE_KEY_PREFIX = 'discord_bot:broker:gintentwake:'

# An intent nobody claimed this long after it was submitted is dropped: a `stop` that a gateway
# finds minutes later, after a restart, would be a surprise, not a service.
INTENT_TTL_SECONDS = 30
# How long a claim holds an intent for one gateway before another may take it.  Long enough for a
# slow join, short enough that a gateway that died mid-claim is covered inside INTENT_TTL_SECONDS.
INTENT_LEASE_SECONDS = 10
# How long the outcome stays readable after the gateway records it.  The asking pod reads it within
# seconds; the rest is slack.
INTENT_RESULT_TTL_SECONDS = 60
# The ordering keys outlive no one: refreshed on every submit, gone a day after the last one.
INTENT_ORDER_TTL_SECONDS = 86400

# The ZSET score and the lease clock are milliseconds, so they are integers all the way through.
_MILLISECONDS = 1000

# KEYS: seq, order, intent, wake         ARGV: id, kind, guild_id, params_json, now_ms, ttl, order_ttl
# A wake is coalesced: the marker exists only while the wake is pending (a claim deletes it, and it
# expires with the intent), so finding one means that wake is still waiting and is returned as is.
_SUBMIT_LUA = """
if ARGV[2] == 'wake' then
    local pending = redis.call('GET', KEYS[4])
    if pending then return pending end
end
local seq = redis.call('INCR', KEYS[1])
redis.call('HSET', KEYS[3], 'guild_id', ARGV[3], 'kind', ARGV[2], 'params', ARGV[4],
           'created', ARGV[5], 'lease_until', '0', 'state', 'pending')
redis.call('EXPIRE', KEYS[3], ARGV[6])
redis.call('ZADD', KEYS[2], seq, ARGV[1])
redis.call('EXPIRE', KEYS[2], ARGV[7])
if ARGV[2] == 'wake' then redis.call('SET', KEYS[4], ARGV[1], 'EX', ARGV[6]) end
return ARGV[1]
"""

# Lease one intent to a gateway, unless it is gone or another gateway holds it.
# KEYS: order, intent, wake              ARGV: id, now_ms, lease_ms, gateway_id
# Returns {kind, guild_id, params_json}, or false.
_CLAIM_ONE_LUA = """
if redis.call('EXISTS', KEYS[2]) == 0 then
    redis.call('ZREM', KEYS[1], ARGV[1])
    return false
end
if tonumber(redis.call('HGET', KEYS[2], 'lease_until')) > tonumber(ARGV[2]) then return false end
redis.call('HSET', KEYS[2], 'lease_until', tonumber(ARGV[2]) + tonumber(ARGV[3]),
           'gateway_id', ARGV[4], 'state', 'claimed')
local kind = redis.call('HGET', KEYS[2], 'kind')
if kind == 'wake' and redis.call('GET', KEYS[3]) == ARGV[1] then redis.call('DEL', KEYS[3]) end
return {kind, redis.call('HGET', KEYS[2], 'guild_id'), redis.call('HGET', KEYS[2], 'params')}
"""

# KEYS: order, intent                    ARGV: id, ok (1/0), error, result_ttl
_COMPLETE_LUA = """
if redis.call('EXISTS', KEYS[2]) == 0 then return 0 end
redis.call('HSET', KEYS[2], 'state', 'done', 'ok', ARGV[2], 'error', ARGV[3])
redis.call('EXPIRE', KEYS[2], ARGV[4])
redis.call('ZREM', KEYS[1], ARGV[1])
return 1
"""


def _guild_of(intent_id: str) -> str:
    '''The guild an intent id names; a malformed id names none, so it matches no key.'''
    return intent_id.split('-', 1)[0]


class GatewayIntentRegistry:
    '''Async access to the gateway-intent keys.  Returns plain values; the handlers shape the wire.'''

    def __init__(self, manager: RedisManager):
        self._manager = manager

    @property
    def _client(self):
        return self._manager.client

    @staticmethod
    def _now_ms() -> int:
        return int(time.time() * _MILLISECONDS)

    @staticmethod
    def _intent_key(intent_id: str) -> str:
        return f'{GINTENT_KEY_PREFIX}{intent_id}'

    async def submit(self, guild_id: int, kind: str, params: dict) -> str:
        '''
        Store an intent for the guild's gateway and return its id.

        A wake for a guild that already has one pending stores nothing and returns that one's id.
        '''
        intent_id = f'{guild_id}-{uuid4().hex}'
        return await self._client.eval(
            _SUBMIT_LUA, 4, GINTENT_SEQ_KEY, GINTENTS_KEY, self._intent_key(intent_id),
            f'{GINTENT_WAKE_KEY_PREFIX}{guild_id}',
            intent_id, kind, guild_id, json.dumps(params), self._now_ms(),
            INTENT_TTL_SECONDS, INTENT_ORDER_TTL_SECONDS)

    async def claim(self, gateway_id: str) -> list[dict]:
        '''
        Lease every intent that is free to this gateway, in the order they were submitted.

        Each dict is {id, guild_id, kind, params}.  An intent another gateway holds is skipped, and
        comes back only if that lease lapses before the intent expires.
        '''
        ids = await self._client.zrange(GINTENTS_KEY, 0, -1)
        claimed = []
        for intent_id in ids:
            got = await self._client.eval(
                _CLAIM_ONE_LUA, 3, GINTENTS_KEY, self._intent_key(intent_id),
                f'{GINTENT_WAKE_KEY_PREFIX}{_guild_of(intent_id)}',
                intent_id, self._now_ms(), INTENT_LEASE_SECONDS * _MILLISECONDS, gateway_id)
            if got:
                kind, guild_id, params = got
                claimed.append({'id': intent_id, 'guild_id': int(guild_id), 'kind': kind,
                                'params': json.loads(params)})
        return claimed

    async def complete(self, intent_id: str, ok: bool, error: str | None) -> bool:
        '''Record the gateway's outcome.  False if the intent had already expired.'''
        recorded = await self._client.eval(
            _COMPLETE_LUA, 2, GINTENTS_KEY, self._intent_key(intent_id),
            intent_id, 1 if ok else 0, error or '', INTENT_RESULT_TTL_SECONDS)
        return bool(recorded)

    async def result(self, intent_id: str) -> dict:
        '''
        Where an intent got to: {done, ok, error}.

        done is False for an intent still pending or claimed, and for one that expired (the
        asking pod cannot tell those apart and does not need to).
        '''
        state, ok, error = await self._client.hmget(self._intent_key(intent_id), 'state', 'ok', 'error')
        if state != 'done':
            return {'done': False, 'ok': None, 'error': None}
        return {'done': True, 'ok': ok == '1', 'error': error or None}
