'''
Persistent schedule of delayed message deletes (the ``delete_after`` feature).

discord.py implements ``delete_after`` as an in-memory ``sleep`` + delete task, so a
dispatcher restart inside the delay window silently loses the delete. The dispatcher
instead records each pending delete here and sweeps due entries itself, so any pod can
finish a delete that another pod (or a pod that has since been replaced) scheduled.
'''
from abc import ABC, abstractmethod
from dataclasses import dataclass

from discord_core.clients.redis_client import RedisManager

DELETE_DUE_KEY = 'discord_bot:delete_after:due'
DELETE_ATTEMPTS_KEY = 'discord_bot:delete_after:attempts'


@dataclass(frozen=True)
class PendingDelete:
    '''One claimed delete. ``attempts`` counts claims, including this one.'''
    guild_id: int
    channel_id: int
    message_id: int
    attempts: int

    @property
    def member(self) -> str:
        '''Stable identity of the message, used as the schedule's member key.'''
        return delete_member(self.guild_id, self.channel_id, self.message_id)


def delete_member(guild_id: int, channel_id: int, message_id: int) -> str:
    '''Schedule member for a message.'''
    return f'{guild_id}:{channel_id}:{message_id}'


class DeleteSchedule(ABC):
    '''Async interface for the persistent delayed-delete schedule.'''

    @abstractmethod
    async def schedule(self, guild_id: int, channel_id: int, message_id: int, delay_seconds: float) -> None:
        '''Delete the message *delay_seconds* from now; replaces any existing entry for it.'''

    @abstractmethod
    async def cancel(self, guild_id: int, channel_id: int, message_id: int) -> None:
        '''Forget a pending delete, e.g. because the message was already deleted.'''

    @abstractmethod
    async def claim_due(self, limit: int, lease_seconds: float) -> list[PendingDelete]:
        '''
        Claim up to *limit* due entries for this caller.

        A claimed entry stays scheduled but is pushed *lease_seconds* into the future, so no
        other pod claims it meanwhile and a claimer that dies mid-delete has it retried once
        the lease runs out. Each claim increments the entry's attempt count.
        '''

    @abstractmethod
    async def complete(self, entry: PendingDelete) -> None:
        '''Remove a claimed entry for good: deleted, already gone, or given up on.'''


# Lua so the due check, the lease push and the attempt count are one atomic step across
# pods, and so "now" is Redis's clock rather than each pod's.
_SCHEDULE_LUA = '''
local t = redis.call('TIME')
local now = tonumber(t[1]) + tonumber(t[2]) / 1000000
redis.call('ZADD', KEYS[1], now + tonumber(ARGV[2]), ARGV[1])
redis.call('HDEL', KEYS[2], ARGV[1])
return 1
'''

_CLAIM_LUA = '''
local t = redis.call('TIME')
local now = tonumber(t[1]) + tonumber(t[2]) / 1000000
local due = redis.call('ZRANGEBYSCORE', KEYS[1], '-inf', now, 'LIMIT', 0, tonumber(ARGV[1]))
local out = {}
for _, member in ipairs(due) do
    redis.call('ZADD', KEYS[1], now + tonumber(ARGV[2]), member)
    out[#out + 1] = member
    out[#out + 1] = redis.call('HINCRBY', KEYS[2], member, 1)
end
return out
'''


class RedisDeleteSchedule(DeleteSchedule):
    '''DeleteSchedule backed by a Redis sorted set (member -> due time) shared by every pod.'''

    def __init__(self, manager: RedisManager):
        self._manager = manager

    async def schedule(self, guild_id: int, channel_id: int, message_id: int, delay_seconds: float) -> None:
        await self._manager.client.eval(
            _SCHEDULE_LUA, 2, DELETE_DUE_KEY, DELETE_ATTEMPTS_KEY,
            delete_member(guild_id, channel_id, message_id), delay_seconds)

    async def cancel(self, guild_id: int, channel_id: int, message_id: int) -> None:
        member = delete_member(guild_id, channel_id, message_id)
        pipe = self._manager.client.pipeline()
        pipe.zrem(DELETE_DUE_KEY, member)
        pipe.hdel(DELETE_ATTEMPTS_KEY, member)
        await pipe.execute()

    async def claim_due(self, limit: int, lease_seconds: float) -> list[PendingDelete]:
        flat = await self._manager.client.eval(
            _CLAIM_LUA, 2, DELETE_DUE_KEY, DELETE_ATTEMPTS_KEY, limit, lease_seconds)
        claimed = []
        for member, attempts in zip(flat[::2], flat[1::2]):
            guild_id, channel_id, message_id = (int(part) for part in member.split(':'))
            claimed.append(PendingDelete(guild_id, channel_id, message_id, int(attempts)))
        return claimed

    async def complete(self, entry: PendingDelete) -> None:
        await self.cancel(entry.guild_id, entry.channel_id, entry.message_id)
