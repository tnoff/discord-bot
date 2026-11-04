'''A fakeredis client that yields to the event loop on every op, for tests of cross-pod races.'''
import asyncio

import fakeredis
import fakeredis.aioredis

YIELDING_OPS = ('get', 'set', 'delete', 'zadd', 'zrange', 'zpopmin', 'zcard', 'zrem', 'zscore',
                'zremrangebyscore')


class YieldingRedis:
    '''Wrap a fakeredis client so every op the racing pop paths touch yields to
    the event loop before executing.

    Load-bearing for the multi-pod tests.  fakeredis resolves its awaits synchronously —
    it never suspends the running coroutine — so two coroutines under
    asyncio.gather run to completion one after the other and an emergent
    read-modify-write race can NEVER interleave.  A real Redis round-trip *does*
    suspend at every await; the ``sleep(0)`` reinstates that suspend point so the
    cross-pod RMW actually races and the pop lock has something to serialise.  The
    wrapped ops stay individually atomic (the yield is *before* the single
    fakeredis op), exactly like Redis.
    '''

    def __init__(self, inner):
        self._inner = inner

    def _wrap(self, name):
        inner_attr = getattr(self._inner, name)

        async def _op(*args, **kwargs):
            await asyncio.sleep(0)
            return await inner_attr(*args, **kwargs)
        return _op

    def __getattr__(self, name):
        if name in YIELDING_OPS:
            return self._wrap(name)
        return getattr(self._inner, name)


def two_pod_clients():
    '''Two independent yielding clients sharing ONE FakeServer — two pods, one Redis.'''
    server = fakeredis.FakeServer()
    return tuple(
        YieldingRedis(fakeredis.aioredis.FakeRedis(server=server, decode_responses=True))
        for _ in range(2))
