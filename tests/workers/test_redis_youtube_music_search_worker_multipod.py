'''Multi-pod coherency tests for RedisYoutubeMusicSearchWorker.

These construct TWO independent ``RedisYoutubeMusicSearchWorker`` instances — each
with its own client — pointed at ONE shared ``fakeredis.FakeServer``, i.e. two
search pods sharing one Redis.  That is the scenario the single-worker tests in
test_redis_youtube_music_search_worker.py cannot reach: their "concurrency" is
concurrent tasks inside one worker sharing in-process Python state, so a broken
lock would still pass.  Here the only shared thing is Redis, so ZPOPMIN atomicity
+ the SET NX pop-lock have to actually serialise the cross-pod round-robin pop.

This is the MR merge gate: the search-queue coherency that only surfaces under
real Redis + multiple search pods.  The load-bearing invariant here is
cross-pod exactly-once — two independent pods sharing one Redis ZSET partition
every request between them, popping each exactly once and losing none.  That is
guaranteed by ZPOPMIN atomicity over the *shared* Redis queue; the
exactly-once-partition test fails if the queue were ever backed by in-process
state instead of Redis (the "accidentally not shared" bug class).

Note on the pop-lock: unlike RedisDownloadWorker — whose lock guards a
load-bearing check-wait-then-claim per-egress read-modify-write — the search
worker's SET NX pop-lock is NOT required for exactly-once (ZPOPMIN alone
provides it, verified: the partition holds with the lock neutered).  The lock
serialises the round-robin zpopmin -> zcard -> zadd/zrem rotation so guild
fairness holds under genuinely-parallel pods (real Redis latency), where the
cooperative fakeredis+sleep(0) scheduling can't reproduce the unfair
interleaving.  So there is no non-hollow single-assertion gate for the lock
itself; it is kept for fairness parity with the downloader, not correctness.
'''
# White-box tests: they drive the workers' protected pop internals across pods.
# pylint: disable=protected-access
import asyncio

import fakeredis.aioredis
import pytest

from discord_bot.clients.redis_client import RedisManager
from discord_bot.cogs.music_helpers.common import SearchType
from discord_bot.types.media_request import MediaRequest
from discord_bot.types.search import SearchResult
from discord_bot.utils.failure_queue import FailureQueue
from discord_bot.workers.redis_youtube_music_search_worker import (
    RedisYoutubeMusicSearchWorker, GUILDS_KEY,
)


class _YieldingRedis:
    '''Wrap a fakeredis client so every op the racing pop paths touch yields to
    the event loop before executing.

    Load-bearing for these tests.  fakeredis resolves its awaits synchronously —
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
        if name in ('get', 'set', 'delete', 'zadd', 'zrange', 'zpopmin',
                    'zcard', 'zrem', 'zscore'):
            return self._wrap(name)
        return getattr(self._inner, name)


def _two_pods():
    '''Return two workers with independent clients sharing ONE FakeServer — two
    pods, one Redis — each wrapped so Redis ops yield and cross-pod races are real.'''
    server = fakeredis.FakeServer()

    def _pod():
        client = _YieldingRedis(
            fakeredis.aioredis.FakeRedis(server=server, decode_responses=True))
        return RedisYoutubeMusicSearchWorker(
            None, object(), FailureQueue(max_size=10, max_age_seconds=600),
            10, 2, redis_manager=RedisManager.from_client(client))

    return _pod(), _pod()


def _mk(*, guild_id=7) -> MediaRequest:
    return MediaRequest(
        guild_id=guild_id, channel_id=2, requester_name='tester', requester_id=9,
        search_result=SearchResult(search_type=SearchType.SEARCH, raw_search_string='song name'),
    )


@pytest.mark.asyncio
async def test_two_pods_no_double_pop_of_one_request():
    '''Two pods race to pop the single queued request; ZPOPMIN atomicity (under the
    pop lock) lets exactly one win and the other sees an empty queue.'''
    pod_a, pod_b = _two_pods()
    mr = _mk(guild_id=7)
    await pod_a.submit(7, mr)

    results = await asyncio.gather(
        pod_a.get_input_nowait(), pod_b.get_input_nowait(),
        return_exceptions=True)
    popped = [r for r in results if isinstance(r, MediaRequest)]
    empties = [r for r in results if isinstance(r, asyncio.QueueEmpty)]
    assert len(popped) == 1
    assert len(empties) == 1
    assert str(popped[0].uuid) == str(mr.uuid)


@pytest.mark.asyncio
async def test_two_pods_partition_every_request_exactly_once():
    '''Many requests across guilds, both pods draining concurrently: every request
    is popped exactly once — no double-pop, none lost.'''
    pod_a, pod_b = _two_pods()
    expected = set()
    for guild_id in (1, 2, 3):
        for _ in range(4):
            mr = _mk(guild_id=guild_id)
            expected.add(str(mr.uuid))
            await pod_a.submit(guild_id, mr)

    async def _drain(pod):
        got = []
        while True:
            try:
                got.append(await pod.get_input_nowait())
            except asyncio.QueueEmpty:
                return got

    drained = await asyncio.gather(_drain(pod_a), _drain(pod_b))
    popped_uuids = [str(mr.uuid) for batch in drained for mr in batch]
    assert sorted(popped_uuids) == sorted(expected)  # exactly-once partition
    assert len(popped_uuids) == len(set(popped_uuids))  # no double-pop


@pytest.mark.asyncio
async def test_two_pods_concurrent_drain_leaves_clean_tracker():
    '''After both pods concurrently drain a guild, the tracker + queue are empty.

    A coherency property the round-robin pop maintains across pods: once every
    request is drained, no guild is left stranded in the tracker and no queue
    entry dangles.  (Held by the zpopmin/zcard/zrem rotation itself — see the
    module docstring's note on why this is not a lock-dependence gate.)'''
    pod_a, pod_b = _two_pods()
    for _ in range(6):
        await pod_a.submit(7, _mk(guild_id=7))

    async def _drain(pod):
        while True:
            try:
                await pod.get_input_nowait()
            except asyncio.QueueEmpty:
                return

    await asyncio.gather(_drain(pod_a), _drain(pod_b))
    assert await pod_a._manager.client.zcard(GUILDS_KEY) == 0
    assert await pod_a.queue_size(7) == 0
