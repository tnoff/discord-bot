'''Multi-pod coherency tests for RedisDownloadWorker.

These construct TWO independent ``RedisDownloadWorker`` instances — each with its
own client — pointed at ONE shared ``fakeredis.FakeServer``, i.e. two downloader
pods sharing one Redis.  That is the scenario the single-worker tests in
test_redis_download_worker.py cannot reach: their "concurrency" is concurrent
tasks inside one worker sharing in-process Python state, so a broken lock would
still pass.  Here the only shared thing is Redis, so the pop lock + ZPOPMIN
atomicity + the per-egress backoff claim have to actually serialise the cross-pod
read-modify-writes.

This is the MR 2b merge gate: the downloader-queue coherency that only surfaces
under real Redis + multiple downloader pods.
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
from discord_bot.workers.redis_download_worker import RedisDownloadWorker


class _YieldingRedis:
    '''Wrap a fakeredis client so every op the racing pop paths touch yields to
    the event loop before executing.

    Load-bearing for these tests.  fakeredis resolves its awaits synchronously —
    it never suspends the running coroutine — so two coroutines under
    asyncio.gather run to completion one after the other and an emergent
    read-modify-write race can NEVER interleave.  A real Redis round-trip *does*
    suspend at every await; the ``sleep(0)`` reinstates that suspend point so the
    cross-pod RMW actually races and the pop lock / claim has something to
    serialise.  The wrapped ops stay individually atomic (the yield is *before*
    the single fakeredis op), exactly like Redis.  Verified load-bearing: drop the
    ``if now < wait_until`` claim check in _atomic_pop_youtube and
    test_two_pods_youtube_claim_serialises_same_egress fails (both pods pop).
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
                    'zcard', 'zrem', 'zscore', 'zremrangebyscore'):
            return self._wrap(name)
        return getattr(self._inner, name)


def _two_pods(*, egress_a='default', egress_b='default'):
    '''Return two workers with independent clients sharing ONE FakeServer — two
    pods, one Redis — each wrapped so Redis ops yield and cross-pod races are real.'''
    server = fakeredis.FakeServer()

    def _pod(egress):
        client = _YieldingRedis(
            fakeredis.aioredis.FakeRedis(server=server, decode_responses=True))
        worker = RedisDownloadWorker(
            None, __import__('pathlib').Path('/tmp'),
            redis_manager=RedisManager.from_client(client),
            youtube_egress_key=egress, wait_period_minimum=10, wait_period_max_variance=2,
        )
        worker._startup_wait_until = 0.0
        worker._wait_timestamp = None
        return worker

    return _pod(egress_a), _pod(egress_b)


def _mk(*, guild_id=7, direct=False) -> MediaRequest:
    return MediaRequest(
        guild_id=guild_id, channel_id=2, requester_name='tester', requester_id=9,
        search_result=SearchResult(
            search_type=SearchType.DIRECT if direct else SearchType.SEARCH,
            raw_search_string='https://example.com/v.mp4' if direct else 'song name',
        ),
    )


@pytest.mark.asyncio
async def test_two_pods_no_double_pop_of_one_request():
    '''Two pods race to pop the single queued DIRECT request; ZPOPMIN atomicity
    (under the pop lock) lets exactly one win and the other sees an empty queue.'''
    pod_a, pod_b = _two_pods()
    mr = _mk(guild_id=7, direct=True)
    await pod_a.submit(7, mr)

    results = await asyncio.gather(
        pod_a._atomic_pop_direct(), pod_b._atomic_pop_direct())
    popped = [r for r in results if r is not None]
    assert len(popped) == 1
    assert popped[0][1] == str(mr.uuid)


@pytest.mark.asyncio
async def test_two_pods_youtube_claim_serialises_same_egress():
    '''Two YouTube pods on the SAME egress race pops with two items queued; the
    winner's pop-and-claim stamps a future wait_until, so the loser sees the claim
    and bails — exactly one item is popped, the other stays queued (rate-limited).
    Without the claim both pods would pop and hammer YouTube past its limit.'''
    pod_a, pod_b = _two_pods(egress_a='vpn', egress_b='vpn')
    await pod_a.submit(7, _mk(guild_id=7))
    await pod_a.submit(8, _mk(guild_id=8))

    results = await asyncio.gather(
        pod_a._atomic_pop_youtube(), pod_b._atomic_pop_youtube())
    popped = [r for r in results if r is not None and r[0] != 'wait']
    waited = [r for r in results if r is not None and r[0] == 'wait']
    assert len(popped) == 1
    assert len(waited) == 1
    # One request remains queued behind the claimed backoff window.
    assert (await pod_a.queue_size(7)) + (await pod_a.queue_size(8)) == 1


@pytest.mark.asyncio
async def test_two_pods_round_robin_fairness_direct():
    '''Two pods drain a two-guild DIRECT backlog concurrently; every request is
    popped exactly once across the pods (no loss, no duplication).'''
    pod_a, pod_b = _two_pods()
    reqs = [_mk(guild_id=7, direct=True), _mk(guild_id=7, direct=True),
            _mk(guild_id=8, direct=True), _mk(guild_id=8, direct=True)]
    for mr in reqs:
        await pod_a.submit(mr.guild_id, mr)

    popped = []
    for _ in range(4):
        got = await asyncio.gather(
            pod_a._atomic_pop_direct(), pod_b._atomic_pop_direct())
        popped.extend(r[1] for r in got if r is not None)

    assert sorted(popped) == sorted(str(mr.uuid) for mr in reqs)


@pytest.mark.asyncio
async def test_per_egress_backoff_isolation_across_pods():
    '''A backoff claimed on egress A must not block a pod on egress B: pod B pops
    its own request cleanly while pod A is backing off.'''
    pod_a, pod_b = _two_pods(egress_a='vpn-a', egress_b='vpn-b')
    await pod_a.submit(7, _mk(guild_id=7))
    await pod_b.submit(9, _mk(guild_id=9))

    a_first = await pod_a._atomic_pop_youtube()      # pops + claims vpn-a
    assert a_first[0] != 'wait'
    # pod A is now backing off on vpn-a; a second A pop bails...
    await pod_a.submit(7, _mk(guild_id=7))
    assert (await pod_a._atomic_pop_youtube())[0] == 'wait'
    # ...but pod B on vpn-b is unaffected.
    b_first = await pod_b._atomic_pop_youtube()
    assert b_first[0] != 'wait'
