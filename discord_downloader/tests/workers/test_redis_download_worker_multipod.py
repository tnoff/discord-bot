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
from pathlib import Path

import pytest

from discord_core.clients.redis_client import RedisManager

from discord_downloader.workers.redis_download_worker import RedisDownloadWorker

from tests.fakes.worker_requests import queue_request
from tests.fakes.yielding_redis import two_pod_clients


def _two_pods(*, egress_a='default', egress_b='default'):
    '''Return two workers with independent clients sharing ONE FakeServer — two
    pods, one Redis — each wrapped so Redis ops yield and cross-pod races are real.'''
    def _pod(client, egress):
        worker = RedisDownloadWorker(
            None, Path('/tmp'),
            redis_manager=RedisManager.from_client(client),
            youtube_egress_key=egress, wait_period_minimum=10, wait_period_max_variance=2,
        )
        worker._startup_wait_until = 0.0
        worker._wait_timestamp = None
        return worker

    client_a, client_b = two_pod_clients()
    return _pod(client_a, egress_a), _pod(client_b, egress_b)


@pytest.mark.asyncio
async def test_two_pods_no_double_pop_of_one_request():
    '''Two pods race to pop the single queued DIRECT request; ZPOPMIN atomicity
    (under the pop lock) lets exactly one win and the other sees an empty queue.'''
    pod_a, pod_b = _two_pods()
    mr = queue_request(guild_id=7, direct=True)
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
    await pod_a.submit(7, queue_request(guild_id=7))
    await pod_a.submit(8, queue_request(guild_id=8))

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
    reqs = [queue_request(guild_id=7, direct=True), queue_request(guild_id=7, direct=True),
            queue_request(guild_id=8, direct=True), queue_request(guild_id=8, direct=True)]
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
    await pod_a.submit(7, queue_request(guild_id=7))
    await pod_b.submit(9, queue_request(guild_id=9))

    a_first = await pod_a._atomic_pop_youtube()      # pops + claims vpn-a
    assert a_first[0] != 'wait'
    # pod A is now backing off on vpn-a; a second A pop bails...
    await pod_a.submit(7, queue_request(guild_id=7))
    assert (await pod_a._atomic_pop_youtube())[0] == 'wait'
    # ...but pod B on vpn-b is unaffected.
    b_first = await pod_b._atomic_pop_youtube()
    assert b_first[0] != 'wait'
