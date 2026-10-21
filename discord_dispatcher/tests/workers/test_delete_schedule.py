'''Tests for the persistent delayed-delete schedule (Redis implementation on fakeredis).'''
import asyncio

import fakeredis.aioredis
import pytest

from discord_core.clients.redis_client import RedisManager

from discord_dispatcher.workers.delete_schedule import (
    DELETE_ATTEMPTS_KEY, DELETE_DUE_KEY, PendingDelete, RedisDeleteSchedule, delete_member,
)

LEASE = 60.0


def _client():
    return fakeredis.aioredis.FakeRedis(decode_responses=True)


def _schedule(client=None):
    '''A RedisDeleteSchedule, i.e. one dispatcher pod; pass a shared client for "another pod".'''
    return RedisDeleteSchedule(RedisManager.from_client(client or _client()))


def test_pending_delete_member_matches_delete_member():
    '''PendingDelete.member is the same key schedule() writes.'''
    assert PendingDelete(1, 2, 3, 1).member == delete_member(1, 2, 3) == '1:2:3'


@pytest.mark.asyncio
async def test_not_due_entry_is_not_claimed():
    '''An entry whose delay has not elapsed is not returned.'''
    schedule = _schedule()
    await schedule.schedule(1, 2, 3, 3600)
    assert await schedule.claim_due(10, LEASE) == []


@pytest.mark.asyncio
async def test_due_entry_is_claimed_with_attempt_count():
    '''A due entry is returned with its ids and attempts == 1.'''
    schedule = _schedule()
    await schedule.schedule(1, 2, 3, 0)
    assert await schedule.claim_due(10, LEASE) == [PendingDelete(1, 2, 3, 1)]


@pytest.mark.asyncio
async def test_claimed_entry_is_leased_until_completed():
    '''A claimed entry is not handed out again while its lease runs, and complete() removes it.'''
    client = _client()
    schedule = _schedule(client)
    await schedule.schedule(1, 2, 3, 0)
    entry = (await schedule.claim_due(10, LEASE))[0]
    assert await schedule.claim_due(10, LEASE) == []
    # Still scheduled: a crash here must leave it for a later claim.
    assert await client.zscore(DELETE_DUE_KEY, entry.member) is not None

    await schedule.complete(entry)
    assert await client.zcard(DELETE_DUE_KEY) == 0
    assert await client.hlen(DELETE_ATTEMPTS_KEY) == 0


@pytest.mark.asyncio
async def test_expired_lease_is_reclaimed_with_higher_attempts():
    '''A claimer that died leaves its entry to be claimed again once the lease lapses.'''
    schedule = _schedule()
    await schedule.schedule(1, 2, 3, 0)
    assert (await schedule.claim_due(10, 0))[0].attempts == 1
    assert (await schedule.claim_due(10, 0))[0].attempts == 2


@pytest.mark.asyncio
async def test_claim_respects_limit():
    '''claim_due returns at most *limit* entries.'''
    schedule = _schedule()
    for message_id in range(5):
        await schedule.schedule(1, 2, message_id, 0)
    assert len(await schedule.claim_due(2, LEASE)) == 2
    assert len(await schedule.claim_due(10, LEASE)) == 3


@pytest.mark.asyncio
async def test_rescheduling_replaces_entry_and_resets_attempts():
    '''A new delete_after for the same message replaces the old due time and attempt count.'''
    client = _client()
    schedule = _schedule(client)
    await schedule.schedule(1, 2, 3, 0)
    await schedule.claim_due(10, 0)
    assert await client.hget(DELETE_ATTEMPTS_KEY, '1:2:3') == '1'

    await schedule.schedule(1, 2, 3, 3600)
    assert await client.zcard(DELETE_DUE_KEY) == 1
    assert await client.hlen(DELETE_ATTEMPTS_KEY) == 0
    assert await schedule.claim_due(10, LEASE) == []


@pytest.mark.asyncio
async def test_cancel_removes_entry():
    '''cancel() drops a pending delete so it is never claimed.'''
    schedule = _schedule()
    await schedule.schedule(1, 2, 3, 0)
    await schedule.cancel(1, 2, 3)
    assert await schedule.claim_due(10, LEASE) == []


@pytest.mark.asyncio
async def test_cancel_of_unknown_entry_is_a_noop():
    '''Cancelling something never scheduled does not raise.'''
    await _schedule().cancel(1, 2, 3)


@pytest.mark.asyncio
async def test_entry_survives_pod_replacement():
    '''An entry scheduled by one pod is claimed by a different pod on the same Redis.'''
    client = _client()
    await _schedule(client).schedule(1, 2, 3, 0)
    # The first pod's object is gone; a fresh one (the replacement pod) sees the entry.
    assert await _schedule(client).claim_due(10, LEASE) == [PendingDelete(1, 2, 3, 1)]


@pytest.mark.asyncio
async def test_two_pods_claim_a_due_entry_once():
    '''Concurrent claims from two pods hand each due entry to exactly one of them.'''
    client = _client()
    pod_a, pod_b = _schedule(client), _schedule(client)
    for message_id in range(20):
        await pod_a.schedule(1, 2, message_id, 0)

    claimed_a, claimed_b = await asyncio.gather(
        pod_a.claim_due(50, LEASE), pod_b.claim_due(50, LEASE))

    ids = [entry.message_id for entry in claimed_a + claimed_b]
    assert sorted(ids) == list(range(20))
