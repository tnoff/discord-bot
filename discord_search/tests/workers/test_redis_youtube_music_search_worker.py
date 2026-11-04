'''
Unit tests for RedisYoutubeMusicSearchWorker.

Single-instance coverage of the Redis queue surface: key schema, submit/routing,
round-robin pops, clear/block, and the shared cross-pod 429 backoff window +
failure ZSET.  Cross-pod coherency lives in
test_redis_youtube_music_search_worker_multipod.py.
'''
# White-box tests: they drive the worker's protected pop/backoff internals and
# inspect its Redis client directly, so protected-access is expected here.
# pylint: disable=protected-access
import asyncio
from asyncio import QueueEmpty

import fakeredis.aioredis
import pytest

from discord_core.clients.redis_client import RedisManager

from discord_core.types.queue import PutsBlocked
from discord_core.utils.failure_queue import FailureQueue

from discord_search.utils.integrations.youtube_music import YoutubeMusicRetryException
from discord_search.workers.redis_youtube_music_search_worker import (
    RedisYoutubeMusicSearchWorker, FAILURES_KEY, GUILDS_KEY, POP_LOCK_KEY, WAIT_UNTIL_KEY,
)

from tests.fakes.worker_requests import queue_request
from tests.guild_queue_worker_suite import GuildQueueWorkerSuite, spy_redis_writes


class _FakeYoutubeMusicClient:
    '''Stand-in for YoutubeMusicClient.search: returns a fixed id or raises 429.'''

    def __init__(self, video_id=None, *, raise_retry=False):
        self._video_id = video_id
        self._raise_retry = raise_retry
        self.calls = []

    def search(self, raw_search_string):
        self.calls.append(raw_search_string)
        if self._raise_retry:
            raise YoutubeMusicRetryException('429 Exhaust Limit Hit')
        return self._video_id


def _manager() -> RedisManager:
    return RedisManager.from_client(fakeredis.aioredis.FakeRedis(decode_responses=True))


def _worker(manager=None, *, client=None, wait_min=10, variance=2) -> RedisYoutubeMusicSearchWorker:
    return RedisYoutubeMusicSearchWorker(
        None,
        client or _FakeYoutubeMusicClient(),
        FailureQueue(max_size=10, max_age_seconds=600),
        wait_min,
        variance,
        redis_manager=manager or _manager(),
    )


class TestSearchGuildQueue(GuildQueueWorkerSuite):
    '''The shared guild-queue cases, against the search worker.'''
    guilds_key = GUILDS_KEY

    def make_worker(self):
        return _worker()

    def make_request(self, guild_id=7):
        return queue_request(guild_id=guild_id)


# --------------------------------------------------------------------------- #
# submit / pop / queue_size
# --------------------------------------------------------------------------- #

@pytest.mark.asyncio
async def test_submit_then_pop_round_trips_the_request():
    '''submit persists + enqueues; get_input_nowait pops the same request back.'''
    w = _worker()
    mr = queue_request(guild_id=7)
    assert await w.queue_size(7) == 0
    await w.submit(7, mr)
    assert await w.queue_size(7) == 1
    popped = await w.get_input_nowait()
    assert str(popped.uuid) == str(mr.uuid)
    assert await w.queue_size(7) == 0


@pytest.mark.asyncio
async def test_get_input_nowait_raises_when_empty():
    '''An empty queue raises QueueEmpty (no guild in the round-robin tracker).'''
    w = _worker()
    with pytest.raises(QueueEmpty):
        await w.get_input_nowait()


@pytest.mark.asyncio
async def test_get_input_nowait_raises_when_payload_missing():
    '''A dangling ZSET entry whose payload TTL'd away raises QueueEmpty.'''
    w = _worker()
    mr = queue_request(guild_id=7)
    await w.submit(7, mr)
    # Simulate the payload expiring out from under the queue entry.
    await w._manager.client.delete(w._request_key(str(mr.uuid)))
    with pytest.raises(QueueEmpty):
        await w.get_input_nowait()


@pytest.mark.asyncio
async def test_round_robin_alternates_across_guilds():
    '''Two guilds each with items pop in round-robin (oldest-guild-first) order.'''
    w = _worker()
    await w.submit(1, queue_request(guild_id=1))
    await w.submit(2, queue_request(guild_id=2))
    await w.submit(1, queue_request(guild_id=1))
    first = await w.get_input_nowait()
    second = await w.get_input_nowait()
    third = await w.get_input_nowait()
    assert [first.guild_id, second.guild_id, third.guild_id] == [1, 2, 1]
    assert await w.queue_size(1) == 0
    assert await w.queue_size(2) == 0
    # Fully drained -> the guilds tracker is empty.
    assert await w._manager.client.zcard(GUILDS_KEY) == 0


@pytest.mark.asyncio
async def test_round_robin_pop_skips_guild_with_empty_queue():
    '''A guild left in the tracker with an emptied queue is dropped, not popped.'''
    w = _worker()
    await w.submit(5, queue_request(guild_id=5))
    # Strand guild 9 in the tracker with no queued items.
    await w._manager.client.zadd(GUILDS_KEY, {'9': 0.0})
    popped = await w.get_input_nowait()
    assert popped.guild_id == 5
    # The stranded guild 9 was cleaned out while scanning for a servable request.
    assert await w._manager.client.zscore(GUILDS_KEY, '9') is None


@pytest.mark.asyncio
async def test_priority_orders_within_a_guild():
    '''Lower priority number pops first (priority bucket dominates the score).'''
    w = _worker()
    low = queue_request(guild_id=3)
    high = queue_request(guild_id=3)
    await w.submit(3, low, priority=100)
    await w.submit(3, high, priority=1)
    first = await w.get_input_nowait()
    assert str(first.uuid) == str(high.uuid)


# --------------------------------------------------------------------------- #
# block / clear
# --------------------------------------------------------------------------- #

@pytest.mark.asyncio
async def test_block_guild_sets_flag():
    '''block_guild marks the per-guild blocked key.'''
    w = _worker()
    assert await w.block_guild(7) is True
    assert await w._manager.client.get(w._guild_blocked_key(7)) == '1'


@pytest.mark.asyncio
async def test_clear_guild_queue_returns_dropped_and_drops_tracker():
    '''clear_guild_queue returns the dropped requests and drops the emptied guild.'''
    w = _worker()
    mr = queue_request(guild_id=7)
    await w.submit(7, mr)
    dropped = await w.clear_guild_queue(7)
    assert [str(d.uuid) for d in dropped] == [str(mr.uuid)]
    assert await w.queue_size(7) == 0
    assert await w._manager.client.zscore(GUILDS_KEY, '7') is None


# --------------------------------------------------------------------------- #
# resolve -> shared backoff / failure state
# --------------------------------------------------------------------------- #

@pytest.mark.asyncio
async def test_resolve_success_returns_video_id_and_pops_a_failure():
    '''A successful resolve returns the videoId and drains one shared failure.'''
    w = _worker(client=_FakeYoutubeMusicClient('vid-123'))
    # Seed a stale failure so the success drain is observable.
    await w._record_failure()
    assert await w._manager.client.zcard(FAILURES_KEY) == 1
    result = await w.resolve(queue_request(guild_id=7))
    assert result == 'vid-123'
    assert await w._manager.client.zcard(FAILURES_KEY) == 0
    assert w.failure_summary == '0 failures in queue'


@pytest.mark.asyncio
async def test_resolve_429_records_failure_and_arms_shared_backoff():
    '''A 429 re-raises, grows the shared failure ZSET, and sets the wait window.'''
    w = _worker(client=_FakeYoutubeMusicClient(raise_retry=True))
    with pytest.raises(YoutubeMusicRetryException):
        await w.resolve(queue_request(guild_id=7))
    assert await w._manager.client.zcard(FAILURES_KEY) == 1
    assert w.failure_summary == '1 failures in queue'
    # The shared wait_until is now in the future; the cache reflects it.
    assert await w._manager.client.get(WAIT_UNTIL_KEY) is not None
    assert w.backoff_seconds_remaining is not None
    assert w.backoff_seconds_remaining > 0


@pytest.mark.asyncio
async def test_extend_wait_until_does_not_shrink_the_window():
    '''A shorter proposed window never overwrites a longer live one.'''
    w = _worker(wait_min=100)
    await w._extend_wait_until(backoff_multiplier=4)
    long_until = float(await w._manager.client.get(WAIT_UNTIL_KEY))
    # A single-multiplier extend proposes a nearer timestamp -> no shrink.
    await w._extend_wait_until(backoff_multiplier=1)
    assert float(await w._manager.client.get(WAIT_UNTIL_KEY)) == long_until
    # And the cache tracks the live (longer) window, not the rejected shorter one.
    assert w._wait_timestamp == long_until


@pytest.mark.asyncio
async def test_backoff_wait_refreshes_shared_window_from_redis():
    '''A pod that never hit the 429 still backs off: refresh reads the shared key.'''
    driver = _worker()
    await driver._extend_wait_until(backoff_multiplier=2)
    shared_until = float(await driver._manager.client.get(WAIT_UNTIL_KEY))
    # A second worker on the SAME redis has no local wait timestamp yet.
    other = _worker(manager=driver._manager)
    assert other._wait_timestamp is None
    await other._refresh_wait_timestamp()
    assert other._wait_timestamp == shared_until


@pytest.mark.asyncio
async def test_backoff_wait_clears_when_window_elapsed():
    '''An expired shared window refreshes the cache to None and returns at once.'''
    w = _worker()
    # A wait_until in the past -> treated as clear.
    await w._manager.client.set(WAIT_UNTIL_KEY, str(w._now_seconds() - 5))
    await w.backoff_wait(asyncio.Event())
    assert w._wait_timestamp is None
    assert w.backoff_seconds_remaining is None


# --------------------------------------------------------------------------- #
# status_snapshot
# --------------------------------------------------------------------------- #

@pytest.mark.asyncio
async def test_status_snapshot_reports_sizes_failures_and_backoff():
    '''status_snapshot surfaces per-guild sizes, the failure count, and backoff.'''
    w = _worker(client=_FakeYoutubeMusicClient(raise_retry=True))
    await w.submit(7, queue_request(guild_id=7))
    await w.submit(8, queue_request(guild_id=8))
    with pytest.raises(YoutubeMusicRetryException):
        await w.resolve(queue_request(guild_id=7))
    snapshot = await w.status_snapshot()
    assert snapshot['queue_sizes'] == {'7': 1, '8': 1}
    assert snapshot['failure_count'] == 1
    assert snapshot['failure_summary'] == '1 failures in queue'
    assert snapshot['backoff_seconds_remaining'] is not None


@pytest.mark.asyncio
async def test_status_snapshot_defaults_when_idle():
    '''With no work + no failures, the snapshot reports empty/None defaults.'''
    w = _worker()
    snapshot = await w.status_snapshot()
    assert snapshot['queue_sizes'] == {}
    assert snapshot['failure_count'] == 0
    assert snapshot['failure_summary'] == '0 failures in queue'
    assert snapshot['backoff_seconds_remaining'] is None


@pytest.mark.asyncio
async def test_backoff_wait_sliced_returns_early_with_window_still_open():
    '''max_wait_seconds truncates the sleep so the caller's loop iteration returns.

    A long shared window must not be slept through in one go: the search loop
    re-arms its LoopHealth only when an iteration returns, and a window of
    wait_period_minimum * 2**failures outgrows the staleness default quickly.
    '''
    w = _worker()
    await w._extend_wait_until(backoff_multiplier=64)  # window far past any slice
    remaining_before = w.backoff_seconds_remaining
    assert remaining_before > 60

    await asyncio.wait_for(w.backoff_wait(asyncio.Event(), max_wait_seconds=0.01), timeout=5)

    # Returned early, and the window is still open — the caller waits another slice.
    assert w.backoff_seconds_remaining > 0


# ---------------------------------------------------------------------------
# Guild block enforcement
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_clear_stale_guild_blocks_drops_keys_with_no_expiry():
    '''
    A block written without an expiry is a leftover from a pre-TTL build.

    Those keys were write-only until submit started reading them, at which point
    each one became a permanent block on its guild — the prod outage this sweep
    exists to prevent recurring.
    '''
    w = _worker()
    key = w._guild_blocked_key(7)
    await w._manager.client.set(key, '1')  # legacy write: no ex=

    assert await w.clear_stale_guild_blocks() == 1
    assert await w._manager.client.get(key) is None


@pytest.mark.asyncio
async def test_clear_stale_guild_blocks_unwedges_submit():
    '''After the sweep the guild accepts searches again — the outage symptom.'''
    w = _worker()
    await w._manager.client.set(w._guild_blocked_key(7), '1')
    with pytest.raises(PutsBlocked):
        await w.submit(7, queue_request(guild_id=7))

    await w.clear_stale_guild_blocks()

    await w.submit(7, queue_request(guild_id=7))
    assert await w.queue_size(7) == 1


@pytest.mark.asyncio
async def test_clear_stale_guild_blocks_leaves_a_live_block_alone():
    '''A teardown racing the sweep keeps its block: TTL'd keys are not stale.'''
    w = _worker()
    await w.block_guild(7)

    assert await w.clear_stale_guild_blocks() == 0
    assert await w.guild_is_blocked(7) is True


@pytest.mark.asyncio
async def test_clear_stale_guild_blocks_ignores_other_keys():
    '''The sweep is scoped to this worker's block keys, not the whole keyspace.'''
    w = _worker()
    await w.submit(7, queue_request(guild_id=7))  # queue ZSET + tracker, no expiry
    await w._manager.client.set('discord_bot:download:guild:7:blocked', '1')

    assert await w.clear_stale_guild_blocks() == 0
    assert await w.queue_size(7) == 1
    assert await w._manager.client.get('discord_bot:download:guild:7:blocked') == '1'


@pytest.mark.asyncio
async def test_clear_stale_guild_blocks_sweeps_every_guild():
    '''Every leftover goes in one pass, not just the first one found.'''
    w = _worker()
    for guild_id in (7, 8, 9):
        await w._manager.client.set(w._guild_blocked_key(guild_id), '1')

    assert await w.clear_stale_guild_blocks() == 3
    for guild_id in (7, 8, 9):
        assert await w.guild_is_blocked(guild_id) is False


@pytest.mark.asyncio
async def test_search_worker_block_key_does_not_collide_with_download():
    '''The two workers key off different prefixes, so blocking a guild's searches
    leaves its downloads alone (and vice versa).'''
    w = _worker()
    assert w._guild_blocked_key(7).startswith('discord_bot:ytmusic_search:')


# --------------------------------------------------------------------------- #
# Idle-poll cost
# --------------------------------------------------------------------------- #

@pytest.mark.asyncio
async def test_queue_is_empty_tracks_the_round_robin_zset():
    '''_queue_is_empty reads the same ZSET _round_robin_pop pops from, so an "empty"
    answer means the pop would have found nothing anyway.'''
    w = _worker()
    assert await w._queue_is_empty() is True
    await w.submit(7, queue_request(guild_id=7))
    assert await w._queue_is_empty() is False
    await w.get_input_nowait()
    assert await w._queue_is_empty() is True


@pytest.mark.asyncio
async def test_idle_pop_takes_no_lock():
    '''An idle poll must not burn a SET NX + GET + DEL lock cycle just to discover an
    empty queue.  The loop polls every 0.25 s regardless of work, so that cycle was
    20 of this service's ~23 redis commands/s against ~0.03 searches/s.'''
    w = _worker()
    writes = spy_redis_writes(w)

    with pytest.raises(QueueEmpty):
        await w.get_input_nowait()
    assert not writes


@pytest.mark.asyncio
async def test_non_empty_queue_still_pops_under_the_lock():
    '''The pre-check is not trusted in the other direction: a non-empty ZSET still
    takes the pop-lock, so concurrent pods cannot pop the same request.'''
    w = _worker()
    await w.submit(7, queue_request(guild_id=7))
    locked = []
    client = w._manager.client
    real_set = client.set

    async def _spy_set(*args, **kwargs):
        if args[0] == POP_LOCK_KEY:
            locked.append(args[0])
        return await real_set(*args, **kwargs)

    client.set = _spy_set

    assert await w.get_input_nowait() is not None
    assert locked == [POP_LOCK_KEY]


@pytest.mark.asyncio
async def test_stale_guild_entry_is_reaped_under_the_lock():
    '''A guild left in the round-robin ZSET with an already-drained queue makes the
    pre-check say "not empty"; the lock is taken and _round_robin_pop reaps it,
    raising QueueEmpty.  This is the case the count must not be trusted for.'''
    w = _worker()
    await w._manager.client.zadd(GUILDS_KEY, {'7': 1.0})
    assert await w._queue_is_empty() is False
    with pytest.raises(QueueEmpty):
        await w.get_input_nowait()
    assert await w._queue_is_empty() is True
