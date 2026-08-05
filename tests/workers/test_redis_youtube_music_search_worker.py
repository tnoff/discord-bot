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

from discord_bot.clients.redis_client import RedisManager
from discord_bot.cogs.music_helpers.common import SearchType
from discord_bot.types.media_request import MediaRequest
from discord_bot.types.search import SearchResult
from discord_bot.utils.failure_queue import FailureQueue
from discord_bot.utils.integrations.youtube_music import YoutubeMusicRetryException
from discord_bot.workers.redis_youtube_music_search_worker import (
    RedisYoutubeMusicSearchWorker, FAILURES_KEY, GUILDS_KEY, WAIT_UNTIL_KEY,
)


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


def _mk(*, guild_id=7) -> MediaRequest:
    return MediaRequest(
        guild_id=guild_id, channel_id=2, requester_name='tester', requester_id=9,
        search_result=SearchResult(search_type=SearchType.SEARCH, raw_search_string='song name'),
    )


# --------------------------------------------------------------------------- #
# submit / pop / queue_size
# --------------------------------------------------------------------------- #

@pytest.mark.asyncio
async def test_submit_then_pop_round_trips_the_request():
    '''submit persists + enqueues; get_input_nowait pops the same request back.'''
    w = _worker()
    mr = _mk(guild_id=7)
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
    mr = _mk(guild_id=7)
    await w.submit(7, mr)
    # Simulate the payload expiring out from under the queue entry.
    await w._manager.client.delete(w._request_key(str(mr.uuid)))
    with pytest.raises(QueueEmpty):
        await w.get_input_nowait()


@pytest.mark.asyncio
async def test_round_robin_alternates_across_guilds():
    '''Two guilds each with items pop in round-robin (oldest-guild-first) order.'''
    w = _worker()
    await w.submit(1, _mk(guild_id=1))
    await w.submit(2, _mk(guild_id=2))
    await w.submit(1, _mk(guild_id=1))
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
    await w.submit(5, _mk(guild_id=5))
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
    low = _mk(guild_id=3)
    high = _mk(guild_id=3)
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
    mr = _mk(guild_id=7)
    await w.submit(7, mr)
    dropped = await w.clear_guild_queue(7)
    assert [str(d.uuid) for d in dropped] == [str(mr.uuid)]
    assert await w.queue_size(7) == 0
    assert await w._manager.client.zscore(GUILDS_KEY, '7') is None


@pytest.mark.asyncio
async def test_clear_guild_queue_honours_preserve_predicate():
    '''A kept request stays queued (and the guild stays in the tracker).'''
    w = _worker()
    keep = _mk(guild_id=7)
    drop = _mk(guild_id=7)
    await w.submit(7, keep)
    await w.submit(7, drop)
    dropped = await w.clear_guild_queue(
        7, preserve_predicate=lambda req: str(req.uuid) == str(keep.uuid))
    assert [str(d.uuid) for d in dropped] == [str(drop.uuid)]
    assert await w.queue_size(7) == 1
    assert await w._manager.client.zscore(GUILDS_KEY, '7') is not None


@pytest.mark.asyncio
async def test_clear_guild_queue_prunes_dangling_entry_with_missing_payload():
    '''A queue entry whose payload already TTL'd is removed and not returned.'''
    w = _worker()
    mr = _mk(guild_id=7)
    await w.submit(7, mr)
    await w._manager.client.delete(w._request_key(str(mr.uuid)))
    dropped = await w.clear_guild_queue(7)
    assert dropped == []
    assert await w.queue_size(7) == 0


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
    result = await w.resolve(_mk(guild_id=7))
    assert result == 'vid-123'
    assert await w._manager.client.zcard(FAILURES_KEY) == 0
    assert w.failure_summary == '0 failures in queue'


@pytest.mark.asyncio
async def test_resolve_429_records_failure_and_arms_shared_backoff():
    '''A 429 re-raises, grows the shared failure ZSET, and sets the wait window.'''
    w = _worker(client=_FakeYoutubeMusicClient(raise_retry=True))
    with pytest.raises(YoutubeMusicRetryException):
        await w.resolve(_mk(guild_id=7))
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
    await w.submit(7, _mk(guild_id=7))
    await w.submit(8, _mk(guild_id=8))
    with pytest.raises(YoutubeMusicRetryException):
        await w.resolve(_mk(guild_id=7))
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
