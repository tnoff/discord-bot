'''
Unit tests for RedisDownloadWorker.

Single-instance coverage of the Redis queue surface: key schema, submit/routing,
round-robin pops, the per-egress YouTube backoff claim, and the shared failure
ZSETs.  Cross-pod coherency lives in test_redis_download_worker_multipod.py.
'''
# White-box tests: they drive the worker's protected pop/backoff internals and
# inspect its Redis client directly, so protected-access is expected here.
# pylint: disable=protected-access
import asyncio
from pathlib import Path
from unittest.mock import AsyncMock

import fakeredis.aioredis
import pytest

from discord_bot.clients.redis_client import RedisManager
from discord_bot.cogs.music_helpers.common import SearchType
from discord_bot.exceptions import ExitEarlyException
from discord_bot.types.download import DownloadErrorType, DownloadResult, DownloadStatus
from discord_bot.types.media_request import MediaRequest
from discord_bot.types.search import SearchResult
from discord_bot.workers.redis_download_worker import (
    RedisDownloadWorker, DirectItemAvailableException,
    FAILURES_DIRECT_KEY, GUILDS_DIRECT_KEY, GUILDS_YOUTUBE_KEY,
    youtube_failures_key, youtube_wait_until_key,
)


def _manager() -> RedisManager:
    return RedisManager.from_client(fakeredis.aioredis.FakeRedis(decode_responses=True))


def _worker(manager=None, *, egress='default', wait_min=10, variance=2):
    worker = RedisDownloadWorker(
        None, Path('/tmp'),
        redis_manager=manager or _manager(),
        youtube_egress_key=egress,
        wait_period_minimum=wait_min,
        wait_period_max_variance=variance,
        max_retries=2,
    )
    # Disable the cold-start floor so pops aren't blocked unless a test sets one.
    worker._startup_wait_until = 0.0
    worker._wait_timestamp = None
    return worker


def _mk(*, guild_id=7, direct=False) -> MediaRequest:
    return MediaRequest(
        guild_id=guild_id, channel_id=2, requester_name='tester', requester_id=9,
        search_result=SearchResult(
            search_type=SearchType.DIRECT if direct else SearchType.SEARCH,
            raw_search_string='https://example.com/v.mp4' if direct else 'song name',
        ),
    )


def _result(media_request, *, success=True, error_type=None, extractor=None) -> DownloadResult:
    return DownloadResult(
        status=DownloadStatus(success=success, error_type=error_type,
                              error_detail=None if success else 'boom'),
        media_request=media_request,
        ytdlp_data={'extractor': extractor} if extractor else {},
        file_name=Path('/tmp/x.pcm') if success else None,
    )


# --------------------------------------------------------------------------- #
# Key / score helpers
# --------------------------------------------------------------------------- #

def test_key_helpers():
    w = _worker()
    assert w._request_key('abc') == 'discord_bot:download:request:abc'
    assert w._guild_queue_key(7, direct=False).endswith(':youtube')
    assert w._guild_queue_key(7, direct=True).endswith(':direct')
    assert w._guild_queue_key(7, direct=False) != w._guild_queue_key(7, direct=True)
    assert w._guild_blocked_key(7) != w._guild_blocked_key(8)
    assert w._guilds_zset_key(direct=False) == GUILDS_YOUTUBE_KEY
    assert w._guilds_zset_key(direct=True) == GUILDS_DIRECT_KEY
    assert w._failures_key(direct=True) == FAILURES_DIRECT_KEY
    assert w._failures_key(direct=False) == youtube_failures_key('default')
    assert w._is_direct(_mk(direct=True)) is True
    assert w._is_direct(_mk(direct=False)) is False


def test_default_egress_key_preserves_legacy_schema():
    w = _worker()
    assert w._youtube_wait_until_key == youtube_wait_until_key('default')
    assert w._youtube_wait_until_key.endswith(':default')
    assert w._youtube_failures_key.endswith(':default')


def test_build_score_priority_then_timestamp():
    w = _worker()
    w._now_seconds = lambda: 5.0
    low = w._build_score(5)
    high = w._build_score(10)
    default = w._build_score(None)  # bucket 100
    assert low < high < default
    assert low == 5.0 * 1_000_000_000 + 5.0


def test_startup_floor_armed_by_default():
    w = _worker()
    w._startup_wait_until = 1000.0
    w._now_seconds = lambda: 990.0
    assert w.backoff_seconds_remaining is None  # cache not yet refreshed


# --------------------------------------------------------------------------- #
# submit / routing
# --------------------------------------------------------------------------- #

@pytest.mark.asyncio
async def test_submit_routes_search_to_youtube_pool():
    w = _worker()
    await w.submit(7, _mk(guild_id=7, direct=False))
    client = w._manager.client
    assert await client.zcard(w._guild_queue_key(7, direct=False)) == 1
    assert await client.zcard(w._guild_queue_key(7, direct=True)) == 0
    assert await client.zrange(GUILDS_YOUTUBE_KEY, 0, -1) == ['7']


@pytest.mark.asyncio
async def test_submit_routes_direct_to_direct_pool_and_sets_cache():
    w = _worker()
    await w.submit(7, _mk(guild_id=7, direct=True))
    client = w._manager.client
    assert await client.zcard(w._guild_queue_key(7, direct=True)) == 1
    assert await client.zrange(GUILDS_DIRECT_KEY, 0, -1) == ['7']
    assert w.has_direct_pending is True


@pytest.mark.asyncio
async def test_submit_priority_orders_lower_first():
    w = _worker()
    high = _mk(guild_id=7)
    low = _mk(guild_id=7)
    await w.submit(7, high, priority=10)
    await w.submit(7, low, priority=5)
    ordered = await w._manager.client.zrange(w._guild_queue_key(7, direct=False), 0, -1)
    assert ordered == [str(low.uuid), str(high.uuid)]


@pytest.mark.asyncio
async def test_submit_zadd_nx_keeps_round_robin_position():
    w = _worker()
    w._now_seconds = lambda: 100.0
    await w.submit(7, _mk(guild_id=7))
    w._now_seconds = lambda: 200.0
    await w.submit(7, _mk(guild_id=7))
    score = await w._manager.client.zscore(GUILDS_YOUTUBE_KEY, '7')
    assert score == 100.0  # NX left the original last-popped position


# --------------------------------------------------------------------------- #
# clear_guild_queue / block_guild / queue_size
# --------------------------------------------------------------------------- #

@pytest.mark.asyncio
async def test_clear_guild_queue_drops_all_and_empties_tracker():
    w = _worker()
    await w.submit(7, _mk(guild_id=7))
    await w.submit(7, _mk(guild_id=7, direct=True))
    dropped = await w.clear_guild_queue(7)
    assert len(dropped) == 2
    client = w._manager.client
    assert await w.queue_size(7) == 0
    assert await client.zrange(GUILDS_YOUTUBE_KEY, 0, -1) == []
    assert await client.zrange(GUILDS_DIRECT_KEY, 0, -1) == []


@pytest.mark.asyncio
async def test_clear_guild_queue_preserve_predicate_keeps_matches():
    w = _worker()
    keep = _mk(guild_id=7)
    drop = _mk(guild_id=7)
    await w.submit(7, keep)
    await w.submit(7, drop)
    dropped = await w.clear_guild_queue(
        7, preserve_predicate=lambda r: str(r.uuid) == str(keep.uuid))
    assert [str(r.uuid) for r in dropped] == [str(drop.uuid)]
    assert await w.queue_size(7) == 1


@pytest.mark.asyncio
async def test_clear_guild_queue_handles_missing_payload():
    w = _worker()
    mr = _mk(guild_id=7)
    await w.submit(7, mr)
    # Simulate the request payload having TTL'd away.
    await w._manager.client.delete(w._request_key(str(mr.uuid)))
    dropped = await w.clear_guild_queue(7)
    assert dropped == []
    assert await w.queue_size(7) == 0


@pytest.mark.asyncio
async def test_block_guild_sets_marker():
    w = _worker()
    assert await w.block_guild(7) is True
    assert await w._manager.client.get(w._guild_blocked_key(7)) == '1'


@pytest.mark.asyncio
async def test_queue_size_sums_both_pools():
    w = _worker()
    await w.submit(7, _mk(guild_id=7))
    await w.submit(7, _mk(guild_id=7))
    await w.submit(7, _mk(guild_id=7, direct=True))
    assert await w.queue_size(7) == 3
    assert await w.queue_size(99) == 0


# --------------------------------------------------------------------------- #
# pops
# --------------------------------------------------------------------------- #

@pytest.mark.asyncio
async def test_dequeue_direct_empty_raises_queue_empty():
    w = _worker()
    with pytest.raises(asyncio.QueueEmpty):
        await w._dequeue_direct()
    assert w.has_direct_pending is False


@pytest.mark.asyncio
async def test_dequeue_direct_returns_request():
    w = _worker()
    mr = _mk(guild_id=7, direct=True)
    await w.submit(7, mr)
    got = await w._dequeue_direct()
    assert str(got.uuid) == str(mr.uuid)


@pytest.mark.asyncio
async def test_merged_get_prefers_direct():
    w = _worker()
    direct = _mk(guild_id=7, direct=True)
    await w.submit(7, _mk(guild_id=7, direct=False))
    await w.submit(7, direct)
    got = await w._merged_get_nowait()
    assert str(got.uuid) == str(direct.uuid)


@pytest.mark.asyncio
async def test_merged_get_youtube_when_no_direct():
    w = _worker()
    yt = _mk(guild_id=7, direct=False)
    await w.submit(7, yt)
    got = await w._merged_get_nowait()
    assert str(got.uuid) == str(yt.uuid)


@pytest.mark.asyncio
async def test_merged_get_empty_raises():
    w = _worker()
    with pytest.raises(asyncio.QueueEmpty):
        await w._merged_get_nowait()


@pytest.mark.asyncio
async def test_merged_get_youtube_backoff_refreshes_cache_and_raises():
    w = _worker()
    w._now_seconds = lambda: 1000.0
    await w.submit(7, _mk(guild_id=7))
    # Arm the shared window in the future so the pop bails.
    await w._manager.client.set(w._youtube_wait_until_key, '1030.0')
    with pytest.raises(asyncio.QueueEmpty):
        await w._merged_get_nowait()
    assert w._wait_timestamp == 1030.0


@pytest.mark.asyncio
async def test_youtube_pop_claims_window_with_ttl():
    w = _worker()
    w._now_seconds = lambda: 1000.0
    await w.submit(7, _mk(guild_id=7))
    got = await w._merged_get_nowait()
    assert got is not None
    client = w._manager.client
    assert float(await client.get(w._youtube_wait_until_key)) == 1010.0
    ttl = await client.ttl(w._youtube_wait_until_key)
    assert 0 < ttl <= w._wait_period_minimum * 4


@pytest.mark.asyncio
async def test_round_robin_rotates_across_guilds_direct():
    w = _worker()
    await w.submit(7, _mk(guild_id=7, direct=True))
    await w.submit(8, _mk(guild_id=8, direct=True))
    first = await w._dequeue_direct()
    second = await w._dequeue_direct()
    assert {first.guild_id, second.guild_id} == {7, 8}


@pytest.mark.asyncio
async def test_round_robin_pop_skips_empty_guild_entry():
    w = _worker()
    # A guild listed in the tracker but with no queued request (stale entry).
    await w._manager.client.zadd(GUILDS_DIRECT_KEY, {'7': 1.0})
    assert await w._round_robin_pop(direct=True) is None
    assert await w._manager.client.zrange(GUILDS_DIRECT_KEY, 0, -1) == []


# --------------------------------------------------------------------------- #
# backoff_wait
# --------------------------------------------------------------------------- #

@pytest.mark.asyncio
async def test_backoff_wait_raises_on_shutdown():
    w = _worker()
    ev = asyncio.Event()
    ev.set()
    with pytest.raises(ExitEarlyException):
        await w.backoff_wait(ev)


@pytest.mark.asyncio
async def test_backoff_wait_interrupts_on_direct_arrival():
    w = _worker()
    w._startup_wait_until = w._now_seconds() + 100  # long window
    await w.submit(7, _mk(guild_id=7, direct=True))
    with pytest.raises(DirectItemAvailableException):
        await w.backoff_wait(asyncio.Event())
    assert w.has_direct_pending is True


@pytest.mark.asyncio
async def test_backoff_wait_returns_when_elapsed():
    w = _worker()
    # No window and no direct items -> returns immediately.
    await w.backoff_wait(asyncio.Event())


@pytest.mark.asyncio
async def test_backoff_wait_shutdown_mid_wait(monkeypatch):
    w = _worker()
    w._startup_wait_until = w._now_seconds() + 100
    ev = asyncio.Event()

    real_sleep = asyncio.sleep

    async def _sleep_then_shutdown(_seconds):
        ev.set()
        await real_sleep(0)
    monkeypatch.setattr('discord_bot.workers.redis_download_worker.sleep', _sleep_then_shutdown)
    with pytest.raises(ExitEarlyException):
        await w.backoff_wait(ev)


# --------------------------------------------------------------------------- #
# backoff / failure state
# --------------------------------------------------------------------------- #

@pytest.mark.asyncio
async def test_effective_backoff_remaining_none_when_unset():
    w = _worker()
    assert await w._effective_backoff_remaining() == 0
    assert w.backoff_seconds_remaining is None


@pytest.mark.asyncio
async def test_effective_backoff_remaining_reads_redis():
    w = _worker()
    w._now_seconds = lambda: 1000.0
    await w._manager.client.set(w._youtube_wait_until_key, '1030.0')
    assert await w._effective_backoff_remaining() == 30


@pytest.mark.asyncio
async def test_effective_backoff_remaining_uses_startup_floor():
    w = _worker()
    w._now_seconds = lambda: 1000.0
    w._startup_wait_until = 1050.0
    await w._manager.client.set(w._youtube_wait_until_key, '1010.0')
    assert await w._effective_backoff_remaining() == 50


@pytest.mark.asyncio
async def test_extend_wait_until_sets_window():
    w = _worker()
    w._now_seconds = lambda: 1000.0
    await w._extend_wait_until()
    stamp = float(await w._manager.client.get(w._youtube_wait_until_key))
    assert 1010.0 <= stamp <= 1015.0


@pytest.mark.asyncio
async def test_extend_wait_until_multiplier():
    w = _worker()
    w._now_seconds = lambda: 1000.0
    await w._extend_wait_until(multiplier=4)
    stamp = float(await w._manager.client.get(w._youtube_wait_until_key))
    assert stamp >= 1040.0


@pytest.mark.asyncio
async def test_extend_wait_until_max_extends_only():
    w = _worker()
    w._now_seconds = lambda: 1000.0
    await w._manager.client.set(w._youtube_wait_until_key, '5000.0')
    await w._extend_wait_until()
    assert float(await w._manager.client.get(w._youtube_wait_until_key)) == 5000.0


@pytest.mark.asyncio
async def test_record_failure_and_success_counts():
    w = _worker()
    assert await w._record_failure(direct=False) == 1
    assert await w._record_failure(direct=False) == 2
    assert await w._manager.client.zcard(w._youtube_failures_key) == 2
    await w._record_success(direct=False)
    assert await w._manager.client.zcard(w._youtube_failures_key) == 1


@pytest.mark.asyncio
async def test_record_failure_trims_expired():
    w = _worker()
    w._now_seconds = lambda: 10_000.0
    # Seed an ancient failure below the cutoff (now - 600).
    await w._manager.client.zadd(w._youtube_failures_key, {'old': 1.0})
    assert await w._record_failure(direct=False) == 1  # old trimmed, new added


@pytest.mark.asyncio
async def test_failure_summary_default_and_refresh():
    w = _worker()
    assert w.failure_summary == '0 failures in queue'
    await w._record_failure(direct=False)
    await w._refresh_failure_summary()
    assert w.failure_summary == '1 failures in queue'


@pytest.mark.asyncio
async def test_direct_failures_independent_of_youtube():
    w = _worker()
    await w._record_failure(direct=True)
    await w._refresh_failure_summary()
    assert w.failure_summary == '0 failures in queue'  # youtube pool empty
    assert await w._manager.client.zcard(FAILURES_DIRECT_KEY) == 1


# --------------------------------------------------------------------------- #
# update_tracking dispatch
# --------------------------------------------------------------------------- #

@pytest.mark.asyncio
async def test_update_tracking_youtube_success_extends_and_pops_failure():
    w = _worker()
    await w._record_failure(direct=False)
    await w.update_tracking(_result(_mk(direct=False), success=True, extractor='youtube'))
    assert await w._manager.client.zcard(w._youtube_failures_key) == 0
    assert await w._manager.client.get(w._youtube_wait_until_key) is not None


@pytest.mark.asyncio
async def test_update_tracking_non_youtube_success_no_backoff():
    w = _worker()
    await w.update_tracking(_result(_mk(direct=False), success=True, extractor='soundcloud'))
    assert await w._manager.client.get(w._youtube_wait_until_key) is None


@pytest.mark.asyncio
async def test_update_tracking_retryable_failure_extends_by_exponent():
    w = _worker()
    w._now_seconds = lambda: 1000.0
    await w.update_tracking(_result(_mk(direct=False), success=False,
                                    error_type=DownloadErrorType.RETRYABLE))
    # count==1 -> multiplier 2**1 -> >= now + wait_min*2
    assert float(await w._manager.client.get(w._youtube_wait_until_key)) >= 1020.0


@pytest.mark.asyncio
async def test_update_tracking_terminal_failure_no_failure_recorded():
    w = _worker()
    await w.update_tracking(_result(_mk(direct=False), success=False,
                                    error_type=DownloadErrorType.PRIVATE_VIDEO))
    assert await w._manager.client.zcard(w._youtube_failures_key) == 0
    assert await w._manager.client.get(w._youtube_wait_until_key) is not None


@pytest.mark.asyncio
async def test_update_tracking_direct_uses_direct_pool():
    w = _worker()
    await w.update_tracking(_result(_mk(direct=True), success=False,
                                    error_type=DownloadErrorType.RETRYABLE))
    assert await w._manager.client.zcard(FAILURES_DIRECT_KEY) == 1
    # DIRECT never touches the youtube backoff window.
    assert await w._manager.client.get(w._youtube_wait_until_key) is None


@pytest.mark.asyncio
async def test_update_tracking_direct_success_pops_direct_failure():
    w = _worker()
    await w._record_failure(direct=True)
    await w.update_tracking(_result(_mk(direct=True), success=True))
    assert await w._manager.client.zcard(FAILURES_DIRECT_KEY) == 0


# --------------------------------------------------------------------------- #
# _parse_raw / lock edges
# --------------------------------------------------------------------------- #

def test_parse_raw_missing_raises():
    with pytest.raises(asyncio.QueueEmpty):
        RedisDownloadWorker._parse_raw(None)


@pytest.mark.asyncio
async def test_pop_lock_falls_through_on_contention(monkeypatch):
    w = _worker()
    # Lock always contested -> the acquire loop should time out and fall through
    # (token=None) rather than deadlock, and skip the release.
    monkeypatch.setattr(
        'discord_bot.workers.redis_download_worker.POP_LOCK_WAIT_SECONDS', 0.0)
    w._manager.client.set = AsyncMock(return_value=None)
    async with w._pop_lock(direct=True):
        pass  # no exception == fell through cleanly
