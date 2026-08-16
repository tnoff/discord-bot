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
from opentelemetry.instrumentation.redis import RedisInstrumentor
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter

from discord_bot.clients.redis_client import RedisManager
from discord_bot.cogs.music_helpers.common import SearchType
from discord_bot.exceptions import ExitEarlyException
from discord_bot.types.download import DownloadErrorType, DownloadResult, DownloadStatus
from discord_bot.types.media_request import MediaRequest
from discord_bot.types.search import SearchResult
from discord_bot.types.queue import PutsBlocked
from discord_bot.workers.redis_guild_queue import GUILD_BLOCK_TTL_SECONDS
from discord_bot.workers.redis_download_worker import (
    RedisDownloadWorker, DirectItemAvailableException,
    DEFERRED_RETRIES_KEY, FAILURES_DIRECT_KEY, GUILDS_DIRECT_KEY, GUILDS_YOUTUBE_KEY,
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


def _pool_worker(manager=None, *, exits=('us-lax-wg-001', 'us-nyc-wg-301'), wait_min=10):
    '''A worker in pool (socks5) egress mode fanning out across the given exits.'''
    worker = RedisDownloadWorker(
        None, Path('/tmp'),
        redis_manager=manager or _manager(),
        wait_period_minimum=wait_min, wait_period_max_variance=2, max_retries=2,
        egress_mode='mullvad-socks5', egress_exits=list(exits),
    )
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
    low = w._build_score(5, 5.0)
    high = w._build_score(10, 5.0)
    default = w._build_score(None, 5.0)  # bucket 100
    assert low < high < default
    assert low == 5.0 * 1_000_000_000 + 5.0
    # Within a priority bucket, the earlier queued_at sorts first.
    assert w._build_score(5, 1.0) < w._build_score(5, 9.0)


@pytest.mark.asyncio
async def test_enqueue_stamps_queue_order_once_and_preserves_on_requeue():
    '''queue_order is stamped on first enqueue and reused on re-enqueue, so a
    re-queued (retry / pool-contention bounce) request keeps its ZSET score and
    thus its queue position instead of jumping to the back.'''
    w = _worker()
    now = [100.0]
    w._now_seconds = lambda: now[0]
    mr = _mk()
    key = w._guild_queue_key(7, direct=False)
    await w._enqueue_request(7, mr)
    assert mr.queue_order == 100.0
    score1 = await w._manager.client.zscore(key, str(mr.uuid))
    now[0] = 500.0  # time advances before the bounce
    await w._enqueue_request(7, mr)
    assert mr.queue_order == 100.0  # not re-stamped
    assert await w._manager.client.zscore(key, str(mr.uuid)) == score1  # position held


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


# --------------------------------------------------------------------------- #
# Per-exit YouTube window (pool mode)
# --------------------------------------------------------------------------- #

@pytest.mark.asyncio
async def test_reserve_youtube_exit_claims_free_window():
    '''Reserving a free exit SET-NX-seeds its per-exit window and returns True.'''
    w = _worker()
    w._now_seconds = lambda: 1000.0
    assert await w._reserve_youtube_exit('us-lax-wg-001') is True
    stamp = float(await w._manager.client.get(youtube_wait_until_key('us-lax-wg-001')))
    assert stamp >= 1000.0 + w._wait_period_minimum


@pytest.mark.asyncio
async def test_reserve_youtube_exit_fails_when_already_claimed():
    '''An exit whose window is already claimed can't be reserved; a sibling can —
    the claim is per-exit, not pod-wide.'''
    w = _worker()
    w._now_seconds = lambda: 1000.0
    await w._manager.client.set(youtube_wait_until_key('us-lax-wg-001'), '1030.0')
    assert await w._reserve_youtube_exit('us-lax-wg-001') is False
    assert await w._reserve_youtube_exit('us-nyc-wg-301') is True


@pytest.mark.asyncio
async def test_reserve_youtube_exit_sets_expiring_ttl():
    '''The reserved window carries a TTL so the exit frees when spacing elapses —
    key-existence, not a value read, is the SET NX gate.'''
    w = _worker()
    w._now_seconds = lambda: 1000.0
    await w._reserve_youtube_exit('us-lax-wg-001')
    ttl = await w._manager.client.ttl(youtube_wait_until_key('us-lax-wg-001'))
    assert 0 < ttl <= w._wait_period_minimum + 1


@pytest.mark.asyncio
async def test_pool_mode_pop_does_not_claim_global_window():
    '''Pool mode pops a YouTube item without touching the pod-global :default window
    — that's what lets concurrent drivers pop in parallel instead of serialising.'''
    w = _pool_worker(exits=('a', 'b'))
    w._now_seconds = lambda: 1000.0
    await w._enqueue_request(7, _mk())
    popped = await w._atomic_pop_youtube()
    assert popped is not None and popped[0] != 'wait' and len(popped) == 3
    assert await w._manager.client.get(w._youtube_wait_until_key) is None


@pytest.mark.asyncio
async def test_soonest_exit_free_zero_when_any_exit_free():
    '''With any exit free the pod can pop now, so the backoff gate is 0.'''
    w = _pool_worker(exits=('a', 'b'))
    w._now_seconds = lambda: 1000.0
    await w._manager.client.set(youtube_wait_until_key('a'), '1030.0')  # a busy, b free
    assert await w._soonest_exit_free_seconds() == 0
    assert await w._effective_backoff_remaining() == 0


@pytest.mark.asyncio
async def test_soonest_exit_free_is_min_when_all_busy():
    '''When every exit is busy, the gate is the soonest one to free.'''
    w = _pool_worker(exits=('a', 'b'))
    w._now_seconds = lambda: 1000.0
    await w._manager.client.set(youtube_wait_until_key('a'), '1050.0')
    await w._manager.client.set(youtube_wait_until_key('b'), '1020.0')
    assert await w._soonest_exit_free_seconds() == 20  # b frees first
    assert await w._effective_backoff_remaining() == 20


@pytest.mark.asyncio
async def test_update_youtube_tracking_failure_keys_by_exit():
    '''A failed pool download extends the leased exit's window + failure ZSET,
    leaving the default bucket and other exits untouched.'''
    w = _worker()
    w._now_seconds = lambda: 1000.0
    result = _result(_mk(), success=False, error_type=DownloadErrorType.RETRYABLE)
    await w._update_youtube_tracking(result, 'us-lax-wg-001')
    client = w._manager.client
    assert float(await client.get(youtube_wait_until_key('us-lax-wg-001'))) > 1000.0
    assert await client.zcard(youtube_failures_key('us-lax-wg-001')) == 1
    # Default bucket + a sibling exit are untouched.
    assert await client.get(w._youtube_wait_until_key) is None
    assert await client.zcard(youtube_failures_key('us-nyc-wg-301')) == 0


@pytest.mark.asyncio
async def test_update_youtube_tracking_success_spaces_leased_exit():
    '''A successful pool download spaces its exit by seeding that exit's window, so
    the exit can't immediately be reserved again.'''
    w = _worker()
    w._now_seconds = lambda: 1000.0
    await w._update_youtube_tracking(_result(_mk(), success=True, extractor='youtube'),
                                     'us-sea-wg-001')
    assert await w._reserve_youtube_exit('us-sea-wg-001') is False


@pytest.mark.asyncio
async def test_update_youtube_tracking_exitless_uses_default_bucket():
    '''Fixed http-proxy mode (exit_name=None) keeps the legacy default bucket.'''
    w = _worker()
    w._now_seconds = lambda: 1000.0
    result = _result(_mk(), success=False, error_type=DownloadErrorType.RETRYABLE)
    await w._update_youtube_tracking(result, None)
    assert float(await w._manager.client.get(w._youtube_wait_until_key)) > 1000.0


@pytest.mark.asyncio
async def test_failures_key_selects_per_exit_bucket():
    '''_failures_key routes YouTube to the per-exit bucket when given an exit.'''
    w = _worker()
    assert w._failures_key(direct=False, exit_name='us-lax-wg-001') == \
        youtube_failures_key('us-lax-wg-001')
    assert w._failures_key(direct=False) == w._youtube_failures_key
    assert w._failures_key(direct=True, exit_name='us-lax-wg-001') == FAILURES_DIRECT_KEY


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
        'discord_bot.workers.redis_guild_queue.POP_LOCK_WAIT_SECONDS', 0.0)
    w._manager.client.set = AsyncMock(return_value=None)
    async with w._pop_lock(direct=True):
        pass  # no exception == fell through cleanly


# --------------------------------------------------------------------------- #
# status_snapshot (download HTTP server's /downloads/status source)
# --------------------------------------------------------------------------- #

@pytest.mark.asyncio
async def test_status_snapshot_reports_per_guild_sizes_and_defaults():
    w = _worker()
    await w.submit(7, _mk(guild_id=7, direct=False))
    await w.submit(7, _mk(guild_id=7, direct=True))
    await w.submit(8, _mk(guild_id=8, direct=False))
    snapshot = await w.status_snapshot()
    # Guild 7 has one youtube + one direct item; guild 8 has one youtube item.
    assert snapshot['queue_sizes'] == {'7': 2, '8': 1}
    assert snapshot['failure_summary'] == '0 failures in queue'
    assert snapshot['failure_count'] == 0
    # Startup floor disabled in _worker() and no backoff claimed -> None.
    assert snapshot['backoff_seconds_remaining'] is None


@pytest.mark.asyncio
async def test_status_snapshot_reports_backoff_and_failures():
    w = _worker()
    # A YouTube failure extends the shared wait window and grows the failure ZSET.
    await w.update_tracking(_result(_mk(direct=False), success=False,
                                    error_type=DownloadErrorType.RETRYABLE))
    snapshot = await w.status_snapshot()
    assert snapshot['backoff_seconds_remaining'] is not None
    assert snapshot['backoff_seconds_remaining'] > 0
    assert snapshot['failure_summary'] == '1 failures in queue'
    assert snapshot['failure_count'] == 1


# --------------------------------------------------------------------------- #
# egress-exit failure log (prod YouTube-failure path)
# --------------------------------------------------------------------------- #

class _FakeExitProbe:
    '''Minimal ExitProbe stand-in exposing cached exit accessors.'''
    def __init__(self, hostname='us-lax-wg-101', ip='1.2.3.4'):
        self.exit_hostname = hostname
        self.exit_ip = ip


@pytest.mark.asyncio
async def test_youtube_failure_logs_egress_exit(mocker):
    '''Prod path: a YouTube failure logs the cached egress exit hostname.'''
    w = _worker()
    w.set_exit_probe(_FakeExitProbe())
    logger = mocker.patch.object(w, 'logger')
    result = _result(_mk(direct=False), success=False, error_type=DownloadErrorType.RETRYABLE)
    await w.update_tracking(result)
    logger.warning.assert_called_once()
    assert 'us-lax-wg-101' in logger.warning.call_args.args


@pytest.mark.asyncio
async def test_direct_failure_does_not_log_egress(mocker):
    '''DIRECT failures don't go through the YouTube path, so nothing is logged there.'''
    w = _worker()
    w.set_exit_probe(_FakeExitProbe())
    log_exit = mocker.patch.object(w, '_log_exit_failure')
    result = _result(_mk(direct=True), success=False, error_type=DownloadErrorType.RETRYABLE)
    await w.update_tracking(result)
    log_exit.assert_not_called()


@pytest.mark.asyncio
async def test_youtube_success_does_not_log_egress(mocker):
    '''A successful YouTube result never emits the by-exit failure log.'''
    w = _worker()
    w.set_exit_probe(_FakeExitProbe())
    log_exit = mocker.patch.object(w, '_log_exit_failure')
    result = _result(_mk(direct=False), success=True, extractor='youtube')
    await w.update_tracking(result)
    log_exit.assert_not_called()


# --------------------------------------------------------------------------- #
# Idle-poll cost
# --------------------------------------------------------------------------- #

@pytest.mark.asyncio
async def test_pool_is_empty_tracks_the_round_robin_zset():
    '''_pool_is_empty reads the same per-pool ZSET _round_robin_pop pops from, so an
    "empty" answer means the pop would have found nothing anyway.'''
    w = _worker()
    assert await w._pool_is_empty(direct=True) is True
    assert await w._pool_is_empty(direct=False) is True
    await w._enqueue_request(7, _mk(direct=True))
    assert await w._pool_is_empty(direct=True) is False
    # The pools are independent — a DIRECT item leaves the YouTube pool empty.
    assert await w._pool_is_empty(direct=False) is True


@pytest.mark.asyncio
async def test_idle_pop_takes_no_lock():
    '''An idle poll must not burn a SET NX + GET + DEL lock cycle per pool just to
    discover an empty queue. Pool mode runs a driver per egress exit and every driver
    polls on the idle interval, so that cycle was the pod's dominant redis traffic.'''
    w = _worker()
    writes = []
    client = w._manager.client
    real_set, real_delete = client.set, client.delete

    async def _spy_set(*args, **kwargs):
        writes.append(('set', args[0]))
        return await real_set(*args, **kwargs)

    async def _spy_delete(*args, **kwargs):
        writes.append(('delete', args[0]))
        return await real_delete(*args, **kwargs)

    client.set, client.delete = _spy_set, _spy_delete

    assert await w._atomic_pop_direct() is None
    assert await w._atomic_pop_youtube() is None
    assert not writes


@pytest.mark.asyncio
async def test_idle_peek_emits_no_spans():
    '''The idle peek suppresses client instrumentation end to end: polling an empty
    queue emits no redis spans, while the same read outside the peek still does.
    Those idle spans were ~98% of the downloader's span volume in pool mode, at a
    rate set by the poll interval rather than by anything actually happening.'''
    exporter = InMemorySpanExporter()
    provider = TracerProvider()
    provider.add_span_processor(SimpleSpanProcessor(exporter))
    instrumentor = RedisInstrumentor()
    instrumentor.instrument(tracer_provider=provider)
    try:
        w = _worker()
        # Baseline: the same queue read is traced when it isn't the idle peek.
        with pytest.raises(asyncio.QueueEmpty):
            await w._merged_get_nowait()
        assert [s.name for s in exporter.get_finished_spans()]
        exporter.clear()

        with pytest.raises(asyncio.QueueEmpty):
            await w._peek_next_request()
        assert not exporter.get_finished_spans()
    finally:
        instrumentor.uninstrument()


# --------------------------------------------------------------------------- #
# Deferred retries
# --------------------------------------------------------------------------- #

@pytest.mark.asyncio
async def test_deferred_retry_is_withheld_until_ready():
    '''A deferred retry is not poppable, then is once its hold-off has passed.

    This is the pacing the pool migration lost: per-exit backoff rotates exits
    but does not hold a single request back at all, so a retry re-queued
    instantly just leases the next free exit and burns it.
    '''
    w = _worker()
    request = _mk()
    await w._enqueue_deferred_request(request.guild_id, request,
                                      w._now_seconds() + 3600)
    # Parked: nothing on the guild queue, so the consumer loop sees an empty pool.
    await w._promote_ready_retries()
    with pytest.raises(asyncio.QueueEmpty):
        await w._merged_get_nowait()

    # Same request, now due.
    await w._manager.client.zadd(
        DEFERRED_RETRIES_KEY,
        {w._deferred_member(request.guild_id, str(request.uuid)): w._now_seconds() - 1})
    await w._promote_ready_retries()
    popped = await w._merged_get_nowait()
    assert str(popped.uuid) == str(request.uuid)
    # The claim is consumed — a second sweep must not re-queue it.
    await w._promote_ready_retries()
    with pytest.raises(asyncio.QueueEmpty):
        await w._merged_get_nowait()


@pytest.mark.asyncio
async def test_promote_ready_retries_drops_expired_payload():
    '''A deferred entry whose request payload TTL'd away is discarded, not raised.'''
    w = _worker()
    request = _mk()
    await w._enqueue_deferred_request(request.guild_id, request, w._now_seconds() - 1)
    await w._manager.client.delete(w._request_key(str(request.uuid)))

    await w._promote_ready_retries()

    assert await w._manager.client.zcard(DEFERRED_RETRIES_KEY) == 0
    with pytest.raises(asyncio.QueueEmpty):
        await w._merged_get_nowait()


@pytest.mark.asyncio
async def test_deferred_retry_claimed_once_across_pods():
    '''Two pods sharing the ZSET promote a due retry exactly once.

    Both sweep the same second; the ZREM decides. Without it each would enqueue
    the request and the video would download twice.
    '''
    manager = _manager()
    pod_a, pod_b = _worker(manager), _worker(manager)
    request = _mk()
    await pod_a._enqueue_deferred_request(request.guild_id, request,
                                          pod_a._now_seconds() - 1)

    await pod_a._promote_ready_retries()
    await pod_b._promote_ready_retries()

    assert await pod_a.queue_size(request.guild_id) == 1


@pytest.mark.asyncio
async def test_deferred_retry_counts_toward_queue_size():
    '''A guild waiting only on a deferred retry does not report an empty queue —
    a caller reading 0 concludes the guild has drained.'''
    w = _worker()
    request = _mk(guild_id=11)
    await w._enqueue_deferred_request(11, request, w._now_seconds() + 3600)
    assert await w.queue_size(11) == 1
    # Scoped to the guild that owns it.
    assert await w.queue_size(12) == 0


@pytest.mark.asyncio
async def test_clear_guild_queue_drops_deferred_retries():
    '''Clearing a guild takes its parked retries with it — otherwise a cleared
    request reappears minutes later when its hold-off elapses.'''
    w = _worker()
    mine, theirs = _mk(guild_id=11), _mk(guild_id=12)
    await w._enqueue_deferred_request(11, mine, w._now_seconds() + 3600)
    await w._enqueue_deferred_request(12, theirs, w._now_seconds() + 3600)

    dropped = await w.clear_guild_queue(11)

    assert [str(r.uuid) for r in dropped] == [str(mine.uuid)]
    assert await w.queue_size(11) == 0
    assert await w.queue_size(12) == 1
    # The payload is gone too, not just the ZSET entry.
    assert await w._manager.client.get(w._request_key(str(mine.uuid))) is None


@pytest.mark.asyncio
async def test_clear_guild_queue_honours_preserve_predicate_for_deferred():
    '''A preserved deferred retry survives the clear and stays parked.'''
    w = _worker()
    request = _mk(guild_id=11)
    await w._enqueue_deferred_request(11, request, w._now_seconds() + 3600)

    dropped = await w.clear_guild_queue(11, preserve_predicate=lambda _r: True)

    assert not dropped
    assert await w.queue_size(11) == 1


@pytest.mark.asyncio
async def test_promote_ready_retries_skips_member_another_pod_claimed(mocker):
    '''Losing the ZREM race means another pod owns the promotion — skip it.

    The window is between this pod's ZRANGEBYSCORE and its ZREM; enqueueing
    anyway would download the same request on two pods.
    '''
    w = _worker()
    request = _mk()
    await w._enqueue_deferred_request(request.guild_id, request, w._now_seconds() - 1)
    mocker.patch.object(w._manager.client, 'zrem', AsyncMock(return_value=0))

    await w._promote_ready_retries()

    with pytest.raises(asyncio.QueueEmpty):
        await w._merged_get_nowait()


@pytest.mark.asyncio
async def test_clear_guild_queue_prunes_deferred_with_expired_payload():
    '''A deferred entry whose payload TTL'd away is pruned by a clear, not returned.'''
    w = _worker()
    request = _mk(guild_id=11)
    await w._enqueue_deferred_request(11, request, w._now_seconds() + 3600)
    await w._manager.client.delete(w._request_key(str(request.uuid)))

    dropped = await w.clear_guild_queue(11)

    assert not dropped
    assert await w.queue_size(11) == 0


# ---------------------------------------------------------------------------
# Guild block enforcement
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_submit_raises_puts_blocked_while_guild_blocked():
    '''The flag is now enforced on submit — it used to be written and never read,
    so a blocked guild kept accepting work.'''
    w = _worker()
    await w.block_guild(7)
    with pytest.raises(PutsBlocked):
        await w.submit(7, _mk(guild_id=7))
    assert await w.queue_size(7) == 0


@pytest.mark.asyncio
async def test_block_guild_expires():
    '''The block carries a TTL.  DistributedQueue.block, which this mirrors,
    blocks a queue object that is dropped when the guild's queue drains or is
    cleared, so the in-process block is inherently transient and has no unblock
    call.  A key with no expiry would wedge the guild permanently instead.'''
    w = _worker()
    await w.block_guild(7)
    ttl = await w._manager.client.ttl(w._guild_blocked_key(7))
    assert 0 < ttl <= GUILD_BLOCK_TTL_SECONDS


@pytest.mark.asyncio
async def test_submit_allowed_again_once_block_expires():
    '''Once the window lapses the guild accepts submissions again.'''
    w = _worker()
    await w.block_guild(7)
    await w._manager.client.delete(w._guild_blocked_key(7))  # simulate expiry
    await w.submit(7, _mk(guild_id=7))
    assert await w.queue_size(7) == 1


@pytest.mark.asyncio
async def test_block_is_scoped_to_one_guild():
    '''Blocking one guild leaves the others accepting work.'''
    w = _worker()
    await w.block_guild(7)
    await w.submit(8, _mk(guild_id=8))
    assert await w.queue_size(8) == 1


@pytest.mark.asyncio
async def test_internal_requeue_is_not_blocked():
    '''Only new submissions are blocked.  The worker's own retry /
    no-exit-available / deferred-promotion re-queues are work already accepted;
    they run on the consumer loop with no PutsBlocked handler above them, so
    raising there would take the loop down rather than shed one request.'''
    w = _worker()
    await w.block_guild(7)
    await w._enqueue_request(7, _mk(guild_id=7))
    assert await w.queue_size(7) == 1
