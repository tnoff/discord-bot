'''Tests for SearchMetrics — the search pod's gauge poller.'''
# NOTE: test names here deliberately avoid being exactly 40 characters long.
# trufflehog's Lob detector matches `test_` + 35 chars — the exact shape of a
# Lob test API key — so a 40-char test function name is reported as a VERIFIED
# secret and fails pr-check:secrets. Renaming is cheaper than an allowlist.
import asyncio
from unittest.mock import AsyncMock

import pytest

from discord_bot.workers.search_metrics import SearchMetrics


def _snapshot(*, queue_sizes=None, backoff=None, failure_count=0):
    '''Build a status_snapshot()-shaped dict.'''
    return {
        'failure_summary': f'{failure_count} failures in queue',
        'failure_count': failure_count,
        'backoff_seconds_remaining': backoff,
        'queue_sizes': queue_sizes or {},
    }


def _metrics(snapshot=None):
    '''Return (worker, SearchMetrics) with a stubbed status_snapshot.'''
    worker = AsyncMock()
    worker.status_snapshot = AsyncMock(return_value=snapshot or _snapshot())
    return worker, SearchMetrics(worker)


def test_initial_observations_are_zero_tagged_search():
    '''Before any refresh, all gauges read 0 under background_job=youtube_music_search.'''
    _, metrics = _metrics()
    for callback in (metrics.queue_depth_observations,
                     metrics.backoff_observations,
                     metrics.failure_count_observations):
        (observation,) = callback()
        assert observation.value == 0
        assert observation.attributes == {'background_job': 'youtube_music_search'}


@pytest.mark.asyncio
async def test_refresh_caches_snapshot_values():
    '''refresh() sums queue depth and caches backoff + failure count.'''
    _, metrics = _metrics(_snapshot(queue_sizes={'7': 3, '12': 1}, backoff=42, failure_count=5))
    await metrics.refresh()
    (depth,) = metrics.queue_depth_observations()
    (backoff,) = metrics.backoff_observations()
    (failures,) = metrics.failure_count_observations()
    assert depth.value == 4
    assert backoff.value == 42
    assert failures.value == 5


@pytest.mark.asyncio
async def test_refresh_treats_a_none_backoff_as_zero():
    '''A None backoff (no active window) is reported as 0, not None.'''
    _, metrics = _metrics(_snapshot(backoff=None))
    await metrics.refresh()
    (backoff,) = metrics.backoff_observations()
    assert backoff.value == 0


@pytest.mark.asyncio
async def test_run_refreshes_then_exits_when_stopped(mocker):
    '''run() refreshes at least once and exits once stop_event is set.'''
    _, metrics = _metrics()
    stop = asyncio.Event()

    async def _refresh_then_stop():
        stop.set()

    refresh_mock = mocker.patch.object(metrics, 'refresh', side_effect=_refresh_then_stop)
    await metrics.run(stop, interval=0.01)
    refresh_mock.assert_awaited()


@pytest.mark.asyncio
async def test_run_guards_refresh_errors(mocker):
    '''A refresh exception is swallowed so the poller keeps running.'''
    _, metrics = _metrics()
    stop = asyncio.Event()
    calls = {'n': 0}

    async def _boom():
        calls['n'] += 1
        if calls['n'] == 1:
            raise RuntimeError('redis blip')
        stop.set()

    mocker.patch.object(metrics, 'refresh', side_effect=_boom)
    await metrics.run(stop, interval=0.001)   # must not raise
    assert calls['n'] == 2


def test_search_gauges_are_distinct_from_the_downloader_metrics():
    '''The search pod publishes its own metric names: the two pods share a shape
    but not a series (their backoff windows and failure ZSETs are separate keys).'''
    assert SearchMetrics.QUEUE_DEPTH_METRIC == 'search_queue_depth'
    assert SearchMetrics.BACKOFF_METRIC == 'search_youtube_backoff_seconds'
    assert SearchMetrics.FAILURE_COUNT_METRIC == 'search_failure_count'
