'''Tests for BrokerMetrics — the standalone broker's Redis-state metrics poller.'''
import asyncio
from unittest.mock import AsyncMock, MagicMock

import pytest

from discord_bot.workers.broker_metrics import BrokerMetrics


def _metrics(depth=0, entries=None, bundles=0):
    result_queue = MagicMock()
    result_queue.depth = AsyncMock(return_value=depth)
    registry = MagicMock()
    registry.all_entries = AsyncMock(return_value=entries if entries is not None else [])
    registry.all_bundles = AsyncMock(return_value=[{}] * bundles)
    return BrokerMetrics(result_queue, registry), result_queue, registry


def _zone_map(observations):
    return {o.attributes['zone']: o.value for o in observations}


class TestObservations:
    def test_queue_depth_defaults_to_zero(self):
        bm, _, _ = _metrics()
        (obs,) = bm.queue_depth_observations(None)
        assert obs.value == 0
        assert obs.attributes == {'background_job': 'broker'}

    def test_entry_observations_report_known_zones_as_zero(self):
        '''Known zones are always emitted (as 0) so a drop is visible, not absent.'''
        bm, _, _ = _metrics()
        assert _zone_map(bm.entry_observations(None)) == {
            'in_flight': 0, 'available': 0, 'checked_out': 0}

    def test_bundle_observation_defaults_to_zero(self):
        bm, _, _ = _metrics()
        (obs,) = bm.bundle_observations(None)
        assert obs.value == 0


def _metrics_with_search(search_depth=0):
    '''BrokerMetrics wired with a search-result queue (the HA broker path).'''
    result_queue = MagicMock()
    result_queue.depth = AsyncMock(return_value=0)
    search_queue = MagicMock()
    search_queue.depth = AsyncMock(return_value=search_depth)
    registry = MagicMock()
    registry.all_entries = AsyncMock(return_value=[])
    registry.all_bundles = AsyncMock(return_value=[])
    return BrokerMetrics(result_queue, registry, search_result_queue=search_queue)


class TestSearchQueueDepth:
    def test_search_queue_depth_defaults_to_zero(self):
        bm = _metrics_with_search()
        (obs,) = bm.search_queue_depth_observations(None)
        assert obs.value == 0
        assert obs.attributes == {'background_job': 'broker'}


@pytest.mark.asyncio
class TestSearchQueueDepthRefresh:
    async def test_refresh_populates_search_queue_depth(self):
        bm = _metrics_with_search(search_depth=7)
        await bm.refresh()
        assert bm.search_queue_depth_observations(None)[0].value == 7


@pytest.mark.asyncio
class TestRefresh:
    async def test_refresh_populates_all_gauges(self):
        entries = [{'zone': 'available'}, {'zone': 'available'}, {'zone': 'checked_out'}]
        bm, _, _ = _metrics(depth=5, entries=entries, bundles=2)
        await bm.refresh()
        assert bm.queue_depth_observations(None)[0].value == 5
        assert _zone_map(bm.entry_observations(None)) == {
            'in_flight': 0, 'available': 2, 'checked_out': 1}
        assert bm.bundle_observations(None)[0].value == 2

    async def test_refresh_counts_extra_and_missing_zones(self):
        '''Unknown/missing zones are tallied alongside the always-present known zones.'''
        bm, _, _ = _metrics(entries=[{'zone': 'weird'}, {}])
        await bm.refresh()
        zones = _zone_map(bm.entry_observations(None))
        assert zones['available'] == 0 and zones['checked_out'] == 0
        assert zones['weird'] == 1 and zones['unknown'] == 1


@pytest.mark.asyncio
class TestRun:
    async def test_run_refreshes_then_stops(self):
        bm, result_queue, _ = _metrics()
        stop = asyncio.Event()

        async def _depth_then_stop():
            stop.set()
            return 0

        result_queue.depth = AsyncMock(side_effect=_depth_then_stop)
        await asyncio.wait_for(bm.run(stop, interval=0.01), timeout=2.0)
        assert result_queue.depth.await_count == 1

    async def test_run_survives_refresh_error_and_keeps_looping(self):
        '''A Redis error in refresh is logged and the loop continues to the next tick.'''
        bm, result_queue, _ = _metrics()
        stop = asyncio.Event()
        calls = {'n': 0}

        async def _flaky_depth():
            calls['n'] += 1
            if calls['n'] == 1:
                raise ConnectionError('redis down')
            stop.set()
            return 0

        result_queue.depth = AsyncMock(side_effect=_flaky_depth)
        await asyncio.wait_for(bm.run(stop, interval=0.01), timeout=2.0)
        assert calls['n'] == 2  # errored first tick, recovered on the second
