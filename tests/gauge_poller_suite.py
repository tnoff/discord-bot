'''
The tests the queue-worker gauge pollers share.

DownloadMetrics and SearchMetrics poll a worker's ``status_snapshot()`` and publish the same
three gauges under a per-pod ``background_job`` label. A pod's test module subclasses
``GaugePollerSuite`` and sets ``metrics_cls`` and ``job``.

Test names here must not be exactly 40 characters long: trufflehog's Lob detector matches
`test_` + 35 chars, the shape of a Lob test API key, and fails pr-check:secrets.
'''
import asyncio
from unittest.mock import AsyncMock

import pytest

from tests.queue_status import status_snapshot


class GaugePollerSuite:
    '''Cases every queue-worker gauge poller must pass.

    Not collected itself; a subclass in a ``test_*`` module supplies the attributes below.
    '''
    metrics_cls: type
    # The background_job label the pod's gauges carry.
    job = ''

    def make_metrics(self, snapshot=None):
        '''A poller over a worker with a stubbed status_snapshot.'''
        worker = AsyncMock()
        worker.status_snapshot = AsyncMock(return_value=snapshot or status_snapshot())
        return self.metrics_cls(worker)

    def test_initial_observations_are_zero_tagged_with_the_job(self):
        '''Before any refresh, all gauges read 0 under the pod's background_job label.'''
        metrics = self.make_metrics()
        for callback in (metrics.queue_depth_observations,
                         metrics.backoff_observations,
                         metrics.failure_count_observations):
            (observation,) = callback()
            assert observation.value == 0
            assert observation.attributes == {'background_job': self.job}

    @pytest.mark.asyncio
    async def test_refresh_caches_snapshot_values(self):
        '''refresh() sums queue depth and caches backoff + failure count.'''
        metrics = self.make_metrics(status_snapshot(
            queue_sizes={'7': 3, '12': 1}, backoff=42, failure_count=5))
        await metrics.refresh()
        (depth,) = metrics.queue_depth_observations()
        (backoff,) = metrics.backoff_observations()
        (failures,) = metrics.failure_count_observations()
        assert depth.value == 4
        assert backoff.value == 42
        assert failures.value == 5

    @pytest.mark.asyncio
    async def test_refresh_treats_a_none_backoff_as_zero(self):
        '''A None backoff (no active window) is reported as 0, not None.'''
        metrics = self.make_metrics(status_snapshot(backoff=None))
        await metrics.refresh()
        (backoff,) = metrics.backoff_observations()
        assert backoff.value == 0

    @pytest.mark.asyncio
    async def test_run_refreshes_then_exits_when_stopped(self, mocker):
        '''run() refreshes at least once and exits once stop_event is set.'''
        metrics = self.make_metrics()
        stop = asyncio.Event()

        async def _refresh_then_stop():
            stop.set()

        refresh_mock = mocker.patch.object(metrics, 'refresh', side_effect=_refresh_then_stop)
        await metrics.run(stop, interval=0.01)
        refresh_mock.assert_awaited()

    @pytest.mark.asyncio
    async def test_run_guards_refresh_errors(self, mocker):
        '''A refresh exception is swallowed so the poller keeps running.'''
        metrics = self.make_metrics()
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
