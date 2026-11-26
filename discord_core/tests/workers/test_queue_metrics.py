'''Tests for the queue-depth gauge poller (QueueMetricsBase).'''
import asyncio
import logging

import pytest

from discord_core.utils.otel import AttributeNaming
from discord_core.workers.queue_metrics import DEFAULT_POLL_INTERVAL_SECONDS, QueueMetricsBase


class _Metrics(QueueMetricsBase):
    '''Concrete poller with test-only metric names.'''
    QUEUE_DEPTH_METRIC = 'qm_test_depth'
    QUEUE_DEPTH_DESCRIPTION = 'depth'
    BACKOFF_METRIC = 'qm_test_backoff'
    BACKOFF_DESCRIPTION = 'backoff'
    FAILURE_COUNT_METRIC = 'qm_test_failures'
    FAILURE_COUNT_DESCRIPTION = 'failures'
    JOB_LABEL = 'qm_test_job'


class _Worker:
    '''Worker whose snapshots are scripted; an Exception entry is raised.'''

    def __init__(self, *snapshots):
        self.snapshots = list(snapshots)
        self.calls = 0

    async def status_snapshot(self):
        '''Return or raise the next scripted snapshot.'''
        self.calls += 1
        item = self.snapshots.pop(0) if len(self.snapshots) > 1 else self.snapshots[0]
        if isinstance(item, Exception):
            raise item
        return item


def _snapshot(sizes, backoff, failures):
    '''Build a status snapshot dict.'''
    return {'failure_summary': '', 'failure_count': failures,
            'backoff_seconds_remaining': backoff, 'queue_sizes': sizes}


def _value(observations):
    '''The single observed value, after checking the job label.'''
    assert len(observations) == 1
    assert observations[0].attributes == {AttributeNaming.BACKGROUND_JOB.value: 'qm_test_job'}
    return observations[0].value


def test_gauges_start_at_zero():
    '''Before any refresh every gauge reports 0 under the job label.'''
    metrics = _Metrics(_Worker(_snapshot({}, None, 0)))
    assert _value(metrics.queue_depth_observations()) == 0
    assert _value(metrics.backoff_observations()) == 0
    assert _value(metrics.failure_count_observations()) == 0


@pytest.mark.asyncio
async def test_refresh_caches_snapshot_values():
    '''Depth is summed across guilds; backoff and failures are copied.'''
    metrics = _Metrics(_Worker(_snapshot({'1': 2, '2': 5}, 30, 4)))
    await metrics.refresh()
    assert _value(metrics.queue_depth_observations()) == 7
    assert _value(metrics.backoff_observations()) == 30
    assert _value(metrics.failure_count_observations()) == 4


@pytest.mark.asyncio
async def test_refresh_none_backoff_is_zero():
    '''An absent backoff window is exposed as 0.'''
    metrics = _Metrics(_Worker(_snapshot({}, 30, 0), _snapshot({}, None, 0)))
    await metrics.refresh()
    await metrics.refresh()
    assert _value(metrics.backoff_observations()) == 0


@pytest.mark.asyncio
async def test_refresh_propagates_errors():
    '''refresh itself raises; keeping last values is run()'s job.'''
    metrics = _Metrics(_Worker(RuntimeError('redis down')))
    with pytest.raises(RuntimeError, match='redis down'):
        await metrics.refresh()


@pytest.mark.asyncio
async def test_run_returns_immediately_when_already_stopped():
    '''A pre-set stop event means the worker is never polled.'''
    worker = _Worker(_snapshot({}, None, 0))
    stop = asyncio.Event()
    stop.set()
    await _Metrics(worker).run(stop)
    assert worker.calls == 0


@pytest.mark.asyncio
async def test_run_polls_each_interval_until_stopped():
    '''run keeps refreshing on every tick and exits once the event is set.'''
    worker = _Worker(_snapshot({'1': 3}, None, 1))
    metrics = _Metrics(worker)
    stop = asyncio.Event()
    task = asyncio.create_task(metrics.run(stop, interval=0.01))
    while worker.calls < 3:
        await asyncio.sleep(0.01)
    stop.set()
    await asyncio.wait_for(task, timeout=2)
    assert _value(metrics.queue_depth_observations()) == 3


@pytest.mark.asyncio
async def test_run_stop_is_prompt_with_long_interval():
    '''Setting the stop event interrupts the inter-tick wait.'''
    worker = _Worker(_snapshot({}, None, 0))
    stop = asyncio.Event()
    task = asyncio.create_task(_Metrics(worker).run(stop, interval=60))
    while worker.calls < 1:
        await asyncio.sleep(0.01)
    stop.set()
    await asyncio.wait_for(task, timeout=2)
    assert worker.calls == 1


@pytest.mark.asyncio
async def test_run_keeps_last_values_on_failure(caplog):
    '''A failed refresh is logged and the previous gauge values stay.'''
    worker = _Worker(_snapshot({'1': 9}, 5, 2), RuntimeError('boom'))
    metrics = _Metrics(worker)
    stop = asyncio.Event()
    with caplog.at_level(logging.ERROR):
        task = asyncio.create_task(metrics.run(stop, interval=0.01))
        while worker.calls < 3:
            await asyncio.sleep(0.01)
        stop.set()
        await asyncio.wait_for(task, timeout=2)
    assert 'refresh failed; keeping last values' in caplog.text
    assert _value(metrics.queue_depth_observations()) == 9
    assert _value(metrics.backoff_observations()) == 5


def test_default_interval_is_positive():
    '''The default polling interval is a sane positive number of seconds.'''
    assert DEFAULT_POLL_INTERVAL_SECONDS > 0
