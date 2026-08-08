'''
Shared background metrics collector for the standalone worker pods.

OTEL observable-gauge callbacks are synchronous, but a worker pod's state lives
in Redis behind async calls (per-guild queue depth, the shared YouTube backoff
window, the failure ZSET).  This poller periodically reads that state — via the
worker's status_snapshot() — into cached values the sync gauge callbacks return,
so a slow or unavailable Redis never blocks the metric export path; a failed
refresh just keeps the last-known values until the next tick.  Mirrors
BrokerMetrics.

The downloader and the YouTube-Music search pod publish the same three gauges
over the same snapshot shape, so the body lives here once and each subclass
supplies its metric names + job label (two copies would trip pylint's
duplicate-code check).
'''
import asyncio
import logging
from typing import ClassVar

from opentelemetry.metrics import Observation

from discord_bot.utils.otel import (create_observable_gauge, METER_PROVIDER,
                                     AttributeNaming)

logger = logging.getLogger(__name__)

DEFAULT_POLL_INTERVAL_SECONDS = 15.0


class QueueMetricsBase:
    '''Polls a Redis-backed worker's status_snapshot into gauge-friendly values.'''

    # Metric names + descriptions and the background_job label — set by subclasses.
    QUEUE_DEPTH_METRIC: ClassVar[str]
    QUEUE_DEPTH_DESCRIPTION: ClassVar[str]
    BACKOFF_METRIC: ClassVar[str]
    BACKOFF_DESCRIPTION: ClassVar[str]
    FAILURE_COUNT_METRIC: ClassVar[str]
    FAILURE_COUNT_DESCRIPTION: ClassVar[str]
    JOB_LABEL: ClassVar[str]

    def __init__(self, worker):
        self._worker = worker
        self._queue_depth = 0
        self._backoff_seconds = 0
        self._failure_count = 0
        self._job_attributes = {AttributeNaming.BACKGROUND_JOB.value: self.JOB_LABEL}
        create_observable_gauge(METER_PROVIDER, self.QUEUE_DEPTH_METRIC,
                                self.queue_depth_observations,
                                self.QUEUE_DEPTH_DESCRIPTION)
        create_observable_gauge(METER_PROVIDER, self.BACKOFF_METRIC,
                                self.backoff_observations,
                                self.BACKOFF_DESCRIPTION, unit='s')
        create_observable_gauge(METER_PROVIDER, self.FAILURE_COUNT_METRIC,
                                self.failure_count_observations,
                                self.FAILURE_COUNT_DESCRIPTION)

    # OTEL observable-gauge callbacks — public so they can be exercised directly.
    def queue_depth_observations(self, _options=None):
        '''Total pending requests across all guild queues (cached).'''
        return [Observation(self._queue_depth, attributes=self._job_attributes)]

    def backoff_observations(self, _options=None):
        '''Seconds left on the shared YouTube backoff window.'''
        return [Observation(self._backoff_seconds, attributes=self._job_attributes)]

    def failure_count_observations(self, _options=None):
        '''Failures currently in the shared YouTube failure ZSET.'''
        return [Observation(self._failure_count, attributes=self._job_attributes)]

    async def refresh(self) -> None:
        '''Read current state from Redis into the cached gauge values.

        Raises on Redis errors — run() is responsible for catching and keeping
        the previous values.
        '''
        snapshot = await self._worker.status_snapshot()
        self._queue_depth = sum(snapshot['queue_sizes'].values())
        self._backoff_seconds = snapshot['backoff_seconds_remaining'] or 0
        self._failure_count = snapshot['failure_count']

    async def run(self, stop_event: asyncio.Event,
                  interval: float = DEFAULT_POLL_INTERVAL_SECONDS) -> None:
        '''Refresh on each tick until stop_event is set.

        The inter-tick wait is a bounded wait on stop_event so a shutdown is
        picked up immediately rather than after the full interval.
        '''
        while not stop_event.is_set():
            try:
                await self.refresh()
            except Exception:
                logger.exception('%s :: refresh failed; keeping last values',
                                 self.__class__.__name__)
            try:
                await asyncio.wait_for(stop_event.wait(), timeout=interval)
            except asyncio.TimeoutError:
                pass
