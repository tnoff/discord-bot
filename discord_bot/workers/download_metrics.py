'''
Background metrics collector for the standalone downloader process.

OTEL observable-gauge callbacks are synchronous, but the downloader's state lives
in Redis behind async calls (per-guild queue depth, the shared YouTube backoff
window, the failure ZSET).  This poller periodically reads that state — via the
worker's status_snapshot() — into cached values the sync gauge callbacks return,
so a slow or unavailable Redis never blocks the metric export path; a failed
refresh just keeps the last-known values until the next tick.  Mirrors
BrokerMetrics.

Gauges (job="discord-downloader", background_job="downloader"):
    download_queue_depth              — total pending downloads across all guilds
    download_youtube_backoff_seconds  — seconds left on the shared YouTube window
    download_failure_count            — failures in the shared YouTube ZSET
'''
import asyncio
import logging

from opentelemetry.metrics import Observation

from discord_bot.utils.otel import (create_observable_gauge, METER_PROVIDER,
                                     MetricNaming, AttributeNaming)
from discord_bot.workers.redis_download_worker import RedisDownloadWorker

logger = logging.getLogger(__name__)

DEFAULT_POLL_INTERVAL_SECONDS = 15.0
_JOB = {AttributeNaming.BACKGROUND_JOB.value: 'downloader'}


class DownloadMetrics:
    '''Polls Redis-backed downloader state into gauge-friendly cached values.'''

    def __init__(self, worker: RedisDownloadWorker):
        self._worker = worker
        self._queue_depth = 0
        self._backoff_seconds = 0
        self._failure_count = 0
        create_observable_gauge(METER_PROVIDER, MetricNaming.DOWNLOAD_QUEUE_DEPTH.value,
                                self.queue_depth_observations,
                                'Pending downloads across all guild queues')
        create_observable_gauge(METER_PROVIDER, MetricNaming.DOWNLOAD_YOUTUBE_BACKOFF.value,
                                self.backoff_observations,
                                'Seconds remaining on the shared YouTube backoff window',
                                unit='s')
        create_observable_gauge(METER_PROVIDER, MetricNaming.DOWNLOAD_FAILURE_COUNT.value,
                                self.failure_count_observations,
                                'Failures in the shared YouTube failure queue')

    # OTEL observable-gauge callbacks — public so they can be exercised directly.
    def queue_depth_observations(self, _options=None):
        '''Total pending downloads (cached from the last refresh).'''
        return [Observation(self._queue_depth, attributes=_JOB)]

    def backoff_observations(self, _options=None):
        '''Seconds left on the shared YouTube backoff window.'''
        return [Observation(self._backoff_seconds, attributes=_JOB)]

    def failure_count_observations(self, _options=None):
        '''Failures currently in the shared YouTube failure ZSET.'''
        return [Observation(self._failure_count, attributes=_JOB)]

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
                logger.exception('DownloadMetrics :: refresh failed; keeping last values')
            try:
                await asyncio.wait_for(stop_event.wait(), timeout=interval)
            except asyncio.TimeoutError:
                pass
