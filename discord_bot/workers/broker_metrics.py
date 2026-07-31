'''
Background metrics collector for the standalone broker process.

OTEL observable-gauge callbacks are synchronous, but the broker's state lives in
Redis behind async calls (result-queue length, registry entry/bundle counts).
This poller periodically reads that state into cached values that the sync gauge
callbacks return, so a slow or unavailable Redis never blocks the metric export
path — a failed refresh just keeps the last-known values until the next tick.

Gauges (job="discord-broker"):
    music.download_result_queue_depth {background_job="broker"} — bot-ready backlog
    music.search_result_queue_depth {background_job="broker"}   — bot-ready search backlog
    broker.entries {zone="available"|"checked_out"}            — registry entries
    broker.bundles                                             — active multi-request bundles
'''
import asyncio
import logging
from collections import Counter

from opentelemetry.metrics import Observation

from discord_bot.interfaces.result_queue import DownloadResultQueue, SearchResultQueue
from discord_bot.utils.otel import (create_observable_gauge, METER_PROVIDER,
                                     MetricNaming, AttributeNaming)
from discord_bot.workers.broker_registry import RedisBrokerRegistry

logger = logging.getLogger(__name__)

DEFAULT_POLL_INTERVAL_SECONDS = 15.0
# Zones always reported so a drop to zero is visible as 0, not an absent series.
_KNOWN_ZONES = ('in_flight', 'available', 'checked_out')


class BrokerMetrics:
    '''Polls Redis-backed broker state into gauge-friendly cached values.'''

    def __init__(self, result_queue: DownloadResultQueue, registry: RedisBrokerRegistry,
                 search_result_queue: SearchResultQueue | None = None):
        self._result_queue = result_queue
        self._search_result_queue = search_result_queue
        self._registry = registry
        self._queue_depth = 0
        self._search_queue_depth = 0
        self._entries_by_zone: dict[str, int] = {}
        self._bundle_count = 0
        create_observable_gauge(METER_PROVIDER, MetricNaming.DOWNLOAD_RESULT_QUEUE_DEPTH.value,
                                self.queue_depth_observations,
                                'Pending download results on the broker bot-ready queue')
        if self._search_result_queue is not None:
            create_observable_gauge(METER_PROVIDER, MetricNaming.SEARCH_RESULT_QUEUE_DEPTH.value,
                                    self.search_queue_depth_observations,
                                    'Pending resolved searches on the broker bot-ready queue')
        create_observable_gauge(METER_PROVIDER, MetricNaming.BROKER_ENTRIES.value,
                                self.entry_observations,
                                'Broker registry entries by zone')
        create_observable_gauge(METER_PROVIDER, MetricNaming.BROKER_BUNDLES.value,
                                self.bundle_observations,
                                'Active multi-request bundles tracked by the broker')

    # OTEL observable-gauge callbacks — public so they can be exercised directly.
    def queue_depth_observations(self, _options=None):
        '''Bot-ready result-queue depth (cached from the last refresh).'''
        return [Observation(self._queue_depth, attributes={
            AttributeNaming.BACKGROUND_JOB.value: 'broker',
        })]

    def search_queue_depth_observations(self, _options=None):
        '''Bot-ready search-result-queue depth (cached from the last refresh).'''
        return [Observation(self._search_queue_depth, attributes={
            AttributeNaming.BACKGROUND_JOB.value: 'broker',
        })]

    def entry_observations(self, _options=None):
        '''Registry entry counts per zone; known zones always reported (even at 0).'''
        counts = {zone: 0 for zone in _KNOWN_ZONES}
        counts.update(self._entries_by_zone)
        return [
            Observation(count, attributes={AttributeNaming.ZONE.value: zone})
            for zone, count in counts.items()
        ]

    def bundle_observations(self, _options=None):
        '''Active multi-request bundle count.'''
        return [Observation(self._bundle_count)]

    async def refresh(self) -> None:
        '''Read current state from Redis into the cached gauge values.

        Raises on Redis errors — run() is responsible for catching and keeping
        the previous values.
        '''
        self._queue_depth = await self._result_queue.depth()
        if self._search_result_queue is not None:
            self._search_queue_depth = await self._search_result_queue.depth()
        entries = await self._registry.all_entries()
        self._entries_by_zone = dict(Counter(e.get('zone', 'unknown') for e in entries))
        self._bundle_count = len(await self._registry.all_bundles())

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
                logger.exception('BrokerMetrics :: refresh failed; keeping last values')
            try:
                await asyncio.wait_for(stop_event.wait(), timeout=interval)
            except asyncio.TimeoutError:
                pass
