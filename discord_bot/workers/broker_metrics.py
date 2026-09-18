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
from enum import Enum
import asyncio
import logging
from collections import Counter

from opentelemetry.metrics import Observation

from discord_bot.interfaces.result_queue import DownloadResultQueue, SearchResultQueue
from discord_bot.utils.otel import create_observable_gauge, METER_PROVIDER, AttributeNaming, MetricNaming
from discord_bot.workers.broker_registry import RedisBrokerRegistry

logger = logging.getLogger(__name__)

DEFAULT_POLL_INTERVAL_SECONDS = 15.0
# Zones always reported so a drop to zero is visible as 0, not an absent series.


class BrokerMetricNaming(Enum):
    '''
    Metric names the broker emits, and nothing else.

    Split out of utils/otel.py (2026-09-18). servers/broker_server.py imports
    from here rather than owning a slice of its own: all three broker modules
    are reached by the broker alone, so the names have one home inside the image
    that emits them.

    Both pairs carry a `result_type` label rather than separate names, because
    both halves come from the broker process -- `job` cannot tell them apart the
    way it separates the downloader from the search pod.
    '''
    # No `broker_` prefix: `job="discord-broker"` already says which pod, and a
    # pod name in a metric name is a dimension in the wrong place. broker_entries
    # and broker_bundles keep theirs because there the prefix is the CONCEPT --
    # bare `entries` and `bundles` mean nothing on their own.
    RESULT_FETCH = 'result_fetch'
    BROKER_ENTRIES = 'broker_entries'
    BROKER_BUNDLES = 'broker_bundles'


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
        # One gauge, one callback, a result_type dimension. Both streams come from
        # the broker, so `job` cannot separate them and a label must.
        create_observable_gauge(METER_PROVIDER, MetricNaming.RESULT_QUEUE_DEPTH.value,
                                self.result_queue_depth_observations,
                                'Pending results on the broker bot-ready queues, by result type')
        create_observable_gauge(METER_PROVIDER, BrokerMetricNaming.BROKER_ENTRIES.value,
                                self.entry_observations,
                                'Broker registry entries by zone')
        create_observable_gauge(METER_PROVIDER, BrokerMetricNaming.BROKER_BUNDLES.value,
                                self.bundle_observations,
                                'Active multi-request bundles tracked by the broker')

    # OTEL observable-gauge callbacks — public so they can be exercised directly.
    def result_queue_depth_observations(self, _options=None):
        '''
        Bot-ready result-queue depths, one observation per result type.

        The search queue is reported only when the broker was built with one --
        emitting a 0 for a queue that does not exist would be a real-looking
        series saying nothing is pending, which is indistinguishable from a
        drained queue and exactly the confusion the absent series avoids.
        '''
        observations = [Observation(self._queue_depth, attributes={
            AttributeNaming.BACKGROUND_JOB.value: 'broker',
            AttributeNaming.RESULT_TYPE.value: 'download',
        })]
        if self._search_result_queue is not None:
            observations.append(Observation(self._search_queue_depth, attributes={
                AttributeNaming.BACKGROUND_JOB.value: 'broker',
                AttributeNaming.RESULT_TYPE.value: 'search',
            }))
        return observations

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
