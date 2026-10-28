'''
The two loops that carry a music request from "searched" to "queued for the player" (#1030).

A search pod resolution is popped from the broker and routed into the download pipeline; a finished
download is popped from the broker and handed to the guild's player. Both used to run in the
gateway. They need no Discord connection, only the clients this pod already holds and the player
reached through ``BrokerPlayerGateway``, so they run here and the gateway only plays what the broker
has queued.

Both pops are destructive: a result popped and then lost is gone. So shutdown lets an iteration in
flight finish (``stop`` waits ``grace`` seconds for the loops to see the stop event) and only then
cancels what is left.
'''
from asyncio import Event, Task, create_task, sleep, wait
from functools import partial
from logging import Logger

from discord_core.exceptions import ExitEarlyException
from discord_core.interfaces.broker_client_protocol import BrokerClient
from discord_core.utils.common import is_peer_unavailable, return_loop_runner
from discord_core.utils.loop_health import LOOP_HEALTH
from discord_core.utils.otel import (METER_PROVIDER, MetricNaming, create_observable_gauge,
                                     loop_heartbeat_observations)

from discord_commands.loop_logic.music_results import MusicResultsLoop

LOOP_PROCESS_DOWNLOAD_RESULTS = 'process_download_results'
LOOP_PROCESS_SEARCH_RESULTS = 'process_search_results'

# Idle wait between polls when the broker has nothing, so an empty queue is not a busy spin
# against a remote GET /results/next.
BROKER_POLL_INTERVAL_SECONDS = 1.0
SHUTDOWN_GRACE_SECONDS = 5.0


class _StopEventBot:
    '''What ``return_loop_runner`` asks of a bot, for a pod that has none: ready now, closed on stop.'''

    def __init__(self, stop_event: Event):
        self.stop_event = stop_event

    async def wait_until_ready(self) -> None:
        '''There is no cache to wait for.'''

    def is_closed(self) -> bool:
        '''True once the pod has been asked to stop.'''
        return self.stop_event.is_set()


class ResultLoops:
    '''Runs the search-result and download-result loops for the pod.'''

    def __init__(self, broker_client: BrokerClient, results: MusicResultsLoop, logger: Logger,
                 poll_interval: float = BROKER_POLL_INTERVAL_SECONDS,
                 grace: float = SHUTDOWN_GRACE_SECONDS):
        self.broker_client = broker_client
        self.results = results
        self.logger = logger
        self.poll_interval = poll_interval
        self.grace = grace
        self.stop_event: Event | None = None
        self.tasks: list[Task] = []

    async def process_search_results(self) -> None:
        '''One iteration: pop one resolved search and route it into the download pipeline.'''
        if self.stop_event.is_set():
            raise ExitEarlyException('Shutdown called, exiting early')
        resolution = await self.broker_client.next_search_result()
        if resolution is None:
            await sleep(self.poll_interval)
            return
        await self.results.process_search_result(resolution)

    async def process_download_results(self) -> None:
        '''One iteration: pop one finished download and hand it to the player or the playlist add.'''
        if self.stop_event.is_set():
            raise ExitEarlyException('Shutdown called, exiting early')
        result = await self.broker_client.next_result()
        if result is None:
            await sleep(self.poll_interval)
            return
        await self.results.process_download_result(result)

    def start(self, stop_event: Event) -> None:
        '''Register the loops' health and heartbeat and start them. Needs a running event loop.'''
        self.stop_event = stop_event
        bot = _StopEventBot(stop_event)
        for name, function, description in (
                (LOOP_PROCESS_DOWNLOAD_RESULTS, self.process_download_results,
                 'Download result processing loop heartbeat'),
                (LOOP_PROCESS_SEARCH_RESULTS, self.process_search_results,
                 'Search result processing loop heartbeat')):
            health = LOOP_HEALTH.register(name)
            create_observable_gauge(METER_PROVIDER, MetricNaming.HEARTBEAT.value,
                                    partial(loop_heartbeat_observations, name), description)
            self.tasks.append(create_task(return_loop_runner(
                function, bot, self.logger, health=health, peer_unavailable=is_peer_unavailable)()))

    async def stop(self) -> None:
        '''Let the loops finish the iteration they are in, then cancel any that did not.'''
        if not self.tasks:
            return
        _, pending = await wait(self.tasks, timeout=self.grace)
        for task in pending:
            task.cancel()
        if pending:
            await wait(pending)
        # A deliberate stop is not a wedge: do not let a draining pod fail its own liveness probe.
        LOOP_HEALTH.mark_stopped(LOOP_PROCESS_DOWNLOAD_RESULTS, LOOP_PROCESS_SEARCH_RESULTS)
        self.tasks = []
