'''
The markov and delete-messages loops (#1030).

Both are a producer that asks the dispatcher for Discord data on a timer (channel history, guild
emojis) and a consumer that handles what comes back. They never needed the gateway connection: the
fetches go through the dispatcher, the markov rows through the db pod, and a delete is a dispatcher
request. They used to run in the gateway because the cogs that own the commands did.

A fetch result arrives on a per-cog queue the dispatch client keeps (``register_cog_queue``), the
same one the gateway's cogs used, so the loop logic is unchanged and only its home moved.

Nothing here is destructive: a result lost at shutdown is fetched again on the next sweep, and the
markov writes are applied a message at a time by the store. So ``stop`` cancels at once rather than
waiting for an iteration, unlike the music result loops (workers/result_loops.py).

Which loops run follows ``general.include`` (``markov``, ``delete_messages``) in the shared config,
as it did for the cogs, so a deployment that left a feature off still has it off.
'''
from asyncio import Event, Queue, Task, create_task, wait
from functools import partial
from logging import Logger
from typing import Awaitable, Callable

from opentelemetry.metrics import Observation

from discord_core.clients.dispatch_client_base import DispatchClientBase
from discord_core.types.dispatch_request import (
    DeleteRequest, FetchChannelHistoryRequest, FetchGuildEmojisRequest,
)
from discord_core.types.dispatch_result import ChannelHistoryResult, GuildEmojisResult
from discord_core.utils.common import is_peer_unavailable, return_loop_runner
from discord_core.utils.loop_health import LOOP_HEALTH, health_aware_queue_get
from discord_core.utils.otel import (AttributeNaming, METER_PROVIDER, MetricNaming, capture_span_context,
                                     create_observable_gauge, loop_heartbeat_observations)

from discord_commands.loop_logic.delete_messages import DeleteMessagesLoop
from discord_commands.loop_logic.markov import MarkovLoop
from discord_commands.types.loop_config import DeleteMessagesConfig, MarkovConfig
from discord_commands.workers.result_loops import StopEventBot

# The cog names the dispatch client keys its result queues by, as the gateway's cogs had them.
COG_MARKOV = 'markov'
COG_DELETE_MESSAGES = 'deletemessages'

# LoopHealth registry keys and heartbeat background_job values, unchanged by the move.
LOOP_MARKOV_CHECK = 'markov_check'
LOOP_MARKOV_RESULT = 'markov_result'
LOOP_DELETE_MESSAGE_CHECK = 'delete_message_check'
LOOP_DELETE_MESSAGE_RESULT = 'delete_message_result'


class DispatchLoopPair:
    '''One producer loop and the consumer of the results it asks the dispatcher for.'''

    def __init__(self, dispatcher: DispatchClientBase, logger: Logger, cog_name: str, check_job: str,
                 result_job: str, sleep_interval: float, request: Callable[[], Awaitable[None]],
                 process: Callable[[object], Awaitable[None]], description: str):
        self.dispatcher = dispatcher
        self.logger = logger
        self.cog_name = cog_name
        self.check_job = check_job
        self.result_job = result_job
        self.sleep_interval = sleep_interval
        self.request = request
        self.process = process
        self.description = description
        self.queue: Queue | None = None
        self.tasks: list[Task] = []

    def _queue_depth(self, _options):
        '''Depth of the dispatch result queue: it climbs if the consumer stalls or dies.'''
        return [Observation(self.queue.qsize() if self.queue else 0, attributes={
            AttributeNaming.BACKGROUND_JOB.value: self.result_job,
            AttributeNaming.RESULT_TYPE.value: 'dispatch',
        })]

    async def _result_loop(self, health) -> None:
        while True:
            result = await health_aware_queue_get(self.queue, health)
            try:
                await self.process(result)
                health.record_success()
            except Exception:  # pylint: disable=broad-except
                health.record_error()
                # A single bad result must not kill the consumer: the producer keeps filling the
                # queue, so a dead consumer leaks memory without limit. Log and take the next.
                self.logger.exception(f'{self.description} :: error processing dispatch result')

    def start(self, stop_event: Event) -> None:
        '''Register the queue, health and heartbeats and start both loops. Needs a running event loop.'''
        self.queue = self.dispatcher.register_cog_queue(self.cog_name)
        # The producer sleeps ``sleep_interval`` per iteration, so its staleness window is sized
        # from that cadence rather than the process default, or it would read as stalled between runs.
        check_health = LOOP_HEALTH.register_for_interval(self.check_job, self.sleep_interval)
        result_health = LOOP_HEALTH.register(self.result_job)
        for job, description in ((self.check_job, f'{self.description} loop heartbeat'),
                                 (self.result_job, f'{self.description} result loop heartbeat')):
            create_observable_gauge(METER_PROVIDER, MetricNaming.HEARTBEAT.value,
                                    partial(loop_heartbeat_observations, job), description)
        create_observable_gauge(METER_PROVIDER, MetricNaming.RESULT_QUEUE_DEPTH.value, self._queue_depth,
                                f'{self.description} dispatch result queue depth')
        self.tasks = [
            create_task(return_loop_runner(self.request, StopEventBot(stop_event), self.logger,
                                           continue_exceptions=TimeoutError, health=check_health,
                                           peer_unavailable=is_peer_unavailable)()),
            create_task(self._result_loop(result_health)),
        ]

    async def stop(self) -> None:
        '''Cancel both loops.'''
        for task in self.tasks:
            task.cancel()
        if self.tasks:
            await wait(self.tasks)
        # A deliberate stop is not a wedge: do not let a draining pod fail its own liveness probe.
        LOOP_HEALTH.mark_stopped(self.check_job, self.result_job)
        self.tasks = []


def history_requester(dispatcher: DispatchClientBase, cog_name: str):
    '''The loops' ``request_history``: ask for a channel's history, the answer lands on the cog's queue.'''
    async def request(guild_id: int, channel_id: int, limit: int = 100, after=None,
                      after_message_id: int | None = None, oldest_first: bool = True) -> None:
        await dispatcher.submit_request(FetchChannelHistoryRequest(
            guild_id=guild_id, channel_id=channel_id, limit=limit, after=after,
            after_message_id=after_message_id, oldest_first=oldest_first, cog_name=cog_name,
            span_context=capture_span_context()))
    return request


def emojis_requester(dispatcher: DispatchClientBase, cog_name: str):
    '''The markov loop's ``request_emojis``.'''
    async def request(guild_id: int, max_retries: int = 3) -> None:
        await dispatcher.submit_request(FetchGuildEmojisRequest(
            guild_id=guild_id, cog_name=cog_name, max_retries=max_retries,
            span_context=capture_span_context()))
    return request


def delete_requester(dispatcher: DispatchClientBase):
    '''The delete-messages loop's ``delete_message``.'''
    async def delete(guild_id: int, channel_id: int, message_id: int) -> None:
        await dispatcher.submit_request(DeleteRequest(
            guild_id=guild_id, channel_id=channel_id, message_id=message_id,
            span_context=capture_span_context()))
    return delete


def build_dispatch_loops(settings: dict, dispatcher: DispatchClientBase, markov_store,
                         logger: Logger) -> list[DispatchLoopPair]:
    '''The loops the shared config turns on (``general.include``), each validated against its own section.'''
    include = settings.get('general', {}).get('include', {})
    loops = []
    if include.get('markov', False):
        config = MarkovConfig.model_validate(settings.get('markov', {}))
        markov = MarkovLoop(logger, markov_store, config.loop_sleep_interval, config.message_check_limit,
                            config.history_retention_days, {}, emojis_requester(dispatcher, COG_MARKOV),
                            history_requester(dispatcher, COG_MARKOV))

        async def process_markov(result) -> None:
            if isinstance(result, GuildEmojisResult):
                await markov.process_emojis_result(result)
            elif isinstance(result, ChannelHistoryResult):
                await markov.process_history_result(result)

        loops.append(DispatchLoopPair(dispatcher, logger, COG_MARKOV, LOOP_MARKOV_CHECK, LOOP_MARKOV_RESULT,
                                      config.loop_sleep_interval, markov.request, process_markov, 'Markov'))
    if include.get('delete_messages', False):
        config = DeleteMessagesConfig.model_validate(settings.get('delete_messages', {}))
        delete_messages = DeleteMessagesLoop(
            logger, [channel.model_dump() for channel in config.discord_channels], config.loop_sleep_interval,
            history_requester(dispatcher, COG_DELETE_MESSAGES), delete_requester(dispatcher))

        async def process_delete(result) -> None:
            if isinstance(result, ChannelHistoryResult):
                await delete_messages.process_result(result)

        loops.append(DispatchLoopPair(
            dispatcher, logger, COG_DELETE_MESSAGES, LOOP_DELETE_MESSAGE_CHECK, LOOP_DELETE_MESSAGE_RESULT,
            config.loop_sleep_interval, delete_messages.request, process_delete, 'Delete message'))
    return loops
