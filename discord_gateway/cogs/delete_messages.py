from functools import partial

from discord.ext.commands import Bot
from discord.errors import DiscordServerError
from opentelemetry.metrics import Observation
from pydantic import BaseModel

from discord_core.exceptions import CogMissingRequiredArg
from discord_core.utils.common import is_peer_unavailable, return_loop_runner
from discord_core.utils.loop_health import LOOP_HEALTH, health_aware_queue_get
from discord_core.utils.otel import MetricNaming, AttributeNaming, METER_PROVIDER, create_observable_gauge, loop_heartbeat_observations

from discord_core.clients.dispatch_client_base import DispatchClientBase
from discord_core.types.dispatch_result import ChannelHistoryResult

from discord_gateway.cogs.common import CogHelperBase
from discord_gateway.loop_logic.delete_messages import DELETE_AFTER_DEFAULT, DeleteMessagesLoop

# Default for how to wait between each loop
LOOP_SLEEP_INTERVAL_DEFAULT = 300

# Background-loop names: LoopHealth registry keys and heartbeat background_job values
LOOP_DELETE_MESSAGE_CHECK = 'delete_message_check'
LOOP_DELETE_MESSAGE_RESULT = 'delete_message_result'

# Pydantic config models
class DiscordChannelConfig(BaseModel):
    '''Discord channel configuration for message deletion'''
    server_id: int
    channel_id: int
    delete_after: int = DELETE_AFTER_DEFAULT

class DeleteMessagesConfig(BaseModel):
    '''Delete messages cog configuration'''
    loop_sleep_interval: float = LOOP_SLEEP_INTERVAL_DEFAULT
    discord_channels: list[DiscordChannelConfig]

class DeleteMessages(CogHelperBase):
    '''
    Delete Messages in Channels after X days
    '''
    def __init__(self, bot: Bot, settings: dict, dispatcher: DispatchClientBase,
                 _stores: object = None, redis_manager=None):
        if not settings.get('general', {}).get('include', {}).get('delete_messages', False):
            raise CogMissingRequiredArg('Delete messages not enabled')

        super().__init__(bot, settings, dispatcher, None,
                         settings_prefix='delete_messages', config_model=DeleteMessagesConfig,
                         redis_manager=redis_manager)
        self.loop_sleep_interval = self.config.loop_sleep_interval
        self.discord_channels = [channel.model_dump() for channel in self.config.discord_channels]
        self._task = None
        self._result_task = None

        # Heartbeats read LoopHealth (successful iterations), the same bit the
        # health server's probe uses — see utils/loop_health.
        create_observable_gauge(METER_PROVIDER, MetricNaming.HEARTBEAT.value,
                                partial(loop_heartbeat_observations, LOOP_DELETE_MESSAGE_CHECK), 'Delete message loop heartbeat')
        create_observable_gauge(METER_PROVIDER, MetricNaming.HEARTBEAT.value,
                                partial(loop_heartbeat_observations, LOOP_DELETE_MESSAGE_RESULT), 'Delete message result loop heartbeat')
        create_observable_gauge(METER_PROVIDER, MetricNaming.RESULT_QUEUE_DEPTH.value, self.__result_queue_depth_callback, 'Delete message dispatch result queue depth')

    def __result_queue_depth_callback(self, _options):
        '''
        Depth of the dispatch result queue — climbs if the consumer stalls or dies.
        '''
        depth = self._result_queue.qsize() if self._result_queue else 0
        return [
            Observation(depth, attributes={
                AttributeNaming.BACKGROUND_JOB.value: 'delete_message_result',
                AttributeNaming.RESULT_TYPE.value: 'dispatch'
            })
        ]

    async def cog_load(self):
        '''Start producer and consumer tasks.'''
        self.register_result_queue()
        # The producer sleeps loop_sleep_interval (default 300 s) per iteration,
        # so its staleness window is sized from that cadence — see markov.
        self._task = self.bot.loop.create_task(
            return_loop_runner(self._delete_request_loop, self.bot, self.logger, continue_exceptions=DiscordServerError,
                               health=LOOP_HEALTH.register_for_interval(LOOP_DELETE_MESSAGE_CHECK, self.loop_sleep_interval),
                               peer_unavailable=is_peer_unavailable)()
        )
        self._result_task = self.bot.loop.create_task(self._delete_result_loop())

    async def cog_unload(self):
        '''Cancel all running tasks.'''
        # Cancellation is a deliberate stop, not a wedge — see Music.cog_unload.
        LOOP_HEALTH.mark_stopped(LOOP_DELETE_MESSAGE_CHECK, LOOP_DELETE_MESSAGE_RESULT)
        if self._task:
            self._task.cancel()
        if self._result_task:
            self._result_task.cancel()

    @property
    def loop_logic(self) -> DeleteMessagesLoop:
        '''One iteration of each loop, over this cog's dispatcher.'''
        return DeleteMessagesLoop(self.logger, self.discord_channels, self.loop_sleep_interval,
                                  self.dispatch_channel_history, self.dispatch_delete)

    async def _delete_request_loop(self):
        '''
        Producer loop: submit channel history fetch requests for each configured channel.
        '''
        await self.loop_logic.request()

    async def _delete_result_loop(self) -> None:
        '''Consumer loop: read channel history results and delete old messages.'''
        health = LOOP_HEALTH.register(LOOP_DELETE_MESSAGE_RESULT)
        while True:
            result = await health_aware_queue_get(self._result_queue, health)
            try:
                if isinstance(result, ChannelHistoryResult):
                    await self.loop_logic.process_result(result)
                health.record_success()
            except Exception:  # pylint: disable=broad-except
                health.record_error()
                # A single bad result must NOT kill the consumer: the producer keeps
                # filling the queue, so a dead consumer leaks memory unboundedly
                # (docs findings/2026-07-19 OOM root cause). Log and drain the next.
                self.logger.exception('DeleteMessages :: error processing dispatch result')
