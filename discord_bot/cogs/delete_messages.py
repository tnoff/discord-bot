from asyncio import sleep
from datetime import datetime, timedelta, timezone
from functools import partial

from discord.ext.commands import Bot
from discord.errors import DiscordServerError
from opentelemetry.trace import SpanKind
from opentelemetry.metrics import Observation
from pydantic import BaseModel
from sqlalchemy.engine.base import Engine

from discord_bot.cogs.cog_helper import CogHelper
from discord_bot.exceptions import CogMissingRequiredArg
from discord_bot.types.dispatch_result import ChannelHistoryResult
from discord_bot.utils.common import return_loop_runner
from discord_bot.utils.loop_health import LOOP_HEALTH, health_aware_queue_get
from discord_bot.utils.otel import async_otel_span_wrapper, DiscordContextNaming, MetricNaming, AttributeNaming, METER_PROVIDER, create_observable_gauge, loop_heartbeat_observations, span_links_from_context
from discord_bot.clients.dispatch_client_base import DispatchClientBase

# Default for deleting messages after X days
DELETE_AFTER_DEFAULT = 7

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

class DeleteMessages(CogHelper):
    '''
    Delete Messages in Channels after X days
    '''
    def __init__(self, bot: Bot, settings: dict, dispatcher: DispatchClientBase,
                 _db_engine: Engine = None, redis_manager=None):
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
        create_observable_gauge(METER_PROVIDER, MetricNaming.DISPATCH_RESULT_QUEUE_DEPTH.value, self.__result_queue_depth_callback, 'Delete message dispatch result queue depth')

    def __result_queue_depth_callback(self, _options):
        '''
        Depth of the dispatch result queue — climbs if the consumer stalls or dies.
        '''
        depth = self._result_queue.qsize() if self._result_queue else 0
        return [
            Observation(depth, attributes={
                AttributeNaming.BACKGROUND_JOB.value: 'delete_message_result'
            })
        ]

    async def cog_load(self):
        '''Start producer and consumer tasks.'''
        self.register_result_queue()
        # The producer sleeps loop_sleep_interval (default 300 s) per iteration,
        # so its staleness window is sized from that cadence — see markov.
        self._task = self.bot.loop.create_task(
            return_loop_runner(self._delete_request_loop, self.bot, self.logger, continue_exceptions=DiscordServerError,
                               health=LOOP_HEALTH.register_for_interval(LOOP_DELETE_MESSAGE_CHECK, self.loop_sleep_interval))()
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

    def _get_channel_config(self, channel_id: int) -> dict:
        '''Return config dict for the given channel_id, or empty dict if not found.'''
        for channel in self.discord_channels:
            if channel['channel_id'] == channel_id:
                return channel
        return {}

    async def _delete_request_loop(self):
        '''
        Producer loop: submit channel history fetch requests for each configured channel.
        '''
        await sleep(self.loop_sleep_interval)
        async with async_otel_span_wrapper('delete_messages.check'):
            for channel_dict in self.discord_channels:
                guild_id = channel_dict['server_id']
                channel_id = channel_dict['channel_id']
                async with async_otel_span_wrapper('delete_messages.channel_check', kind=SpanKind.CONSUMER, attributes={'discord.channel': channel_id}):
                    self.logger.debug(f'Checking Channel ID {channel_id}')
                    await self.dispatch_channel_history(guild_id, channel_id)

    async def _process_delete_result(self, result: ChannelHistoryResult) -> None:
        '''
        Process a single channel history result, deleting old messages.

        Opens its own span linked back to the requesting one: this runs in the
        result-consumer task, by which point the span that dispatched the fetch
        has long closed and these logs would otherwise carry no trace.
        '''
        async with async_otel_span_wrapper('delete_messages.history_result', kind=SpanKind.CONSUMER,
                                           attributes={DiscordContextNaming.CHANNEL.value: result.channel_id,
                                                       DiscordContextNaming.GUILD.value: result.guild_id},
                                           links=span_links_from_context(result.span_context)):
            return await self._apply_delete_result(result)

    async def _apply_delete_result(self, result: ChannelHistoryResult) -> None:
        '''Body of _process_delete_result, run inside the consumer span.'''
        if result.error:
            self.logger.error(
                f'DeleteMessages :: Failed to fetch history for channel {result.channel_id} '
                f'in server {result.guild_id}: {result.error}'
            )
            return
        channel_config = self._get_channel_config(result.channel_id)
        delete_after = channel_config.get('delete_after', DELETE_AFTER_DEFAULT)
        cutoff_period = (datetime.now(timezone.utc) - timedelta(days=delete_after))
        for message in result.messages:
            if message.created_at < cutoff_period:
                self.logger.info(
                    f'Deleting message id {message.id}, in channel {result.channel_id}, '
                    f'in server {result.guild_id}'
                )
                await self.dispatch_delete(result.guild_id, result.channel_id, message.id)

    async def _delete_result_loop(self) -> None:
        '''Consumer loop: read channel history results and delete old messages.'''
        health = LOOP_HEALTH.register(LOOP_DELETE_MESSAGE_RESULT)
        while True:
            result = await health_aware_queue_get(self._result_queue, health)
            try:
                if isinstance(result, ChannelHistoryResult):
                    await self._process_delete_result(result)
                health.record_success()
            except Exception:  # pylint: disable=broad-except
                health.record_error()
                # A single bad result must NOT kill the consumer: the producer keeps
                # filling the queue, so a dead consumer leaks memory unboundedly
                # (docs findings/2026-07-19 OOM root cause). Log and drain the next.
                self.logger.exception('DeleteMessages :: error processing dispatch result')
