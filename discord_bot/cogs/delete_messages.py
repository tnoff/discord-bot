from asyncio import sleep
from datetime import datetime, timedelta, timezone

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
from discord_bot.utils.otel import async_otel_span_wrapper, MetricNaming, AttributeNaming, METER_PROVIDER, create_observable_gauge

# Default for deleting messages after X days
DELETE_AFTER_DEFAULT = 7

# Default for how to wait between each loop
LOOP_SLEEP_INTERVAL_DEFAULT = 300

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
    def __init__(self, bot: Bot, settings: dict, _db_engine: Engine, redis_manager=None):
        if not settings.get('general', {}).get('include', {}).get('delete_messages', False):
            raise CogMissingRequiredArg('Delete messages not enabled')

        super().__init__(bot, settings, None, settings_prefix='delete_messages', config_model=DeleteMessagesConfig,
                         redis_manager=redis_manager)
        self.loop_sleep_interval = self.config.loop_sleep_interval
        self.discord_channels = [channel.model_dump() for channel in self.config.discord_channels]
        self._task = None
        self._result_task = None

        create_observable_gauge(METER_PROVIDER, MetricNaming.HEARTBEAT.value, self.__loop_active_callback, 'Delete message loop heartbeat')

    def __loop_active_callback(self, _options):
        '''
        Loop active callback check
        '''
        value = 1 if (self._task and not self._task.done()) else 0
        return [
            Observation(value, attributes={
                AttributeNaming.BACKGROUND_JOB.value: 'delete_message_check'
            })
        ]

    async def cog_load(self):
        '''Start producer and consumer tasks.'''
        self.register_result_queue()
        self._task = self.bot.loop.create_task(return_loop_runner(self._delete_request_loop, self.bot, self.logger, continue_exceptions=DiscordServerError)())
        self._result_task = self.bot.loop.create_task(self._delete_result_loop())

    async def cog_unload(self):
        '''Cancel all running tasks.'''
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
        '''Process a single channel history result, deleting old messages.'''
        if result.error:
            self.logger.error(
                f'DeleteMessages :: Failed to fetch history for channel {result.channel_id}: {result.error}'
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
        while True:
            result = await self._result_queue.get()
            if isinstance(result, ChannelHistoryResult):
                await self._process_delete_result(result)
