from asyncio import sleep
from datetime import datetime, timedelta, timezone

from discord.ext.commands import Bot
from discord.errors import DiscordServerError
from opentelemetry.trace import SpanKind
from opentelemetry.metrics import Observation
from pydantic import BaseModel
from sqlalchemy.engine.base import Engine

from discord_bot.cogs.common import CogHelper
from discord_bot.exceptions import CogMissingRequiredArg
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
    def __init__(self, bot: Bot, settings: dict, _db_engine: Engine):
        if not settings.get('general', {}).get('include', {}).get('delete_messages', False):
            raise CogMissingRequiredArg('Delete messages not enabled')

        super().__init__(bot, settings, None, settings_prefix='delete_messages', config_model=DeleteMessagesConfig)
        self.loop_sleep_interval = self.config.loop_sleep_interval
        self.discord_channels = [channel.model_dump() for channel in self.config.discord_channels]
        self._task = None

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
        self._task = self.bot.loop.create_task(return_loop_runner(self.delete_messages_loop, self.bot, self.logger, continue_exceptions=DiscordServerError)())

    async def cog_unload(self):
        if self._task:
            self._task.cancel()

    async def delete_messages_loop(self):
        '''
        Main loop runner
        '''
        # Set heartbeat metric
        await sleep(self.loop_sleep_interval)
        async with async_otel_span_wrapper('delete_messages.check'):
            for channel_dict in self.discord_channels:
                guild_id = channel_dict['server_id']
                channel_id = channel_dict['channel_id']
                async with async_otel_span_wrapper('delete_messages.channel_check', kind=SpanKind.CONSUMER, attributes={'discord.channel': channel_id}):
                    self.logger.debug(f'Checking Channel ID {channel_id}')
                    delete_after = channel_dict.get('delete_after', DELETE_AFTER_DEFAULT)
                    cutoff_period = (datetime.now(timezone.utc) - timedelta(days=delete_after))
                    messages = await self.dispatch_channel_history(guild_id, channel_id)
                    for message in messages:
                        if message.created_at < cutoff_period:
                            self.logger.info(f'Deleting message id {message.id}, in channel {channel_id}, in server {guild_id}')
                            await self.dispatch_delete(guild_id, channel_id, message.id)
