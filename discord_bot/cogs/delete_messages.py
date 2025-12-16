from asyncio import sleep
from datetime import datetime, timedelta, timezone
from functools import partial

from discord.ext.commands import Bot
from discord.errors import DiscordServerError
from opentelemetry.trace import SpanKind
from opentelemetry.metrics import Observation
from pydantic import BaseModel
from sqlalchemy.engine.base import Engine

from discord_bot.cogs.common import CogHelper
from discord_bot.exceptions import CogMissingRequiredArg
from discord_bot.utils.common import async_retry_discord_message_command, return_loop_runner
from discord_bot.utils.common import create_observable_gauge
from discord_bot.utils.otel import otel_span_wrapper, MetricNaming, AttributeNaming, METER_PROVIDER

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
        async def fetch_messages(channel):
            return [m async for m in channel.history(limit=100, oldest_first=True)]
        # Set heartbeat metric
        await sleep(self.loop_sleep_interval)
        with otel_span_wrapper('delete_messages.check'):
            for channel_dict in self.discord_channels:
                with otel_span_wrapper('delete_messages.channel_check', kind=SpanKind.CONSUMER, attributes={'discord.channel': channel_dict['channel_id']}):
                    self.logger.debug(f'Checking Channel ID {channel_dict["channel_id"]}')
                    channel = await async_retry_discord_message_command(partial(self.bot.fetch_channel, channel_dict["channel_id"]))

                    delete_after = channel_dict.get('delete_after', DELETE_AFTER_DEFAULT)
                    cutoff_period = (datetime.now(timezone.utc) - timedelta(days=delete_after))
                    messages = await async_retry_discord_message_command(partial(fetch_messages, channel))
                    for message in messages:
                        if message.created_at < cutoff_period:
                            self.logger.info(f'Deleting message id {message.id}, in channel {channel.id}, in server {channel_dict["server_id"]}')
                            await async_retry_discord_message_command(partial(message.delete))
