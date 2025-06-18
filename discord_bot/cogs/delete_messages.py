from asyncio import sleep
from datetime import datetime, timedelta, timezone

from discord.ext.commands import Bot
from discord.errors import DiscordServerError
from opentelemetry.trace import SpanKind
from opentelemetry.metrics import get_meter_provider
from sqlalchemy.engine.base import Engine

from discord_bot.cogs.common import CogHelper
from discord_bot.exceptions import CogMissingRequiredArg
from discord_bot.cogs.schema import SERVER_ID
from discord_bot.utils.common import retry_discord_message_command, async_retry_discord_message_command, return_loop_runner
from discord_bot.utils.otel import otel_span_wrapper, MetricNaming, AttributeNaming

# Default for deleting messages after X days
DELETE_AFTER_DEFAULT = 7

# Default for how to wait between each loop
LOOP_SLEEP_INTERVAL_DEFAULT = 300

DELETE_MESSAGES_SCHEMA  = {
    'type': 'object',
    'properties': {
        'loop_sleep_interval': {
            'type': 'number',
        },
        'discord_channels': {
            'type': 'array',
            'items': {
                'type': 'object',
                'properties': {
                    'server_id': SERVER_ID,
                    'channel_id': {
                        'type': 'string'
                    },
                    'delete_after': {
                        'type': 'integer'
                    },
                },
                'required': [
                    'server_id',
                    'channel_id',
                ]
            }
        },
    },
    'required': [
        'discord_channels',
    ],
}

METER_PROVIDER = get_meter_provider().get_meter(__name__, '0.0.1')
HEARTBEAT_COUNTER = METER_PROVIDER.create_counter(MetricNaming.HEARTBEAT.value, unit='number', description='Delete messages heartbeat')

class DeleteMessages(CogHelper):
    '''
    Delete Messages in Channels after X days
    '''
    def __init__(self, bot: Bot, settings: dict, _db_engine: Engine):
        if not settings.get('general', {}).get('include', {}).get('delete_messages', False):
            raise CogMissingRequiredArg('Delete messages not enabled')

        super().__init__(bot, settings, None, settings_prefix='delete_messages', section_schema=DELETE_MESSAGES_SCHEMA)
        self.loop_sleep_interval = self.settings.get('delete_messages', {}).get('loop_sleep_interval', LOOP_SLEEP_INTERVAL_DEFAULT)
        self.discord_channels = self.settings.get('delete_messages', {}).get('discord_channels', [])
        self._task = None
        self.loop_timestamp = None

    async def cog_load(self):
        self._task = self.bot.loop.create_task(return_loop_runner(self.delete_messages_loop, self.bot, self.logger, continue_exceptions=DiscordServerError)())

    async def cog_unload(self):
        if self._task:
            self._task.cancel()

    def update_loop_timestamp(self):
        '''
        Update timestamp for looping
        '''
        self.loop_timestamp = int(datetime.now(timezone.utc).timestamp())

    def check_wait(self):
        '''
        Check if should run, based on loop timestamp
        '''
        if not self.loop_timestamp:
            self.update_loop_timestamp()
            return False
        now = int(datetime.now(timezone.utc).timestamp())
        if (now - self.loop_timestamp) < self.loop_sleep_interval:
            return True
        return False

    async def delete_messages_loop(self):
        '''
        Main loop runner
        '''
        # Set heartbeat metric
        await sleep(1)
        HEARTBEAT_COUNTER.add(1, attributes={
            AttributeNaming.BACKGROUND_JOB.value: 'delete_message_check'
        })
        if self.check_wait():
            return
        with otel_span_wrapper('delete_messages.check'):
            for channel_dict in self.discord_channels:
                with otel_span_wrapper('delete_messages.channel_check', kind=SpanKind.INTERNAL, attributes={'discord.channel': channel_dict['channel_id']}):
                    self.logger.debug(f'Checking Channel ID {channel_dict["channel_id"]}')
                    channel = await async_retry_discord_message_command(self.bot.fetch_channel, channel_dict["channel_id"])

                    delete_after = channel_dict.get('delete_after', DELETE_AFTER_DEFAULT)
                    cutoff_period = (datetime.now(timezone.utc) - timedelta(days=delete_after))
                    messages = [m async for m in retry_discord_message_command(channel.history, limit=128, oldest_first=True)]
                    for message in messages:
                        if message.created_at < cutoff_period:
                            self.logger.info(f'Deleting message id {message.id}, in channel {channel.id}, in server {channel_dict["server_id"]}')
                            await async_retry_discord_message_command(message.delete)
            self.update_loop_timestamp()
