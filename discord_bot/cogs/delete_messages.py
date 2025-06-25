from asyncio import sleep
from datetime import datetime, timedelta, timezone
from functools import partial
from pathlib import Path
from tempfile import NamedTemporaryFile

from discord.ext.commands import Bot
from discord.errors import DiscordServerError
from opentelemetry.trace import SpanKind
from opentelemetry.metrics import get_meter_provider
from opentelemetry.metrics import Observation
from sqlalchemy.engine.base import Engine

from discord_bot.cogs.common import CogHelper
from discord_bot.exceptions import CogMissingRequiredArg
from discord_bot.cogs.schema import SERVER_ID
from discord_bot.utils.common import async_retry_discord_message_command, return_loop_runner
from discord_bot.utils.common import create_observable_gauge
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
        self.loop_checkfile = Path(NamedTemporaryFile(delete=False).name) #pylint:disable=consider-using-with

        create_observable_gauge(METER_PROVIDER, MetricNaming.HEARTBEAT.value, self.__loop_active_callback, 'Delete message loop heartbeat')

    def __loop_active_callback(self, _options):
        '''
        Loop active callback check
        '''
        value = int(self.loop_checkfile.read_text())
        return [
            Observation(value, attributes={
                AttributeNaming.BACKGROUND_JOB.value: 'delete_message_check'
            })
        ]

    async def cog_load(self):
        self._task = self.bot.loop.create_task(return_loop_runner(self.delete_messages_loop, self.bot, self.logger, self.loop_checkfile, continue_exceptions=DiscordServerError)())

    async def cog_unload(self):
        if self._task:
            self._task.cancel()
        if self.loop_checkfile.exists():
            self.loop_checkfile.unlink()

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
