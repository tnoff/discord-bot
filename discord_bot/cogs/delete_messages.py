from asyncio import sleep
from logging import RootLogger
from datetime import datetime, timedelta, timezone

from discord.ext.commands import Bot
from sqlalchemy.engine.base import Engine

from discord_bot.cogs.common import CogHelper
from discord_bot.exceptions import CogMissingRequiredArg
from discord_bot.cogs.schema import SERVER_ID
from discord_bot.utils.common import retry_discord_message_command, async_retry_discord_message_command, return_loop_runner


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

class DeleteMessages(CogHelper):
    '''
    Delete Messages in Channels after X days
    '''
    def __init__(self, bot: Bot, logger: RootLogger, settings: dict, _db_engine: Engine):
        if not settings.get('general', {}).get('include', {}).get('delete_messages', False):
            raise CogMissingRequiredArg('Delete messages not enabled')

        super().__init__(bot, logger, settings, None, settings_prefix='delete_messages', section_schema=DELETE_MESSAGES_SCHEMA)
        self.loop_sleep_interval = self.settings.get('delete_messages', {}).get('loop_sleep_interval', LOOP_SLEEP_INTERVAL_DEFAULT)
        self.discord_channels = self.settings.get('delete_messages', {}).get('discord_channels', [])
        self._task = None

    async def cog_load(self):
        self._task = self.bot.loop.create_task(return_loop_runner(self.delete_messages_loop, self.bot, self.logger)())

    async def cog_unload(self):
        if self._task:
            self._task.cancel()

    async def delete_messages_loop(self):
        '''
        Main loop runner
        '''
        for channel_dict in self.discord_channels:
            self.logger.debug(f'Delete Messages :: Checking Channel ID {channel_dict["channel_id"]}')
            channel = await async_retry_discord_message_command(self.bot.fetch_channel, channel_dict["channel_id"])

            delete_after = channel_dict.get('delete_after', DELETE_AFTER_DEFAULT)
            cutoff_period = (datetime.now(timezone.utc) - timedelta(days=delete_after))
            messages = [m async for m in retry_discord_message_command(channel.history, limit=128, oldest_first=True)]
            for message in messages:
                if message.created_at < cutoff_period:
                    self.logger.info(f'Deleting message id {message.id}, in channel {channel.id}, in server {channel_dict["server_id"]}')
                    await async_retry_discord_message_command(message.delete)
        await sleep(self.loop_sleep_interval)
