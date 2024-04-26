from asyncio import sleep
from datetime import datetime, timedelta

from pytz import UTC

from discord_bot.cogs.common import CogHelper
from discord_bot.exceptions import CogMissingRequiredArg
from discord_bot.utils import retry_discord_message_command, async_retry_discord_message_command

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
                    'server_id': {
                        'type': 'integer',
                    },
                    'channel_id': {
                        'type': 'integer'
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
    def __init__(self, bot, logger, settings, db_engine):
        super().__init__(bot, logger, settings, None, enable_loop=True, settings_prefix='delete_messages', section_schema=DELETE_MESSAGES_SCHEMA)
        if not self.settings.get('include', {}).get('delete_messages', False):
            raise CogMissingRequiredArg('Delete messages cog not enabled')

        self.loop_sleep_interval = settings.get('delete_messages', {}).get('loop_sleep_interval', LOOP_SLEEP_INTERVAL_DEFAULT)
        self.discord_channels = settings.get('delete_messages', {}).get('discord_channels', [])
        self._task = None

    async def __main_loop(self):
        '''
        Main loop runner
        '''
        for channel_dict in self.discord_channels:
            await sleep(.01)
            self.logger.debug(f'Delete Messages :: Checking Channel ID {channel_dict["channel_id"]}')
            channel = await async_retry_discord_message_command(self.bot.fetch_channel, channel_dict["channel_id"])

            delete_after = channel_dict.get('delete_after', DELETE_AFTER_DEFAULT)
            cutoff_period = (datetime.utcnow() - timedelta(days=delete_after)).replace(tzinfo=UTC)
            messages = [m async for m in retry_discord_message_command(channel.history, limit=128, oldest_first=True)]
            for message in messages:
                if message.created_at < cutoff_period:
                    self.logger.info(f'Deleting message id {message.id}, in channel {channel.id}, in server {channel_dict["server_id"]}')
                    await async_retry_discord_message_command(message.delete)
        await sleep(self.loop_sleep_interval)
