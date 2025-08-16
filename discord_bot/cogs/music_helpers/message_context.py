from datetime import datetime, timezone
from uuid import uuid4

from discord import Message

from discord_bot.cogs.music_helpers.common import MessageLifecycleStage

class MessageContext():
    '''
    Keep track of metadata messages that are sent and then later edited or deleted
    '''
    def __init__(self, guild_id: int, channel_id: int):
        self.guild_id = guild_id
        self.channel_id = channel_id
        self.uuid = uuid4()
        self.created_at = datetime.now(timezone.utc)
        self.lifecycle_stage = MessageLifecycleStage.SEND

        # Set after
        self.message_id = None
        self.message = None
        self.message_content = None
        self.delete_after = None
        self.function = None

    def set_message(self, message: Message):
        '''
        Set message that was sent to channel when video was requested

        message : Message object
        '''
        self.message = message
        self.message_id = message.id

    async def delete_message(self, _message_content: str, **_kwargs):
        '''
        Delete message if existing
        '''
        if not self.message:
            return False

        await self.message.delete()
        return True

    async def edit_message(self, content: str, delete_after: int = None):
        '''
        Edit message contents

        content : Message content
        delete_after : Delete after X seconds
        '''
        if not self.message:
            return False
        await self.message.edit(content=content, delete_after=delete_after)
        return True
