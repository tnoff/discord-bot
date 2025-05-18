from uuid import uuid4
from typing import Callable, List, Literal

from discord import Message

from discord_bot.cogs.music_helpers.common import SearchType
from discord_bot.utils.otel import MusicSourceDictNaming


class SourceDict():
    '''
    Original source of play request
    '''
    def __init__(self, guild_id: int, requester_name: str, requester_id: int, search_string: str,
                 search_type: Literal[SearchType.SPOTIFY, SearchType.DIRECT, SearchType.SEARCH],
                 added_from_history: bool = False,
                 download_file: bool = True,
                 video_non_exist_callback_functions: List[Callable] = None,
                 post_download_callback_functions: List[Callable] = None):
        '''
        Generate new source dict options

        guild_id : Guild where video was requested
        requester_name: Display name of original requester
        requester_id : User id of original requester
        search_string : Search string of original request
        search_type : Type of search it was
        added_from_history : Whether or not this was added from history
        download_file : Download file eventually
        video_non_exist_callback_functions: Call these functions if video not found or not available
        post_download_callback_functions : Call these functions after video downloads
        '''
        self.guild_id = guild_id
        self.requester_name =  requester_name
        self.requester_id = requester_id
        # Keep original search string for later
        self.original_search_string = search_string
        self.search_string = search_string
        self.search_type = search_type
        self.uuid = uuid4()
        # Optional values
        self.added_from_history = added_from_history
        self.download_file = download_file
        self.video_non_exist_callback_functions = video_non_exist_callback_functions or []
        self.post_download_callback_functions = post_download_callback_functions or []
        # Set message for later
        self.message = None

    def add_youtube_result(self, video_url: str) -> bool:
        '''
        Add result from cache or youtube music
        '''
        self.search_string = video_url
        return True

    def set_message(self, message: Message):
        '''
        Set message that was sent to channel when video was requested

        message : Message object
        '''
        self.message = message

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

    def __str__(self):
        '''
        Expose as string
        Fix embed issues
        https://support.discord.com/hc/en-us/articles/206342858--How-do-I-disable-auto-embed
        '''
        if 'https://' in self.original_search_string:
            return f'<{self.original_search_string}>'
        return self.original_search_string


def source_dict_attributes(source_dict: SourceDict) -> dict:
    '''
    Return source dict attributes for spans
    '''
    return {
        MusicSourceDictNaming.SEARCH_STRING.value: source_dict.search_string,
        MusicSourceDictNaming.REQUESTER.value: source_dict.requester_id,
        MusicSourceDictNaming.GUILD.value: source_dict.guild_id,
        MusicSourceDictNaming.SEARCH_TYPE.value: source_dict.search_type.value,
        MusicSourceDictNaming.UUID.value: str(source_dict.uuid),
    }
