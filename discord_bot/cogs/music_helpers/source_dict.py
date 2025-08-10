from typing import Callable, List, Literal

from discord_bot.cogs.music_helpers.message_context import MessageContext
from discord_bot.cogs.music_helpers.common import SearchType
from discord_bot.utils.otel import MusicSourceDictNaming

class SourceDict(MessageContext):
    '''
    Original source of play request
    '''
    def __init__(self, guild_id: int, channel_id: int, requester_name: str, requester_id: int, search_string: str,
                 search_type: Literal[SearchType.SPOTIFY, SearchType.DIRECT, SearchType.SEARCH, SearchType.OTHER],
                 added_from_history: bool = False,
                 download_file: bool = True,
                 video_non_exist_callback_functions: List[Callable] = None,
                 post_download_callback_functions: List[Callable] = None):
        '''
        Generate new source dict options

        guild_id : Guild where video was requested
        channel_id : Channel where video was requested
        requester_name: Display name of original requester
        requester_id : User id of original requester
        search_string : Search string of original request
        search_type : Type of search it was
        added_from_history : Whether or not this was added from history
        download_file : Download file eventually
        video_non_exist_callback_functions: Call these functions if video not found or not available
        post_download_callback_functions : Call these functions after video downloads
        '''
        super().__init__(guild_id, channel_id)
        self.requester_name =  requester_name
        self.requester_id = requester_id
        # Keep original search string for later
        self.original_search_string = search_string
        self.search_string = search_string
        self.search_type = search_type
        # Optional values
        self.added_from_history = added_from_history
        self.download_file = download_file
        self.video_non_exist_callback_functions = video_non_exist_callback_functions or []
        self.post_download_callback_functions = post_download_callback_functions or []

    def add_youtube_result(self, video_url: str) -> bool:
        '''
        Add result from cache or youtube music
        '''
        self.search_string = video_url
        return True

    def __str__(self):
        '''
        Expose as string
        Fix embed issues
        https://support.discord.com/hc/en-us/articles/206342858--How-do-I-disable-auto-embed
        '''
        return_string = self.original_search_string or self.search_string
        if 'https://' in return_string:
            return f'<{return_string}>'
        return return_string


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
