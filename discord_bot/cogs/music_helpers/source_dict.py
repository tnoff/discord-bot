from typing import Callable, List, Literal

from discord import Message

from discord_bot.cogs.music_helpers.common import SearchType

class SourceDict():
    '''
    Original source of play request
    '''
    def __init__(self, guild_id: str, requester_name: str, requester_id: str, search_string: str,
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
        self.search_string = search_string
        self.search_type = search_type
        # Optional values
        self.added_from_history = added_from_history
        self.download_file = download_file
        self.video_non_exist_callback_functions = video_non_exist_callback_functions or []
        self.post_download_callback_functions = post_download_callback_functions or []
        # Set message for later
        self.message = None

    def set_message(self, message: Message):
        '''
        Set message that was sent to channel when video was requested

        message : Message object
        '''
        self.message = message

    def __str__(self):
        '''
        Expose as string
        Fix embed issues
        https://support.discord.com/hc/en-us/articles/206342858--How-do-I-disable-auto-embed
        '''
        if 'https://' in self.search_string:
            return f'<{self.search_string}>'
        return self.search_string
