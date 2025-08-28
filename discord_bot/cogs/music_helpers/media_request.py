from typing import Literal
from uuid import uuid4

from discord_bot.cogs.music_helpers.message_context import MessageContext
from discord_bot.cogs.music_helpers.common import SearchType
from discord_bot.utils.otel import MediaRequestNaming


class MediaRequest():
    '''
    Original source of play request
    '''
    def __init__(self, guild_id: int, channel_id: int, requester_name: str, requester_id: int, search_string: str,
                 search_type: Literal[SearchType.SPOTIFY, SearchType.DIRECT, SearchType.SEARCH, SearchType.OTHER],
                 added_from_history: bool = False,
                 download_file: bool = True,
                 message_context: MessageContext = None,
                 add_to_playlist: int = None,
                 history_playlist_item_id: int = None,
                 multi_input_search_string: str = None):
        '''
        Generate new media request options

        guild_id : Guild where video was requested
        channel_id : Channel where video was requested
        requester_name: Display name of original requester
        requester_id : User id of original requester
        search_string : Search string of original request
        multi_input_search_string : Input for playlist type searches
        search_type : Type of search it was
        added_from_history : Whether or not this was added from history
        download_file : Download file eventually
        add_to_playlist : Set to add to playlist after download
        history_playlist_item_id : Delete item from history playlist, pass in database id
        '''
        self.guild_id = guild_id
        self.channel_id = channel_id
        self.requester_name =  requester_name
        self.requester_id = requester_id
        # Keep original search string for later
        # In these cases, original search is what was passed into the search and search string is often youtube url
        # For example original_search_string can be 'foo title foo artist' and search_string can be the direct url after yt music search
        self.original_search_string = search_string
        self.search_string = search_string
        self.search_type = search_type
        # Optional values
        self.added_from_history = added_from_history
        self.download_file = download_file
        self.history_playlist_item_id = history_playlist_item_id
        self.add_to_playlist = add_to_playlist
        self.multi_input_search_string = multi_input_search_string
        # Message Context
        self.message_context = message_context
        self.uuid = uuid4()


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


def media_request_attributes(media_request: MediaRequest) -> dict:
    '''
    Return media request attributes for spans
    '''
    return {
        MediaRequestNaming.SEARCH_STRING.value: media_request.search_string,
        MediaRequestNaming.REQUESTER.value: media_request.requester_id,
        MediaRequestNaming.GUILD.value: media_request.guild_id,
        MediaRequestNaming.SEARCH_TYPE.value: media_request.search_type.value,
        MediaRequestNaming.UUID.value: str(media_request.uuid),
    }
