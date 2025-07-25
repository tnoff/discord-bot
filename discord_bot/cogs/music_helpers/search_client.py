from asyncio import AbstractEventLoop
from functools import partial
from itertools import islice
from re import match
from random import shuffle
from typing import List

from discord import TextChannel
from googleapiclient.errors import HttpError
from opentelemetry.trace import SpanKind
from spotipy.exceptions import SpotifyException, SpotifyOauthError

from discord_bot.cogs.music_helpers.common import SearchType
from discord_bot.cogs.music_helpers.common import FXTWITTER_VIDEO_PREFIX, TWITTER_VIDEO_PREFIX
from discord_bot.cogs.music_helpers.common import YOUTUBE_SHORT_PREFIX, YOUTUBE_VIDEO_PREFIX
from discord_bot.cogs.music_helpers.search_cache_client import SearchCacheClient
from discord_bot.cogs.music_helpers.message_queue import MessageQueue, SourceLifecycleStage
from discord_bot.cogs.music_helpers.source_dict import SourceDict
from discord_bot.utils.clients.spotify import SpotifyClient
from discord_bot.utils.clients.youtube import YoutubeClient
from discord_bot.utils.clients.youtube_music import YoutubeMusicClient
from discord_bot.utils.otel import otel_span_wrapper, MusicSourceDictNaming

SPOTIFY_PLAYLIST_REGEX = r'^https://open.spotify.com/playlist/(?P<playlist_id>([a-zA-Z0-9]+))(?P<extra_query>(\?[a-zA-Z0-9=&_-]+)?)(?P<shuffle>( *shuffle)?)'
SPOTIFY_ALBUM_REGEX = r'^https://open.spotify.com/album/(?P<album_id>([a-zA-Z0-9]+))(?P<extra_query>(\?[a-zA-Z0-9=&_-]+)?)(?P<shuffle>( *shuffle)?)'
SPOTIFY_TRACK_REGEX = r'^https://open.spotify.com/track/(?P<track_id>([a-zA-Z0-9]+))(?P<extra_query>(\?[a-zA-Z0-9=&_-]+)?)'

YOUTUBE_PLAYLIST_REGEX = r'^https://(www.)?youtube.com/playlist\?list=(?P<playlist_id>[a-zA-Z0-9_-]+)(?P<shuffle> *(shuffle)?)'
YOUTUBE_VIDEO_REGEX = r'https://(www.)?youtu(.)?be(.com)?\/(watch\?v=)?(?P<video_id>.{11})'
YOUTUBE_SHORT_REGEX = r'^https:\/\/(www\.)?youtube.com\/shorts\/(?P<video_id>.{11})'

class SearchException(Exception):
    '''
    For issues with Search
    '''
    def __init__(self, message, user_message=None):
        self.message = message
        super().__init__(self.message)
        self.user_message = user_message


class ThirdPartyException(SearchException):
    '''
    Issue with 3rd Party Library
    '''
class InvalidSearchURL(SearchException):
    '''
    Invalid URL to give bot
    '''

OTEL_SPAN_PREFIX = 'music.search_client'

def check_youtube_video(search: str) -> bool:
    '''
    Check if search is a youtube video
    '''
    youtube_short_match = match(YOUTUBE_SHORT_REGEX, search)
    youtube_video_match = match(YOUTUBE_VIDEO_REGEX, search)
    return youtube_short_match or youtube_video_match

class SearchClient():
    '''
    Wraps search functions
    '''
    def __init__(self, message_queue: MessageQueue,
                 spotify_client: SpotifyClient = None, youtube_client: YoutubeClient = None, youtube_music_client: YoutubeMusicClient = None,
                 search_cache_client: SearchCacheClient = None,
                 number_shuffles: int = 5):
        '''
        Init download client

        message_queue : The bots message queue
        spotify_client : Spotify Client
        youtube_client : Youtube Client
        youtube_music_client : Youtube Music Client
        search_cache_client: The bots search cache client
        number_shuffles : Number of shuffles post api calls
        '''
        self.message_queue = message_queue
        self.spotify_client = spotify_client
        self.youtube_client = youtube_client
        self.search_cache_client = search_cache_client
        self.youtube_music_client = youtube_music_client
        self.number_shuffles = number_shuffles

    def __check_spotify_source(self, playlist_id: str = None, album_id: str = None, track_id: str = None):
        '''
        Get search strings from spotify

        playlist_id : Playlist id
        album_id : Album id
        track_id : Track ID
        '''
        assert playlist_id or album_id or track_id, 'Playlist or album id must be passed'

        data = []
        if playlist_id:
            data = self.spotify_client.playlist_get(playlist_id)
        if album_id:
            data = self.spotify_client.album_get(album_id)
        if track_id:
            data = self.spotify_client.track_get(track_id)

        search_strings = []
        for item in data:
            search_string = f'{item["track_name"]} {item["track_artists"]}'
            search_strings.append(search_string)
        return search_strings

    def __check_youtube_source(self, playlist_id: str):
        '''
        Generate youtube sources

        playlist_id : ID of youtube playlist
        '''
        items = []
        for item in self.youtube_client.playlist_get(playlist_id):
            items.append(f'{YOUTUBE_VIDEO_PREFIX}{item}')
        return items

    async def __check_source_types(self, search: str, loop: AbstractEventLoop, text_channel: TextChannel):
        '''
        Create source types

        search : Original search string
        loop: Bot event loop
        '''
        with otel_span_wrapper(f'{OTEL_SPAN_PREFIX}.check_source', kind=SpanKind.CLIENT, attributes={MusicSourceDictNaming.SEARCH_STRING.value: search}):
            spotify_playlist_matcher = match(SPOTIFY_PLAYLIST_REGEX, search)
            spotify_album_matcher = match(SPOTIFY_ALBUM_REGEX, search)
            spotify_track_matcher = match(SPOTIFY_TRACK_REGEX, search)
            youtube_playlist_matcher = match(YOUTUBE_PLAYLIST_REGEX, search)
            youtube_short_match = match(YOUTUBE_SHORT_REGEX, search)
            youtube_video_match = match(YOUTUBE_VIDEO_REGEX, search)

            if spotify_playlist_matcher or spotify_album_matcher or spotify_track_matcher:
                if not self.spotify_client:
                    raise InvalidSearchURL('Missing spotify creds', user_message='Spotify URLs invalid, no spotify credentials available to bot')

                sd = SourceDict(text_channel.guild.id, None, None, search, SearchType.OTHER)
                search_string_message = search.replace(' shuffle', '')
                self.message_queue.iterate_source_lifecycle(sd, SourceLifecycleStage.SEND, text_channel.send, f'Gathering spotify data from url "<{search_string_message}>"')
                spotify_args = {}
                should_shuffle = False
                if spotify_album_matcher:
                    spotify_args['album_id'] = spotify_album_matcher.group('album_id')
                    should_shuffle = spotify_album_matcher.group('shuffle') != ''
                if spotify_playlist_matcher:
                    spotify_args['playlist_id'] = spotify_playlist_matcher.group('playlist_id')
                    should_shuffle = spotify_playlist_matcher.group('shuffle') != ''
                if spotify_track_matcher:
                    spotify_args['track_id'] = spotify_track_matcher.group('track_id')

                to_run = partial(self.__check_spotify_source, **spotify_args)
                try:
                    search_strings = await loop.run_in_executor(None, to_run)
                except SpotifyOauthError as e:
                    self.message_queue.iterate_source_lifecycle(sd, SourceLifecycleStage.DELETE, sd.delete_message, '')
                    message = 'Issue gathering info from spotify, credentials seem invalid'
                    raise ThirdPartyException('Issue fetching spotify info', user_message=message) from e
                except SpotifyException as e:
                    self.message_queue.iterate_source_lifecycle(sd, SourceLifecycleStage.DELETE, sd.delete_message, '')
                    message = 'Issue gathering info from spotify url "{search}"'
                    if e.http_status == 404:
                        message = f'Unable to find url "{search}" via Spotify API\nIf this is an official Spotify playlist, [it might not be available via the api](https://developer.spotify.com/blog/2024-11-27-changes-to-the-web-api)'
                    raise ThirdPartyException('Issue fetching spotify info', user_message=message) from e
                if should_shuffle:
                    for _ in range(self.number_shuffles):
                        shuffle(search_strings)
                return SearchType.SPOTIFY, search_strings, sd

            if youtube_playlist_matcher:
                if not self.youtube_client:
                    raise InvalidSearchURL('Missing youtube creds', user_message='Youtube Playlist URLs invalid, no youtube api credentials given to bot')

                sd = SourceDict(text_channel.guild.id, None, None, search, SearchType.OTHER)
                search_string_message = search.replace(' shuffle', '')
                self.message_queue.iterate_source_lifecycle(sd, SourceLifecycleStage.SEND, text_channel.send, f'Gathering youtube data from url "<{search_string_message}>"')
                should_shuffle = youtube_playlist_matcher.group('shuffle') != ''
                to_run = partial(self.__check_youtube_source, youtube_playlist_matcher.group('playlist_id'))
                try:
                    search_strings = await loop.run_in_executor(None, to_run)
                except HttpError as e:
                    self.message_queue.iterate_source_lifecycle(sd, SourceLifecycleStage.DELETE, sd.delete_message, '')
                    raise ThirdPartyException('Issue fetching youtube info', user_message=f'Issue gathering info from youtube url "{search}"') from e
                if should_shuffle:
                    for _ in range(self.number_shuffles):
                        shuffle(search_strings)
                return SearchType.YOUTUBE, search_strings, sd

            if youtube_short_match:
                return SearchType.YOUTUBE, [f'{YOUTUBE_SHORT_PREFIX}{youtube_short_match.group("video_id")}'], None

            if youtube_video_match:
                return SearchType.YOUTUBE, [f'{YOUTUBE_VIDEO_PREFIX}{youtube_video_match.group("video_id")}'], None

            if FXTWITTER_VIDEO_PREFIX in search:
                return SearchType.DIRECT, [search.replace(FXTWITTER_VIDEO_PREFIX, TWITTER_VIDEO_PREFIX)], None

            # If we have https:// in url, assume its a direct
            if 'https://' in search:
                return SearchType.DIRECT, [search], None

            # Else assume this was a search message to put into youtube music
            return SearchType.SEARCH, [search], None

    def __search_youtube_music(self, search_string: str):
        '''
        Search youtube music

        search_string : Search string to look for
        '''
        return self.youtube_music_client.search(search_string)

    async def __check_youtube_music(self, search_type: SearchType, search_string: str, loop: AbstractEventLoop):
        '''
        Check result in youtube music

        search_type: Original search type
        search_string: New search string
        loop: Loop to run function in
        '''
        print('Search type', search_type, 'search string', search_string, 'loop', loop)
        if search_type in [SearchType.YOUTUBE, SearchType.DIRECT]:
            return None
        to_run = partial(self.__search_youtube_music, search_string)
        return await loop.run_in_executor(None, to_run)

    async def check_source(self, search: str, guild_id: int, requester_name: str, requester_id: str, loop: AbstractEventLoop,
                           max_results: int, text_channel: TextChannel) -> List[SourceDict]:
        '''
        Generate sources from input

        search : Search string
        guild_id : Server/Guild id
        requester_name : Display name of requester
        requester_id : ID of requester
        loop : Bot run loop
        max_results : Max results of items
        text_channel : Text channel to send messages to
        '''
        print('Original loop', loop)
        search_type, search_strings, sent_message = await self.__check_source_types(search, loop, text_channel)
        if max_results:
            search_strings = islice(search_strings, max_results)

        all_entries = []
        for search_string in search_strings:
            entry = SourceDict(guild_id, requester_name, requester_id, search_string, search_type)
            # Check in search cache
            # Else fallback to youtube music check
            if self.search_cache_client:
                result = self.search_cache_client.check_cache(entry)
                if result:
                    entry.add_youtube_result(result)
                    all_entries.append(entry)
                    continue
            if self.youtube_music_client:
                result = await self.__check_youtube_music(entry.search_type, entry.search_string, loop)
                if result:
                    entry.add_youtube_result(f'{YOUTUBE_VIDEO_PREFIX}{result}')
                    all_entries.append(entry)
                    continue
            all_entries.append(entry)
        if sent_message:
            self.message_queue.iterate_source_lifecycle(sent_message, SourceLifecycleStage.DELETE, sent_message.delete_message, '')
        return all_entries
