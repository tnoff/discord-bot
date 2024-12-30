from asyncio import AbstractEventLoop
from functools import partial
from re import match
from pathlib import Path
from random import shuffle
from typing import List

from googleapiclient.errors import HttpError
from spotipy.exceptions import SpotifyException
from yt_dlp import YoutubeDL
from yt_dlp.utils import DownloadError

from discord_bot.database import VideoCache

from discord_bot.cogs.music_helpers.common import SearchType
from discord_bot.cogs.music_helpers.common import FXTWITTER_VIDEO_PREFIX, TWITTER_VIDEO_PREFIX
from discord_bot.cogs.music_helpers.common import YOUTUBE_SHORT_PREFIX, YOUTUBE_VIDEO_PREFIX
from discord_bot.cogs.music_helpers.source_dict import SourceDict
from discord_bot.cogs.music_helpers.source_download import SourceDownload
from discord_bot.utils.clients.spotify import SpotifyClient
from discord_bot.utils.clients.youtube import YoutubeClient

SPOTIFY_PLAYLIST_REGEX = r'^https://open.spotify.com/playlist/(?P<playlist_id>([a-zA-Z0-9]+))(?P<extra_query>(\?[a-zA-Z0-9=&_-]+)?)(?P<shuffle>( *shuffle)?)'
SPOTIFY_ALBUM_REGEX = r'^https://open.spotify.com/album/(?P<album_id>([a-zA-Z0-9]+))(?P<extra_query>(\?[a-zA-Z0-9=&_-]+)?)(?P<shuffle>( *shuffle)?)'
SPOTIFY_TRACK_REGEX = r'^https://open.spotify.com/track/(?P<track_id>([a-zA-Z0-9]+))(?P<extra_query>(\?[a-zA-Z0-9=&_-]+)?)'

YOUTUBE_PLAYLIST_REGEX = r'^https://(www.)?youtube.com/playlist\?list=(?P<playlist_id>[a-zA-Z0-9_-]+)(?P<shuffle> *(shuffle)?)'
YOUTUBE_VIDEO_REGEX = r'https://(www.)?youtu(.)?be(.com)?\/(watch\?v=)?(?P<video_id>.{11})'
YOUTUBE_SHORT_REGEX = r'^https:\/\/(www\.)?youtube.com\/shorts\/(?P<video_id>.{11})'

class DownloadClientException(Exception):
    '''
    Generic class for download client errors
    '''
    def __init__(self, message, user_message=None):
        self.message = message
        super().__init__(self.message)
        self.user_message = user_message

class InvalidSearchURL(DownloadClientException):
    '''
    Invalid URL to give bot
    '''

class ThirdPartyException(DownloadClientException):
    '''
    Issue with 3rd Party Library
    '''

class VideoAgeRestrictedException(DownloadClientException):
    '''
    Video has age restrictions, cannot download
    '''

class VideoUnavailableException(DownloadClientException):
    '''
    Video Unavailable while downloading
    '''

class PrivateVideoException(DownloadClientException):
    '''
    Private Video while downloading
    '''

class ExistingFileException(Exception):
    '''
    Throw when existing file found
    '''
    def __init__(self, message, video_cache: VideoCache = None):
        self.message = message
        super().__init__(message)
        self.video_cache = video_cache


class DownloadClient():
    '''
    Download Client using yt-dlp
    '''
    def __init__(self, ytdl: YoutubeDL,
                 spotify_client: SpotifyClient = None, youtube_client: YoutubeClient = None,
                 number_shuffles: int = 5):
        '''
        Init download client

        ytdl : YoutubeDL Client
        spotify_client : Spotify Client
        youtube_client : Youtube Client
        number_shuffles : Number of shuffles post api calls
        '''
        self.ytdl = ytdl
        self.spotify_client = spotify_client
        self.youtube_client = youtube_client
        self.number_shuffles = number_shuffles

    def __prepare_data_source(self, source_dict: SourceDict):
        '''
        Prepare source from youtube url
        '''
        try:
            data = self.ytdl.extract_info(source_dict.search_string, download=source_dict.download_file)
        except DownloadError as error:
            if 'Private video' in str(error):
                raise PrivateVideoException('Video is private', user_message=f'Video from search "{str(source_dict)}" is unvailable, cannot download') from error
            if 'Video unavailable' in str(error):
                raise VideoUnavailableException('Video is unavailable', user_message=f'Video from search "{str(source_dict)}" is unavailable, cannot download') from error
            if 'Sign in to confirm your age. This video may be inappropriate for some users' in str(error):
                raise VideoAgeRestrictedException('Video Aged restricted', user_message=f'Video from search "{str(source_dict)}" is age restricted, cannot download') from error
            raise
        # Make sure we get the first source_dict here
        # Since we don't pass "url" directly anymore
        try:
            data = data['entries'][0]
        # Key Error if a single video is passed
        except KeyError:
            pass

        file_path = None
        if source_dict.download_file:
            try:
                file_path = Path(data['requested_downloads'][0]['filepath'])
                if not file_path.exists():
                    file_path = None
            except (KeyError, IndexError):
                file_path = None
        return SourceDownload(file_path, data, source_dict)

    async def create_source(self, source_dict: SourceDict, loop):
        '''
        Download data from youtube search
        '''
        to_run = partial(self.__prepare_data_source, source_dict=source_dict)
        return await loop.run_in_executor(None, to_run)

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
        return self.youtube_client.playlist_get(playlist_id)

    async def __check_source_types(self, search: str, loop: AbstractEventLoop):
        '''
        Create source types

        search : Original search string
        loop: Bot event loop
        '''
        spotify_playlist_matcher = match(SPOTIFY_PLAYLIST_REGEX, search)
        spotify_album_matcher = match(SPOTIFY_ALBUM_REGEX, search)
        spotify_track_matcher = match(SPOTIFY_TRACK_REGEX, search)
        youtube_playlist_matcher = match(YOUTUBE_PLAYLIST_REGEX, search)
        youtube_short_match = match(YOUTUBE_SHORT_REGEX, search)
        youtube_video_match = match(YOUTUBE_VIDEO_REGEX, search)

        if spotify_playlist_matcher or spotify_album_matcher or spotify_track_matcher:
            if not self.spotify_client:
                raise InvalidSearchURL('Missing spotify creds', user_message='Spotify URLs invalid, no spotify credentials available to bot')

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
            except SpotifyException as e:
                raise ThirdPartyException('Issue fetching spotify info', user_message=f'Issue gathering info from spotify url "{search}"') from e
            if should_shuffle:
                for _ in range(self.number_shuffles):
                    shuffle(search_strings)
            return SearchType.SPOTIFY, search_strings

        if youtube_playlist_matcher:
            if not self.youtube_client:
                raise InvalidSearchURL('Missing youtube creds', user_message='Youtube Playlist URLs invalid, no youtube api credentials given to bot')

            should_shuffle = youtube_playlist_matcher.group('shuffle') != ''
            to_run = partial(self.__check_youtube_source, youtube_playlist_matcher.group('playlist_id'))
            try:
                search_strings = await loop.run_in_executor(None, to_run)
            except HttpError as e:
                raise ThirdPartyException('Issue fetching youtube info', user_message=f'Issue gathering info from youtube url "{search}"') from e
            if should_shuffle:
                for _ in range(self.number_shuffles):
                    shuffle(search_strings)
            return SearchType.DIRECT, search_strings

        if youtube_short_match:
            return SearchType.DIRECT, [f'{YOUTUBE_SHORT_PREFIX}{youtube_short_match.group("video_id")}']

        if youtube_video_match:
            return SearchType.DIRECT, [f'{YOUTUBE_VIDEO_PREFIX}{youtube_video_match.group("video_id")}']

        if FXTWITTER_VIDEO_PREFIX in search:
            return SearchType.DIRECT, [search.replace(FXTWITTER_VIDEO_PREFIX, TWITTER_VIDEO_PREFIX)]

        return SearchType.SEARCH, [search]

    async def check_source(self, search: str, guild_id: str, requester_name: str, requester_id: str, loop: AbstractEventLoop) -> List[SourceDict]:
        '''
        Generate sources from input

        search : Search string
        guild_id : Server/Guild id
        requester_name : Display name of requester
        requester_id : ID of requester
        loop : Bot run loop
        '''
        search_type, search_strings = await self.__check_source_types(search, loop)

        all_entries = []
        for search_string in search_strings:
            all_entries.append(SourceDict(guild_id, requester_name, requester_id, search_string, search_type))
        return all_entries
