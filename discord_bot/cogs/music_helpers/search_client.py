from asyncio import AbstractEventLoop
from functools import partial
from itertools import islice
from re import match
import random
from time import time

from googleapiclient.errors import HttpError
from opentelemetry.trace import SpanKind
from spotipy.exceptions import SpotifyException, SpotifyOauthError

from discord_bot.cogs.music_helpers.common import SearchType
from discord_bot.utils.integrations.common import YOUTUBE_SHORT_PREFIX, YOUTUBE_VIDEO_PREFIX
from discord_bot.utils.integrations.spotify import SpotifyClient
from discord_bot.utils.integrations.youtube import YoutubeClient
from discord_bot.types.catalog import CatalogResponse
from discord_bot.types.search import SearchResult, SearchCollection
from discord_bot.utils.otel import async_otel_span_wrapper, MediaRequestNaming

SPOTIFY_PLAYLIST_REGEX = r'^https://open.spotify.com/playlist/(?P<playlist_id>([a-zA-Z0-9]+))(?P<extra_query>(\?[a-zA-Z0-9=&_-]+)?)(?P<shuffle>( *shuffle)?)'
SPOTIFY_ALBUM_REGEX = r'^https://open.spotify.com/album/(?P<album_id>([a-zA-Z0-9]+))(?P<extra_query>(\?[a-zA-Z0-9=&_-]+)?)(?P<shuffle>( *shuffle)?)'
SPOTIFY_TRACK_REGEX = r'^https://open.spotify.com/track/(?P<track_id>([a-zA-Z0-9]+))(?P<extra_query>(\?[a-zA-Z0-9=&_-]+)?)'

YOUTUBE_PLAYLIST_REGEX = r'^https://(www\.)?youtube\.com/playlist\?list=(?P<playlist_id>[a-zA-Z0-9_-]+)(?P<shuffle> *(shuffle)?)'
YOUTUBE_VIDEO_REGEX = r'^https://(www\.)?youtu(\.)?be(\.com)?/(watch\?v=)?(?P<video_id>[a-zA-Z0-9_-]{11})'
YOUTUBE_SHORT_REGEX = r'^https://(www\.)?youtube\.com/shorts/(?P<video_id>[a-zA-Z0-9_-]{11})'

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
    def __init__(self, spotify_client: SpotifyClient = None, youtube_client: YoutubeClient = None):
        '''
        Init download client

        spotify_client : Spotify Client
        youtube_client : Youtube Client
        '''
        self.spotify_client: SpotifyClient | None = spotify_client
        self.youtube_client: YoutubeClient | None = youtube_client

    def __check_spotify_source(self, playlist_id: str = None, album_id: str = None, track_id: str = None) -> CatalogResponse:
        '''
        Get search strings from spotify

        playlist_id : Playlist id
        album_id : Album id
        track_id : Track ID
        '''
        assert playlist_id or album_id or track_id, 'Playlist or album id must be passed'

        if playlist_id:
            return self.spotify_client.playlist_get(playlist_id)
        if album_id:
            return self.spotify_client.album_get(album_id)
        if track_id:
            return self.spotify_client.track_get(track_id)
        return None

    def __check_youtube_source(self, playlist_id: str) -> CatalogResponse:
        '''
        Generate youtube sources

        playlist_id : ID of youtube playlist
        '''
        return self.youtube_client.playlist_get(playlist_id)

    async def __check_source_types(self, search: str, loop: AbstractEventLoop) -> SearchCollection:
        '''
        Create source types

        search : Original search string
        loop: Bot event loop
        '''
        async with async_otel_span_wrapper(f'{OTEL_SPAN_PREFIX}.check_source', kind=SpanKind.CLIENT, attributes={MediaRequestNaming.SEARCH_STRING.value: search}):
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
                    catalog_result = await loop.run_in_executor(None, to_run)
                except SpotifyOauthError as e:
                    message = 'Issue gathering info from spotify, credentials seem invalid'
                    raise ThirdPartyException('Issue fetching spotify info', user_message=message) from e
                except SpotifyException as e:
                    message = 'Issue gathering info from spotify url "{search}"'
                    if e.http_status == 404:
                        message = f'Unable to find url "{search}" via Spotify API\nIf this is an official Spotify playlist, [it might not be available via the api](https://developer.spotify.com/blog/2024-11-27-changes-to-the-web-api)'
                    raise ThirdPartyException('Issue fetching spotify info', user_message=message) from e
                if should_shuffle:
                    # https://stackoverflow.com/a/51295230
                    random.seed(time())
                    random.shuffle(catalog_result.items)
                collection_name = catalog_result.collection_name or search.replace(' shuffle', '')
                results = []
                for item in catalog_result.items:
                    results.append(SearchResult(search_type=SearchType.SPOTIFY, raw_search_string=item.search_string, proper_name=item.title))
                return SearchCollection(search_results=results, collection_name=collection_name)

            if youtube_playlist_matcher:
                if not self.youtube_client:
                    raise InvalidSearchURL('Missing youtube creds', user_message='Youtube Playlist URLs invalid, no youtube api credentials given to bot')

                should_shuffle = youtube_playlist_matcher.group('shuffle') != ''
                to_run = partial(self.__check_youtube_source, youtube_playlist_matcher.group('playlist_id'))
                try:
                    catalog_result = await loop.run_in_executor(None, to_run)
                except HttpError as e:
                    raise ThirdPartyException('Issue fetching youtube info', user_message=f'Issue gathering info from youtube url "{search}"') from e
                if should_shuffle:
                    # https://stackoverflow.com/a/51295230
                    random.seed(time())
                    random.shuffle(catalog_result.items)
                results = []
                for item in catalog_result.items:
                    results.append(SearchResult(search_type=SearchType.YOUTUBE_PLAYLIST, raw_search_string=item.search_string, proper_name=item.title))
                return SearchCollection(search_results=results, collection_name=catalog_result.collection_name)

            if youtube_short_match:
                return SearchCollection(search_results=[SearchResult(search_type=SearchType.YOUTUBE, raw_search_string=f'{YOUTUBE_SHORT_PREFIX}{youtube_short_match.group("video_id")}')])

            if youtube_video_match:
                return SearchCollection(search_results=[SearchResult(search_type=SearchType.YOUTUBE, raw_search_string=f'{YOUTUBE_VIDEO_PREFIX}{youtube_video_match.group("video_id")}')])

            # If we have https:// in url, assume its a direct
            if search.startswith('https://'):
                return SearchCollection(search_results=[SearchResult(search_type=SearchType.DIRECT, raw_search_string=search)])

            # Else assume this was a search message to put into youtube music
            return SearchCollection(search_results=[SearchResult(search_type=SearchType.SEARCH, raw_search_string=search)])

    async def check_source(self, search: str, loop: AbstractEventLoop,
                           max_results: int) -> SearchCollection:
        '''
        Generate sources from input

        search : Search string
        max_results : Max results of items
        '''
        collection = await self.__check_source_types(search, loop)
        if max_results is not None:
            collection.search_results = list(islice(collection.search_results, max_results))

        return collection
