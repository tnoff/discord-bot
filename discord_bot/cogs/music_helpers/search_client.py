from itertools import islice
from re import match
import random
from time import time

from opentelemetry.trace import SpanKind

from discord_bot.cogs.music_helpers.common import SearchType
from discord_bot.exceptions import (
    InvalidSearchURL, MediaSearchError, SearchException, ThirdPartyException,
)
from discord_bot.interfaces.media_search_protocols import MediaSearchClient
from discord_bot.utils.integrations.common import YOUTUBE_SHORT_PREFIX, YOUTUBE_VIDEO_PREFIX
from discord_bot.types.search import SearchResult, SearchCollection
from discord_bot.utils.otel import async_otel_span_wrapper, MediaRequestNaming

SPOTIFY_PLAYLIST_REGEX = r'^https://open.spotify.com/playlist/(?P<playlist_id>([a-zA-Z0-9]+))(?P<extra_query>(\?[a-zA-Z0-9=&_-]+)?)(?P<shuffle>( *shuffle)?)'
SPOTIFY_ALBUM_REGEX = r'^https://open.spotify.com/album/(?P<album_id>([a-zA-Z0-9]+))(?P<extra_query>(\?[a-zA-Z0-9=&_-]+)?)(?P<shuffle>( *shuffle)?)'
SPOTIFY_TRACK_REGEX = r'^https://open.spotify.com/track/(?P<track_id>([a-zA-Z0-9]+))(?P<extra_query>(\?[a-zA-Z0-9=&_-]+)?)'

YOUTUBE_PLAYLIST_REGEX = r'^https://(www\.)?youtube\.com/playlist\?list=(?P<playlist_id>[a-zA-Z0-9_-]+)(?P<shuffle> *(shuffle)?)'
YOUTUBE_VIDEO_REGEX = r'^https://(www\.)?youtu(\.)?be(\.com)?/(watch\?v=)?(?P<video_id>[a-zA-Z0-9_-]{11})'
YOUTUBE_SHORT_REGEX = r'^https://(www\.)?youtube\.com/shorts/(?P<video_id>[a-zA-Z0-9_-]{11})'

# SearchException / ThirdPartyException / InvalidSearchURL are defined in
# discord_bot.exceptions and imported above. They are re-exported from here
# because this is where they were defined and cogs/music.py and the tests still
# import them from this path.
__all__ = [
    'SearchClient', 'SearchException', 'ThirdPartyException', 'InvalidSearchURL',
    'check_youtube_video',
]

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
    def __init__(self, media_search_client: MediaSearchClient):
        '''
        Init search client

        media_search_client : Expands third-party URLs into a CatalogResponse.
            The bot passes the HTTP one and the search pod builds the in-process
            one; this class has never had to know which, which is the point of
            taking it as one argument instead of a SpotifyClient and a
            YoutubeClient. It is also why the cutover changed one line in the cog
            and nothing at all in here.
        '''
        self.media_search_client: MediaSearchClient = media_search_client

    @staticmethod
    def __render_provider_error(error: MediaSearchError, search: str) -> SearchException:
        '''
        Turn a provider failure into the exception the user will read.

        The provider clients report what went wrong (provider, reason, status);
        this decides what a Discord user is told about it. Keeping the two apart
        is what lets the providers move to the search pod without the pod owning
        any Discord-facing copy.

        error : Raised by the media search client
        search : The original search string, for the messages that quote it
        '''
        if error.reason == MediaSearchError.MISSING_CREDENTIALS:
            if error.provider == MediaSearchError.SPOTIFY:
                return InvalidSearchURL('Missing spotify creds',
                                        user_message='Spotify URLs invalid, no spotify credentials available to bot')
            return InvalidSearchURL('Missing youtube creds',
                                    user_message='Youtube Playlist URLs invalid, no youtube api credentials given to bot')
        if error.provider == MediaSearchError.YOUTUBE:
            return ThirdPartyException('Issue fetching youtube info',
                                       user_message=f'Issue gathering info from youtube url "{search}"')
        if error.reason == MediaSearchError.AUTH_ERROR:
            return ThirdPartyException('Issue fetching spotify info',
                                       user_message='Issue gathering info from spotify, credentials seem invalid')
        if error.reason == MediaSearchError.NOT_FOUND:
            return ThirdPartyException('Issue fetching spotify info',
                                       user_message=f'Unable to find url "{search}" via Spotify API\n'
                                                    'If this is an official Spotify playlist, '
                                                    '[it might not be available via the api]'
                                                    '(https://developer.spotify.com/blog/2024-11-27-changes-to-the-web-api)')
        return ThirdPartyException('Issue fetching spotify info',
                                   user_message=f'Issue gathering info from spotify url "{search}"')

    async def __check_source_types(self, search: str) -> SearchCollection:
        '''
        Create source types

        search : Original search string
        '''
        async with async_otel_span_wrapper(f'{OTEL_SPAN_PREFIX}.check_source', kind=SpanKind.CLIENT, attributes={MediaRequestNaming.SEARCH_STRING.value: search}):
            spotify_playlist_matcher = match(SPOTIFY_PLAYLIST_REGEX, search)
            spotify_album_matcher = match(SPOTIFY_ALBUM_REGEX, search)
            spotify_track_matcher = match(SPOTIFY_TRACK_REGEX, search)
            youtube_playlist_matcher = match(YOUTUBE_PLAYLIST_REGEX, search)
            youtube_short_match = match(YOUTUBE_SHORT_REGEX, search)
            youtube_video_match = match(YOUTUBE_VIDEO_REGEX, search)

            if spotify_playlist_matcher or spotify_album_matcher or spotify_track_matcher:
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

                try:
                    catalog_result = await self.media_search_client.spotify_source(**spotify_args)
                except MediaSearchError as e:
                    raise self.__render_provider_error(e, search) from e
                if should_shuffle:
                    # https://stackoverflow.com/a/51295230
                    random.seed(time())
                    random.shuffle(catalog_result.items)
                collection_name = catalog_result.collection_name or search.replace(' shuffle', '')
                results = []
                for item in catalog_result.items:
                    results.append(SearchResult(search_type=SearchType.SEARCH, raw_search_string=item.search_string, proper_name=item.title))
                return SearchCollection(search_results=results, collection_name=collection_name)

            if youtube_playlist_matcher:
                should_shuffle = youtube_playlist_matcher.group('shuffle') != ''
                try:
                    catalog_result = await self.media_search_client.youtube_source(
                        youtube_playlist_matcher.group('playlist_id'))
                except MediaSearchError as e:
                    raise self.__render_provider_error(e, search) from e
                if should_shuffle:
                    # https://stackoverflow.com/a/51295230
                    random.seed(time())
                    random.shuffle(catalog_result.items)
                results = []
                for item in catalog_result.items:
                    results.append(SearchResult(search_type=SearchType.YOUTUBE, raw_search_string=item.search_string, proper_name=item.title))
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

    async def check_source(self, search: str, max_results: int) -> SearchCollection:
        '''
        Generate sources from input

        search : Search string
        max_results : Max results of items

        No event loop argument: the only thing it was ever used for was the
        executor offload, which now belongs to the media search client -- and the
        HTTP client, which does no offloading at all, would have carried a dead
        parameter forever.
        '''
        collection = await self.__check_source_types(search)
        if max_results is not None:
            collection.search_results = list(islice(collection.search_results, max_results))

        return collection
