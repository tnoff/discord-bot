'''
Bot-side MediaSearchClient that forwards source expansion to the search pod.

The mirror of servers/media_search_server.py. Two calls, both request/response:
the cog cannot enqueue anything from !play until the expansion returns, so there
is no queue, no poller and no cached read surface here -- which is why this shares
nothing with HttpQueueWorkerClient beyond the session mixin.

**Its own module, separate from clients/media_search_client.py, and that is
load-bearing.** The in-memory client imports spotipy and googleapiclient at module
scope to catch their exceptions. If this class lived beside it, importing the HTTP
client would drag both SDKs into whichever process did the importing -- which is
the bot, the exact process the media_search extraction exists to get them out of.
Same split, and the same reason, as HttpBrokerClient moving out of
clients/broker_client.py (see reference: slim pod import-chain leaks).

The cog constructs this and nothing else -- there is no in-process branch left
on the bot side, which is what took spotipy and google-api-python-client out
of [bot].
'''
import logging

from opentelemetry.trace import SpanKind

from discord_bot.clients.http_client_base import HttpClientMixin
from discord_bot.types.catalog import CatalogResponse
from discord_bot.types.media_search import MediaSearchResponse
from discord_bot.utils.otel import async_otel_span_wrapper

logger = logging.getLogger(__name__)

SPAN_PREFIX = 'media_search'
ROUTE_PREFIX = '/search'


class HttpMediaSearchClient(HttpClientMixin):
    '''Forwards spotify_source / youtube_source to a remote search pod.'''

    def __init__(self, base_url: str, session=None):
        '''
        base_url : Root URL of the search pod, e.g. http://discord-search:8084
        session : Pre-built aiohttp session; the mixin makes one lazily otherwise
        '''
        self._base_url = base_url.rstrip('/')
        self._session = session

    async def _expand(self, route: str, body: dict) -> CatalogResponse:
        '''
        POST one provider route and return its catalog, or raise its failure.

        A provider failure arrives as HTTP 200 with a typed error body rather than
        a status code, so it is re-raised here as the same MediaSearchError the
        in-process client would have raised -- which is what keeps the cog's
        `except MediaSearchError` working identically on both sides of the split.
        '''
        payload = await self._http('POST', f'{self._base_url}{ROUTE_PREFIX}/{route}', body)
        response = MediaSearchResponse.model_validate(payload)
        if response.error is not None:
            raise response.error.to_exception()
        return response.catalog

    async def spotify_source(self, playlist_id: str = None, album_id: str = None,
                             track_id: str = None) -> CatalogResponse:
        '''
        Expand exactly one of a Spotify playlist, album or track.

        playlist_id : Playlist id
        album_id : Album id
        track_id : Track id
        '''
        if not (playlist_id or album_id or track_id):
            # Raised here rather than at the pod so the caller gets the same
            # ValueError it gets in-process, without paying a round trip to learn
            # it passed nothing.
            raise ValueError('Playlist, album, or track id must be passed')
        body = {'playlist_id': playlist_id, 'album_id': album_id, 'track_id': track_id}
        async with async_otel_span_wrapper(f'{SPAN_PREFIX}.spotify', kind=SpanKind.CLIENT):
            return await self._expand('spotify', {k: v for k, v in body.items() if v})

    async def youtube_source(self, playlist_id: str) -> CatalogResponse:
        '''
        Expand a YouTube playlist.

        playlist_id : ID of youtube playlist
        '''
        async with async_otel_span_wrapper(f'{SPAN_PREFIX}.youtube', kind=SpanKind.CLIENT):
            return await self._expand('youtube', {'playlist_id': playlist_id})
