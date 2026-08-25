'''
HTTP server for the media-search providers — the search pod's 2nd route family.

Fronts a MediaSearchClient (in practice InMemoryMediaSearchClient, holding the
real SpotifyClient and YoutubeClient) so bot pods can expand a Spotify or YouTube
URL without carrying those SDKs themselves.

    POST /search/spotify   {playlist_id | album_id | track_id} -> MediaSearchResponse
    POST /search/youtube   {playlist_id}                       -> MediaSearchResponse

Not a QueueWorkerHttpServer subclass, and that is the structural difference from
every other pod route in this repo. There is no queue, no worker and no consumer
loop here: source expansion is request/response inside !play -- the cog cannot
enqueue anything until the expansion returns -- so the four-verb
submit/clear/block/status shape has nothing to describe.

The routes sit under /search/ because the search pod reserved that namespace for
exactly this (see youtube_music_search_server's docstring): co-hosting providers
keeps one image, one pin and one netpol tier, and every extra pod is another pin
a revert-then-auto-bump can strand out of step with the bot.

**No heartbeat gauge here, on purpose.** AiohttpServerBase sets _serving in
serve(), so a server whose routes are merged into a composite app -- which is how
this one will actually run, alongside YoutubeMusicSearchHttpServer on one bind --
would report a permanent 0 while serving fine. Whoever owns the composite owns
the listener heartbeat; publishing a second one from here would just add a series
that reads dead.
'''
import logging

from aiohttp import web
from opentelemetry.trace import SpanKind

from discord_bot.exceptions import MediaSearchError
from discord_bot.interfaces.media_search_protocols import MediaSearchClient
from discord_bot.servers.base import AiohttpServerBase
from discord_bot.types.media_search import MediaSearchErrorBody, MediaSearchResponse
from discord_bot.utils.otel import otel_span_wrapper

logger = logging.getLogger(__name__)

SPAN_PREFIX = 'media_search'
ROUTE_PREFIX = '/search'
DEFAULT_PORT = 8084

SPOTIFY_ID_FIELDS = ('playlist_id', 'album_id', 'track_id')


class MediaSearchHttpServer(AiohttpServerBase):
    '''aiohttp HTTP server fronting a MediaSearchClient's two provider calls.'''

    def __init__(self, media_search_client: MediaSearchClient,
                 host: str = '0.0.0.0',  # nosec B104
                 port: int = DEFAULT_PORT):
        '''
        media_search_client : Does the provider work; the server only speaks HTTP
        host : Bind address; 0.0.0.0 because bot pods reach this across the network
        port : Bind port
        '''
        super().__init__()
        self._media_search_client = media_search_client
        self._host = host
        self._port = port

    def build_app(self) -> web.Application:
        '''Build and return the aiohttp Application. Exposed for testing.'''
        app = web.Application(middlewares=[self._get_drain_middleware()])
        app.router.add_post(f'{ROUTE_PREFIX}/spotify', self._handle_spotify)
        app.router.add_post(f'{ROUTE_PREFIX}/youtube', self._handle_youtube)
        return app

    @staticmethod
    def _respond(catalog) -> web.Response:
        '''Serialise a successful expansion.'''
        return web.json_response(
            MediaSearchResponse(catalog=catalog).model_dump(mode='json'))

    @staticmethod
    def _respond_error(error: MediaSearchError, span) -> web.Response:
        '''
        Serialise a provider failure as a 200 with a typed error body.

        Kept OK on the span as well as on the wire. A provider saying "no such
        playlist" is an answer, not a fault of this pod, and marking it ERROR
        would put every mistyped Spotify URL on the pod's error-rate panels --
        the same call queue_worker_server makes for a blocked-guild submit.
        '''
        span.set_attributes({'media_search.provider': error.provider,
                             'media_search.reason': error.reason})
        return web.json_response(
            MediaSearchResponse(error=MediaSearchErrorBody.from_exception(error)).model_dump(mode='json'))

    async def _handle_spotify(self, request: web.Request) -> web.Response:
        '''POST /search/spotify — expand a Spotify playlist, album or track.'''
        ctx, body = await self._read_body(request)
        ids = {field: body[field] for field in SPOTIFY_ID_FIELDS if body.get(field)}
        if len(ids) != 1:
            # Exactly one, not at least one: the in-process client picks by
            # precedence when given several, and two callers disagreeing about
            # that precedence is a bug worth failing on rather than guessing.
            raise web.HTTPUnprocessableEntity()
        with otel_span_wrapper(f'{SPAN_PREFIX}.spotify', context=ctx, kind=SpanKind.SERVER) as span:
            try:
                catalog = await self._media_search_client.spotify_source(**ids)
            except MediaSearchError as exc:
                return self._respond_error(exc, span)
            return self._respond(catalog)

    async def _handle_youtube(self, request: web.Request) -> web.Response:
        '''POST /search/youtube — expand a YouTube playlist.'''
        ctx, body = await self._read_body(request)
        playlist_id = body.get('playlist_id')
        if not playlist_id:
            raise web.HTTPUnprocessableEntity()
        with otel_span_wrapper(f'{SPAN_PREFIX}.youtube', context=ctx, kind=SpanKind.SERVER) as span:
            try:
                catalog = await self._media_search_client.youtube_source(playlist_id)
            except MediaSearchError as exc:
                return self._respond_error(exc, span)
            return self._respond(catalog)
