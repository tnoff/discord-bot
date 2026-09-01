'''
HTTP server for the persistence tier — the db pod's route family.

Fronts the stores from interfaces/database_protocols.py so the bot and the
broker can read and write postgres without either of them holding an engine.
MR 2 of projects/discord-db-tier-extraction, and **inert**: nothing constructs
this yet. The entrypoint that does is MR 3.

    POST /database/guild_analytics/get_analytics  {guild_id} -> GuildAnalyticsEntry
    POST /database/guild_analytics/record_play    {guild_id, duration_seconds,
                                                   cache_hit} -> bool

Like media_search_server and unlike every queue-backed route in this repo, this
subclasses AiohttpServerBase directly. There is no queue, no worker and no
consumer loop: a store call is request/response by definition -- the caller
cannot proceed until the row comes back -- so the submit/clear/block/status shape
has nothing to describe.

**Every route answers 200, including failures.** See types/database_wire for the
full reasoning; the short version is that `raise_for_status()` lives inside the
client's retry wrapper, so a non-2xx both triggers a retry ladder and discards
the body explaining why. A store that has already exhausted its own retries
against the local engine has nothing to gain from three more attempts from
across the network. Non-2xx is left to mean "this pod is broken", which aiohttp
raises on its own and which the client's ladder is genuinely the right response
to.

**No heartbeat gauge here, on purpose** -- same reason as media_search_server.
Whoever owns the composite that fronts these routes owns the listener heartbeat.
'''
import logging
from typing import Callable

from aiohttp import web
from opentelemetry.trace import SpanKind

from discord_bot.interfaces.database_protocols import GuildAnalyticsStore
from discord_bot.servers.base import AiohttpServerBase
from discord_bot.types.database_wire import DatabaseErrorBody, DatabaseResponse
from discord_bot.utils.otel import otel_span_wrapper

logger = logging.getLogger(__name__)

SPAN_PREFIX = 'database'
ROUTE_PREFIX = '/database'
DEFAULT_PORT = 8085


class DatabaseHttpServer(AiohttpServerBase):
    '''aiohttp HTTP server fronting the persistence-tier stores.'''

    def __init__(self, guild_analytics_store: GuildAnalyticsStore,
                 host: str = '0.0.0.0',  # nosec B104
                 port: int = DEFAULT_PORT):
        '''
        guild_analytics_store : Does the database work; the server only speaks HTTP
        host : Bind address; 0.0.0.0 because bot and broker pods reach this
               across the network
        port : Bind port
        '''
        super().__init__()
        self._guild_analytics_store = guild_analytics_store
        self._host = host
        self._port = port

    def build_app(self) -> web.Application:
        '''Build and return the aiohttp Application. Exposed for testing.'''
        app = web.Application(middlewares=[self._get_drain_middleware()])
        app.router.add_post(f'{ROUTE_PREFIX}/guild_analytics/get_analytics',
                            self._handle_get_analytics)
        app.router.add_post(f'{ROUTE_PREFIX}/guild_analytics/record_play',
                            self._handle_record_play)
        return app

    @staticmethod
    def _required(body: dict, *fields):
        '''
        Pull required fields out of a request body, 422 if any is absent.

        422 rather than the error envelope, and the distinction is worth holding:
        the envelope describes what the *database* said, and a caller that
        omitted `guild_id` never reached the database. It is also the one class
        of failure a retry genuinely cannot fix, which is why
        async_retry_broker_command propagates 4xx immediately.

        body : Parsed JSON request body
        fields : Field names that must be present and not None
        '''
        try:
            values = [body[field] for field in fields]
        except KeyError as exc:
            raise web.HTTPUnprocessableEntity() from exc
        if any(value is None for value in values):
            raise web.HTTPUnprocessableEntity()
        return values

    async def _respond(self, span_name: str, ctx, call: Callable) -> web.Response:
        '''
        Run one store call and serialise whatever it produced.

        The single place a store exception becomes a wire error, so that every
        route gets the same envelope without repeating the try/except. Broad on
        purpose: what reaches here has already been through the store's own
        retries, and the caller can do nothing different for an OperationalError
        than for anything else the tier failed with -- both mean "the pod tried
        and could not". Narrowing to SQLAlchemy's hierarchy would also put
        SQLAlchemy in the signature of a module whose whole job is to keep it out
        of the callers.

        span_name : Suffix for the server span
        ctx : Inbound trace context from the request headers
        call : Zero-arg coroutine function running the store method
        '''
        with otel_span_wrapper(f'{SPAN_PREFIX}.{span_name}', context=ctx,
                               kind=SpanKind.SERVER) as span:
            try:
                result = await call()
            except Exception as exc:  # pylint:disable=broad-exception-caught
                # ERROR on the span, unlike media_search's provider errors: this
                # one is the pod failing, not the pod reporting an answer, and it
                # belongs on the tier's error-rate panels.
                span.record_exception(exc)
                logger.exception('database tier failed serving %s', span_name)
                return web.json_response(DatabaseResponse(
                    error=DatabaseErrorBody.from_exception(exc)).model_dump(mode='json'))
            return web.json_response(DatabaseResponse(result=result).model_dump(mode='json'))

    async def _handle_get_analytics(self, request: web.Request) -> web.Response:
        '''POST /database/guild_analytics/get_analytics — a guild's play totals.'''
        ctx, body = await self._read_body(request)
        (guild_id,) = self._required(body, 'guild_id')

        async def call():
            entry = await self._guild_analytics_store.get_analytics(int(guild_id))
            return entry.model_dump(mode='json')

        return await self._respond('guild_analytics.get_analytics', ctx, call)

    async def _handle_record_play(self, request: web.Request) -> web.Response:
        '''POST /database/guild_analytics/record_play — add one play to the totals.'''
        ctx, body = await self._read_body(request)
        guild_id, duration_seconds, cache_hit = self._required(
            body, 'guild_id', 'duration_seconds', 'cache_hit')

        async def call():
            return await self._guild_analytics_store.record_play(
                int(guild_id), int(duration_seconds), bool(cache_hit))

        return await self._respond('guild_analytics.record_play', ctx, call)
