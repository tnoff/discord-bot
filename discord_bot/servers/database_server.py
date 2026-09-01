'''
HTTP server for the persistence tier — the db pod's route family.

Fronts the stores from interfaces/database_protocols.py so the bot and the
broker can read and write postgres without either of them holding an engine.
MR 2 of projects/discord-db-tier-extraction, and **inert**: nothing constructs
this yet. The entrypoint that does is MR 3.

    POST /database/guild_analytics/get_analytics  {guild_id} -> GuildAnalyticsEntry
    POST /database/guild_analytics/record_play    {guild_id, duration_seconds,
                                                   cache_hit} -> bool
    POST /database/markov/list_channels           {} -> [MarkovChannelEntry]
    POST /database/markov/list_guild_channel_ids  {guild_id} -> [int]
    POST /database/markov/get_channel             {guild_id, channel_id}
                                                  -> MarkovChannelEntry | null
    POST /database/markov/add_channel             {guild_id, channel_id}
                                                  -> MarkovChannelEntry
    POST /database/markov/remove_channel          {guild_id, channel_id} -> bool
    POST /database/markov/reset_channel           {guild_id, channel_id} -> bool
    POST /database/markov/save_messages           {guild_id, channel_id,
                                                   messages} -> int | null
    POST /database/markov/generate_words          {guild_id, count, first_word?}
                                                  -> [str]
    POST /database/markov/prune_relations_before  {cutoff} -> bool

**Stores are optional and routes are registered per store given.** The groups
cross the seam one at a time, and a constructor that grew one required
positional per slice would rewrite every existing test on each of them --
churn that says nothing about the change being made. A group with no store
registers no routes and answers 404, which `async_retry_broker_command`
propagates immediately rather than laddering, so a misconfigured pod fails
loudly and fast. MR 3 constructs this with all four.

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
from datetime import datetime
import logging
from typing import Callable

from aiohttp import web
from opentelemetry.trace import SpanKind
from pydantic import ValidationError

from discord_bot.interfaces.database_protocols import GuildAnalyticsStore, MarkovStore
from discord_bot.servers.base import AiohttpServerBase
from discord_bot.types.database_wire import DatabaseErrorBody, DatabaseResponse
from discord_bot.types.markov import MarkovMessageWrite
from discord_bot.utils.otel import otel_span_wrapper

logger = logging.getLogger(__name__)

SPAN_PREFIX = 'database'
ROUTE_PREFIX = '/database'
DEFAULT_PORT = 8085


class DatabaseHttpServer(AiohttpServerBase):
    '''aiohttp HTTP server fronting the persistence-tier stores.'''

    def __init__(self, guild_analytics_store: GuildAnalyticsStore | None = None,
                 markov_store: MarkovStore | None = None,
                 host: str = '0.0.0.0',  # nosec B104
                 port: int = DEFAULT_PORT):
        '''
        guild_analytics_store : Serves the guild-analytics routes, or None to omit them
        markov_store : Serves the markov routes, or None to omit them
        host : Bind address; 0.0.0.0 because bot and broker pods reach this
               across the network
        port : Bind port
        '''
        super().__init__()
        self._guild_analytics_store = guild_analytics_store
        self._markov_store = markov_store
        self._host = host
        self._port = port

    def build_app(self) -> web.Application:
        '''Build and return the aiohttp Application. Exposed for testing.'''
        app = web.Application(middlewares=[self._get_drain_middleware()])
        routes = {}
        if self._guild_analytics_store is not None:
            routes['guild_analytics'] = {
                'get_analytics': self._handle_get_analytics,
                'record_play': self._handle_record_play,
            }
        if self._markov_store is not None:
            routes['markov'] = {
                'list_channels': self._handle_list_channels,
                'list_guild_channel_ids': self._handle_list_guild_channel_ids,
                'get_channel': self._handle_get_channel,
                'add_channel': self._handle_add_channel,
                'remove_channel': self._handle_remove_channel,
                'reset_channel': self._handle_reset_channel,
                'save_messages': self._handle_save_messages,
                'generate_words': self._handle_generate_words,
                'prune_relations_before': self._handle_prune_relations_before,
            }
        for group, handlers in routes.items():
            for name, handler in handlers.items():
                app.router.add_post(f'{ROUTE_PREFIX}/{group}/{name}', handler)
        logger.info('database server serving route groups: %s',
                    ', '.join(sorted(routes)) or 'none')
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

    async def _handle_list_channels(self, request: web.Request) -> web.Response:
        '''POST /database/markov/list_channels — every tracked channel.'''
        ctx, _body = await self._read_body(request)

        async def call():
            return [entry.model_dump(mode='json')
                    for entry in await self._markov_store.list_channels()]

        return await self._respond('markov.list_channels', ctx, call)

    async def _handle_list_guild_channel_ids(self, request: web.Request) -> web.Response:
        '''POST /database/markov/list_guild_channel_ids — a guild's channel ids.'''
        ctx, body = await self._read_body(request)
        (guild_id,) = self._required(body, 'guild_id')

        async def call():
            return await self._markov_store.list_guild_channel_ids(int(guild_id))

        return await self._respond('markov.list_guild_channel_ids', ctx, call)

    async def _handle_get_channel(self, request: web.Request) -> web.Response:
        '''POST /database/markov/get_channel — one tracked channel, or null.'''
        ctx, body = await self._read_body(request)
        guild_id, channel_id = self._required(body, 'guild_id', 'channel_id')

        async def call():
            entry = await self._markov_store.get_channel(int(guild_id), int(channel_id))
            return entry.model_dump(mode='json') if entry else None

        return await self._respond('markov.get_channel', ctx, call)

    async def _handle_add_channel(self, request: web.Request) -> web.Response:
        '''POST /database/markov/add_channel — start tracking a channel.'''
        ctx, body = await self._read_body(request)
        guild_id, channel_id = self._required(body, 'guild_id', 'channel_id')

        async def call():
            entry = await self._markov_store.add_channel(int(guild_id), int(channel_id))
            return entry.model_dump(mode='json')

        return await self._respond('markov.add_channel', ctx, call)

    async def _handle_remove_channel(self, request: web.Request) -> web.Response:
        '''POST /database/markov/remove_channel — stop tracking a channel.'''
        ctx, body = await self._read_body(request)
        guild_id, channel_id = self._required(body, 'guild_id', 'channel_id')

        async def call():
            return await self._markov_store.remove_channel(int(guild_id), int(channel_id))

        return await self._respond('markov.remove_channel', ctx, call)

    async def _handle_reset_channel(self, request: web.Request) -> web.Response:
        '''POST /database/markov/reset_channel — forget a channel's read position.'''
        ctx, body = await self._read_body(request)
        guild_id, channel_id = self._required(body, 'guild_id', 'channel_id')

        async def call():
            return await self._markov_store.reset_channel(int(guild_id), int(channel_id))

        return await self._respond('markov.reset_channel', ctx, call)

    async def _handle_save_messages(self, request: web.Request) -> web.Response:
        '''POST /database/markov/save_messages — write a batch of messages.'''
        ctx, body = await self._read_body(request)
        guild_id, channel_id, messages = self._required(
            body, 'guild_id', 'channel_id', 'messages')
        try:
            writes = [MarkovMessageWrite.model_validate(message) for message in messages]
        except ValidationError as exc:
            # 422, not the envelope: a malformed batch never reached the database,
            # and re-sending the same bytes cannot make it valid.
            raise web.HTTPUnprocessableEntity() from exc

        async def call():
            return await self._markov_store.save_messages(
                int(guild_id), int(channel_id), writes)

        return await self._respond('markov.save_messages', ctx, call)

    async def _handle_generate_words(self, request: web.Request) -> web.Response:
        '''POST /database/markov/generate_words — walk the chain for a sentence.'''
        ctx, body = await self._read_body(request)
        guild_id, count = self._required(body, 'guild_id', 'count')

        async def call():
            # first_word is genuinely optional -- `!markov speak` with no seed
            # sends null -- so it is read with .get rather than required.
            return await self._markov_store.generate_words(
                int(guild_id), int(count), body.get('first_word'))

        return await self._respond('markov.generate_words', ctx, call)

    async def _handle_prune_relations_before(self, request: web.Request) -> web.Response:
        '''POST /database/markov/prune_relations_before — drop stale relations.'''
        ctx, body = await self._read_body(request)
        (cutoff,) = self._required(body, 'cutoff')
        try:
            parsed = datetime.fromisoformat(cutoff)
        except (TypeError, ValueError) as exc:
            raise web.HTTPUnprocessableEntity() from exc

        async def call():
            return await self._markov_store.prune_relations_before(parsed)

        return await self._respond('markov.prune_relations_before', ctx, call)
