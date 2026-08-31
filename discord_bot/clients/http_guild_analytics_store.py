'''
Bot-side GuildAnalyticsStore that forwards to the db pod.

The mirror of servers/database_server.py's guild-analytics routes, and the first
HTTP implementation of a persistence Protocol. Satisfies the same
interfaces.database_protocols.GuildAnalyticsStore that GuildAnalyticsClient does,
which is the property MR 1 was built to make true: the Music cog annotates
against the Protocol, so selecting this one is a constructor change and nothing
else.

**Inert.** Nothing constructs this yet -- the cog still builds the in-process
client, and will until MR 4's cutover. It ships now so the wire has tests before
anything depends on it.

**Its own module, not beside GuildAnalyticsClient, and that is load-bearing.**
The in-process client imports SQLAlchemy models at module scope. If this class
lived next to it, importing the HTTP client would drag SQLAlchemy into whichever
process did the importing -- the bot, the exact process this extraction exists to
get it out of. Same split, and the same reason, as HttpMediaSearchClient living
apart from the in-memory one (see reference: slim pod import-chain leaks).

**This class does not retry.** `_http` already wraps every call in
async_retry_broker_command, which handles the failure this side is nearest to --
the pod being absent or restarting. A `DatabaseUnavailable` in the envelope means
the pod was reachable and its store already exhausted its own retries against the
engine; re-running it from here would be nine attempts for one query. See
types/database_wire.
'''
import logging

from opentelemetry.trace import SpanKind

from discord_bot.clients.http_client_base import HttpClientMixin
from discord_bot.types.database_wire import DatabaseResponse
from discord_bot.types.guild_analytics import GuildAnalyticsEntry
from discord_bot.utils.otel import async_otel_span_wrapper, DiscordContextNaming

logger = logging.getLogger(__name__)

SPAN_PREFIX = 'guild_analytics_store'
ROUTE_PREFIX = '/database/guild_analytics'


class HttpGuildAnalyticsStore(HttpClientMixin):
    '''Forwards get_analytics / record_play to a remote db pod.'''

    def __init__(self, base_url: str, session=None):
        '''
        base_url : Root URL of the db pod, e.g. http://discord-db:8085
        session : Pre-built aiohttp session; the mixin makes one lazily otherwise
        '''
        self._base_url = base_url.rstrip('/')
        self._session = session

    async def _call(self, route: str, body: dict):
        '''
        POST one store route and return its result, or raise its failure.

        route : Route name under the guild-analytics prefix
        body : Request body
        '''
        payload = await self._http('POST', f'{self._base_url}{ROUTE_PREFIX}/{route}', body)
        response = DatabaseResponse.model_validate(payload)
        if response.error is not None:
            raise response.error.to_exception()
        return response.result

    async def get_analytics(self, guild_id: int) -> GuildAnalyticsEntry:
        '''
        Return a guild's play totals, creating the rows on first call.

        guild_id : Discord guild id
        '''
        attributes = {DiscordContextNaming.GUILD.value: guild_id}
        async with async_otel_span_wrapper(f'{SPAN_PREFIX}.get_analytics',
                                           kind=SpanKind.CLIENT, attributes=attributes):
            result = await self._call('get_analytics', {'guild_id': guild_id})
            return GuildAnalyticsEntry.model_validate(result)

    async def record_play(self, guild_id: int, duration_seconds: int,
                          cache_hit: bool) -> bool:
        '''
        Add one play to a guild's totals.

        guild_id : Discord guild id
        duration_seconds : Length of the track that just played
        cache_hit : True when the download was served from cache
        '''
        attributes = {DiscordContextNaming.GUILD.value: guild_id}
        async with async_otel_span_wrapper(f'{SPAN_PREFIX}.record_play',
                                           kind=SpanKind.CLIENT, attributes=attributes):
            return await self._call('record_play', {
                'guild_id': guild_id,
                'duration_seconds': duration_seconds,
                'cache_hit': cache_hit,
            })
