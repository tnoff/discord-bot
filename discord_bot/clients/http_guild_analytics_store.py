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

**This class does not retry**, and the envelope handling that makes that true
lives in HttpStoreBase now -- shared with the markov and playlist stores rather
than copied into each. See clients/http_store_base and types/database_wire.
'''
import logging

from discord_bot.clients.http_store_base import HttpStoreBase
from discord_bot.types.guild_analytics import GuildAnalyticsEntry
from discord_bot.utils.otel import DiscordContextNaming

logger = logging.getLogger(__name__)


class HttpGuildAnalyticsStore(HttpStoreBase):
    '''Forwards get_analytics / record_play to a remote db pod.'''

    SPAN_PREFIX = 'guild_analytics_store'
    ROUTE_PREFIX = '/database/guild_analytics'

    async def get_analytics(self, guild_id: int) -> GuildAnalyticsEntry:
        '''
        Return a guild's play totals, creating the rows on first call.

        guild_id : Discord guild id
        '''
        async with self._span('get_analytics',
                              {DiscordContextNaming.GUILD.value: guild_id}):
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
        async with self._span('record_play',
                              {DiscordContextNaming.GUILD.value: guild_id}):
            return await self._call('record_play', {
                'guild_id': guild_id,
                'duration_seconds': duration_seconds,
                'cache_hit': cache_hit,
            })
