'''
Bot-side MarkovStore that forwards to the db pod.

The mirror of servers/database_server.py's markov routes, and the second HTTP
implementation of a persistence Protocol. Satisfies the same
interfaces.database_protocols.MarkovStore that MarkovClient does, so the Markov
cog -- which annotates against the Protocol -- selects one or the other with a
constructor change and nothing else.

**Inert.** Nothing constructs this yet; the cog still builds the in-process
client, and will until MR 4's cutover.

**Its own module, not beside MarkovClient**, for the reason the video-cache and
media-search splits already established: the in-process client imports SQLAlchemy
models at module scope, so co-locating them would pull SQLAlchemy into whichever
process imported this one.

**The batch stays a batch.** `save_messages` sends a channel's whole cycle of
messages in one request, which is the same reason MarkovMessageWrite exists at
all: a per-message signature would be one round trip per message here, exactly
the cost `!267` removed one layer down. The Protocol was sized for this seam
before the seam existed, and this class is what collects on that.
'''
import logging
from datetime import datetime
from typing import List

from opentelemetry.trace import SpanKind

from discord_bot.clients.http_client_base import HttpClientMixin
from discord_bot.types.database_wire import DatabaseResponse
from discord_bot.types.markov import MarkovChannelEntry, MarkovMessageWrite
from discord_bot.utils.otel import async_otel_span_wrapper, DiscordContextNaming

logger = logging.getLogger(__name__)

SPAN_PREFIX = 'markov_store'
ROUTE_PREFIX = '/database/markov'


class HttpMarkovStore(HttpClientMixin):
    '''Forwards the markov store's nine calls to a remote db pod.'''

    def __init__(self, base_url: str, session=None):
        '''
        base_url : Root URL of the db pod, e.g. http://discord-db:8085
        session : Pre-built aiohttp session; the mixin makes one lazily otherwise
        '''
        self._base_url = base_url.rstrip('/')
        self._session = session

    async def _call(self, route: str, body: dict = None):
        '''
        POST one store route and return its result, or raise its failure.

        route : Route name under the markov prefix
        body : Request body; {} for the routes that take no arguments
        '''
        payload = await self._http('POST', f'{self._base_url}{ROUTE_PREFIX}/{route}',
                                   body if body is not None else {})
        response = DatabaseResponse.model_validate(payload)
        if response.error is not None:
            raise response.error.to_exception()
        return response.result

    async def _channel_call(self, route: str, guild_id: int, channel_id: int):
        '''
        Run one of the four routes keyed by (guild, channel).

        route : Route name under the markov prefix
        guild_id : Discord guild id
        channel_id : Discord channel id
        '''
        attributes = {DiscordContextNaming.GUILD.value: guild_id,
                      DiscordContextNaming.CHANNEL.value: channel_id}
        async with async_otel_span_wrapper(f'{SPAN_PREFIX}.{route}',
                                           kind=SpanKind.CLIENT, attributes=attributes):
            return await self._call(route, {'guild_id': guild_id, 'channel_id': channel_id})

    async def list_channels(self) -> List[MarkovChannelEntry]:
        '''Return every tracked channel.'''
        async with async_otel_span_wrapper(f'{SPAN_PREFIX}.list_channels',
                                           kind=SpanKind.CLIENT):
            result = await self._call('list_channels')
            return [MarkovChannelEntry.model_validate(entry) for entry in result]

    async def list_guild_channel_ids(self, guild_id: int) -> List[int]:
        '''
        Return the channel ids tracked for one guild.

        guild_id : Discord guild id
        '''
        attributes = {DiscordContextNaming.GUILD.value: guild_id}
        async with async_otel_span_wrapper(f'{SPAN_PREFIX}.list_guild_channel_ids',
                                           kind=SpanKind.CLIENT, attributes=attributes):
            return await self._call('list_guild_channel_ids', {'guild_id': guild_id})

    async def get_channel(self, guild_id: int, channel_id: int) -> MarkovChannelEntry | None:
        '''
        Return one tracked channel, or None when it is not tracked.

        guild_id : Discord guild id
        channel_id : Discord channel id
        '''
        result = await self._channel_call('get_channel', guild_id, channel_id)
        return MarkovChannelEntry.model_validate(result) if result else None

    async def add_channel(self, guild_id: int, channel_id: int) -> MarkovChannelEntry:
        '''
        Start tracking a channel and return its row.

        guild_id : Discord guild id
        channel_id : Discord channel id
        '''
        result = await self._channel_call('add_channel', guild_id, channel_id)
        return MarkovChannelEntry.model_validate(result)

    async def remove_channel(self, guild_id: int, channel_id: int) -> bool:
        '''
        Stop tracking a channel. False when it was not tracked.

        guild_id : Discord guild id
        channel_id : Discord channel id
        '''
        return await self._channel_call('remove_channel', guild_id, channel_id)

    async def reset_channel(self, guild_id: int, channel_id: int) -> bool:
        '''
        Forget a channel's read position. False when it is not tracked.

        guild_id : Discord guild id
        channel_id : Discord channel id
        '''
        return await self._channel_call('reset_channel', guild_id, channel_id)

    async def save_messages(self, guild_id: int, channel_id: int,
                            messages: List[MarkovMessageWrite]) -> int | None:
        '''
        Write a batch of messages' word pairs. None when the channel is gone.

        One request for the whole batch, never one per message.

        guild_id : Discord guild id
        channel_id : Discord channel id
        messages : The cycle's messages, in the order they were read
        '''
        attributes = {DiscordContextNaming.GUILD.value: guild_id,
                      DiscordContextNaming.CHANNEL.value: channel_id}
        async with async_otel_span_wrapper(f'{SPAN_PREFIX}.save_messages',
                                           kind=SpanKind.CLIENT, attributes=attributes):
            return await self._call('save_messages', {
                'guild_id': guild_id,
                'channel_id': channel_id,
                'messages': [message.model_dump(mode='json') for message in messages],
            })

    async def generate_words(self, guild_id: int, count: int,
                             first_word: str | None = None) -> List[str]:
        '''
        Walk the chain and return up to `count` words.

        guild_id : Discord guild id
        count : How many words to walk
        first_word : Seed word, or None to start anywhere
        '''
        attributes = {DiscordContextNaming.GUILD.value: guild_id}
        async with async_otel_span_wrapper(f'{SPAN_PREFIX}.generate_words',
                                           kind=SpanKind.CLIENT, attributes=attributes):
            return await self._call('generate_words', {
                'guild_id': guild_id, 'count': count, 'first_word': first_word})

    async def prune_relations_before(self, cutoff: datetime) -> bool:
        '''
        Delete relations older than a cutoff.

        cutoff : Relations created before this are removed
        '''
        async with async_otel_span_wrapper(f'{SPAN_PREFIX}.prune_relations_before',
                                           kind=SpanKind.CLIENT):
            return await self._call('prune_relations_before',
                                    {'cutoff': cutoff.isoformat()})
