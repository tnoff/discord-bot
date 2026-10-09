'''
The two db-pod calls the history worker makes, as HTTP store clients.

The gateway has full clients for both groups (discord_gateway/clients/http_playlist_store and
http_guild_analytics_store), but the broker cannot import the gateway package and only needs three
of their calls: the ones MusicPlayer's post-play loop made.  Recording a played track moved to the
broker with the queue, so these three calls moved with it; the gateway's copies lose them when the
gateway stops recording plays itself.

Same shape and same envelope handling as the gateway's (they share HttpStoreBase), including
that nothing here retries: the worker owns the retry policy, because it is the one that knows a
failed record must be kept and tried again.
'''
from discord_core.clients.http_store_base import HttpStoreBase
from discord_core.routes import database as database_routes
from discord_core.types.playlist import PlaylistItemWrite
from discord_core.utils.discord_context import DiscordContextNaming


class HttpHistoryPlaylistStore(HttpStoreBase):
    '''The playlist group's two history calls.'''

    SPAN_PREFIX = 'playlist_store'
    GROUP = database_routes.PLAYLIST

    async def ensure_history_playlist(self, guild_id: int) -> int:
        '''
        Return the guild's history playlist id, creating the playlist if absent.

        One request, never a read followed by a conditional write.

        guild_id : Discord guild id
        '''
        async with self._span('ensure_history_playlist', {DiscordContextNaming.GUILD.value: guild_id}):
            return await self._call('ensure_history_playlist', {'guild_id': guild_id})

    async def record_history_item(self, playlist_id: int, item: PlaylistItemWrite, max_size: int) -> bool:
        '''
        Write one played track to the history playlist, evicting to make room.

        Returns False when the playlist no longer exists.

        playlist_id : History playlist row id
        item : The track that just played
        max_size : Ceiling on the playlist's item count, enforced on the db side in one transaction
        '''
        async with self._span('record_history_item', {'playlist.id': playlist_id}):
            return await self._call('record_history_item', {
                'playlist_id': playlist_id, 'item': item.model_dump(mode='json'), 'max_size': max_size})


class HttpPlayAnalyticsStore(HttpStoreBase):
    '''The guild-analytics group's play counter.'''

    SPAN_PREFIX = 'guild_analytics_store'
    GROUP = database_routes.GUILD_ANALYTICS

    async def record_play(self, guild_id: int, duration_seconds: int, cache_hit: bool) -> bool:
        '''
        Add one play to a guild's totals.

        guild_id : Discord guild id
        duration_seconds : Length of the track that just played
        cache_hit : True when the download was served from cache
        '''
        async with self._span('record_play', {DiscordContextNaming.GUILD.value: guild_id}):
            return await self._call('record_play', {
                'guild_id': guild_id, 'duration_seconds': duration_seconds, 'cache_hit': cache_hit})
