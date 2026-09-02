'''
Bot-side PlaylistStore that forwards to the db pod.

The mirror of servers/database_server.py's playlist routes, and the third HTTP
implementation of a persistence Protocol. Satisfies the same
interfaces.database_protocols.PlaylistStore that PlaylistClient does, so the
Music cog -- which annotates against the Protocol -- selects one or the other
with a constructor change and nothing else.

**Inert.** Nothing constructs this yet; the cog still builds the in-process
client, and will until MR 4's cutover.

**Its own module, not beside PlaylistClient**, for the reason the video-cache and
media-search splits already established: the in-process client imports SQLAlchemy
models at module scope, so co-locating them would pull SQLAlchemy into whichever
process imported this one.

**The compound calls stay compound.** `add_items`, `record_history_item`,
`ensure_history_playlist` and `delete_item_by_index` each cross in one request,
and this is the layer where that stops being a style preference. `add_items`
against a per-item route would be one round trip per track in a queue save;
`ensure_history_playlist` split back into a read and a conditional write would be
a race between two players starting at once, resolved by a unique-constraint
violation on a path with no way to report one. The Protocol was sized per unit of
work before there was a wire; this class is what collects on that.

**The ceiling is enforced on the far side.** `max_size` is an argument rather
than something the caller checks first, because the count and the insert have to
be one transaction. Over HTTP the check-then-act version is not merely racy, it
is racy across processes with a network hop in the gap.
'''
import logging
from typing import List

from discord_bot.clients.http_store_base import HttpStoreBase
from discord_bot.types.playlist import (
    PlaylistEntry,
    PlaylistItemAddOutcome,
    PlaylistItemEntry,
    PlaylistItemWrite,
)
from discord_bot.utils.otel import DiscordContextNaming

logger = logging.getLogger(__name__)


class HttpPlaylistStore(HttpStoreBase):
    '''Forwards the playlist store's sixteen calls to a remote db pod.'''

    SPAN_PREFIX = 'playlist_store'
    ROUTE_PREFIX = '/database/playlist'

    async def _guild_call(self, route: str, guild_id: int):
        '''
        Run one of the routes keyed by guild alone.

        route : Route name under the playlist prefix
        guild_id : Discord guild id
        '''
        async with self._span(route, {DiscordContextNaming.GUILD.value: guild_id}):
            return await self._call(route, {'guild_id': guild_id})

    async def _playlist_call(self, route: str, playlist_id: int, **extra):
        '''
        Run one of the routes keyed by playlist row id.

        The id is a span attribute rather than only a body field: a slow or
        failing playlist call is almost always about one playlist, and the
        alternative is reading the body off the request to find out which.

        route : Route name under the playlist prefix
        playlist_id : Playlist row id
        extra : Any further body fields the route takes
        '''
        async with self._span(route, {'playlist.id': playlist_id}):
            return await self._call(route, {'playlist_id': playlist_id, **extra})

    async def list_playlists(self, guild_id: int) -> List[PlaylistEntry]:
        '''
        Return a guild's non-history playlists, newest first.

        Order is the public index users type, and JSON array order is what
        carries it -- nothing downstream re-sorts.

        guild_id : Discord guild id
        '''
        result = await self._guild_call('list_playlists', guild_id)
        return [PlaylistEntry.model_validate(entry) for entry in result]

    async def count_playlists(self, guild_id: int) -> int:
        '''
        Return how many non-history playlists a guild has.

        guild_id : Discord guild id
        '''
        return await self._guild_call('count_playlists', guild_id)

    async def get_playlist(self, playlist_id: int) -> PlaylistEntry | None:
        '''
        Return one playlist by row id, or None.

        playlist_id : Playlist row id
        '''
        result = await self._playlist_call('get_playlist', playlist_id)
        return PlaylistEntry.model_validate(result) if result else None

    async def get_playlist_by_name(self, guild_id: int, name: str) -> PlaylistEntry | None:
        '''
        Return a guild's playlist with this name, or None.

        guild_id : Discord guild id
        name : Playlist name to look for
        '''
        async with self._span('get_playlist_by_name', {DiscordContextNaming.GUILD.value: guild_id}):
            result = await self._call('get_playlist_by_name',
                                      {'guild_id': guild_id, 'name': name})
        return PlaylistEntry.model_validate(result) if result else None

    async def get_history_playlist(self, guild_id: int) -> PlaylistEntry | None:
        '''
        Return a guild's history playlist, or None when it has none yet.

        guild_id : Discord guild id
        '''
        result = await self._guild_call('get_history_playlist', guild_id)
        return PlaylistEntry.model_validate(result) if result else None

    async def ensure_history_playlist(self, guild_id: int) -> int:
        '''
        Return the guild's history playlist id, creating it if absent.

        One request, never a read followed by a conditional write.

        guild_id : Discord guild id
        '''
        return await self._guild_call('ensure_history_playlist', guild_id)

    async def create_playlist(self, guild_id: int, name: str) -> PlaylistEntry:
        '''
        Create a playlist and return it.

        guild_id : Discord guild id
        name : Playlist name
        '''
        async with self._span('create_playlist', {DiscordContextNaming.GUILD.value: guild_id}):
            result = await self._call('create_playlist',
                                      {'guild_id': guild_id, 'name': name})
        return PlaylistEntry.model_validate(result)

    async def delete_playlist(self, playlist_id: int) -> bool:
        '''
        Delete a playlist and every item in it.

        playlist_id : Playlist row id
        '''
        return await self._playlist_call('delete_playlist', playlist_id)

    async def rename_playlist(self, playlist_id: int, name: str) -> bool:
        '''
        Rename a playlist. False when there is no such playlist.

        playlist_id : Playlist row id
        name : New name
        '''
        return await self._playlist_call('rename_playlist', playlist_id, name=name)

    async def mark_queued(self, playlist_id: int) -> bool:
        '''
        Record that a playlist was just queued.

        playlist_id : Playlist row id
        '''
        return await self._playlist_call('mark_queued', playlist_id)

    async def get_playlist_size(self, playlist_id: int) -> int:
        '''
        Return how many items a playlist holds.

        playlist_id : Playlist row id
        '''
        return await self._playlist_call('get_playlist_size', playlist_id)

    async def list_items(self, playlist_id: int) -> List[PlaylistItemEntry]:
        '''
        Return a playlist's items, oldest first.

        playlist_id : Playlist row id
        '''
        result = await self._playlist_call('list_items', playlist_id)
        return [PlaylistItemEntry.model_validate(entry) for entry in result]

    async def add_items(self, playlist_id: int, items: List[PlaylistItemWrite],
                        max_size: int) -> List[PlaylistItemAddOutcome]:
        '''
        Add items to a playlist, stopping when it is full.

        One request for the whole batch. Fewer outcomes than items means the
        playlist filled: the item that hit the ceiling reports PLAYLIST_FULL and
        the ones after it were never attempted.

        playlist_id : Playlist row id
        items : Items to add, in order
        max_size : Ceiling on the playlist's item count
        '''
        result = await self._playlist_call(
            'add_items', playlist_id,
            items=[item.model_dump(mode='json') for item in items],
            max_size=max_size)
        return [PlaylistItemAddOutcome.model_validate(outcome) for outcome in result]

    async def delete_item(self, item_id: int) -> bool:
        '''
        Delete one item by row id. False when it is already gone.

        item_id : PlaylistItem row id
        '''
        async with self._span('delete_item'):
            return await self._call('delete_item', {'item_id': item_id})

    async def delete_item_by_index(self, playlist_id: int,
                                   index: int) -> PlaylistItemEntry | None:
        '''
        Delete the item at a zero-based position, returning what was deleted.

        Zero is a position, not an absence -- the first item in the list is the
        most commonly deleted one.

        playlist_id : Playlist row id
        index : Zero-based position in list order
        '''
        result = await self._playlist_call('delete_item_by_index', playlist_id, index=index)
        return PlaylistItemEntry.model_validate(result) if result else None

    async def record_history_item(self, playlist_id: int, item: PlaylistItemWrite,
                                  max_size: int) -> bool:
        '''
        Write one played track to the history playlist, evicting to make room.

        One request for what the post-play loop ran as six queries.

        playlist_id : History playlist row id
        item : The track that just played
        max_size : Ceiling on the playlist's item count
        '''
        return await self._playlist_call(
            'record_history_item', playlist_id,
            item=item.model_dump(mode='json'), max_size=max_size)
