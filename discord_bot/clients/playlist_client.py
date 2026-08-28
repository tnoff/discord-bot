'''
In-process PlaylistStore: the playlist tables, over the local engine.

Satisfies interfaces.database_protocols.PlaylistStore, which the Music cog
annotates against. Same shape as MarkovClient and VideoCacheClient -- a session
generator handed in by the caller.

Three things concentrate here that were spread across `cogs/music.py`:

**The max-size ceiling.** `__playlist_insert_item` counted the items, compared
the count to the limit, and then inserted, with the caller's session open across
all three. That is a check-then-act on a table two players can be writing to,
and the gap widens the moment the store is remote. `add_items` does the count,
the decision and the insert in one transaction, and enforces the ceiling once
per item rather than once per batch.

**The history write.** The post-play loop ran a delete-by-url, a count, a
conditional bulk delete and an insert -- four statements plus the insert's own
count and duplicate check. `record_history_item` is that sequence, named, in one
call.

**Sessions held across network I/O.** `!playlist queue` kept a session open
while dispatching searches and enqueuing downloads, and `!playlist merge` kept
one open while sending a Discord message per copied item. Neither needs a
database connection for that work; both had one because the rows they were
iterating were live.
'''
from typing import List

from dappertable import shorten_string
from opentelemetry.trace import SpanKind
from sqlalchemy import delete as sa_delete, select
from sqlalchemy.sql.functions import count as sql_count

from discord_bot.clients.session_store import SessionStoreBase
from discord_bot.cogs.music_helpers.common import PLAYHISTORY_PREFIX
from discord_bot.database import Playlist, PlaylistItem, utcnow
from discord_bot.types.playlist import (
    PlaylistEntry,
    PlaylistItemAddOutcome,
    PlaylistItemAddStatus,
    PlaylistItemEntry,
    PlaylistItemWrite,
)
from discord_bot.utils.otel import async_otel_span_wrapper, DiscordContextNaming
from discord_bot.utils.sql_retry import async_retry_database_commands

OTEL_SPAN_PREFIX = 'music.playlist_store'

# Both columns are varchar(256); a longer value is truncated rather than
# rejected, which is what the cog did before this moved.
_STRING_COLUMN_WIDTH = 256


def history_playlist_name(guild_id: int) -> str:
    '''
    Build the internal name for a guild's history playlist.

    The timestamp suffix is not meaningful -- the row is found by
    `is_history`, never by name. It exists because the table has a unique
    constraint on (name, server_id) and the prefix alone would collide with a
    history playlist that had been deleted and recreated.

    guild_id : Discord guild id
    '''
    return f'{PLAYHISTORY_PREFIX}{guild_id}_{utcnow().timestamp()}'


def _items_in_order(playlist_id: int):
    '''
    Select a playlist's items in the order the public interface promises.

    The id tiebreak is not decoration: rows written before `created_at` had a
    default all tie on the backfilled value's neighbours, and rows added in one
    commit can share a timestamp. A tie in postgres is heap order, which moves
    as rows are deleted and reinserted -- what the history playlist does on
    every play.

    playlist_id : Playlist row id
    '''
    return (
        select(PlaylistItem)
        .where(PlaylistItem.playlist_id == playlist_id)
        .order_by(PlaylistItem.created_at.asc(), PlaylistItem.id.asc())
    )


class PlaylistClient(SessionStoreBase):
    '''
    The playlist tables -- the in-process PlaylistStore.

    Server playlists, the per-guild history playlist, and the items in both.
    '''

    async def list_playlists(self, guild_id: int) -> List[PlaylistEntry]:
        '''
        Return a guild's non-history playlists, newest first.

        The order is the public index users type (`!playlist 1`). It shipped
        briefly as oldest-first on the theory that `created_at` was NULL
        everywhere and the DESC had never applied -- true of `playlist_item`,
        false of `playlist`, where every production row already carried a
        distinct timestamp. Restored; see list_playlist_non_history.

        guild_id : Discord guild id
        '''
        attributes = {DiscordContextNaming.GUILD.value: guild_id}
        async with async_otel_span_wrapper(f'{OTEL_SPAN_PREFIX}.list_playlists',
                                           kind=SpanKind.INTERNAL, attributes=attributes):
            return await self._select_all(
                select(Playlist)
                .where(Playlist.server_id == guild_id)
                .where(Playlist.is_history == False)  # noqa: E712  pylint:disable=singleton-comparison
                .order_by(Playlist.created_at.desc(), Playlist.id.desc()),
                PlaylistEntry.from_row)

    async def count_playlists(self, guild_id: int) -> int:
        '''
        Return how many non-history playlists a guild has.

        guild_id : Discord guild id
        '''
        async with self.session_generator() as db_session:
            return await async_retry_database_commands(
                db_session,
                lambda: self.__count_playlists(db_session, guild_id))

    async def get_playlist(self, playlist_id: int) -> PlaylistEntry | None:
        '''
        Return one playlist by row id, or None.

        playlist_id : Playlist row id
        '''
        async with self.session_generator() as db_session:
            row = await async_retry_database_commands(
                db_session, lambda: db_session.get(Playlist, playlist_id))
            return PlaylistEntry.from_row(row) if row else None

    async def get_playlist_by_name(self, guild_id: int, name: str) -> PlaylistEntry | None:
        '''
        Return a guild's playlist with this name, or None.

        guild_id : Discord guild id
        name : Playlist name to look for
        '''
        async with self.session_generator() as db_session:
            row = await async_retry_database_commands(
                db_session,
                lambda: self.__playlist_by_name(db_session, guild_id, name))
            return PlaylistEntry.from_row(row) if row else None

    async def get_history_playlist(self, guild_id: int) -> PlaylistEntry | None:
        '''
        Return a guild's history playlist, or None when it has none yet.

        guild_id : Discord guild id
        '''
        async with self.session_generator() as db_session:
            row = await async_retry_database_commands(
                db_session, lambda: self.__history_playlist(db_session, guild_id))
            return PlaylistEntry.from_row(row) if row else None

    async def ensure_history_playlist(self, guild_id: int) -> int:
        '''
        Return the guild's history playlist id, creating it if absent.

        guild_id : Discord guild id
        '''
        attributes = {DiscordContextNaming.GUILD.value: guild_id}
        async with async_otel_span_wrapper(f'{OTEL_SPAN_PREFIX}.ensure_history_playlist',
                                           kind=SpanKind.INTERNAL, attributes=attributes):
            async with self.session_generator() as db_session:
                row = await async_retry_database_commands(
                    db_session, lambda: self.__history_playlist(db_session, guild_id))
                if row:
                    return row.id
                history_playlist = Playlist(
                    server_id=guild_id,
                    name=history_playlist_name(guild_id),
                    is_history=True,
                )
                db_session.add(history_playlist)
                await async_retry_database_commands(db_session, db_session.commit)
                return history_playlist.id

    async def create_playlist(self, guild_id: int, name: str) -> PlaylistEntry:
        '''
        Create a playlist and return it.

        guild_id : Discord guild id
        name : Playlist name
        '''
        attributes = {DiscordContextNaming.GUILD.value: guild_id}
        async with async_otel_span_wrapper(f'{OTEL_SPAN_PREFIX}.create_playlist',
                                           kind=SpanKind.INTERNAL, attributes=attributes):
            async with self.session_generator() as db_session:
                playlist = Playlist(
                    name=name,
                    server_id=guild_id,
                    is_history=False,
                )
                db_session.add(playlist)
                await async_retry_database_commands(db_session, db_session.commit)
                return PlaylistEntry.from_row(playlist)

    async def delete_playlist(self, playlist_id: int) -> bool:
        '''
        Delete a playlist and every item in it.

        playlist_id : Playlist row id
        '''
        async with async_otel_span_wrapper(f'{OTEL_SPAN_PREFIX}.delete_playlist', kind=SpanKind.INTERNAL):
            async with self.session_generator() as db_session:
                async def delete_records():
                    await db_session.execute(
                        sa_delete(PlaylistItem).where(PlaylistItem.playlist_id == playlist_id))
                    playlist = await db_session.get(Playlist, playlist_id)
                    if playlist:
                        await db_session.delete(playlist)
                    await db_session.commit()

                await async_retry_database_commands(db_session, delete_records)
                return True

    async def rename_playlist(self, playlist_id: int, name: str) -> bool:
        '''
        Rename a playlist. False when there is no such playlist.

        playlist_id : Playlist row id
        name : New name
        '''
        async with self.session_generator() as db_session:
            async def rename():
                playlist = await db_session.get(Playlist, playlist_id)
                if not playlist:
                    return False
                playlist.name = name
                await db_session.commit()
                return True

            return await async_retry_database_commands(db_session, rename)

    async def mark_queued(self, playlist_id: int) -> bool:
        '''
        Record that a playlist was just queued.

        playlist_id : Playlist row id
        '''
        async with self.session_generator() as db_session:
            async def mark():
                playlist = await db_session.get(Playlist, playlist_id)
                if not playlist:
                    return False
                # `last_queued`, the mapped column. Assigning `last_queued_at`
                # here wrote nothing at all and went unnoticed for the life of
                # the feature -- see the migration that backfilled around it.
                playlist.last_queued = utcnow()
                await db_session.commit()
                return True

            return await async_retry_database_commands(db_session, mark)

    async def get_playlist_size(self, playlist_id: int) -> int:
        '''
        Return how many items a playlist holds.

        playlist_id : Playlist row id
        '''
        async with self.session_generator() as db_session:
            return await async_retry_database_commands(
                db_session, lambda: self.__playlist_size(db_session, playlist_id))

    async def list_items(self, playlist_id: int) -> List[PlaylistItemEntry]:
        '''
        Return a playlist's items, oldest first.

        playlist_id : Playlist row id
        '''
        async with async_otel_span_wrapper(f'{OTEL_SPAN_PREFIX}.list_items', kind=SpanKind.INTERNAL):
            return await self._select_all(_items_in_order(playlist_id),
                                          PlaylistItemEntry.from_row)

    async def add_items(self, playlist_id: int, items: List[PlaylistItemWrite],
                        max_size: int) -> List[PlaylistItemAddOutcome]:
        '''
        Add items to a playlist, stopping when it is full.

        playlist_id : Playlist row id
        items : Items to add, in order
        max_size : Ceiling on the playlist's item count
        '''
        async with async_otel_span_wrapper(f'{OTEL_SPAN_PREFIX}.add_items', kind=SpanKind.INTERNAL):
            outcomes = []
            async with self.session_generator() as db_session:
                for item in items:
                    outcome = await async_retry_database_commands(
                        db_session,
                        lambda item=item: self.__insert_item(db_session, playlist_id, item, max_size))
                    outcomes.append(outcome)
                    if outcome.status == PlaylistItemAddStatus.PLAYLIST_FULL:
                        break
            return outcomes

    async def delete_item(self, item_id: int) -> bool:
        '''
        Delete one item by row id. False when it is already gone.

        item_id : PlaylistItem row id
        '''
        return await self._delete_row(PlaylistItem, item_id)

    async def delete_item_by_index(self, playlist_id: int,
                                   index: int) -> PlaylistItemEntry | None:
        '''
        Delete the item at a zero-based position, returning what was deleted.

        playlist_id : Playlist row id
        index : Zero-based position in list order
        '''
        async with async_otel_span_wrapper(f'{OTEL_SPAN_PREFIX}.delete_item_by_index', kind=SpanKind.INTERNAL):
            async with self.session_generator() as db_session:
                async def delete_at_index():
                    rows = (await db_session.execute(_items_in_order(playlist_id))).scalars().all()
                    if not 0 <= index < len(rows):
                        return None
                    target = rows[index]
                    # Built before the delete: afterwards the instance is
                    # detached and the caller wants its title for a message.
                    entry = PlaylistItemEntry.from_row(target)
                    await db_session.delete(target)
                    await db_session.commit()
                    return entry

                return await async_retry_database_commands(db_session, delete_at_index)

    async def record_history_item(self, playlist_id: int, item: PlaylistItemWrite,
                                  max_size: int) -> bool:
        '''
        Write one played track to the history playlist, evicting to make room.

        playlist_id : History playlist row id
        item : The track that just played
        max_size : Ceiling on the playlist's item count
        '''
        async with async_otel_span_wrapper(f'{OTEL_SPAN_PREFIX}.record_history_item', kind=SpanKind.INTERNAL):
            async with self.session_generator() as db_session:
                async def record():
                    playlist = await db_session.get(Playlist, playlist_id)
                    if not playlist:
                        return False
                    # Move-to-most-recent: the same track played again should
                    # sit at the end of history, not keep its original position.
                    existing = (await db_session.execute(
                        select(PlaylistItem)
                        .where(PlaylistItem.video_url == item.video_url)
                        .where(PlaylistItem.playlist_id == playlist_id))).scalars().first()
                    if existing:
                        await db_session.delete(existing)
                        await db_session.flush()

                    delta = (await self.__playlist_size(db_session, playlist_id) + 1) - max_size
                    if delta > 0:
                        oldest = (await db_session.execute(
                            _items_in_order(playlist_id).limit(delta))).scalars().all()
                        for row in oldest:
                            await db_session.delete(row)
                        await db_session.flush()

                    db_session.add(_new_item(playlist_id, item))
                    await db_session.commit()
                    return True

                return await async_retry_database_commands(db_session, record)

    async def __insert_item(self, db_session, playlist_id: int,
                            item: PlaylistItemWrite, max_size: int) -> PlaylistItemAddOutcome:
        '''
        Insert one item, reporting why it did or did not land.

        The count, the ceiling check and the write are one transaction here.
        Split across a caller they are a check-then-act, and two players saving
        queues at once can both pass the check.

        db_session : Sqlalchemy async db session
        playlist_id : Playlist row id
        item : Item to add
        max_size : Ceiling on the playlist's item count
        '''
        if await self.__playlist_size(db_session, playlist_id) >= max_size:
            return PlaylistItemAddOutcome(video_url=item.video_url, title=item.title,
                                          status=PlaylistItemAddStatus.PLAYLIST_FULL)
        existing = (await db_session.execute(
            select(PlaylistItem)
            .where(PlaylistItem.playlist_id == playlist_id)
            .where(PlaylistItem.video_url == item.video_url))).scalars().first()
        if existing:
            return PlaylistItemAddOutcome(video_url=item.video_url, title=item.title,
                                          status=PlaylistItemAddStatus.DUPLICATE)
        playlist_item = _new_item(playlist_id, item)
        db_session.add(playlist_item)
        await db_session.commit()
        return PlaylistItemAddOutcome(video_url=item.video_url, title=item.title,
                                      status=PlaylistItemAddStatus.ADDED,
                                      item_id=playlist_item.id)

    async def __playlist_size(self, db_session, playlist_id: int) -> int:
        '''
        Count a playlist's items.

        db_session : Sqlalchemy async db session
        playlist_id : Playlist row id
        '''
        return (await db_session.execute(
            select(sql_count()).select_from(PlaylistItem)
            .where(PlaylistItem.playlist_id == playlist_id))).scalar()

    async def __count_playlists(self, db_session, guild_id: int) -> int:
        '''
        Count a guild's non-history playlists.

        db_session : Sqlalchemy async db session
        guild_id : Discord guild id
        '''
        return (await db_session.execute(
            select(sql_count()).select_from(Playlist)
            .where(Playlist.server_id == guild_id)
            .where(Playlist.is_history == False))).scalar()  # noqa: E712  pylint:disable=singleton-comparison

    async def __history_playlist(self, db_session, guild_id: int):
        '''
        Return the live history Playlist row for a guild, or None.

        db_session : Sqlalchemy async db session
        guild_id : Discord guild id
        '''
        return (await db_session.execute(
            select(Playlist)
            .where(Playlist.server_id == guild_id)
            .where(Playlist.is_history == True))).scalars().first()  # noqa: E712  pylint:disable=singleton-comparison

    async def __playlist_by_name(self, db_session, guild_id: int, name: str):
        '''
        Return the live Playlist row matching a name in a guild, or None.

        db_session : Sqlalchemy async db session
        guild_id : Discord guild id
        name : Playlist name
        '''
        return (await db_session.execute(
            select(Playlist)
            .where(Playlist.name == name)
            .where(Playlist.server_id == guild_id))).scalars().first()


def _new_item(playlist_id: int, item: PlaylistItemWrite) -> PlaylistItem:
    '''
    Build a PlaylistItem, truncating to the columns' widths.

    playlist_id : Playlist row id
    item : Item to add
    '''
    return PlaylistItem(
        title=shorten_string(item.title, _STRING_COLUMN_WIDTH) if item.title else None,
        video_url=shorten_string(item.video_url, _STRING_COLUMN_WIDTH) if item.video_url else None,
        uploader=shorten_string(item.uploader, _STRING_COLUMN_WIDTH) if item.uploader else None,
        playlist_id=playlist_id,
    )
