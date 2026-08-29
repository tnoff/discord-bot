"""
Database functions for music-related operations.
Centralized location for all database functions that take db_session: AsyncSession as first parameter.
"""
from datetime import datetime, timezone

from sqlalchemy import select, delete, asc
from sqlalchemy.sql.functions import count as sql_count
from sqlalchemy.ext.asyncio import AsyncSession

from discord_bot.database import (
    VideoCache, Guild,
    Playlist, PlaylistItem, GuildVideoAnalytics
)

#
# Guild Analytics Functions
#

async def ensure_guild_video_analytics(db_session: AsyncSession, guild_id: int):
    '''
    Ensure guild video analytics table exists
    '''
    guild = await ensure_guild(db_session, guild_id)
    existing = (await db_session.execute(
        select(GuildVideoAnalytics).where(GuildVideoAnalytics.guild_id == guild.id)
    )).scalars().first()
    if existing:
        return existing
    now_timestamp = datetime.now(timezone.utc)
    new_row = GuildVideoAnalytics(
        guild_id=guild.id,
        total_plays=0,
        cached_plays=0,
        total_duration_seconds=0,
        created_at=now_timestamp,
        updated_at=now_timestamp,
    )
    db_session.add(new_row)
    await db_session.commit()
    return new_row

async def update_video_guild_analytics(db_session: AsyncSession, guild_id: int, duration: int, cache_hit: bool):
    '''
    Update video guild analytics from history item
    '''
    guild_analytics = await ensure_guild_video_analytics(db_session, guild_id)
    guild_analytics.total_plays += 1
    new_duration = guild_analytics.total_duration_seconds + duration
    new_days = new_duration // ( 60 * 60 * 24) # 1 day in seconds
    new_duration = new_duration % ( 60 * 60 * 24)
    guild_analytics.total_duration_days += new_days
    guild_analytics.total_duration_seconds = new_duration
    if cache_hit:
        guild_analytics.cached_plays += 1
    guild_analytics.updated_at = datetime.now(timezone.utc)
    await db_session.commit()
    return True

#
# Guild Functions
#

async def ensure_guild(db_session: AsyncSession, guild_id: int):
    '''
    Find existing guild or create new one
    '''
    existing = (await db_session.execute(
        select(Guild).where(Guild.server_id == guild_id)
    )).scalars().first()
    if existing:
        return existing
    new_guild = Guild(server_id=guild_id)
    db_session.add(new_guild)
    await db_session.commit()
    return new_guild

#
# VideoCache Functions
#

async def list_video_cache_where_delete_ready(db_session: AsyncSession):
    """List cache files ready for processing"""
    return (await db_session.execute(
        select(VideoCache).where(VideoCache.ready_for_deletion == True)  # noqa: E712
    )).scalars().all()

async def get_video_cache_by_url(db_session: AsyncSession, webpage_url: str):
    """Get video cache by url"""
    return (await db_session.execute(
        select(VideoCache).where(VideoCache.video_url == webpage_url)
    )).scalars().first()

async def delete_video_cache(db_session: AsyncSession, video_cache_id: int):
    """Remove video cache with guild associations"""
    item = await db_session.get(VideoCache, video_cache_id)
    if not item:
        return False
    await db_session.delete(item)
    await db_session.commit()
    return True


async def count_video_cache(db_session: AsyncSession):
    """Get video cache count"""
    return (await db_session.execute(
        select(sql_count()).select_from(VideoCache)
    )).scalar()


async def video_cache_mark_deletion(db_session: AsyncSession, num_to_remove: int):
    """Mark items for deletion based on last iterated timestamp"""
    items = (await db_session.execute(
        select(VideoCache).order_by(asc(VideoCache.last_iterated_at)).limit(num_to_remove)
    )).scalars().all()
    for video_cache in items:
        video_cache.ready_for_deletion = True
    await db_session.commit()


async def video_cache_mark_deletion_for_size(db_session: AsyncSession, max_size_bytes: int):
    '''Mark oldest non-flagged entries for deletion until total size <= max_size_bytes.
    Already-flagged entries are excluded from the total so count and size eviction compose correctly.'''
    entries = (await db_session.execute(
        select(VideoCache)
        .where(VideoCache.ready_for_deletion == False)  # noqa: E712
        .order_by(asc(VideoCache.last_iterated_at))
    )).scalars().all()
    total = sum(e.file_size_bytes or 0 for e in entries)
    for entry in entries:
        if total <= max_size_bytes:
            break
        entry.ready_for_deletion = True
        total -= (entry.file_size_bytes or 0)
    await db_session.commit()


#
# Playlist Functions
#


# History Functions

async def get_history_playlist(db_session: AsyncSession, guild_id: int):
    """Find history playlist for guild"""
    return (await db_session.execute(
        select(Playlist)
        .where(Playlist.server_id == guild_id)
        .where(Playlist.is_history == True)  # noqa: E712
    )).scalars().first()


# Regular Playlist Functions

async def list_playlist_non_history(db_session: AsyncSession, guild_id: int, offset: int):
    """List non-history playlists for guild, newest first.

    The order defines the public index users type (`!playlist 1`), so it is
    part of the interface rather than a detail.

    It briefly shipped as `asc`, on the theory that `created_at` was NULL on
    every row and the DESC had therefore never taken effect. That was measured
    against a fresh database and was true of `playlist_item` -- 1463 rows with
    no timestamp -- but NOT of `playlist`, where all 32 production rows carried
    distinct timestamps. So the DESC had been working all along, the public
    index has always been newest-first, and shipping `asc` reversed the
    numbering for every guild with more than one playlist (one has sixteen).
    Restored, and the id tiebreak kept: rows added in one commit can share a
    timestamp, and a tie in postgres means heap order.
    """
    return (await db_session.execute(
        select(Playlist)
        .where(Playlist.server_id == guild_id)
        .where(Playlist.is_history == False)  # noqa: E712
        .order_by(Playlist.created_at.desc(), Playlist.id.desc())
        .offset(offset)
    )).scalars().all()

async def get_playlist(db_session: AsyncSession, playlist_id: int):
    """Get playlist by id"""
    return await db_session.get(Playlist, playlist_id)

async def playlist_count(db_session: AsyncSession, guild_id: int):
    """Check playlist count for guild"""
    return (await db_session.execute(
        select(sql_count()).select_from(Playlist)
        .where(Playlist.server_id == guild_id)
        .where(Playlist.is_history == False)  # noqa: E712
    )).scalar()

async def get_playlist_by_name_and_guild(db_session: AsyncSession, name: str, guild_id: int):
    """Check if playlist exists with name in guild"""
    return (await db_session.execute(
        select(Playlist)
        .where(Playlist.name == name)
        .where(Playlist.server_id == guild_id)
    )).scalars().first()

async def get_playlist_size(db_session: AsyncSession, playlist_id: int):
    """Get playlist size"""
    return (await db_session.execute(
        select(sql_count()).select_from(PlaylistItem)
        .where(PlaylistItem.playlist_id == playlist_id)
    )).scalar()

async def delete_playlist(db_session: AsyncSession, playlist_id: int):
    """Delete playlist and all its items"""
    await db_session.execute(
        delete(PlaylistItem).where(PlaylistItem.playlist_id == playlist_id)
    )
    playlist = await db_session.get(Playlist, playlist_id)
    if playlist:
        await db_session.delete(playlist)
    await db_session.commit()

async def rename_playlist(db_session: AsyncSession, playlist_id: int, playlist_name: str):
    """Rename playlist"""
    playlist = await db_session.get(Playlist, playlist_id)
    if playlist:
        playlist.name = playlist_name
        await db_session.commit()
        return True
    return False

async def get_playlist_name(db_session: AsyncSession, playlist_id: int):
    """Get playlist name"""
    playlist = await db_session.get(Playlist, playlist_id)
    return playlist.name if playlist else None


async def update_playlist_queued_at(db_session: AsyncSession, playlist_id: int):
    """Mark a playlist as queued.

    Writes `last_queued`, the mapped column. It used to assign
    `last_queued_at`, a name that exists nowhere in the schema -- SQLAlchemy
    accepts an assignment to an unmapped attribute silently, so the commit
    emitted no UPDATE and the column stayed NULL for every playlist ever
    queued. `!playlist list` reads it back and has therefore always shown N/A.
    """
    playlist = await db_session.get(Playlist, playlist_id)
    if playlist:
        playlist.last_queued = datetime.now(timezone.utc)
        await db_session.commit()

#
# PlaylistItem Functions
#


async def delete_playlist_item_by_url(db_session: AsyncSession, webpage_url: str, playlist_id: int):
    """Delete existing playlist item by URL"""
    item = (await db_session.execute(
        select(PlaylistItem)
        .where(PlaylistItem.video_url == webpage_url)
        .where(PlaylistItem.playlist_id == playlist_id)
    )).scalars().first()
    if item:
        await db_session.delete(item)
        await db_session.commit()


async def delete_playlist_item_limit(db_session: AsyncSession, playlist_id: int, delta: int):
    """Delete extra items from playlist"""
    items = (await db_session.execute(
        select(PlaylistItem)
        .where(PlaylistItem.playlist_id == playlist_id)
        .order_by(PlaylistItem.created_at.asc(), PlaylistItem.id.asc())
        .limit(delta)
    )).scalars().all()
    for item in items:
        await db_session.delete(item)
    await db_session.commit()

async def list_playlist_items(db_session: AsyncSession, playlist_id: int):
    """Get playlist items by playlist id"""
    return (await db_session.execute(
        select(PlaylistItem)
        .where(PlaylistItem.playlist_id == playlist_id)
        .order_by(PlaylistItem.created_at.asc(), PlaylistItem.id.asc())
    )).scalars().all()

async def get_playlist_item_by_url(db_session: AsyncSession, playlist_id: int, video_url: str):
    """Check if item exists in playlist"""
    return (await db_session.execute(
        select(PlaylistItem)
        .where(PlaylistItem.playlist_id == playlist_id)
        .where(PlaylistItem.video_url == video_url)
    )).scalars().first()

async def delete_playlist_item_by_index(db_session: AsyncSession, playlist_id: int, index_id: int):
    """Remove playlist item by index"""
    items = (await db_session.execute(
        select(PlaylistItem)
        .where(PlaylistItem.playlist_id == playlist_id)
        .order_by(PlaylistItem.created_at.asc(), PlaylistItem.id.asc())
    )).scalars().all()
    if 0 <= index_id < len(items):
        item_to_delete = items[index_id]
        await db_session.delete(item_to_delete)
        await db_session.commit()
        return item_to_delete
    return None
