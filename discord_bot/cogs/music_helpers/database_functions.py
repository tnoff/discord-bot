"""
Session-taking helpers for the `video_cache` catalog.

What is left of what used to be every database function in the music cog. The
markov, playlist and guild-analytics groups have crossed the seam into
`clients/`, and their helpers went with them -- these are the last functions
that take an open `AsyncSession` as their first argument.

They stay for now because `VideoCacheClient` calls them and it lives in the
broker, not the bot: `cache_cleanup` interleaves the catalog row, the Redis
checkout registry and the S3 object in one loop, so the orchestration cannot
follow the rows across without dragging the broker's Redis registry into the
persistence tier. See interfaces/database_protocols.VideoCacheStore.
"""
from sqlalchemy import select, asc
from sqlalchemy.sql.functions import count as sql_count
from sqlalchemy.ext.asyncio import AsyncSession

from discord_bot.database import VideoCache

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
