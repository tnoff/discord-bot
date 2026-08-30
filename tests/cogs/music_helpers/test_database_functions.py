from datetime import datetime, timezone, timedelta

import pytest
from sqlalchemy import select

from discord_bot.database import VideoCache, Playlist, PlaylistItem
from discord_bot.cogs.music_helpers.database_functions import (
    video_cache_mark_deletion_for_size,
    delete_video_cache,
)

from tests.helpers import fake_engine, async_mock_session #pylint:disable=unused-import


async def _make_cache_entry(session, file_size_bytes, offset_seconds=0):
    now = datetime.now(timezone.utc)
    entry = VideoCache(
        video_id='vid',
        video_url=f'https://example.com/{offset_seconds}',
        title='t',
        uploader='u',
        duration=120,
        extractor='youtube',
        last_iterated_at=datetime(2020, 1, 1, tzinfo=timezone.utc) + timedelta(seconds=offset_seconds),
        created_at=now,
        base_path='/tmp/x',
        count=1,
        ready_for_deletion=False,
        file_size_bytes=file_size_bytes,
    )
    session.add(entry)
    await session.commit()
    return entry


@pytest.mark.asyncio
async def test_video_cache_mark_deletion_for_size(fake_engine):  #pylint:disable=redefined-outer-name
    '''video_cache_mark_deletion_for_size marks oldest entries until total <= budget'''
    async with async_mock_session(fake_engine) as session:
        # Three entries: 200, 300, 400 bytes, oldest first
        await _make_cache_entry(session, 200, offset_seconds=0)
        await _make_cache_entry(session, 300, offset_seconds=1)
        await _make_cache_entry(session, 400, offset_seconds=2)

        # Budget: 400 bytes; total is 900, so we must evict until <= 400
        # Evict oldest (200) → 700 still > 400
        # Evict next (300) → 400 <= 400 → stop
        await video_cache_mark_deletion_for_size(session, 400)

        flagged = (await session.execute(select(VideoCache).where(VideoCache.ready_for_deletion.is_(True)))).scalars().all()
        assert len(flagged) == 2
        flagged_sizes = sorted(e.file_size_bytes for e in flagged)
        assert flagged_sizes == [200, 300]


async def _make_video_cache(session, url='https://example.com/video', ready_for_deletion=False,
                      file_size_bytes=1000):
    now = datetime.now(timezone.utc)
    item = VideoCache(
        video_id='abc', video_url=url, title='Test', uploader='uploader',
        duration=60, extractor='youtube', last_iterated_at=now, created_at=now,
        count=1, ready_for_deletion=ready_for_deletion, file_size_bytes=file_size_bytes,
        base_path='/tmp/test.mp4',
    )
    session.add(item)
    await session.commit()
    return item


# ---------------------------------------------------------------------------
# VideoCache functions
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_delete_video_cache_returns_false_when_not_found(fake_engine):  #pylint:disable=redefined-outer-name
    '''delete_video_cache returns False when the id does not exist'''
    async with async_mock_session(fake_engine) as session:
        result = await delete_video_cache(session, 99999)

    assert result is False


# ---------------------------------------------------------------------------
# Playlist functions
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_playlist_rows_get_a_created_at_without_being_given_one(fake_engine):  #pylint:disable=redefined-outer-name
    '''Inserting a Playlist or PlaylistItem stamps created_at.

    No construction site ever passed it and the column carried no default, so
    every row in both tables had NULL here -- which is what made every
    `ORDER BY created_at` over them return heap order.
    '''
    async with async_mock_session(fake_engine) as session:
        playlist = Playlist(name='stamped', server_id=1, is_history=False)
        session.add(playlist)
        await session.commit()
        item = PlaylistItem(title='t', video_url='u', uploader='up', playlist_id=playlist.id)
        session.add(item)
        await session.commit()

    async with async_mock_session(fake_engine) as session:
        stored_playlist = (await session.execute(select(Playlist))).scalars().first()
        stored_item = (await session.execute(select(PlaylistItem))).scalars().first()
    assert stored_playlist.created_at is not None
    assert stored_item.created_at is not None
