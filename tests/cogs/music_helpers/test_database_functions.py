from datetime import datetime, timezone, timedelta

import pytest
from sqlalchemy import select
from sqlalchemy.sql.functions import count as sql_count

from discord_bot.database import GuildVideoAnalytics, VideoCache, VideoCacheBackup, Playlist
from discord_bot.cogs.music_helpers.database_functions import (
    ensure_guild_video_analytics, update_video_guild_analytics,
    video_cache_mark_deletion_for_size,
    list_video_cache, get_video_cache_by_id, delete_video_cache,
    list_video_cache_where_no_backup, get_video_cache_backup,
    delete_video_cache_backup, rename_playlist,
)

from tests.helpers import fake_engine, fake_context, async_mock_session #pylint:disable=unused-import


@pytest.mark.asyncio
async def test_ensure_guild_video_analytics_creates_new(fake_engine, fake_context):  #pylint:disable=redefined-outer-name
    '''Test that ensure_guild_video_analytics creates a new analytics record'''
    async with async_mock_session(fake_engine) as session:
        # Verify no analytics exist initially
        assert (await session.execute(select(sql_count()).select_from(GuildVideoAnalytics))).scalar() == 0

        # Call the function
        analytics = await ensure_guild_video_analytics(session, fake_context['guild'].id)

        # Verify analytics record was created
        assert analytics is not None
        assert analytics.total_plays == 0
        assert analytics.cached_plays == 0
        assert analytics.total_duration_seconds == 0
        assert analytics.created_at is not None
        assert analytics.updated_at is not None

        # Verify it was persisted to database
        assert (await session.execute(select(sql_count()).select_from(GuildVideoAnalytics))).scalar() == 1


@pytest.mark.asyncio
async def test_ensure_guild_video_analytics_returns_existing(fake_engine, fake_context):  #pylint:disable=redefined-outer-name
    '''Test that ensure_guild_video_analytics returns existing analytics record'''
    async with async_mock_session(fake_engine) as session:
        # Create existing analytics record
        analytics1 = await ensure_guild_video_analytics(session, fake_context['guild'].id)
        original_id = analytics1.id

        # Call function again
        analytics2 = await ensure_guild_video_analytics(session, fake_context['guild'].id)

        # Verify same record is returned
        assert analytics2.id == original_id
        assert (await session.execute(select(sql_count()).select_from(GuildVideoAnalytics))).scalar() == 1


@pytest.mark.asyncio
async def test_update_video_guild_analytics_basic(fake_engine, fake_context):  #pylint:disable=redefined-outer-name
    '''Test basic update of guild video analytics'''
    async with async_mock_session(fake_engine) as session:
        # Update analytics with a duration
        duration = 3600  # 1 hour in seconds
        result = await update_video_guild_analytics(session, fake_context['guild'].id, duration, False)

        # Verify function returned True
        assert result is True

        # Get the analytics record
        analytics = await ensure_guild_video_analytics(session, fake_context['guild'].id)

        # Verify stats were updated
        assert analytics.total_plays == 1
        assert analytics.cached_plays == 0
        assert analytics.total_duration_seconds == 3600
        assert analytics.total_duration_days == 0


@pytest.mark.asyncio
async def test_update_video_guild_analytics_with_cache_hit(fake_engine, fake_context):  #pylint:disable=redefined-outer-name
    '''Test that cache hits are tracked correctly'''
    async with async_mock_session(fake_engine) as session:
        # Update with cache hit
        duration = 1800  # 30 minutes
        await update_video_guild_analytics(session, fake_context['guild'].id, duration, True)

        # Get the analytics record
        analytics = await ensure_guild_video_analytics(session, fake_context['guild'].id)

        # Verify cache hit was counted
        assert analytics.total_plays == 1
        assert analytics.cached_plays == 1
        assert analytics.total_duration_seconds == 1800


@pytest.mark.asyncio
async def test_update_video_guild_analytics_days_calculation(fake_engine, fake_context):  #pylint:disable=redefined-outer-name
    '''Test that days are calculated correctly when duration exceeds 24 hours'''
    async with async_mock_session(fake_engine) as session:
        # Add duration that exceeds one day
        one_day_seconds = 60 * 60 * 24
        duration = one_day_seconds + 3600  # 1 day and 1 hour

        await update_video_guild_analytics(session, fake_context['guild'].id, duration, False)

        # Get the analytics record
        analytics = await ensure_guild_video_analytics(session, fake_context['guild'].id)

        # Verify days and remaining seconds
        assert analytics.total_plays == 1
        assert analytics.total_duration_days == 1
        assert analytics.total_duration_seconds == 3600


@pytest.mark.asyncio
async def test_update_video_guild_analytics_multiple_updates(fake_engine, fake_context):  #pylint:disable=redefined-outer-name
    '''Test that multiple updates accumulate correctly'''
    async with async_mock_session(fake_engine) as session:
        # First update
        await update_video_guild_analytics(session, fake_context['guild'].id, 1800, False)

        # Second update with cache hit
        await update_video_guild_analytics(session, fake_context['guild'].id, 3600, True)

        # Third update
        await update_video_guild_analytics(session, fake_context['guild'].id, 7200, False)

        # Get the analytics record
        analytics = await ensure_guild_video_analytics(session, fake_context['guild'].id)

        # Verify accumulated stats
        assert analytics.total_plays == 3
        assert analytics.cached_plays == 1
        assert analytics.total_duration_seconds == 1800 + 3600 + 7200


@pytest.mark.asyncio
async def test_update_video_guild_analytics_rollover_to_days(fake_engine, fake_context):  #pylint:disable=redefined-outer-name
    '''Test that seconds correctly roll over into days'''
    async with async_mock_session(fake_engine) as session:
        # Add 20 hours
        await update_video_guild_analytics(session, fake_context['guild'].id, 20 * 3600, False)

        analytics = await ensure_guild_video_analytics(session, fake_context['guild'].id)
        assert analytics.total_duration_days == 0
        assert analytics.total_duration_seconds == 20 * 3600

        # Add another 10 hours (should push us over 1 day)
        await update_video_guild_analytics(session, fake_context['guild'].id, 10 * 3600, False)

        analytics = await ensure_guild_video_analytics(session, fake_context['guild'].id)
        assert analytics.total_duration_days == 1
        assert analytics.total_duration_seconds == 6 * 3600  # 30 - 24 = 6 hours remaining


@pytest.mark.asyncio
async def test_update_video_guild_analytics_multiple_days(fake_engine, fake_context):  #pylint:disable=redefined-outer-name
    '''Test handling of multiple days worth of content'''
    async with async_mock_session(fake_engine) as session:
        one_day_seconds = 60 * 60 * 24

        # Add 2.5 days worth of content
        duration = int(2.5 * one_day_seconds)
        await update_video_guild_analytics(session, fake_context['guild'].id, duration, False)

        analytics = await ensure_guild_video_analytics(session, fake_context['guild'].id)
        assert analytics.total_duration_days == 2
        assert analytics.total_duration_seconds == 12 * 3600  # 0.5 days = 12 hours


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
async def test_list_video_cache(fake_engine):  #pylint:disable=redefined-outer-name
    '''list_video_cache returns all cache entries'''
    async with async_mock_session(fake_engine) as session:
        await _make_video_cache(session, url='https://a.com')
        await _make_video_cache(session, url='https://b.com')

        result = await list_video_cache(session)

    assert len(result) == 2


@pytest.mark.asyncio
async def test_get_video_cache_by_id(fake_engine):  #pylint:disable=redefined-outer-name
    '''get_video_cache_by_id returns the correct entry'''
    async with async_mock_session(fake_engine) as session:
        item = await _make_video_cache(session)

        result = await get_video_cache_by_id(session, item.id)

    assert result.id == item.id


@pytest.mark.asyncio
async def test_delete_video_cache_returns_false_when_not_found(fake_engine):  #pylint:disable=redefined-outer-name
    '''delete_video_cache returns False when the id does not exist'''
    async with async_mock_session(fake_engine) as session:
        result = await delete_video_cache(session, 99999)

    assert result is False


# ---------------------------------------------------------------------------
# VideoCacheBackup functions
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_list_video_cache_where_no_backup(fake_engine):  #pylint:disable=redefined-outer-name
    '''list_video_cache_where_no_backup excludes entries that have a backup'''
    async with async_mock_session(fake_engine) as session:
        with_backup = await _make_video_cache(session, url='https://backed-up.com')
        without_backup = await _make_video_cache(session, url='https://no-backup.com')
        with_backup_id = with_backup.id
        without_backup_id = without_backup.id

        backup = VideoCacheBackup(
            video_cache_id=with_backup_id,
            storage='s3', bucket_name='my-bucket', object_path='path/file.mp4',
        )
        session.add(backup)
        await session.commit()

        result = await list_video_cache_where_no_backup(session)
        ids = [r.id for r in result]

    assert without_backup_id in ids
    assert with_backup_id not in ids


@pytest.mark.asyncio
async def test_get_video_cache_backup(fake_engine):  #pylint:disable=redefined-outer-name
    '''get_video_cache_backup returns the backup entry for a given cache id'''
    async with async_mock_session(fake_engine) as session:
        item = await _make_video_cache(session)
        backup = VideoCacheBackup(
            video_cache_id=item.id,
            storage='s3', bucket_name='my-bucket', object_path='path/file.mp4',
        )
        session.add(backup)
        await session.commit()

        result = await get_video_cache_backup(session, item.id)

    assert result is not None
    assert result.video_cache_id == item.id


@pytest.mark.asyncio
async def test_delete_video_cache_backup_returns_false_when_not_found(fake_engine):  #pylint:disable=redefined-outer-name
    '''delete_video_cache_backup returns False when the id does not exist'''
    async with async_mock_session(fake_engine) as session:
        result = await delete_video_cache_backup(session, 99999)

    assert result is False


@pytest.mark.asyncio
async def test_delete_video_cache_backup_removes_entry(fake_engine):  #pylint:disable=redefined-outer-name
    '''delete_video_cache_backup removes the entry and returns True'''
    async with async_mock_session(fake_engine) as session:
        item = await _make_video_cache(session)
        backup = VideoCacheBackup(
            video_cache_id=item.id,
            storage='s3', bucket_name='my-bucket', object_path='path/file.mp4',
        )
        session.add(backup)
        await session.commit()

        result = await delete_video_cache_backup(session, backup.id)
        remaining = await get_video_cache_backup(session, item.id)

    assert result is True
    assert remaining is None


# ---------------------------------------------------------------------------
# Playlist functions
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_rename_playlist_returns_false_when_not_found(fake_engine):  #pylint:disable=redefined-outer-name
    '''rename_playlist returns False when the playlist id does not exist'''
    async with async_mock_session(fake_engine) as session:
        result = await rename_playlist(session, 99999, 'new name')

    assert result is False


@pytest.mark.asyncio
async def test_rename_playlist_returns_true_and_updates(fake_engine):  #pylint:disable=redefined-outer-name
    '''rename_playlist returns True and persists the new name'''
    async with async_mock_session(fake_engine) as session:
        playlist = Playlist(server_id='1', name='old name', is_history=False)
        session.add(playlist)
        await session.commit()

        result = await rename_playlist(session, playlist.id, 'new name')
        updated = await session.get(Playlist, playlist.id)

    assert result is True
    assert updated.name == 'new name'
