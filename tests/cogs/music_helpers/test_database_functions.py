from datetime import datetime, timezone, timedelta

from discord_bot.database import GuildVideoAnalytics, VideoCache, VideoCacheBackup, Playlist
from discord_bot.cogs.music_helpers.database_functions import (
    ensure_guild_video_analytics, update_video_guild_analytics,
    video_cache_mark_deletion_for_size,
    list_video_cache, get_video_cache_by_id, delete_video_cache,
    list_video_cache_where_no_backup, get_video_cache_backup,
    delete_video_cache_backup, rename_playlist,
)

from tests.helpers import fake_engine, fake_context, mock_session #pylint:disable=unused-import


def test_ensure_guild_video_analytics_creates_new(fake_engine, fake_context):  #pylint:disable=redefined-outer-name
    '''Test that ensure_guild_video_analytics creates a new analytics record'''
    with mock_session(fake_engine) as session:
        # Verify no analytics exist initially
        assert session.query(GuildVideoAnalytics).count() == 0

        # Call the function
        analytics = ensure_guild_video_analytics(session, fake_context['guild'].id)

        # Verify analytics record was created
        assert analytics is not None
        assert analytics.total_plays == 0
        assert analytics.cached_plays == 0
        assert analytics.total_duration_seconds == 0
        assert analytics.created_at is not None
        assert analytics.updated_at is not None

        # Verify it was persisted to database
        assert session.query(GuildVideoAnalytics).count() == 1


def test_ensure_guild_video_analytics_returns_existing(fake_engine, fake_context):  #pylint:disable=redefined-outer-name
    '''Test that ensure_guild_video_analytics returns existing analytics record'''
    with mock_session(fake_engine) as session:
        # Create existing analytics record
        analytics1 = ensure_guild_video_analytics(session, fake_context['guild'].id)
        original_id = analytics1.id

        # Call function again
        analytics2 = ensure_guild_video_analytics(session, fake_context['guild'].id)

        # Verify same record is returned
        assert analytics2.id == original_id
        assert session.query(GuildVideoAnalytics).count() == 1


def test_update_video_guild_analytics_basic(fake_engine, fake_context):  #pylint:disable=redefined-outer-name
    '''Test basic update of guild video analytics'''
    with mock_session(fake_engine) as session:
        # Update analytics with a duration
        duration = 3600  # 1 hour in seconds
        result = update_video_guild_analytics(session, fake_context['guild'].id, duration, False)

        # Verify function returned True
        assert result is True

        # Get the analytics record
        analytics = ensure_guild_video_analytics(session, fake_context['guild'].id)

        # Verify stats were updated
        assert analytics.total_plays == 1
        assert analytics.cached_plays == 0
        assert analytics.total_duration_seconds == 3600
        assert analytics.total_duration_days == 0


def test_update_video_guild_analytics_with_cache_hit(fake_engine, fake_context):  #pylint:disable=redefined-outer-name
    '''Test that cache hits are tracked correctly'''
    with mock_session(fake_engine) as session:
        # Update with cache hit
        duration = 1800  # 30 minutes
        update_video_guild_analytics(session, fake_context['guild'].id, duration, True)

        # Get the analytics record
        analytics = ensure_guild_video_analytics(session, fake_context['guild'].id)

        # Verify cache hit was counted
        assert analytics.total_plays == 1
        assert analytics.cached_plays == 1
        assert analytics.total_duration_seconds == 1800


def test_update_video_guild_analytics_days_calculation(fake_engine, fake_context):  #pylint:disable=redefined-outer-name
    '''Test that days are calculated correctly when duration exceeds 24 hours'''
    with mock_session(fake_engine) as session:
        # Add duration that exceeds one day
        one_day_seconds = 60 * 60 * 24
        duration = one_day_seconds + 3600  # 1 day and 1 hour

        update_video_guild_analytics(session, fake_context['guild'].id, duration, False)

        # Get the analytics record
        analytics = ensure_guild_video_analytics(session, fake_context['guild'].id)

        # Verify days and remaining seconds
        assert analytics.total_plays == 1
        assert analytics.total_duration_days == 1
        assert analytics.total_duration_seconds == 3600


def test_update_video_guild_analytics_multiple_updates(fake_engine, fake_context):  #pylint:disable=redefined-outer-name
    '''Test that multiple updates accumulate correctly'''
    with mock_session(fake_engine) as session:
        # First update
        update_video_guild_analytics(session, fake_context['guild'].id, 1800, False)

        # Second update with cache hit
        update_video_guild_analytics(session, fake_context['guild'].id, 3600, True)

        # Third update
        update_video_guild_analytics(session, fake_context['guild'].id, 7200, False)

        # Get the analytics record
        analytics = ensure_guild_video_analytics(session, fake_context['guild'].id)

        # Verify accumulated stats
        assert analytics.total_plays == 3
        assert analytics.cached_plays == 1
        assert analytics.total_duration_seconds == 1800 + 3600 + 7200


def test_update_video_guild_analytics_rollover_to_days(fake_engine, fake_context):  #pylint:disable=redefined-outer-name
    '''Test that seconds correctly roll over into days'''
    with mock_session(fake_engine) as session:
        # Add 20 hours
        update_video_guild_analytics(session, fake_context['guild'].id, 20 * 3600, False)

        analytics = ensure_guild_video_analytics(session, fake_context['guild'].id)
        assert analytics.total_duration_days == 0
        assert analytics.total_duration_seconds == 20 * 3600

        # Add another 10 hours (should push us over 1 day)
        update_video_guild_analytics(session, fake_context['guild'].id, 10 * 3600, False)

        analytics = ensure_guild_video_analytics(session, fake_context['guild'].id)
        assert analytics.total_duration_days == 1
        assert analytics.total_duration_seconds == 6 * 3600  # 30 - 24 = 6 hours remaining


def test_update_video_guild_analytics_multiple_days(fake_engine, fake_context):  #pylint:disable=redefined-outer-name
    '''Test handling of multiple days worth of content'''
    with mock_session(fake_engine) as session:
        one_day_seconds = 60 * 60 * 24

        # Add 2.5 days worth of content
        duration = int(2.5 * one_day_seconds)
        update_video_guild_analytics(session, fake_context['guild'].id, duration, False)

        analytics = ensure_guild_video_analytics(session, fake_context['guild'].id)
        assert analytics.total_duration_days == 2
        assert analytics.total_duration_seconds == 12 * 3600  # 0.5 days = 12 hours


def _make_cache_entry(session, file_size_bytes, offset_seconds=0):
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
    session.commit()
    return entry


def test_video_cache_mark_deletion_for_size(fake_engine):  #pylint:disable=redefined-outer-name
    '''video_cache_mark_deletion_for_size marks oldest entries until total <= budget'''
    with mock_session(fake_engine) as session:
        # Three entries: 200, 300, 400 bytes, oldest first
        _make_cache_entry(session, 200, offset_seconds=0)
        _make_cache_entry(session, 300, offset_seconds=1)
        _make_cache_entry(session, 400, offset_seconds=2)

        # Budget: 400 bytes; total is 900, so we must evict until <= 400
        # Evict oldest (200) → 700 still > 400
        # Evict next (300) → 400 <= 400 → stop
        video_cache_mark_deletion_for_size(session, 400)

        flagged = session.query(VideoCache).filter(VideoCache.ready_for_deletion.is_(True)).all()
        assert len(flagged) == 2
        flagged_sizes = sorted(e.file_size_bytes for e in flagged)
        assert flagged_sizes == [200, 300]


def _make_video_cache(session, url='https://example.com/video', ready_for_deletion=False,
                      file_size_bytes=1000):
    now = datetime.now(timezone.utc)
    item = VideoCache(
        video_id='abc', video_url=url, title='Test', uploader='uploader',
        duration=60, extractor='youtube', last_iterated_at=now, created_at=now,
        count=1, ready_for_deletion=ready_for_deletion, file_size_bytes=file_size_bytes,
        base_path='/tmp/test.mp4',
    )
    session.add(item)
    session.commit()
    return item


# ---------------------------------------------------------------------------
# VideoCache functions
# ---------------------------------------------------------------------------

def test_list_video_cache(fake_engine):  #pylint:disable=redefined-outer-name
    '''list_video_cache returns all cache entries'''
    with mock_session(fake_engine) as session:
        _make_video_cache(session, url='https://a.com')
        _make_video_cache(session, url='https://b.com')

        result = list_video_cache(session)

    assert len(result) == 2


def test_get_video_cache_by_id(fake_engine):  #pylint:disable=redefined-outer-name
    '''get_video_cache_by_id returns the correct entry'''
    with mock_session(fake_engine) as session:
        item = _make_video_cache(session)

        result = get_video_cache_by_id(session, item.id)

    assert result.id == item.id


def test_delete_video_cache_returns_false_when_not_found(fake_engine):  #pylint:disable=redefined-outer-name
    '''delete_video_cache returns False when the id does not exist'''
    with mock_session(fake_engine) as session:
        result = delete_video_cache(session, 99999)

    assert result is False


# ---------------------------------------------------------------------------
# VideoCacheBackup functions
# ---------------------------------------------------------------------------

def test_list_video_cache_where_no_backup(fake_engine):  #pylint:disable=redefined-outer-name
    '''list_video_cache_where_no_backup excludes entries that have a backup'''
    with mock_session(fake_engine) as session:
        with_backup = _make_video_cache(session, url='https://backed-up.com')
        without_backup = _make_video_cache(session, url='https://no-backup.com')
        with_backup_id = with_backup.id
        without_backup_id = without_backup.id

        backup = VideoCacheBackup(
            video_cache_id=with_backup_id,
            storage='s3', bucket_name='my-bucket', object_path='path/file.mp4',
        )
        session.add(backup)
        session.commit()

        result = list_video_cache_where_no_backup(session)
        ids = [r.id for r in result]

    assert without_backup_id in ids
    assert with_backup_id not in ids


def test_get_video_cache_backup(fake_engine):  #pylint:disable=redefined-outer-name
    '''get_video_cache_backup returns the backup entry for a given cache id'''
    with mock_session(fake_engine) as session:
        item = _make_video_cache(session)
        backup = VideoCacheBackup(
            video_cache_id=item.id,
            storage='s3', bucket_name='my-bucket', object_path='path/file.mp4',
        )
        session.add(backup)
        session.commit()

        result = get_video_cache_backup(session, item.id)

    assert result is not None
    assert result.video_cache_id == item.id


def test_delete_video_cache_backup_returns_false_when_not_found(fake_engine):  #pylint:disable=redefined-outer-name
    '''delete_video_cache_backup returns False when the id does not exist'''
    with mock_session(fake_engine) as session:
        result = delete_video_cache_backup(session, 99999)

    assert result is False


def test_delete_video_cache_backup_removes_entry(fake_engine):  #pylint:disable=redefined-outer-name
    '''delete_video_cache_backup removes the entry and returns True'''
    with mock_session(fake_engine) as session:
        item = _make_video_cache(session)
        backup = VideoCacheBackup(
            video_cache_id=item.id,
            storage='s3', bucket_name='my-bucket', object_path='path/file.mp4',
        )
        session.add(backup)
        session.commit()

        result = delete_video_cache_backup(session, backup.id)
        remaining = get_video_cache_backup(session, item.id)

    assert result is True
    assert remaining is None


# ---------------------------------------------------------------------------
# Playlist functions
# ---------------------------------------------------------------------------

def test_rename_playlist_returns_false_when_not_found(fake_engine):  #pylint:disable=redefined-outer-name
    '''rename_playlist returns False when the playlist id does not exist'''
    with mock_session(fake_engine) as session:
        result = rename_playlist(session, 99999, 'new name')

    assert result is False


def test_rename_playlist_returns_true_and_updates(fake_engine):  #pylint:disable=redefined-outer-name
    '''rename_playlist returns True and persists the new name'''
    with mock_session(fake_engine) as session:
        playlist = Playlist(server_id='1', name='old name', is_history=False)
        session.add(playlist)
        session.commit()

        result = rename_playlist(session, playlist.id, 'new name')
        updated = session.get(Playlist, playlist.id)

    assert result is True
    assert updated.name == 'new name'
