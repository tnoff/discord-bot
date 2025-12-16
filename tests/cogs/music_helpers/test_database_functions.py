from discord_bot.database import GuildVideoAnalytics
from discord_bot.cogs.music_helpers.database_functions import ensure_guild_video_analytics, update_video_guild_analytics

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
