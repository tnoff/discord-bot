from discord_bot.database import GuildVideoAnalytics
from discord_bot.cogs.music_helpers.database_functions import ensure_guild_video_analytics

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
