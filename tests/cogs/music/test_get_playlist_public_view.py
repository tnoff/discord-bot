"""
Tests for __get_playlist_public_view function in Music cog
"""
import asyncio
from datetime import datetime, timezone, timedelta

import pytest

from discord_bot.cogs.music import Music
from discord_bot.database import Playlist
from tests.cogs.test_music import BASE_MUSIC_CONFIG
from tests.helpers import generate_fake_context, fake_engine  #pylint:disable=unused-import


@pytest.fixture
def fake_context():  #pylint:disable=redefined-outer-name
    """Generate fake context for tests"""
    return generate_fake_context()


def test_get_playlist_public_view_history_playlist_returns_zero(fake_engine, fake_context):  #pylint:disable=redefined-outer-name
    """Test that history playlists return public view index 0"""
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, fake_engine)

    # Create a test history playlist
    with cog.with_db_session() as db_session:  #pylint:disable=no-member
        history_playlist = Playlist(
            name="Channel History",
            server_id=str(fake_context['guild'].id),
            is_history=True
        )
        db_session.add(history_playlist)  #pylint:disable=no-member
        db_session.commit()  #pylint:disable=no-member

        # Test the function
        result = asyncio.run(cog._Music__get_playlist_public_view(history_playlist.id, str(fake_context['guild'].id)))  #pylint:disable=protected-access

        assert result == 0


def test_get_playlist_public_view_first_playlist_returns_one(fake_engine, fake_context):  #pylint:disable=redefined-outer-name
    """Test that the first non-history playlist returns public view index 1"""
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, fake_engine)

    # Create test playlists
    with cog.with_db_session() as db_session:  #pylint:disable=no-member
        # Create the first playlist (should be index 1)
        playlist1 = Playlist(
            name="First Playlist",
            server_id=str(fake_context['guild'].id),
            is_history=False
        )
        db_session.add(playlist1)  #pylint:disable=no-member
        db_session.commit()  #pylint:disable=no-member

        # Test the function
        result = asyncio.run(cog._Music__get_playlist_public_view(playlist1.id, str(fake_context['guild'].id)))  #pylint:disable=protected-access

        assert result == 1


def test_get_playlist_public_view_multiple_playlists_correct_ordering(fake_engine, fake_context):  #pylint:disable=redefined-outer-name
    """Test that multiple playlists return correct public view indices based on creation order"""
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, fake_engine)

    # Create test playlists in specific order
    with cog.with_db_session() as db_session:  #pylint:disable=no-member
        # Create playlists in order
        playlist1 = Playlist(
            name="First Playlist",
            server_id=str(fake_context['guild'].id),
            is_history=False
        )
        playlist2 = Playlist(
            name="Second Playlist",
            server_id=str(fake_context['guild'].id),
            is_history=False
        )
        playlist3 = Playlist(
            name="Third Playlist",
            server_id=str(fake_context['guild'].id),
            is_history=False
        )

        db_session.add(playlist1)  #pylint:disable=no-member
        db_session.add(playlist2)  #pylint:disable=no-member
        db_session.add(playlist3)  #pylint:disable=no-member
        db_session.commit()  #pylint:disable=no-member

        # Test each playlist returns correct index
        result1 = asyncio.run(cog._Music__get_playlist_public_view(playlist1.id, str(fake_context['guild'].id)))  #pylint:disable=protected-access
        result2 = asyncio.run(cog._Music__get_playlist_public_view(playlist2.id, str(fake_context['guild'].id)))  #pylint:disable=protected-access
        result3 = asyncio.run(cog._Music__get_playlist_public_view(playlist3.id, str(fake_context['guild'].id)))  #pylint:disable=protected-access

        assert result1 == 1
        assert result2 == 2
        assert result3 == 3


def test_get_playlist_public_view_ignores_history_playlists_in_ordering(fake_engine, fake_context):  #pylint:disable=redefined-outer-name
    """Test that history playlists don't affect the public view ordering of regular playlists"""
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, fake_engine)

    with cog.with_db_session() as db_session:  #pylint:disable=no-member
        # Create a history playlist first
        history_playlist = Playlist(
            name="Channel History",
            server_id=str(fake_context['guild'].id),
            is_history=True
        )

        # Create regular playlists
        playlist1 = Playlist(
            name="First Regular Playlist",
            server_id=str(fake_context['guild'].id),
            is_history=False
        )
        playlist2 = Playlist(
            name="Second Regular Playlist",
            server_id=str(fake_context['guild'].id),
            is_history=False
        )

        db_session.add(history_playlist)  #pylint:disable=no-member
        db_session.add(playlist1)  #pylint:disable=no-member
        db_session.add(playlist2)  #pylint:disable=no-member
        db_session.commit()  #pylint:disable=no-member

        # History playlist should return 0
        history_result = asyncio.run(cog._Music__get_playlist_public_view(history_playlist.id, str(fake_context['guild'].id)))  #pylint:disable=protected-access

        # Regular playlists should be ordered 1, 2 (ignoring history)
        result1 = asyncio.run(cog._Music__get_playlist_public_view(playlist1.id, str(fake_context['guild'].id)))  #pylint:disable=protected-access
        result2 = asyncio.run(cog._Music__get_playlist_public_view(playlist2.id, str(fake_context['guild'].id)))  #pylint:disable=protected-access

        assert history_result == 0
        assert result1 == 1
        assert result2 == 2


def test_get_playlist_public_view_different_servers_isolated(fake_engine, fake_context):  #pylint:disable=redefined-outer-name
    """Test that playlists from different servers don't affect each other's public view indices"""
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, fake_engine)

    # Create second fake guild for testing
    other_guild_id = str(int(fake_context['guild'].id) + 1)

    with cog.with_db_session() as db_session:  #pylint:disable=no-member
        # Create playlists for first server
        server1_playlist1 = Playlist(
            name="Server 1 - Playlist 1",
            server_id=str(fake_context['guild'].id),
            is_history=False
        )
        server1_playlist2 = Playlist(
            name="Server 1 - Playlist 2",
            server_id=str(fake_context['guild'].id),
            is_history=False
        )

        # Create playlists for second server
        server2_playlist1 = Playlist(
            name="Server 2 - Playlist 1",
            server_id=str(other_guild_id),
            is_history=False
        )

        db_session.add(server1_playlist1)  #pylint:disable=no-member
        db_session.add(server1_playlist2)  #pylint:disable=no-member
        db_session.add(server2_playlist1)  #pylint:disable=no-member
        db_session.commit()  #pylint:disable=no-member

        # Server 1 playlists should be ordered 1, 2
        s1_result1 = asyncio.run(cog._Music__get_playlist_public_view(server1_playlist1.id, str(fake_context['guild'].id)))  #pylint:disable=protected-access
        s1_result2 = asyncio.run(cog._Music__get_playlist_public_view(server1_playlist2.id, str(fake_context['guild'].id)))  #pylint:disable=protected-access

        # Server 2 playlist should be index 1 (not affected by server 1)
        s2_result1 = asyncio.run(cog._Music__get_playlist_public_view(server2_playlist1.id, str(other_guild_id)))  #pylint:disable=protected-access

        assert s1_result1 == 1
        assert s1_result2 == 2
        assert s2_result1 == 1


def test_get_playlist_public_view_nonexistent_playlist_returns_none(fake_engine, fake_context):  #pylint:disable=redefined-outer-name
    """Test that requesting a non-existent playlist returns None"""
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, fake_engine)

    # Create a regular playlist for comparison
    with cog.with_db_session() as db_session:  #pylint:disable=no-member
        playlist = Playlist(
            name="Test Playlist",
            server_id=str(fake_context['guild'].id),
            is_history=False
        )
        db_session.add(playlist)  #pylint:disable=no-member
        db_session.commit()  #pylint:disable=no-member

        # Test with non-existent playlist ID
        nonexistent_id = 99999
        result = asyncio.run(cog._Music__get_playlist_public_view(nonexistent_id, str(fake_context['guild'].id)))  #pylint:disable=protected-access

        assert result is None


def test_get_playlist_public_view_cross_server_playlist_returns_none(fake_engine, fake_context):  #pylint:disable=redefined-outer-name
    """Test that requesting a playlist from a different server returns None"""
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, fake_engine)

    # Create second fake guild
    other_guild_id = str(int(fake_context['guild'].id) + 1)

    with cog.with_db_session() as db_session:  #pylint:disable=no-member
        # Create playlist for first server
        playlist = Playlist(
            name="Server 1 Playlist",
            server_id=str(fake_context['guild'].id),
            is_history=False
        )
        db_session.add(playlist)  #pylint:disable=no-member
        db_session.commit()  #pylint:disable=no-member

        # Try to get the playlist's public view from a different server
        result = asyncio.run(cog._Music__get_playlist_public_view(playlist.id, str(other_guild_id)))  #pylint:disable=protected-access

        assert result is None


def test_get_playlist_public_view_ordering_by_creation_time(fake_engine, fake_context):  #pylint:disable=redefined-outer-name
    """Test that playlists are ordered by creation_at timestamp (ASC)"""
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, fake_engine)

    with cog.with_db_session() as db_session:  #pylint:disable=no-member
        # Create playlists with specific creation timestamps
        base_time = datetime.now(timezone.utc)

        # Create in reverse chronological order to test ordering
        playlist_newest = Playlist(
            name="Newest Playlist",
            server_id=str(fake_context['guild'].id),
            is_history=False,
            created_at=base_time + timedelta(hours=2)
        )
        playlist_middle = Playlist(
            name="Middle Playlist",
            server_id=str(fake_context['guild'].id),
            is_history=False,
            created_at=base_time + timedelta(hours=1)
        )
        playlist_oldest = Playlist(
            name="Oldest Playlist",
            server_id=str(fake_context['guild'].id),
            is_history=False,
            created_at=base_time
        )

        # Add in non-chronological order
        db_session.add(playlist_newest)  #pylint:disable=no-member
        db_session.add(playlist_oldest)  #pylint:disable=no-member
        db_session.add(playlist_middle)  #pylint:disable=no-member
        db_session.commit()  #pylint:disable=no-member

        # Test that ordering is by creation_at ASC, not insert order
        oldest_result = asyncio.run(cog._Music__get_playlist_public_view(playlist_oldest.id, str(fake_context['guild'].id)))  #pylint:disable=protected-access
        middle_result = asyncio.run(cog._Music__get_playlist_public_view(playlist_middle.id, str(fake_context['guild'].id)))  #pylint:disable=protected-access
        newest_result = asyncio.run(cog._Music__get_playlist_public_view(playlist_newest.id, str(fake_context['guild'].id)))  #pylint:disable=protected-access

        assert oldest_result == 1  # Oldest created = index 1
        assert middle_result == 2   # Second created = index 2
        assert newest_result == 3   # Newest created = index 3


def test_get_playlist_public_view_handles_empty_server(fake_engine, fake_context):  #pylint:disable=redefined-outer-name
    """Test behavior when server has no playlists"""
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, fake_engine)

    # Try to get public view for non-existent playlist on server with no playlists
    result = asyncio.run(cog._Music__get_playlist_public_view(1, str(fake_context['guild'].id)))  #pylint:disable=protected-access

    assert result is None


def test_get_playlist_public_view_mixed_history_and_regular_complex(fake_engine, fake_context):  #pylint:disable=redefined-outer-name
    """Test complex scenario with mixed history and regular playlists"""
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, fake_engine)

    with cog.with_db_session() as db_session:  #pylint:disable=no-member
        base_time = datetime.now(timezone.utc)

        # Create complex mix of playlists
        playlists = [
            Playlist(name="Regular 1", server_id=str(fake_context['guild'].id), is_history=False, created_at=base_time),
            Playlist(name="History 1", server_id=str(fake_context['guild'].id), is_history=True, created_at=base_time + timedelta(minutes=10)),
            Playlist(name="Regular 2", server_id=str(fake_context['guild'].id), is_history=False, created_at=base_time + timedelta(minutes=20)),
            Playlist(name="History 2", server_id=str(fake_context['guild'].id), is_history=True, created_at=base_time + timedelta(minutes=30)),
            Playlist(name="Regular 3", server_id=str(fake_context['guild'].id), is_history=False, created_at=base_time + timedelta(minutes=40)),
        ]

        for playlist in playlists:
            db_session.add(playlist)  #pylint:disable=no-member
        db_session.commit()  #pylint:disable=no-member

        results = []
        for playlist in playlists:
            result = asyncio.run(cog._Music__get_playlist_public_view(playlist.id, str(fake_context['guild'].id)))  #pylint:disable=protected-access
            results.append(result)

        # History playlists should return 0
        # Regular playlists should be ordered 1, 2, 3 based on creation time
        expected = [1, 0, 2, 0, 3]  # Regular 1=1, History 1=0, Regular 2=2, History 2=0, Regular 3=3

        assert results == expected
