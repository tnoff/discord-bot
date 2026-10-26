import logging
from datetime import datetime, timezone, timedelta
from unittest.mock import AsyncMock, MagicMock

import pytest
from sqlalchemy import select
from sqlalchemy.sql.functions import count as sql_count

from discord_core.types.command_envelope import CommandEnvelope

from discord_db.database import Playlist
from discord_gateway.command_logic.playlist import PlaylistCommands
from tests.helpers import async_mock_session
from tests.helpers import fake_engine, fake_context, fake_stores #pylint:disable=unused-import


def make_logic(stores):
    '''
    The store-only playlist commands over the real test store (or no store, when the database
    is not enabled), with a mock dispatcher. They never touch the broker, the player or the
    enqueuer, so those are stand-ins.
    '''
    dispatcher = MagicMock()
    playlist_store = stores.playlist if stores else None
    logic = PlaylistCommands(dispatcher, playlist_store, logging.getLogger('test'), 300, 64,
                             AsyncMock(), AsyncMock(), AsyncMock())
    return logic, dispatcher


def make_env(fakes):
    '''The envelope the cog would build from the fake context.'''
    return CommandEnvelope(guild_id=fakes['guild'].id, channel_id=fakes['channel'].id,
                           author_id=fakes['author'].id, author_display_name=fakes['author'].display_name)


@pytest.mark.asyncio
async def test_create_playlist(fake_engine, fake_context, fake_stores):  #pylint:disable=redefined-outer-name
    logic, _ = make_logic(fake_stores)
    env = make_env(fake_context)
    await logic.create(env, 'new-playlist')
    async with async_mock_session(fake_engine) as db_session:
        assert (await db_session.execute(select(sql_count()).select_from(Playlist))).scalar()

@pytest.mark.asyncio
async def test_create_playlist_invalid_name(fake_engine, fake_context, fake_stores):  #pylint:disable=redefined-outer-name
    logic, _ = make_logic(fake_stores)
    env = make_env(fake_context)
    await logic.create(env, '__playhistory__derp')
    async with async_mock_session(fake_engine) as db_session:
        assert not (await db_session.execute(select(sql_count()).select_from(Playlist))).scalar()

@pytest.mark.asyncio
async def test_create_playlist_same_name_twice(fake_engine, fake_context, fake_stores):  #pylint:disable=redefined-outer-name
    logic, _ = make_logic(fake_stores)
    env = make_env(fake_context)
    await logic.create(env, 'new-playlist')
    await logic.create(env, 'new-playlist')
    async with async_mock_session(fake_engine) as db_session:
        assert (await db_session.execute(select(sql_count()).select_from(Playlist))).scalar() == 1

@pytest.mark.asyncio
async def test_create_playlist_message_includes_public_id(fake_engine, fake_context, fake_stores):  #pylint:disable=redefined-outer-name
    """Test that playlist creation message includes the public playlist ID"""
    logic, dispatcher = make_logic(fake_stores)
    env = make_env(fake_context)

    # Create first playlist - should get public ID 1
    await logic.create(env, 'first-playlist')
    assert dispatcher.send_message.call_args_list[0][0][2] == 'Created playlist "first-playlist" with ID 1'

    # Create second playlist - should get public ID 2
    await logic.create(env, 'second-playlist')
    # Newest first, so the playlist just created is always index 1 and the
    # earlier one has shifted down. That is what production has always done.
    assert dispatcher.send_message.call_args_list[1][0][2] == 'Created playlist "second-playlist" with ID 1'

    # Verify playlists were actually created in database
    async with async_mock_session(fake_engine) as db_session:
        assert (await db_session.execute(select(sql_count()).select_from(Playlist))).scalar() == 2

@pytest.mark.asyncio
async def test_create_playlist_message_with_none_public_id(mocker, fake_context, fake_stores):  #pylint:disable=redefined-outer-name
    """Test playlist creation message handles None public ID gracefully"""
    logic, dispatcher = make_logic(fake_stores)
    env = make_env(fake_context)

    # Mock __get_playlist_public_view to return None
    mocker.patch.object(PlaylistCommands, 'get_playlist_public_view', return_value=None)

    await logic.create(env, 'test-playlist')
    assert dispatcher.send_message.call_args[0][2] == 'Created playlist "test-playlist" with ID None'

@pytest.mark.asyncio
async def test_get_playlist_public_view_history_playlist_returns_zero(fake_engine, fake_context, fake_stores):  #pylint:disable=redefined-outer-name
    """Test that history playlists return public view index 0"""
    logic, _ = make_logic(fake_stores)

    # Create a test history playlist
    async with async_mock_session(fake_engine) as db_session:
        history_playlist = Playlist(
            name="Channel History",
            server_id=fake_context['guild'].id,
            is_history=True
        )
        db_session.add(history_playlist)  #pylint:disable=no-member
        await db_session.commit()  #pylint:disable=no-member
        await db_session.refresh(history_playlist)

        # Test the function
        result = await logic.get_playlist_public_view(history_playlist.id, fake_context['guild'].id)  #pylint:disable=protected-access

        assert result == 0


@pytest.mark.asyncio
async def test_get_playlist_public_view_first_playlist_returns_one(fake_engine, fake_context, fake_stores):  #pylint:disable=redefined-outer-name
    """Test that the first non-history playlist returns public view index 1"""
    logic, _ = make_logic(fake_stores)

    # Create test playlists
    async with async_mock_session(fake_engine) as db_session:
        # Create the first playlist (should be index 1)
        playlist1 = Playlist(
            name="First Playlist",
            server_id=fake_context['guild'].id,
            is_history=False
        )
        db_session.add(playlist1)  #pylint:disable=no-member
        await db_session.commit()  #pylint:disable=no-member
        await db_session.refresh(playlist1)

        # Test the function
        result = await logic.get_playlist_public_view(playlist1.id, fake_context['guild'].id)  #pylint:disable=protected-access

        assert result == 1


@pytest.mark.asyncio
async def test_get_playlist_public_view_multiple_playlists_correct_ordering(fake_engine, fake_context, fake_stores):  #pylint:disable=redefined-outer-name
    """Test that multiple playlists return correct public view indices based on creation order"""
    logic, _ = make_logic(fake_stores)

    # Create test playlists in specific order
    async with async_mock_session(fake_engine) as db_session:
        # Create playlists in order
        playlist1 = Playlist(
            name="First Playlist",
            server_id=fake_context['guild'].id,
            is_history=False
        )
        playlist2 = Playlist(
            name="Second Playlist",
            server_id=fake_context['guild'].id,
            is_history=False
        )
        playlist3 = Playlist(
            name="Third Playlist",
            server_id=fake_context['guild'].id,
            is_history=False
        )

        db_session.add(playlist1)  #pylint:disable=no-member
        db_session.add(playlist2)  #pylint:disable=no-member
        db_session.add(playlist3)  #pylint:disable=no-member
        await db_session.commit()  #pylint:disable=no-member
        await db_session.refresh(playlist1)
        await db_session.refresh(playlist2)
        await db_session.refresh(playlist3)

        # Test each playlist returns correct index
        result1 = await logic.get_playlist_public_view(playlist1.id, fake_context['guild'].id)  #pylint:disable=protected-access
        result2 = await logic.get_playlist_public_view(playlist2.id, fake_context['guild'].id)  #pylint:disable=protected-access
        result3 = await logic.get_playlist_public_view(playlist3.id, fake_context['guild'].id)  #pylint:disable=protected-access

        # Newest first. These carry no explicit created_at, so they used to tie
        # on NULL and come back in heap order -- which is why this test and
        # test_get_playlist_public_view_ordering_by_creation_time asserted
        # opposite directions and both passed. The model default gives them
        # distinct timestamps now, so only one direction can be right, and
        # production says it is this one.
        assert result3 == 1
        assert result2 == 2
        assert result1 == 3


@pytest.mark.asyncio
async def test_get_playlist_public_view_ignores_history_playlists_in_ordering(fake_engine, fake_context, fake_stores):  #pylint:disable=redefined-outer-name
    """Test that history playlists don't affect the public view ordering of regular playlists"""
    logic, _ = make_logic(fake_stores)

    async with async_mock_session(fake_engine) as db_session:
        # Create a history playlist first
        history_playlist = Playlist(
            name="Channel History",
            server_id=fake_context['guild'].id,
            is_history=True
        )

        # Create regular playlists
        playlist1 = Playlist(
            name="First Regular Playlist",
            server_id=fake_context['guild'].id,
            is_history=False
        )
        playlist2 = Playlist(
            name="Second Regular Playlist",
            server_id=fake_context['guild'].id,
            is_history=False
        )

        db_session.add(history_playlist)  #pylint:disable=no-member
        db_session.add(playlist1)  #pylint:disable=no-member
        db_session.add(playlist2)  #pylint:disable=no-member
        await db_session.commit()  #pylint:disable=no-member
        await db_session.refresh(history_playlist)
        await db_session.refresh(playlist1)
        await db_session.refresh(playlist2)

        # History playlist should return 0
        history_result = await logic.get_playlist_public_view(history_playlist.id, fake_context['guild'].id)  #pylint:disable=protected-access

        # Regular playlists should be ordered 1, 2 (ignoring history)
        result1 = await logic.get_playlist_public_view(playlist1.id, fake_context['guild'].id)  #pylint:disable=protected-access
        result2 = await logic.get_playlist_public_view(playlist2.id, fake_context['guild'].id)  #pylint:disable=protected-access

        assert history_result == 0
        # Newest first among the non-history playlists.
        assert result2 == 1
        assert result1 == 2


@pytest.mark.asyncio
async def test_get_playlist_public_view_different_servers_isolated(fake_engine, fake_context, fake_stores):  #pylint:disable=redefined-outer-name
    """Test that playlists from different servers don't affect each other's public view indices"""
    logic, _ = make_logic(fake_stores)

    # Create second fake guild for testing
    other_guild_id = fake_context['guild'].id + 1

    async with async_mock_session(fake_engine) as db_session:
        # Create playlists for first server
        server1_playlist1 = Playlist(
            name="Server 1 - Playlist 1",
            server_id=fake_context['guild'].id,
            is_history=False
        )
        server1_playlist2 = Playlist(
            name="Server 1 - Playlist 2",
            server_id=fake_context['guild'].id,
            is_history=False
        )

        # Create playlists for second server
        server2_playlist1 = Playlist(
            name="Server 2 - Playlist 1",
            server_id=other_guild_id,
            is_history=False
        )

        db_session.add(server1_playlist1)  #pylint:disable=no-member
        db_session.add(server1_playlist2)  #pylint:disable=no-member
        db_session.add(server2_playlist1)  #pylint:disable=no-member
        await db_session.commit()  #pylint:disable=no-member
        await db_session.refresh(server1_playlist1)
        await db_session.refresh(server1_playlist2)
        await db_session.refresh(server2_playlist1)

        # Server 1 playlists should be ordered 1, 2
        s1_result1 = await logic.get_playlist_public_view(server1_playlist1.id, fake_context['guild'].id)  #pylint:disable=protected-access
        s1_result2 = await logic.get_playlist_public_view(server1_playlist2.id, fake_context['guild'].id)  #pylint:disable=protected-access

        # Server 2 playlist should be index 1 (not affected by server 1)
        s2_result1 = await logic.get_playlist_public_view(server2_playlist1.id, other_guild_id)  #pylint:disable=protected-access

        # Newest first within each guild; the guilds stay independent.
        assert s1_result2 == 1
        assert s1_result1 == 2
        assert s2_result1 == 1


@pytest.mark.asyncio
async def test_get_playlist_public_view_nonexistent_playlist_returns_none(fake_engine, fake_context, fake_stores):  #pylint:disable=redefined-outer-name
    """Test that requesting a non-existent playlist returns None"""
    logic, _ = make_logic(fake_stores)

    # Create a regular playlist for comparison
    async with async_mock_session(fake_engine) as db_session:
        playlist = Playlist(
            name="Test Playlist",
            server_id=fake_context['guild'].id,
            is_history=False
        )
        db_session.add(playlist)  #pylint:disable=no-member
        await db_session.commit()  #pylint:disable=no-member

        # Test with non-existent playlist ID
        nonexistent_id = 99999
        result = await logic.get_playlist_public_view(nonexistent_id, fake_context['guild'].id)  #pylint:disable=protected-access

        assert result is None


@pytest.mark.asyncio
async def test_get_playlist_public_view_cross_server_playlist_returns_none(fake_engine, fake_context, fake_stores):  #pylint:disable=redefined-outer-name
    """Test that requesting a playlist from a different server returns None"""
    logic, _ = make_logic(fake_stores)

    # Create second fake guild
    other_guild_id = str(int(fake_context['guild'].id) + 1)

    async with async_mock_session(fake_engine) as db_session:
        # Create playlist for first server
        playlist = Playlist(
            name="Server 1 Playlist",
            server_id=fake_context['guild'].id,
            is_history=False
        )
        db_session.add(playlist)  #pylint:disable=no-member
        await db_session.commit()  #pylint:disable=no-member
        await db_session.refresh(playlist)

        # Try to get the playlist's public view from a different server
        result = await logic.get_playlist_public_view(playlist.id, str(other_guild_id))  #pylint:disable=protected-access

        assert result is None


@pytest.mark.asyncio
async def test_get_playlist_public_view_ordering_by_creation_time(fake_engine, fake_context, fake_stores):  #pylint:disable=redefined-outer-name
    """Playlists are ordered by created_at DESC -- newest first, which is index 1.

    This and the neighbouring test used to assert opposite directions and both
    passed, because the ORDER BY applied here (explicit timestamps) and did
    nothing there (none supplied, and every row NULL). Populating created_at
    forced a choice, and production answered it: all 32 playlist rows already
    carried distinct timestamps, so DESC had been in effect the whole time and
    the public index has always been newest-first. The neighbour now agrees
    with this one rather than the other way round.
    """
    logic, _ = make_logic(fake_stores)

    async with async_mock_session(fake_engine) as db_session:
        # Create playlists with specific creation timestamps
        base_time = datetime.now(timezone.utc)

        # Create in reverse chronological order to test ordering
        playlist_newest = Playlist(
            name="Newest Playlist",
            server_id=fake_context['guild'].id,
            is_history=False,
            created_at=base_time + timedelta(hours=2)
        )
        playlist_middle = Playlist(
            name="Middle Playlist",
            server_id=fake_context['guild'].id,
            is_history=False,
            created_at=base_time + timedelta(hours=1)
        )
        playlist_oldest = Playlist(
            name="Oldest Playlist",
            server_id=fake_context['guild'].id,
            is_history=False,
            created_at=base_time
        )

        # Add in non-chronological order
        db_session.add(playlist_newest)  #pylint:disable=no-member
        db_session.add(playlist_oldest)  #pylint:disable=no-member
        db_session.add(playlist_middle)  #pylint:disable=no-member
        await db_session.commit()  #pylint:disable=no-member
        await db_session.refresh(playlist_newest)
        await db_session.refresh(playlist_oldest)
        await db_session.refresh(playlist_middle)

        # Ordering is by created_at DESC, not by insert order: these were added
        # newest-first above and come back newest-first regardless.
        oldest_result = await logic.get_playlist_public_view(playlist_oldest.id, fake_context['guild'].id)  #pylint:disable=protected-access
        middle_result = await logic.get_playlist_public_view(playlist_middle.id, fake_context['guild'].id)  #pylint:disable=protected-access
        newest_result = await logic.get_playlist_public_view(playlist_newest.id, fake_context['guild'].id)  #pylint:disable=protected-access

        assert newest_result == 1   # Newest created = index 1
        assert middle_result == 2   # Second newest = index 2
        assert oldest_result == 3   # Oldest created = index 3


@pytest.mark.asyncio
async def test_get_playlist_public_view_handles_empty_server(fake_context, fake_stores):  #pylint:disable=redefined-outer-name
    """Test behavior when server has no playlists"""
    logic, _ = make_logic(fake_stores)

    # Try to get public view for non-existent playlist on server with no playlists
    result = await logic.get_playlist_public_view(1, fake_context['guild'].id)  #pylint:disable=protected-access

    assert result is None


@pytest.mark.asyncio
async def test_get_playlist_public_view_mixed_history_and_regular_complex(fake_engine, fake_context, fake_stores):  #pylint:disable=redefined-outer-name
    """Test complex scenario with mixed history and regular playlists"""
    logic, _ = make_logic(fake_stores)

    async with async_mock_session(fake_engine) as db_session:
        base_time = datetime.now(timezone.utc)

        # Create complex mix of playlists
        playlists = [
            Playlist(name="Regular 1", server_id=fake_context['guild'].id, is_history=False, created_at=base_time),
            Playlist(name="History 1", server_id=fake_context['guild'].id, is_history=True, created_at=base_time + timedelta(minutes=10)),
            Playlist(name="Regular 2", server_id=fake_context['guild'].id, is_history=False, created_at=base_time + timedelta(minutes=20)),
            Playlist(name="History 2", server_id=fake_context['guild'].id, is_history=True, created_at=base_time + timedelta(minutes=30)),
            Playlist(name="Regular 3", server_id=fake_context['guild'].id, is_history=False, created_at=base_time + timedelta(minutes=40)),
        ]

        for playlist in playlists:
            db_session.add(playlist)  #pylint:disable=no-member
        await db_session.commit()  #pylint:disable=no-member
        for playlist in playlists:
            await db_session.refresh(playlist)

        results = []
        for playlist in playlists:
            result = await logic.get_playlist_public_view(playlist.id, fake_context['guild'].id)  #pylint:disable=protected-access
            results.append(result)

        # History playlists should return 0
        # Regular playlists are ordered by creation time DESC (newest first)
        # Regular 1 (oldest): created at base_time -> index 3
        # Regular 2 (middle): created at base_time+20min -> index 2
        # Regular 3 (newest): created at base_time+40min -> index 1
        expected = [3, 0, 2, 0, 1]  # Regular 1=3, History 1=0, Regular 2=2, History 2=0, Regular 3=1

        assert results == expected

@pytest.mark.asyncio
async def test_playlist_create_no_db(fake_context):  #pylint:disable=redefined-outer-name
    """__playlist_create returns early when db_engine is None"""
    logic, dispatcher = make_logic(None)
    env = make_env(fake_context)
    await logic.create(env, 'test')
    assert 'database is not enabled' in dispatcher.send_message.call_args[0][2]


@pytest.mark.asyncio
async def test_get_playlist_invalid_string_index(fake_context, fake_stores):  #pylint:disable=redefined-outer-name
    """__get_playlist sends error message and returns (None, False) for non-numeric index"""
    logic, dispatcher = make_logic(fake_stores)
    env = make_env(fake_context)
    result = await logic.get_playlist('abc', env)
    assert result == (None, False)
    assert 'Invalid playlist index abc' in dispatcher.send_message.call_args[0][2]


@pytest.mark.asyncio
async def test_get_playlist_no_playlists_in_database(fake_context, fake_stores):  #pylint:disable=redefined-outer-name
    """__get_playlist sends 'No playlists in database' when index>0 and DB is empty"""
    logic, dispatcher = make_logic(fake_stores)
    env = make_env(fake_context)
    result = await logic.get_playlist(1, env)
    assert result == (None, False)
    assert 'No playlists in database' in dispatcher.send_message.call_args[0][2]


@pytest.mark.asyncio
async def test_get_playlist_history_not_found(fake_context, fake_stores):  #pylint:disable=redefined-outer-name
    """__get_playlist sends 'Invalid playlist index 0' when index=0 and no history playlist"""
    logic, dispatcher = make_logic(fake_stores)
    env = make_env(fake_context)
    result = await logic.get_playlist(0, env)
    assert result == (None, False)
    assert 'Invalid playlist index 0' in dispatcher.send_message.call_args[0][2]
