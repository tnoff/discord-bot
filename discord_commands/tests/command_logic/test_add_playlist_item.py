from datetime import datetime
from unittest.mock import AsyncMock

import pytest

from discord_core.cogs.music_helpers.common import SearchType
from discord_core.types.download import LifecycleEvent
from discord_core.types.playlist_add_request import PlaylistAddRequest
from discord_core.types.search import SearchResult

from discord_commands.types.playlist_add_result import PlaylistAddResult
from discord_commands.utils.media_enqueuer import MediaEnqueuer

from discord_db.database import Playlist, PlaylistItem

from tests.helpers import async_mock_session
from tests.helpers import fake_engine, fake_context, fake_stores  #pylint:disable=unused-import
from tests.results_pod import ResultsPod, music_settings


async def saved_playlist(fake_engine, fake_context, name, item_url):  #pylint:disable=redefined-outer-name
    '''A playlist for the guild holding one item, and its id.'''
    async with async_mock_session(fake_engine) as session:
        playlist = Playlist(server_id=fake_context['guild'].id, name=name,
                            created_at=datetime.now(), is_history=False)
        session.add(playlist)
        await session.commit()
        await session.refresh(playlist)
        session.add(PlaylistItem(title='t', video_url=item_url, uploader='u', playlist_id=playlist.id))
        await session.commit()
        return playlist.id


def add_request(fake_context, playlist_id, url):  #pylint:disable=redefined-outer-name
    return PlaylistAddRequest(
        guild_id=fake_context['guild'].id,
        channel_id=fake_context['channel'].id,
        requester_id=fake_context['author'].id,
        requester_name=fake_context['author'].display_name,
        search_result=SearchResult(search_type=SearchType.SEARCH, raw_search_string=url),
        playlist_id=playlist_id,
    )


@pytest.mark.asyncio
async def test_add_playlist_item_marks_failed_when_playlist_full(mocker, fake_engine, fake_context, fake_stores):  #pylint:disable=redefined-outer-name
    """add_playlist_item pushes FAILED with the 'playlist too long' reason when the playlist is at its limit."""
    pod = ResultsPod(fake_context, fake_stores, music_settings({'playlist': {'server_playlist_max_size': 1}}))
    push_mock = AsyncMock()
    mocker.patch.object(MediaEnqueuer, 'push_state', push_mock)
    playlist_id = await saved_playlist(fake_engine, fake_context, 'full', 'https://ex.com/a')
    request = add_request(fake_context, playlist_id, 'https://ex.com/b')

    await pod.enqueuer.add_playlist_item(request, PlaylistAddResult(webpage_url='https://ex.com/b', title='B', uploader='u'))

    push_mock.assert_awaited_once()
    args, kwargs = push_mock.await_args
    assert args[0] is request
    assert args[1] is LifecycleEvent.FAILED
    assert 'playlist too long' in kwargs['failure_reason']


@pytest.mark.asyncio
async def test_add_playlist_item_marks_failed_when_item_already_exists(mocker, fake_engine, fake_context, fake_stores):  #pylint:disable=redefined-outer-name
    """add_playlist_item pushes FAILED with 'already exists' when the item URL is already in the playlist."""
    pod = ResultsPod(fake_context, fake_stores)
    push_mock = AsyncMock()
    mocker.patch.object(MediaEnqueuer, 'push_state', push_mock)
    playlist_id = await saved_playlist(fake_engine, fake_context, 'dup', 'https://ex.com/same')
    request = add_request(fake_context, playlist_id, 'https://ex.com/same')

    await pod.enqueuer.add_playlist_item(request, PlaylistAddResult(webpage_url='https://ex.com/same', title='same', uploader='u'))

    push_mock.assert_awaited_once()
    args, kwargs = push_mock.await_args
    assert args[0] is request
    assert args[1] is LifecycleEvent.FAILED
    assert 'already exists' in kwargs['failure_reason']
