import logging
from unittest.mock import AsyncMock

import pytest

from discord_core.cogs.music_helpers.common import SearchType
from discord_core.types.playlist import PlaylistItemWrite

from discord_commands.command_logic.playlist import PlaylistCommands

from tests.helpers import fake_engine, fake_stores #pylint:disable=unused-import

DELETE_AFTER = 300
YOUTUBE_URL = 'https://www.youtube.com/watch?v=abcdefghijk'
DIRECT_URL = 'https://example.com/song.mp3'


def _logic(reply, fake_stores, finished_all=True):  #pylint:disable=redefined-outer-name
    broker = AsyncMock()
    broker.create_bundle.return_value = 'bundle-1'
    enqueuer = AsyncMock()
    enqueuer.enqueue_media_requests.return_value = finished_all
    logic = PlaylistCommands(reply, logging.getLogger('test'), fake_stores.playlist, DELETE_AFTER, 64,
                             broker, AsyncMock(), enqueuer)
    return logic, broker, enqueuer


async def _playlist(fake_stores, env, *urls):  #pylint:disable=redefined-outer-name
    '''A playlist holding the urls, returning its db id.'''
    playlist = await fake_stores.playlist.create_playlist(env.guild_id, 'mine')
    await fake_stores.playlist.add_items(
        playlist.id, [PlaylistItemWrite(video_url=url, title=f't{index}', uploader='up')
                      for index, url in enumerate(urls)], 64)
    return playlist.id


def _queued(enqueuer):
    '''The media requests the enqueuer was handed.'''
    return enqueuer.enqueue_media_requests.await_args.args[1]


@pytest.mark.asyncio
async def test_create_returns_the_new_id(reply, fake_stores, envelope):  #pylint:disable=redefined-outer-name
    logic, _, _ = _logic(reply, fake_stores)
    playlist_id = await logic.create(envelope, 'mine')
    assert playlist_id == (await fake_stores.playlist.get_playlist_by_name(envelope.guild_id, 'mine')).id
    assert await logic.create(envelope, 'mine') is None


@pytest.mark.asyncio
async def test_item_add_searches_into_the_playlist(reply, fake_stores, envelope):  #pylint:disable=redefined-outer-name
    logic, _, enqueuer = _logic(reply, fake_stores)
    playlist_id = await _playlist(fake_stores, envelope)
    await logic.item_add(envelope, 1, 'a song')
    enqueuer.generate_media_requests_from_search.assert_awaited_once_with(envelope, 'a song', add_to_playlist=playlist_id)


@pytest.mark.asyncio
async def test_item_add_refuses_the_history_playlist(reply, sent, fake_stores, envelope):  #pylint:disable=redefined-outer-name
    logic, _, enqueuer = _logic(reply, fake_stores)
    await fake_stores.playlist.ensure_history_playlist(envelope.guild_id)
    await logic.item_add(envelope, 0, 'a song')
    assert sent == [(11, 22, 'Unable to add "a song" to history playlist, is reserved and cannot be added to manually')]
    enqueuer.generate_media_requests_from_search.assert_not_called()


@pytest.mark.asyncio
async def test_item_add_to_a_playlist_that_is_not_there(reply, sent, fake_stores, envelope):  #pylint:disable=redefined-outer-name
    logic, _, enqueuer = _logic(reply, fake_stores)
    await logic.item_add(envelope, 'abc', 'a song')
    assert sent == [(11, 22, 'Invalid playlist index abc')]
    enqueuer.generate_media_requests_from_search.assert_not_called()


@pytest.mark.asyncio
async def test_queue_items_builds_a_request_per_item(reply, fake_stores, envelope):  #pylint:disable=redefined-outer-name
    logic, broker, enqueuer = _logic(reply, fake_stores)
    playlist_id = await _playlist(fake_stores, envelope, YOUTUBE_URL, DIRECT_URL)
    items = await fake_stores.playlist.list_items(playlist_id)

    await logic.queue_items(envelope, playlist_id, False, None)

    requests = _queued(enqueuer)
    assert [request.search_result.search_type for request in requests] == [SearchType.YOUTUBE, SearchType.DIRECT]
    assert [request.search_result.raw_search_string for request in requests] == [YOUTUBE_URL, DIRECT_URL]
    assert [request.history_playlist_item_id for request in requests] == [item.id for item in items]
    assert not any(request.added_from_history for request in requests)
    assert {(request.guild_id, request.channel_id, request.requester_id, request.requester_name)
            for request in requests} == {(11, 22, 33, 'Tyler')}
    broker.create_bundle.assert_awaited_once_with(11, 22, input_string='mine', has_search_banner=True)
    assert enqueuer.enqueue_media_requests.await_args.args[2] == 'bundle-1'


@pytest.mark.asyncio
async def test_queue_items_marks_the_playlist_queued(reply, fake_stores, envelope):  #pylint:disable=redefined-outer-name
    logic, _, _ = _logic(reply, fake_stores)
    playlist_id = await _playlist(fake_stores, envelope, YOUTUBE_URL)
    assert (await fake_stores.playlist.get_playlist(playlist_id)).last_queued is None
    await logic.queue_items(envelope, playlist_id, False, None)
    assert (await fake_stores.playlist.get_playlist(playlist_id)).last_queued is not None


@pytest.mark.asyncio
async def test_queue_items_shuffles(reply, fake_stores, envelope):  #pylint:disable=redefined-outer-name
    logic, _, enqueuer = _logic(reply, fake_stores)
    urls = [f'https://example.com/{index}.mp3' for index in range(12)]
    playlist_id = await _playlist(fake_stores, envelope, *urls)
    await logic.queue_items(envelope, playlist_id, True, None)
    queued = [request.search_result.raw_search_string for request in _queued(enqueuer)]
    assert sorted(queued) == sorted(urls)


@pytest.mark.asyncio
@pytest.mark.parametrize('max_num,expected', [(2, 2), (5, 3), (3, 3)])
async def test_queue_items_stops_at_max_num(reply, fake_stores, envelope, max_num, expected):  #pylint:disable=redefined-outer-name
    logic, _, enqueuer = _logic(reply, fake_stores)
    playlist_id = await _playlist(fake_stores, envelope, *[f'https://example.com/{index}.mp3' for index in range(3)])
    await logic.queue_items(envelope, playlist_id, False, max_num)
    assert len(_queued(enqueuer)) == expected


@pytest.mark.asyncio
async def test_queue_items_refuses_a_negative_count(reply, sent, delete_afters, fake_stores, envelope):  #pylint:disable=redefined-outer-name
    logic, broker, enqueuer = _logic(reply, fake_stores)
    playlist_id = await _playlist(fake_stores, envelope, YOUTUBE_URL)
    await logic.queue_items(envelope, playlist_id, False, -1)
    assert sent == [(11, 22, 'Invalid number of videos -1')]
    assert delete_afters == [DELETE_AFTER]
    broker.create_bundle.assert_not_called()
    enqueuer.enqueue_media_requests.assert_not_called()


@pytest.mark.asyncio
async def test_queue_items_of_an_empty_playlist_says_so(reply, sent, fake_stores, envelope):  #pylint:disable=redefined-outer-name
    logic, broker, enqueuer = _logic(reply, fake_stores)
    playlist_id = await _playlist(fake_stores, envelope)
    await logic.queue_items(envelope, playlist_id, False, None)
    assert sent == [(11, 22, 'Playlist "mine" contains no items to queue')]
    broker.create_bundle.assert_not_called()
    enqueuer.enqueue_media_requests.assert_not_called()


@pytest.mark.asyncio
async def test_queue_items_of_the_history_playlist_uses_its_display_name(reply, sent, fake_stores, envelope):  #pylint:disable=redefined-outer-name
    logic, broker, enqueuer = _logic(reply, fake_stores)
    history_id = await fake_stores.playlist.ensure_history_playlist(envelope.guild_id)

    await logic.queue_items(envelope, history_id, False, None, is_history=True)
    assert sent == [(11, 22, 'Playlist "Channel History" contains no items to queue')]

    await fake_stores.playlist.record_history_item(history_id, PlaylistItemWrite(video_url=YOUTUBE_URL, title='t', uploader='up'), 64)
    await logic.queue_items(envelope, history_id, False, None, is_history=True)
    assert broker.create_bundle.await_args.kwargs['input_string'] == 'Channel History'
    assert all(request.added_from_history for request in _queued(enqueuer))


@pytest.mark.asyncio
async def test_queue_items_says_when_it_hit_the_limit(reply, sent, fake_stores, envelope):  #pylint:disable=redefined-outer-name
    logic, _, _ = _logic(reply, fake_stores, finished_all=False)
    playlist_id = await _playlist(fake_stores, envelope, YOUTUBE_URL)
    await logic.queue_items(envelope, playlist_id, False, None)
    assert sent == [(11, 22, 'Added as many videos in playlist "mine" to queue as possible, but hit limit')]
