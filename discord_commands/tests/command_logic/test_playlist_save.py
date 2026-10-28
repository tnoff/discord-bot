import logging
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from discord_core.types.playlist import PlaylistItemAddStatus
from discord_core.interfaces.player_gateway import PlayerStatus

from discord_commands.command_logic.playlist import PlaylistCommands


def outcome(status, title):
    return SimpleNamespace(status=status, title=title)


def make_logic(player_exists=True, queue=(), history=(), outcomes=(), created=7):
    sent = []

    async def reply(_guild_id, _channel_id, content, **_kwargs):
        sent.append(content)
        return content

    store = AsyncMock()
    store.add_items.return_value = list(outcomes)
    broker = AsyncMock()
    broker.get_guild_queue.return_value = SimpleNamespace(items=list(queue))
    broker.get_guild_history.return_value = list(history)
    gateway = AsyncMock()
    gateway.status.return_value = PlayerStatus(exists=player_exists)
    logic = PlaylistCommands(reply, logging.getLogger('test'), store, 300, 50, broker, gateway, AsyncMock())
    logic.create = AsyncMock(return_value=created)
    return logic, sent, store


def track(title):
    return SimpleNamespace(webpage_url=f'https://x/{title}', title=title, uploader='up')


@pytest.mark.asyncio
async def test_save_stops_when_the_playlist_was_not_created(envelope):  #pylint:disable=redefined-outer-name
    logic, sent, store = make_logic(created=None)
    await logic.save(envelope, 'mine')
    logic.player_gateway.status.assert_not_called()
    store.add_items.assert_not_called()
    assert not sent


@pytest.mark.asyncio
async def test_save_without_a_player(envelope):  #pylint:disable=redefined-outer-name
    logic, sent, store = make_logic(player_exists=False, queue=[track('a')])
    await logic.save(envelope, 'mine')
    assert sent == ['No player connected, no queue to save']
    store.add_items.assert_not_called()


@pytest.mark.asyncio
@pytest.mark.parametrize('is_history', [False, True])
async def test_save_with_nothing_to_save(envelope, is_history):  #pylint:disable=redefined-outer-name
    logic, sent, store = make_logic()
    await logic.save(envelope, 'mine', is_history=is_history)
    assert sent == ['There are no videos to add to playlist']
    store.add_items.assert_not_called()


@pytest.mark.asyncio
async def test_save_queue_reports_each_outcome(envelope):  #pylint:disable=redefined-outer-name
    logic, sent, store = make_logic(
        queue=[track('a'), track('b')],
        outcomes=[outcome(PlaylistItemAddStatus.ADDED, 'a'), outcome(PlaylistItemAddStatus.DUPLICATE, 'b')])
    await logic.save(envelope, 'mine')
    playlist_id, items, max_size = store.add_items.call_args.args
    assert (playlist_id, [i.title for i in items], max_size) == (7, ['a', 'b'], 50)
    assert sent == [
        'Added item "a" to playlist',
        'Unable to add playlist item "b", likely already exists',
        'Finished adding items to playlist "mine"']
    logic.broker_client.get_guild_history.assert_not_called()


@pytest.mark.asyncio
async def test_save_history_reads_the_history(envelope):  #pylint:disable=redefined-outer-name
    logic, sent, store = make_logic(
        history=[{'webpage_url': 'https://x/h', 'title': 'h', 'uploader': 'u'}, {'webpage_url': 'https://x/i'}],
        outcomes=[outcome(PlaylistItemAddStatus.ADDED, 'h'), outcome(PlaylistItemAddStatus.ADDED, None)])
    await logic.save(envelope, 'mine', is_history=True)
    items = store.add_items.call_args.args[1]
    assert [(i.video_url, i.title, i.uploader) for i in items] == [('https://x/h', 'h', 'u'), ('https://x/i', None, None)]
    logic.broker_client.get_guild_queue.assert_not_called()
    assert sent[-1] == 'Finished adding items to playlist "mine"'


@pytest.mark.asyncio
async def test_save_stops_at_a_full_playlist(envelope):  #pylint:disable=redefined-outer-name
    logic, sent, _ = make_logic(
        queue=[track('a'), track('b')],
        outcomes=[outcome(PlaylistItemAddStatus.ADDED, 'a'), outcome(PlaylistItemAddStatus.PLAYLIST_FULL, 'b'),
                  outcome(PlaylistItemAddStatus.ADDED, 'c')])
    await logic.save(envelope, 'mine')
    assert sent == [
        'Added item "a" to playlist',
        'Cannot add more items to playlist, already max size',
        'Finished adding items to playlist "mine"']
