import logging
from dataclasses import replace
from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest

from discord_gateway.command_logic.music import MusicCommands
from discord_gateway.command_logic.player_gateway import EnsureResult, PlayerStatus

IN_VOICE = {'author_voice_channel_id': 55, 'author_voice_channel_name': 'music', 'author_voice_guild_id': 11}
NOT_PLAYING = 'I am not currently playing anything'


class FakePlayerGateway:
    '''Records calls and answers with canned results.'''

    def __init__(self, status=None, ensure=None):
        self.status_result = status or PlayerStatus(exists=False)
        self.ensure_result = ensure or EnsureResult(ok=True)
        self.calls = []

    async def status(self, guild_id):
        self.calls.append(('status', guild_id))
        return self.status_result

    async def ensure(self, guild_id, voice_channel_id, text_channel_id):
        self.calls.append(('ensure', guild_id, voice_channel_id, text_channel_id))
        return self.ensure_result

    async def stop(self, guild_id, text_channel_id):
        self.calls.append(('stop', guild_id, text_channel_id))

    async def move_messages(self, guild_id, text_channel_id):
        self.calls.append(('move_messages', guild_id, text_channel_id))

    async def add_cached(self, guild_id, media_download):
        self.calls.append(('add_cached', guild_id, media_download))
        return True



def make_logic(gateway, analytics=None, broker=None, enqueuer=None, playlist=None):
    dispatcher = MagicMock()
    logic = MusicCommands(dispatcher, broker or AsyncMock(), analytics, gateway,
                          logging.getLogger('test'), 300, enqueuer or AsyncMock(), playlist or AsyncMock())
    return logic, dispatcher


def sent(dispatcher):
    '''(channel_id, content, delete_after) for each message sent.'''
    return [(c.args[1], c.args[2], c.kwargs.get('delete_after', 'unset')) for c in dispatcher.send_message.call_args_list]


@pytest.fixture
def voiced(envelope):  #pylint:disable=redefined-outer-name
    return replace(envelope, channel_name='general', **IN_VOICE)


@pytest.mark.asyncio
async def test_voice_check_not_in_voice(envelope):  #pylint:disable=redefined-outer-name
    logic, dispatcher = make_logic(FakePlayerGateway())
    assert await logic.check_author_voice_chat(envelope) is None
    assert sent(dispatcher) == [(22, 'Tyler not in voice chat channel. Please join one and try again', 300)]


@pytest.mark.asyncio
async def test_voice_check_other_guild(voiced):  #pylint:disable=redefined-outer-name
    logic, dispatcher = make_logic(FakePlayerGateway())
    other = replace(voiced, author_voice_guild_id=99)
    assert await logic.check_author_voice_chat(other) is None
    assert sent(dispatcher) == [(22, 'User not joined to channel bot is in, ignoring command', 300)]
    # join does not care which guild the channel is in
    assert await logic.check_author_voice_chat(other, check_voice_chats=False) == 55


@pytest.mark.asyncio
async def test_voice_check_passes(voiced):  #pylint:disable=redefined-outer-name
    logic, dispatcher = make_logic(FakePlayerGateway())
    assert await logic.check_author_voice_chat(voiced) == 55
    dispatcher.send_message.assert_not_called()


@pytest.mark.asyncio
async def test_player_active_gate(envelope):  #pylint:disable=redefined-outer-name
    '''No player answers in the command's channel; a stalled one in the player's own channel'''
    logic, dispatcher = make_logic(FakePlayerGateway(PlayerStatus(exists=False)))
    assert await logic.check_player_active(envelope) is None
    assert sent(dispatcher) == [(22, NOT_PLAYING, 300)]

    for status in (PlayerStatus(exists=True, voice_connected=False, text_channel_id=77),
                   PlayerStatus(exists=True, voice_connected=True, is_playing=False, text_channel_id=77)):
        logic, dispatcher = make_logic(FakePlayerGateway(status))
        assert await logic.check_player_active(envelope) is None
        assert sent(dispatcher) == [(77, NOT_PLAYING, 300)]


@pytest.mark.asyncio
async def test_player_active_gate_passes(envelope):  #pylint:disable=redefined-outer-name
    for status in (PlayerStatus(exists=True, voice_connected=True, is_playing=True, text_channel_id=77),
                   PlayerStatus(exists=True, voice_connected=True, pending_downloads=2, text_channel_id=77)):
        logic, dispatcher = make_logic(FakePlayerGateway(status))
        assert await logic.check_player_active(envelope) == status
        dispatcher.send_message.assert_not_called()


@pytest.mark.asyncio
async def test_music_stats_without_store(envelope):  #pylint:disable=redefined-outer-name
    logic, dispatcher = make_logic(FakePlayerGateway())
    await logic.music_stats(envelope)
    # No delete timer, as it has always been
    assert sent(dispatcher) == [(22, 'Functionality not available, database is not enabled', 'unset')]


@pytest.mark.asyncio
async def test_music_stats(envelope):  #pylint:disable=redefined-outer-name
    analytics = AsyncMock()
    analytics.get_analytics.return_value = SimpleNamespace(
        total_duration_seconds=3725, total_plays=9, cached_plays=4, total_duration_days=0,
        created_at=datetime(2026, 1, 2, 3, 4, 5, tzinfo=timezone.utc))
    logic, dispatcher = make_logic(FakePlayerGateway(), analytics=analytics)
    await logic.music_stats(envelope)
    assert sent(dispatcher) == [(22, '```Music Stats for Server\nTotal Plays: 9\nCached Plays: 4\n'
                                 'Total Time Played: 0 days, 1 hours, 2 minutes, and 5 seconds\n'
                                 'Tracked Since: 2026-01-02 03:04:05 UTC\n```', 300)]


@pytest.mark.asyncio
async def test_history(voiced):  #pylint:disable=redefined-outer-name
    broker = AsyncMock()
    broker.get_guild_history.return_value = []
    logic, dispatcher = make_logic(FakePlayerGateway(), broker=broker)
    await logic.history(voiced)
    assert sent(dispatcher) == [(22, 'There have been no videos played.', 300)]

    broker.get_guild_history.return_value = [{'title': 'one', 'uploader': 'up'}, {'title': 'two'}]
    logic, dispatcher = make_logic(FakePlayerGateway(), broker=broker)
    await logic.history(voiced)
    assert sent(dispatcher)[0][1].startswith('History\n```Pos|| Title')
    assert 'one' in sent(dispatcher)[0][1] and 'two' in sent(dispatcher)[0][1]


@pytest.mark.asyncio
async def test_history_needs_voice(envelope):  #pylint:disable=redefined-outer-name
    broker = AsyncMock()
    logic, _ = make_logic(FakePlayerGateway(), broker=broker)
    await logic.history(envelope)
    broker.get_guild_history.assert_not_called()


@pytest.mark.asyncio
async def test_join(voiced, envelope):  #pylint:disable=redefined-outer-name
    gateway = FakePlayerGateway()
    logic, dispatcher = make_logic(gateway)
    await logic.join(voiced)
    assert gateway.calls == [('ensure', 11, 55, 22)]
    assert sent(dispatcher) == [(22, 'Connected to: music', 300)]

    gateway = FakePlayerGateway()
    logic, dispatcher = make_logic(gateway)
    await logic.join(envelope)
    assert not gateway.calls


@pytest.mark.asyncio
async def test_join_failure_still_says_connected(voiced):  #pylint:disable=redefined-outer-name
    '''The error is shown, then the connected message anyway, as it has always been'''
    logic, dispatcher = make_logic(FakePlayerGateway(ensure=EnsureResult(ok=False, error='Bot cannot join channel music')))
    await logic.join(voiced)
    assert [content for _, content, _ in sent(dispatcher)] == ['Bot cannot join channel music', 'Connected to: music']


def broker_playing(title='Song', uuid='u-1', skip_result='ok', download=True):
    '''A broker that reports one playing track and answers skip_track with skip_result.'''
    broker = AsyncMock()
    playing = SimpleNamespace(uuid=uuid, download=SimpleNamespace(title=title) if download else None)
    broker.get_guild_queue.return_value = SimpleNamespace(playing=playing)
    broker.skip_track.return_value = skip_result
    return broker


PLAYING = PlayerStatus(exists=True, voice_connected=True, is_playing=True, text_channel_id=22)


@pytest.mark.asyncio
async def test_skip_asks_the_broker_to_skip_the_playing_track(voiced):  #pylint:disable=redefined-outer-name
    gateway = FakePlayerGateway(PLAYING)
    broker = broker_playing()
    logic, dispatcher = make_logic(gateway, broker=broker)
    await logic.skip(voiced)
    broker.skip_track.assert_awaited_once_with(11, 'u-1')
    assert gateway.calls == [('status', 11)]
    assert sent(dispatcher) == [(22, 'Skipping video "Song"', 300)]


@pytest.mark.asyncio
async def test_skip_names_the_track_even_when_its_entry_expired(voiced):  #pylint:disable=redefined-outer-name
    logic, dispatcher = make_logic(FakePlayerGateway(PLAYING), broker=broker_playing(download=False))
    await logic.skip(voiced)
    assert sent(dispatcher) == [(22, 'Skipping current video', 300)]


@pytest.mark.asyncio
async def test_skip_with_nothing_playing_is_silent(voiced):  #pylint:disable=redefined-outer-name
    '''Downloads pending but nothing playing: the gate passes, the broker has no track, no message'''
    pending = PlayerStatus(exists=True, voice_connected=True, is_playing=False, pending_downloads=1, text_channel_id=22)
    broker = AsyncMock()
    broker.get_guild_queue.return_value = SimpleNamespace(playing=None)
    logic, dispatcher = make_logic(FakePlayerGateway(pending), broker=broker)
    await logic.skip(voiced)
    broker.skip_track.assert_not_called()
    dispatcher.send_message.assert_not_called()


@pytest.mark.asyncio
async def test_skip_the_broker_refused_is_silent(voiced):  #pylint:disable=redefined-outer-name
    '''The track ended (or was already skipped) between reading the queue and the skip'''
    for result in ('not_current', 'no_player'):
        logic, dispatcher = make_logic(FakePlayerGateway(PLAYING), broker=broker_playing(skip_result=result))
        await logic.skip(voiced)
        dispatcher.send_message.assert_not_called()


@pytest.mark.asyncio
async def test_skip_stops_at_each_gate(envelope, voiced):  #pylint:disable=redefined-outer-name
    gateway = FakePlayerGateway()
    broker = broker_playing()
    logic, _ = make_logic(gateway, broker=broker)
    await logic.skip(envelope)
    assert not gateway.calls
    await logic.skip(voiced)
    assert gateway.calls == [('status', 11)]
    broker.skip_track.assert_not_called()


@pytest.mark.asyncio
async def test_stop(voiced, envelope):  #pylint:disable=redefined-outer-name
    gateway = FakePlayerGateway()
    logic, _ = make_logic(gateway)
    await logic.stop(voiced)
    assert gateway.calls == [('stop', 11, 22)]

    gateway = FakePlayerGateway()
    logic, _ = make_logic(gateway)
    await logic.stop(envelope)
    assert not gateway.calls


@pytest.mark.asyncio
async def test_move_messages(voiced):  #pylint:disable=redefined-outer-name
    broker = AsyncMock()
    status = PlayerStatus(exists=True, voice_connected=True, is_playing=True, text_channel_id=33)
    gateway = FakePlayerGateway(status)
    logic, _ = make_logic(gateway, broker=broker)
    await logic.move_messages(voiced)
    broker.open_guild.assert_awaited_once_with(11, 22)
    assert gateway.calls[-1] == ('move_messages', 11, 22)


@pytest.mark.asyncio
async def test_move_messages_already_there(voiced):  #pylint:disable=redefined-outer-name
    broker = AsyncMock()
    status = PlayerStatus(exists=True, voice_connected=True, is_playing=True, text_channel_id=22)
    gateway = FakePlayerGateway(status)
    logic, dispatcher = make_logic(gateway, broker=broker)
    await logic.move_messages(voiced)
    assert sent(dispatcher) == [(22, 'I am already sending messages to channel general', 300)]
    broker.open_guild.assert_not_called()
    assert ('move_messages', 11, 22) not in gateway.calls


@pytest.mark.asyncio
async def test_move_messages_stops_at_each_gate(envelope, voiced):  #pylint:disable=redefined-outer-name
    gateway = FakePlayerGateway()
    logic, _ = make_logic(gateway)
    await logic.move_messages(envelope)
    assert not gateway.calls
    await logic.move_messages(voiced)
    assert gateway.calls == [('status', 11)]


ACTIVE = PlayerStatus(exists=True, voice_connected=True, is_playing=True, text_channel_id=77)


def queued(*titles):
    return SimpleNamespace(items=[SimpleNamespace(title=t, media_request=SimpleNamespace(uuid=f'u-{t}')) for t in titles])


def queue_broker(*titles):
    broker = AsyncMock()
    broker.get_guild_queue.return_value = queued(*titles)
    return broker


QUEUE_COMMANDS = [
    lambda logic, env: logic.clear(env),
    lambda logic, env: logic.shuffle(env),
    lambda logic, env: logic.remove(env, '1'),
    lambda logic, env: logic.bump(env, '1'),
]


@pytest.mark.asyncio
@pytest.mark.parametrize('run', QUEUE_COMMANDS)
async def test_queue_commands_need_voice_and_an_active_player(run, envelope, voiced):  #pylint:disable=redefined-outer-name
    gateway = FakePlayerGateway(ACTIVE)
    logic, dispatcher = make_logic(gateway, broker=queue_broker('a'))
    await run(logic, envelope)
    assert sent(dispatcher) == [(22, 'Tyler not in voice chat channel. Please join one and try again', 300)]

    gateway = FakePlayerGateway(PlayerStatus(exists=False))
    logic, dispatcher = make_logic(gateway, broker=queue_broker('a'))
    await run(logic, voiced)
    assert sent(dispatcher) == [(22, NOT_PLAYING, 300)]


@pytest.mark.asyncio
@pytest.mark.parametrize('run', QUEUE_COMMANDS)
async def test_queue_commands_with_nothing_queued(run, voiced):  #pylint:disable=redefined-outer-name
    gateway = FakePlayerGateway(ACTIVE)
    broker = queue_broker()
    logic, dispatcher = make_logic(gateway, broker=broker)
    await run(logic, voiced)
    assert sent(dispatcher) == [(22, 'There are currently no more queued videos.', 300)]
    assert [c[0] for c in gateway.calls] == ['status']
    broker.clear_queue.assert_not_called()


@pytest.mark.asyncio
async def test_clear(voiced):  #pylint:disable=redefined-outer-name
    gateway = FakePlayerGateway(ACTIVE)
    broker = queue_broker('a', 'b')
    logic, dispatcher = make_logic(gateway, broker=broker)
    await logic.clear(voiced)
    broker.clear_queue.assert_awaited_once_with(11)
    assert gateway.calls == [('status', 11)]
    assert sent(dispatcher) == [(22, 'Cleared player queue', 300)]


@pytest.mark.asyncio
async def test_shuffle(voiced):  #pylint:disable=redefined-outer-name
    gateway = FakePlayerGateway(ACTIVE)
    broker = queue_broker('a', 'b')
    logic, dispatcher = make_logic(gateway, broker=broker)
    await logic.shuffle(voiced)
    broker.shuffle_queue.assert_awaited_once_with(11)
    assert gateway.calls == [('status', 11)]
    dispatcher.send_message.assert_not_called()


@pytest.mark.asyncio
async def test_remove(voiced):  #pylint:disable=redefined-outer-name
    gateway = FakePlayerGateway(ACTIVE)
    broker = queue_broker('a', 'b')
    broker.remove_queued_track.return_value = SimpleNamespace(title='b')
    logic, dispatcher = make_logic(gateway, broker=broker)
    await logic.remove(voiced, '2')
    broker.remove_queued_track.assert_awaited_once_with(11, 'u-b')
    assert sent(dispatcher) == [(22, 'Removed item b from queue', 300)]
    assert gateway.calls == [('status', 11)]


@pytest.mark.asyncio
async def test_bump(voiced):  #pylint:disable=redefined-outer-name
    gateway = FakePlayerGateway(ACTIVE)
    broker = queue_broker('a', 'b')
    broker.bump_queued_track.return_value = SimpleNamespace(title='b')
    logic, dispatcher = make_logic(gateway, broker=broker)
    await logic.bump(voiced, '2')
    broker.bump_queued_track.assert_awaited_once_with(11, 'u-b')
    assert sent(dispatcher) == [(22, 'Bumped item "b" to top of queue', 300)]
    assert gateway.calls == [('status', 11)]


@pytest.mark.asyncio
@pytest.mark.parametrize('verb', ['remove', 'bump'])
async def test_index_that_is_not_a_number(verb, voiced):  #pylint:disable=redefined-outer-name
    gateway = FakePlayerGateway(ACTIVE)
    broker = queue_broker('a')
    logic, dispatcher = make_logic(gateway, broker=broker)
    await getattr(logic, verb)(voiced, 'abc')
    assert sent(dispatcher) == [(22, 'Invalid queue index abc', 300)]
    assert [c[0] for c in gateway.calls] == ['status']


@pytest.mark.asyncio
@pytest.mark.parametrize('verb', ['remove', 'bump'])
@pytest.mark.parametrize('index', ['0', '3', '-1'])
async def test_index_outside_the_queue_never_reaches_the_broker(verb, index, voiced):  #pylint:disable=redefined-outer-name
    gateway = FakePlayerGateway(ACTIVE)
    broker = queue_broker('a', 'b')
    logic, dispatcher = make_logic(gateway, broker=broker)
    await getattr(logic, verb)(voiced, index)
    assert sent(dispatcher) == [(22, f'Unable to {verb} queue index {index}', 300)]
    getattr(broker, 'remove_queued_track' if verb == 'remove' else 'bump_queued_track').assert_not_called()


@pytest.mark.asyncio
@pytest.mark.parametrize('verb', ['remove', 'bump'])
async def test_track_that_moved_since_the_user_looked(verb, voiced):  #pylint:disable=redefined-outer-name
    gateway = FakePlayerGateway(ACTIVE)
    broker = queue_broker('a')
    broker.remove_queued_track.return_value = None
    broker.bump_queued_track.return_value = None
    logic, dispatcher = make_logic(gateway, broker=broker)
    await getattr(logic, verb)(voiced, '1')
    assert sent(dispatcher) == [(22, f'Unable to {verb} queue index 1', 300)]
    assert [c[0] for c in gateway.calls] == ['status']


@pytest.mark.asyncio
async def test_play_needs_voice(envelope):  #pylint:disable=redefined-outer-name
    enqueuer = AsyncMock()
    logic, dispatcher = make_logic(FakePlayerGateway(), enqueuer=enqueuer)
    await logic.play(envelope, 'song')
    assert sent(dispatcher) == [(22, 'Tyler not in voice chat channel. Please join one and try again', 300)]
    enqueuer.generate_media_requests_from_search.assert_not_called()


@pytest.mark.asyncio
async def test_play_stops_when_the_player_cannot_be_ensured(voiced):  #pylint:disable=redefined-outer-name
    enqueuer = AsyncMock()
    gateway = FakePlayerGateway(ensure=EnsureResult(ok=False, error='Bot cannot join channel music'))
    logic, dispatcher = make_logic(gateway, enqueuer=enqueuer)
    await logic.play(voiced, 'song')
    assert sent(dispatcher) == [(22, 'Bot cannot join channel music', 300)]
    enqueuer.generate_media_requests_from_search.assert_not_called()


@pytest.mark.asyncio
async def test_play_stops_quietly_when_the_player_already_explained(voiced):  #pylint:disable=redefined-outer-name
    enqueuer = AsyncMock()
    logic, dispatcher = make_logic(FakePlayerGateway(ensure=EnsureResult(ok=False)), enqueuer=enqueuer)
    await logic.play(voiced, 'song')
    dispatcher.send_message.assert_not_called()
    enqueuer.generate_media_requests_from_search.assert_not_called()


@pytest.mark.asyncio
async def test_play_searches(voiced):  #pylint:disable=redefined-outer-name
    enqueuer = AsyncMock()
    gateway = FakePlayerGateway()
    logic, _ = make_logic(gateway, enqueuer=enqueuer)
    await logic.play(voiced, 'song')
    assert ('ensure', 11, 55, 22) in gateway.calls
    enqueuer.generate_media_requests_from_search.assert_awaited_once_with(voiced, 'song')


def playlist_commands(playlist_id=5, is_history=False, database=True):
    playlist = AsyncMock()
    playlist.check_database_session.return_value = database
    playlist.get_playlist.return_value = (playlist_id, is_history)
    return playlist


@pytest.mark.asyncio
@pytest.mark.parametrize('args,shuffle,max_num', [
    ((), False, None),
    (('shuffle',), True, None),
    (('SHUFFLE', '16'), True, 16),
    (('16', 'shuffle'), True, 16),
    (('4', '9'), False, 4),
    (('nonsense',), False, None),
])
async def test_playlist_queue_parses_its_arguments(args, shuffle, max_num, voiced):  #pylint:disable=redefined-outer-name
    playlist = playlist_commands(is_history=True)
    logic, _ = make_logic(FakePlayerGateway(), playlist=playlist)
    await logic.playlist_queue(voiced, 3, args)
    playlist.get_playlist.assert_awaited_once_with(3, voiced)
    playlist.queue_items.assert_awaited_once_with(voiced, 5, shuffle, max_num, is_history=True)


@pytest.mark.asyncio
async def test_playlist_queue_gates(envelope, voiced):  #pylint:disable=redefined-outer-name
    # not in voice
    playlist = playlist_commands()
    logic, dispatcher = make_logic(FakePlayerGateway(), playlist=playlist)
    await logic.playlist_queue(envelope, 1, ())
    assert len(sent(dispatcher)) == 1
    playlist.check_database_session.assert_not_called()

    # no database: asked before the player is touched
    playlist = playlist_commands(database=False)
    gateway = FakePlayerGateway()
    logic, _ = make_logic(gateway, playlist=playlist)
    await logic.playlist_queue(voiced, 1, ())
    assert not gateway.calls
    playlist.queue_items.assert_not_called()

    # player cannot be ensured
    playlist = playlist_commands()
    logic, _ = make_logic(FakePlayerGateway(ensure=EnsureResult(ok=False)), playlist=playlist)
    await logic.playlist_queue(voiced, 1, ())
    playlist.get_playlist.assert_not_called()

    # unknown playlist
    playlist = playlist_commands(playlist_id=None)
    logic, _ = make_logic(FakePlayerGateway(), playlist=playlist)
    await logic.playlist_queue(voiced, 1, ())
    playlist.queue_items.assert_not_called()


@pytest.mark.asyncio
async def test_random_play(voiced):  #pylint:disable=redefined-outer-name
    playlist = playlist_commands(playlist_id=9, is_history=True)
    logic, _ = make_logic(FakePlayerGateway(), playlist=playlist)
    await logic.random_play(voiced)
    playlist.get_playlist.assert_awaited_once_with(0, voiced)
    playlist.queue_items.assert_awaited_once_with(voiced, 9, shuffle=True, max_num=32, is_history=True)


@pytest.mark.asyncio
async def test_random_play_gates(envelope, voiced):  #pylint:disable=redefined-outer-name
    playlist = playlist_commands()
    logic, _ = make_logic(FakePlayerGateway(), playlist=playlist)
    await logic.random_play(envelope)
    playlist.queue_items.assert_not_called()

    playlist = playlist_commands(database=False)
    logic, _ = make_logic(FakePlayerGateway(), playlist=playlist)
    await logic.random_play(voiced)
    playlist.queue_items.assert_not_called()

    playlist = playlist_commands()
    logic, _ = make_logic(FakePlayerGateway(ensure=EnsureResult(ok=False)), playlist=playlist)
    await logic.random_play(voiced)
    playlist.get_playlist.assert_not_called()

    playlist = playlist_commands(playlist_id=None)
    logic, _ = make_logic(FakePlayerGateway(), playlist=playlist)
    await logic.random_play(voiced)
    playlist.queue_items.assert_not_called()
