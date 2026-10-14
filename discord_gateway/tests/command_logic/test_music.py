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

    def __init__(self, status=None, ensure=None, skip=None):
        self.status_result = status or PlayerStatus(exists=False)
        self.ensure_result = ensure or EnsureResult(ok=True)
        self.skip_result = skip
        self.calls = []

    async def status(self, guild_id):
        self.calls.append(('status', guild_id))
        return self.status_result

    async def ensure(self, guild_id, voice_channel_id, text_channel_id):
        self.calls.append(('ensure', guild_id, voice_channel_id, text_channel_id))
        return self.ensure_result

    async def skip(self, guild_id):
        self.calls.append(('skip', guild_id))
        return self.skip_result

    async def stop(self, guild_id, text_channel_id):
        self.calls.append(('stop', guild_id, text_channel_id))

    async def move_messages(self, guild_id, text_channel_id):
        self.calls.append(('move_messages', guild_id, text_channel_id))


def make_logic(gateway, analytics=None, broker=None):
    dispatcher = MagicMock()
    logic = MusicCommands(dispatcher, broker or AsyncMock(), analytics, gateway,
                          logging.getLogger('test'), 300)
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


@pytest.mark.asyncio
async def test_skip(voiced):  #pylint:disable=redefined-outer-name
    playing = PlayerStatus(exists=True, voice_connected=True, is_playing=True, text_channel_id=22)
    gateway = FakePlayerGateway(playing, skip='Song')
    logic, dispatcher = make_logic(gateway)
    await logic.skip(voiced)
    assert ('skip', 11) in gateway.calls
    assert sent(dispatcher) == [(22, 'Skipping video "Song"', 300)]


@pytest.mark.asyncio
async def test_skip_nothing_playing_is_silent(voiced):  #pylint:disable=redefined-outer-name
    '''Downloads pending but nothing playing: the gate passes, the skip finds nothing, no message'''
    pending = PlayerStatus(exists=True, voice_connected=True, is_playing=False, pending_downloads=1, text_channel_id=22)
    logic, dispatcher = make_logic(FakePlayerGateway(pending, skip=None))
    await logic.skip(voiced)
    dispatcher.send_message.assert_not_called()


@pytest.mark.asyncio
async def test_skip_stops_at_each_gate(envelope, voiced):  #pylint:disable=redefined-outer-name
    gateway = FakePlayerGateway()
    logic, _ = make_logic(gateway)
    await logic.skip(envelope)
    assert not gateway.calls
    await logic.skip(voiced)
    assert gateway.calls == [('status', 11)]


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
