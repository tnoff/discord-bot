"""Tests for Music command edge cases (no voice, no player, empty queue, etc.)"""
import asyncio
from contextlib import ExitStack, asynccontextmanager
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import MagicMock, AsyncMock

import pytest

from discord_gateway.cogs.music import Music
from discord_gateway.cogs.music_helpers.music_player import MusicPlayer

from discord_gateway.tests.cogs.test_music import BASE_MUSIC_CONFIG
from tests.helpers import fake_engine, fake_context, FakeChannel, fake_media_download  # pylint: disable=unused-import
from tests.helpers import attach_in_process_broker, queue_in_broker


# ---------------------------------------------------------------------------
# Helper
# ---------------------------------------------------------------------------

class _FakeVoiceState:
    """Minimal voice-state stub: just needs .channel"""
    def __init__(self, channel):
        self.channel = channel


def _set_author_voice(fake_context, channel=None):  # pylint: disable=redefined-outer-name
    """Set the author's voice state to the given channel (defaults to fake_context['channel'])."""
    chan = channel if channel is not None else fake_context['channel']
    fake_context['author'].voice = _FakeVoiceState(chan)


@asynccontextmanager
async def _queued_tracks(cog, fake_context, count):  # pylint: disable=redefined-outer-name
    """`count` downloaded tracks in the broker's queue for the fixture guild, in order."""
    with TemporaryDirectory() as tmp_dir, ExitStack() as stack:
        downloads = [stack.enter_context(fake_media_download(Path(tmp_dir), fake_context=fake_context))
                     for _ in range(count)]
        await queue_in_broker(cog, *downloads)
        yield downloads


async def _queued_titles(cog, fake_context):  # pylint: disable=redefined-outer-name
    return [item.title for item in (await cog.broker_client.get_guild_queue(fake_context['guild'].id)).items]


async def _player_in_voice(cog, fake_context, mocker):  # pylint: disable=redefined-outer-name
    mocker.patch.object(MusicPlayer, 'start_tasks')
    mocker.patch('discord_gateway.cogs.music.sleep', return_value=True)
    await cog.get_player(fake_context['guild'].id, ctx=fake_context['context'],
                         join_channel=fake_context['channel'])
    return cog.players[fake_context['guild'].id]


# ---------------------------------------------------------------------------
# __check_author_voice_chat
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_check_author_voice_chat_no_voice(fake_context):  # pylint: disable=redefined-outer-name
    """Returns None and sends message when author is not in any voice channel."""
    # author.voice is None by default → AttributeError → early return
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, fake_context['dispatcher'])
    attach_in_process_broker(cog)
    cog.dispatcher = MagicMock()
    result = await cog._Music__check_author_voice_chat(fake_context['context'])  # pylint: disable=protected-access
    assert result is None
    cog.dispatcher.send_message.assert_called_once()
    assert 'not in voice chat channel' in cog.dispatcher.send_message.call_args[0][2]


@pytest.mark.asyncio
async def test_check_author_voice_chat_wrong_guild(fake_context):  # pylint: disable=redefined-outer-name
    """Returns None when author's voice channel belongs to a different guild."""
    # FakeChannel() creates its own new FakeGuild → different guild.id object
    other_channel = FakeChannel()
    _set_author_voice(fake_context, channel=other_channel)
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, fake_context['dispatcher'])
    attach_in_process_broker(cog)
    cog.dispatcher = MagicMock()
    result = await cog._Music__check_author_voice_chat(fake_context['context'])  # pylint: disable=protected-access
    assert result is None
    cog.dispatcher.send_message.assert_called_once()
    assert 'not joined to channel bot is in' in cog.dispatcher.send_message.call_args[0][2]


@pytest.mark.asyncio
async def test_check_author_voice_chat_no_check_returns_channel(fake_context):  # pylint: disable=redefined-outer-name
    """check_voice_chats=False skips guild comparison and returns channel even for 'other' guild."""
    other_channel = FakeChannel()
    _set_author_voice(fake_context, channel=other_channel)
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, fake_context['dispatcher'])
    attach_in_process_broker(cog)
    cog.dispatcher = MagicMock()
    result = await cog._Music__check_author_voice_chat(  # pylint: disable=protected-access
        fake_context['context'], check_voice_chats=False
    )
    assert result is other_channel


# ---------------------------------------------------------------------------
# __ensure_player
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_ensure_player_async_timeout(fake_context, mocker):  # pylint: disable=redefined-outer-name
    """Returns None and sends message when joining voice channel times out."""
    _set_author_voice(fake_context)
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, fake_context['dispatcher'])
    attach_in_process_broker(cog)
    cog.dispatcher = MagicMock()
    mocker.patch.object(cog, 'get_player', side_effect=asyncio.TimeoutError('timed out'))
    result = await cog._Music__ensure_player(fake_context['context'], fake_context['channel'])  # pylint: disable=protected-access
    assert result is None
    cog.dispatcher.send_message.assert_called_once()
    assert 'cannot join channel' in cog.dispatcher.send_message.call_args[0][2]


# ---------------------------------------------------------------------------
# skip_
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_skip_no_voice(fake_context):  # pylint: disable=redefined-outer-name
    """skip_ returns early when author is not in voice."""
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, fake_context['dispatcher'])
    attach_in_process_broker(cog)
    cog.dispatcher = MagicMock()
    await cog.skip_.callback(cog, fake_context['context'])
    cog.dispatcher.send_message.assert_called_once()
    assert 'not in voice chat channel' in cog.dispatcher.send_message.call_args[0][2]


@pytest.mark.asyncio
async def test_skip_no_player(fake_context, mocker):  # pylint: disable=redefined-outer-name
    """skip_ returns early with 'not currently playing' when no player exists."""
    _set_author_voice(fake_context)
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, fake_context['dispatcher'])
    attach_in_process_broker(cog)
    cog.dispatcher = MagicMock()
    mocker.patch.object(MusicPlayer, 'start_tasks')
    # No player created → check_voice_client_active=True → sends "not currently playing"
    await cog.skip_.callback(cog, fake_context['context'])
    cog.dispatcher.send_message.assert_called_once()
    assert 'not currently playing' in cog.dispatcher.send_message.call_args[0][2]


@pytest.mark.asyncio
async def test_skip_not_playing(fake_context, mocker):  # pylint: disable=redefined-outer-name
    """skip_ returns early at is_playing() check when nothing is playing."""
    _set_author_voice(fake_context)
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, fake_context['dispatcher'])
    attach_in_process_broker(cog)
    cog.dispatcher = MagicMock()
    mocker.patch.object(MusicPlayer, 'start_tasks')
    mocker.patch('discord_gateway.cogs.music.sleep', return_value=True)
    # Create player with voice channel so check_voice_client_active passes
    await cog.get_player(fake_context['guild'].id, ctx=fake_context['context'],
                         join_channel=fake_context['channel'])
    # Make is_playing return True (for get_player check) then False (for skip_'s own check)
    fake_context['guild'].voice_client.is_playing = MagicMock(side_effect=[True, False])
    await cog.skip_.callback(cog, fake_context['context'])
    # No "Skipping video" message should have been sent
    for call in cog.dispatcher.send_message.call_args_list:
        assert 'Skipping' not in call[0][2]


# ---------------------------------------------------------------------------
# clear
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_clear_no_voice(fake_context):  # pylint: disable=redefined-outer-name
    """clear returns early when author is not in voice."""
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, fake_context['dispatcher'])
    attach_in_process_broker(cog)
    cog.dispatcher = MagicMock()
    await cog.clear.callback(cog, fake_context['context'])
    cog.dispatcher.send_message.assert_called_once()
    assert 'not in voice chat channel' in cog.dispatcher.send_message.call_args[0][2]


@pytest.mark.asyncio
async def test_clear_no_player(fake_context, mocker):  # pylint: disable=redefined-outer-name
    """clear returns early with 'not currently playing' when no player exists."""
    _set_author_voice(fake_context)
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, fake_context['dispatcher'])
    attach_in_process_broker(cog)
    cog.dispatcher = MagicMock()
    mocker.patch.object(MusicPlayer, 'start_tasks')
    await cog.clear.callback(cog, fake_context['context'])
    cog.dispatcher.send_message.assert_called_once()
    assert 'not currently playing' in cog.dispatcher.send_message.call_args[0][2]


@pytest.mark.asyncio
async def test_clear_empty_queue(fake_context, mocker):  # pylint: disable=redefined-outer-name
    """clear sends 'no more queued videos' when the queue is empty."""
    _set_author_voice(fake_context)
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, fake_context['dispatcher'])
    attach_in_process_broker(cog)
    cog.dispatcher = MagicMock()
    mocker.patch.object(MusicPlayer, 'start_tasks')
    mocker.patch('discord_gateway.cogs.music.sleep', return_value=True)
    await cog.get_player(fake_context['guild'].id, ctx=fake_context['context'],
                         join_channel=fake_context['channel'])
    await cog.clear.callback(cog, fake_context['context'])
    sent = [c[0][2] for c in cog.dispatcher.send_message.call_args_list]
    assert any('no more queued videos' in m for m in sent)


@pytest.mark.asyncio
async def test_clear_empties_the_broker_queue(fake_context, mocker):  # pylint: disable=redefined-outer-name
    """clear takes every queued track out of the broker and drops each one's staged copy."""
    _set_author_voice(fake_context)
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, fake_context['dispatcher'])
    attach_in_process_broker(cog)
    cog.dispatcher = MagicMock()
    player = await _player_in_voice(cog, fake_context, mocker)
    discard = mocker.patch.object(player, 'discard_staged')
    async with _queued_tracks(cog, fake_context, 2) as (first, second):
        await cog.clear.callback(cog, fake_context['context'])
        assert await _queued_titles(cog, fake_context) == []
        sent = [c[0][2] for c in cog.dispatcher.send_message.call_args_list]
        assert any('Cleared player queue' in m for m in sent)
        assert [str(call.args[0].media_request.uuid) for call in discard.call_args_list] == [
            str(first.media_request.uuid), str(second.media_request.uuid)]
        assert await cog.broker_client.local_broker.get_entry(str(first.media_request.uuid)) is None


# ---------------------------------------------------------------------------
# history_
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_history_no_voice(fake_context):  # pylint: disable=redefined-outer-name
    """history_ returns early when author is not in voice."""
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, fake_context['dispatcher'])
    attach_in_process_broker(cog)
    cog.dispatcher = MagicMock()
    await cog.history_.callback(cog, fake_context['context'])
    cog.dispatcher.send_message.assert_called_once()
    assert 'not in voice chat channel' in cog.dispatcher.send_message.call_args[0][2]


@pytest.mark.asyncio
async def test_history_empty(fake_context, mocker):  # pylint: disable=redefined-outer-name
    """history_ sends 'no videos played' when history is empty."""
    _set_author_voice(fake_context)
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, fake_context['dispatcher'])
    attach_in_process_broker(cog)
    cog.dispatcher = MagicMock()
    mocker.patch.object(MusicPlayer, 'start_tasks')
    mocker.patch('discord_gateway.cogs.music.sleep', return_value=True)
    # history_ uses get_player without check_voice_client_active so it creates a player
    await cog.history_.callback(cog, fake_context['context'])
    sent = [c[0][2] for c in cog.dispatcher.send_message.call_args_list]
    assert any('no videos played' in m for m in sent)


# ---------------------------------------------------------------------------
# shuffle_
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_shuffle_no_voice(fake_context):  # pylint: disable=redefined-outer-name
    """shuffle_ returns early when author is not in voice."""
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, fake_context['dispatcher'])
    attach_in_process_broker(cog)
    cog.dispatcher = MagicMock()
    await cog.shuffle_.callback(cog, fake_context['context'])
    cog.dispatcher.send_message.assert_called_once()
    assert 'not in voice chat channel' in cog.dispatcher.send_message.call_args[0][2]


@pytest.mark.asyncio
async def test_shuffle_no_player(fake_context, mocker):  # pylint: disable=redefined-outer-name
    """shuffle_ returns early with 'not currently playing' when no player exists."""
    _set_author_voice(fake_context)
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, fake_context['dispatcher'])
    attach_in_process_broker(cog)
    cog.dispatcher = MagicMock()
    mocker.patch.object(MusicPlayer, 'start_tasks')
    await cog.shuffle_.callback(cog, fake_context['context'])
    cog.dispatcher.send_message.assert_called_once()
    assert 'not currently playing' in cog.dispatcher.send_message.call_args[0][2]


@pytest.mark.asyncio
async def test_shuffle_empty_queue(fake_context, mocker):  # pylint: disable=redefined-outer-name
    """shuffle_ sends 'no more queued videos' when queue is empty."""
    _set_author_voice(fake_context)
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, fake_context['dispatcher'])
    attach_in_process_broker(cog)
    cog.dispatcher = MagicMock()
    mocker.patch.object(MusicPlayer, 'start_tasks')
    mocker.patch('discord_gateway.cogs.music.sleep', return_value=True)
    await cog.get_player(fake_context['guild'].id, ctx=fake_context['context'],
                         join_channel=fake_context['channel'])
    await cog.shuffle_.callback(cog, fake_context['context'])
    sent = [c[0][2] for c in cog.dispatcher.send_message.call_args_list]
    assert any('no more queued videos' in m for m in sent)


# ---------------------------------------------------------------------------
# remove_item
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_remove_item_no_voice(fake_context):  # pylint: disable=redefined-outer-name
    """remove_item returns early when author is not in voice."""
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, fake_context['dispatcher'])
    attach_in_process_broker(cog)
    cog.dispatcher = MagicMock()
    await cog.remove_item.callback(cog, fake_context['context'], '1')
    cog.dispatcher.send_message.assert_called_once()
    assert 'not in voice chat channel' in cog.dispatcher.send_message.call_args[0][2]


@pytest.mark.asyncio
async def test_remove_item_no_player(fake_context, mocker):  # pylint: disable=redefined-outer-name
    """remove_item returns early when no player exists."""
    _set_author_voice(fake_context)
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, fake_context['dispatcher'])
    attach_in_process_broker(cog)
    cog.dispatcher = MagicMock()
    mocker.patch.object(MusicPlayer, 'start_tasks')
    await cog.remove_item.callback(cog, fake_context['context'], '1')
    cog.dispatcher.send_message.assert_called_once()
    assert 'not currently playing' in cog.dispatcher.send_message.call_args[0][2]


@pytest.mark.asyncio
async def test_remove_item_empty_queue(fake_context, mocker):  # pylint: disable=redefined-outer-name
    """remove_item sends 'no more queued videos' when queue is empty."""
    _set_author_voice(fake_context)
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, fake_context['dispatcher'])
    attach_in_process_broker(cog)
    cog.dispatcher = MagicMock()
    mocker.patch.object(MusicPlayer, 'start_tasks')
    mocker.patch('discord_gateway.cogs.music.sleep', return_value=True)
    await cog.get_player(fake_context['guild'].id, ctx=fake_context['context'],
                         join_channel=fake_context['channel'])
    await cog.remove_item.callback(cog, fake_context['context'], '1')
    sent = [c[0][2] for c in cog.dispatcher.send_message.call_args_list]
    assert any('no more queued videos' in m for m in sent)


@pytest.mark.asyncio
async def test_remove_item_invalid_index(fake_context, mocker):  # pylint: disable=redefined-outer-name
    """remove_item sends 'Invalid queue index' when a non-integer index is given."""
    _set_author_voice(fake_context)
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, fake_context['dispatcher'])
    attach_in_process_broker(cog)
    cog.dispatcher = MagicMock()
    await _player_in_voice(cog, fake_context, mocker)
    async with _queued_tracks(cog, fake_context, 1):
        await cog.remove_item.callback(cog, fake_context['context'], 'not-a-number')
        sent = [c[0][2] for c in cog.dispatcher.send_message.call_args_list]
        assert any('Invalid queue index' in m for m in sent)
        assert len(await _queued_titles(cog, fake_context)) == 1


@pytest.mark.asyncio
@pytest.mark.parametrize('index', ['0', '99', '-1'])
async def test_remove_item_not_found(fake_context, mocker, index):  # pylint: disable=redefined-outer-name
    """remove_item sends 'Unable to remove' when the index is outside the queue, and removes nothing."""
    _set_author_voice(fake_context)
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, fake_context['dispatcher'])
    attach_in_process_broker(cog)
    cog.dispatcher = MagicMock()
    await _player_in_voice(cog, fake_context, mocker)
    async with _queued_tracks(cog, fake_context, 2):
        await cog.remove_item.callback(cog, fake_context['context'], index)
        sent = [c[0][2] for c in cog.dispatcher.send_message.call_args_list]
        assert any('Unable to remove' in m for m in sent)
        assert len(await _queued_titles(cog, fake_context)) == 2


@pytest.mark.asyncio
async def test_remove_item_takes_the_track_out_of_the_broker_queue(fake_context, mocker):  # pylint: disable=redefined-outer-name
    """remove_item takes the numbered track out, tells the channel, and drops its staged copy."""
    _set_author_voice(fake_context)
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, fake_context['dispatcher'])
    attach_in_process_broker(cog)
    cog.dispatcher = MagicMock()
    player = await _player_in_voice(cog, fake_context, mocker)
    discard = mocker.patch.object(player, 'discard_staged')
    async with _queued_tracks(cog, fake_context, 3) as (first, second, third):
        await cog.remove_item.callback(cog, fake_context['context'], '2')
        assert await _queued_titles(cog, fake_context) == [first.title, third.title]
        sent = [c[0][2] for c in cog.dispatcher.send_message.call_args_list]
        assert any(f'Removed item {second.title} from queue' in m for m in sent)
        discard.assert_called_once()
        assert str(discard.call_args.args[0].media_request.uuid) == str(second.media_request.uuid)
        # Its entry is released too, not left registered in the broker
        assert await cog.broker_client.local_broker.get_entry(str(second.media_request.uuid)) is None


# ---------------------------------------------------------------------------
# bump_item
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_bump_item_no_voice(fake_context):  # pylint: disable=redefined-outer-name
    """bump_item returns early when author is not in voice."""
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, fake_context['dispatcher'])
    attach_in_process_broker(cog)
    cog.dispatcher = MagicMock()
    await cog.bump_item.callback(cog, fake_context['context'], '1')
    cog.dispatcher.send_message.assert_called_once()
    assert 'not in voice chat channel' in cog.dispatcher.send_message.call_args[0][2]


@pytest.mark.asyncio
async def test_bump_item_no_player(fake_context, mocker):  # pylint: disable=redefined-outer-name
    """bump_item returns early when no player exists."""
    _set_author_voice(fake_context)
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, fake_context['dispatcher'])
    attach_in_process_broker(cog)
    cog.dispatcher = MagicMock()
    mocker.patch.object(MusicPlayer, 'start_tasks')
    await cog.bump_item.callback(cog, fake_context['context'], '1')
    cog.dispatcher.send_message.assert_called_once()
    assert 'not currently playing' in cog.dispatcher.send_message.call_args[0][2]


@pytest.mark.asyncio
async def test_bump_item_empty_queue(fake_context, mocker):  # pylint: disable=redefined-outer-name
    """bump_item sends 'no more queued videos' when queue is empty."""
    _set_author_voice(fake_context)
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, fake_context['dispatcher'])
    attach_in_process_broker(cog)
    cog.dispatcher = MagicMock()
    mocker.patch.object(MusicPlayer, 'start_tasks')
    mocker.patch('discord_gateway.cogs.music.sleep', return_value=True)
    await cog.get_player(fake_context['guild'].id, ctx=fake_context['context'],
                         join_channel=fake_context['channel'])
    await cog.bump_item.callback(cog, fake_context['context'], '1')
    sent = [c[0][2] for c in cog.dispatcher.send_message.call_args_list]
    assert any('no more queued videos' in m for m in sent)


@pytest.mark.asyncio
async def test_bump_item_invalid_index(fake_context, mocker):  # pylint: disable=redefined-outer-name
    """bump_item sends 'Invalid queue index' when a non-integer index is given."""
    _set_author_voice(fake_context)
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, fake_context['dispatcher'])
    attach_in_process_broker(cog)
    cog.dispatcher = MagicMock()
    await _player_in_voice(cog, fake_context, mocker)
    async with _queued_tracks(cog, fake_context, 1):
        await cog.bump_item.callback(cog, fake_context['context'], 'not-a-number')
        sent = [c[0][2] for c in cog.dispatcher.send_message.call_args_list]
        assert any('Invalid queue index' in m for m in sent)


@pytest.mark.asyncio
@pytest.mark.parametrize('index', ['0', '99', '-1'])
async def test_bump_item_not_found(fake_context, mocker, index):  # pylint: disable=redefined-outer-name
    """bump_item sends 'Unable to bump' when the index is outside the queue, and reorders nothing."""
    _set_author_voice(fake_context)
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, fake_context['dispatcher'])
    attach_in_process_broker(cog)
    cog.dispatcher = MagicMock()
    await _player_in_voice(cog, fake_context, mocker)
    async with _queued_tracks(cog, fake_context, 2) as (first, second):
        await cog.bump_item.callback(cog, fake_context['context'], index)
        sent = [c[0][2] for c in cog.dispatcher.send_message.call_args_list]
        assert any('Unable to bump' in m for m in sent)
        assert await _queued_titles(cog, fake_context) == [first.title, second.title]


@pytest.mark.asyncio
async def test_bump_item_moves_the_track_to_the_top(fake_context, mocker):  # pylint: disable=redefined-outer-name
    """bump_item moves the numbered track to the head of the broker's queue and restages the window."""
    _set_author_voice(fake_context)
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, fake_context['dispatcher'])
    attach_in_process_broker(cog)
    cog.dispatcher = MagicMock()
    player = await _player_in_voice(cog, fake_context, mocker)
    prefetch = mocker.patch.object(player, 'trigger_prefetch')
    async with _queued_tracks(cog, fake_context, 3) as (first, second, third):
        await cog.bump_item.callback(cog, fake_context['context'], '3')
        assert await _queued_titles(cog, fake_context) == [third.title, first.title, second.title]
        sent = [c[0][2] for c in cog.dispatcher.send_message.call_args_list]
        assert any(f'Bumped item "{third.title}" to top of queue' in m for m in sent)
        prefetch.assert_called_once()


# ---------------------------------------------------------------------------
# stop_
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_stop_no_voice(fake_context):  # pylint: disable=redefined-outer-name
    """stop_ returns early when author is not in voice."""
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, fake_context['dispatcher'])
    attach_in_process_broker(cog)
    cog.dispatcher = MagicMock()
    await cog.stop_.callback(cog, fake_context['context'])
    cog.dispatcher.send_message.assert_called_once()
    assert 'not in voice chat channel' in cog.dispatcher.send_message.call_args[0][2]


# ---------------------------------------------------------------------------
# move_messages_here
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_move_messages_here_no_voice(fake_context):  # pylint: disable=redefined-outer-name
    """move_messages_here returns early when author is not in voice."""
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, fake_context['dispatcher'])
    attach_in_process_broker(cog)
    cog.dispatcher = MagicMock()
    await cog.move_messages_here.callback(cog, fake_context['context'])
    cog.dispatcher.send_message.assert_called_once()
    assert 'not in voice chat channel' in cog.dispatcher.send_message.call_args[0][2]


@pytest.mark.asyncio
async def test_move_messages_here_no_player(fake_context, mocker):  # pylint: disable=redefined-outer-name
    """move_messages_here returns early when no player exists."""
    _set_author_voice(fake_context)
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, fake_context['dispatcher'])
    attach_in_process_broker(cog)
    cog.dispatcher = MagicMock()
    mocker.patch.object(MusicPlayer, 'start_tasks')
    await cog.move_messages_here.callback(cog, fake_context['context'])
    cog.dispatcher.send_message.assert_called_once()
    assert 'not currently playing' in cog.dispatcher.send_message.call_args[0][2]


@pytest.mark.asyncio
async def test_move_messages_here_same_channel(fake_context, mocker):  # pylint: disable=redefined-outer-name
    """move_messages_here sends 'already sending messages' when channel unchanged."""
    _set_author_voice(fake_context)
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, fake_context['dispatcher'])
    attach_in_process_broker(cog)
    cog.dispatcher = MagicMock()
    mocker.patch.object(MusicPlayer, 'start_tasks')
    mocker.patch('discord_gateway.cogs.music.sleep', return_value=True)
    await cog.get_player(fake_context['guild'].id, ctx=fake_context['context'],
                         join_channel=fake_context['channel'])
    # text_channel is set to ctx.channel during get_player; command is invoked
    # in the same channel → triggers the "already here" path
    await cog.move_messages_here.callback(cog, fake_context['context'])
    sent = [c[0][2] for c in cog.dispatcher.send_message.call_args_list]
    assert any('already sending messages' in m for m in sent)


# ---------------------------------------------------------------------------
# play_
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_play_no_player(fake_context, mocker):  # pylint: disable=redefined-outer-name
    """play_ returns early when __ensure_player returns None (e.g., timeout joining channel)."""
    _set_author_voice(fake_context)
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, fake_context['dispatcher'])
    attach_in_process_broker(cog)
    cog.dispatcher = MagicMock()
    # Patch the name-mangled method to return None (simulates timeout / failure)
    mocker.patch.object(cog, '_Music__ensure_player', new=AsyncMock(return_value=None))
    await cog.play_.callback(cog, fake_context['context'], search='anything')
    # __ensure_player returned None → play_ returns before calling _generate_media_requests_from_search
    cog.dispatcher.send_message.assert_not_called()
