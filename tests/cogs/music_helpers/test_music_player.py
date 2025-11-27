from contextlib import contextmanager
import logging
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import Mock

import pytest

from discord_bot.exceptions import ExitEarlyException

from discord_bot.cogs.music_helpers.message_queue import MessageQueue, MessageType
from discord_bot.cogs.music_helpers.music_player import MusicPlayer
from discord_bot.utils.queue import Queue

from tests.helpers import FakeChannel, fake_context, fake_media_download, FakeVoiceClient #pylint:disable=unused-import

@contextmanager
def with_music_player(fake_context): #pylint:disable=redefined-outer-name
    with TemporaryDirectory() as tmp_dir:
        message_queue = MessageQueue()
        history_queue = Queue()
        player = MusicPlayer(logging, fake_context['context'], 10, 0.01, Path(tmp_dir), message_queue, None, history_queue)
        yield player

def test_music_player_basic(fake_context): #pylint:disable=redefined-outer-name
    with with_music_player(fake_context) as player:
        assert player is not None

@pytest.mark.asyncio
async def test_music_player_loop_exit_with_async_timeout(fake_context): #pylint:disable=redefined-outer-name
    with with_music_player(fake_context) as player:
        with pytest.raises(ExitEarlyException) as exc:
            await player.player_loop()
        assert 'MusicPlayer hit async timeout on player wait' in str(exc.value)

@pytest.mark.asyncio
async def test_music_player_loop_exiting_voice_client(fake_context): #pylint:disable=redefined-outer-name
    fake_context['guild'].voice_client = None
    with with_music_player(fake_context) as player:
        with fake_media_download(player.file_dir, fake_context=fake_context) as media_download:
            player.add_to_play_queue(media_download)
            with pytest.raises(ExitEarlyException) as exc:
                await player.player_loop()
            assert 'No voice client in guild, ending loop' in str(exc.value)


@pytest.mark.asyncio
async def test_music_player_loop_basic(fake_context): #pylint:disable=redefined-outer-name
    fake_context['guild'].voice_client = FakeVoiceClient()
    with with_music_player(fake_context) as player:
        with fake_media_download(player.file_dir, fake_context=fake_context) as media_download:
            player.add_to_play_queue(media_download)
            await player.player_loop()
            assert player._history.get_nowait() == media_download #pylint:disable=protected-access
            assert player._play_queue.empty() #pylint:disable=protected-access
            assert player.message_queue.get_next_message() == (MessageType.MULTIPLE_MUTABLE, f'play_order-{fake_context["guild"].id}')

@pytest.mark.asyncio
async def test_music_player_join_already_there(fake_context): #pylint:disable=redefined-outer-name
    with with_music_player(fake_context) as player:
        c = FakeChannel()
        assert await player.join_voice(c) is True

@pytest.mark.asyncio
async def test_music_player_join_no_voice(fake_context): #pylint:disable=redefined-outer-name
    with with_music_player(fake_context) as player:
        c = FakeChannel()
        assert await player.join_voice(c) is True

@pytest.mark.asyncio
async def test_music_player_join_move_to(fake_context): #pylint:disable=redefined-outer-name
    fake_context['guild'].voice_client = FakeVoiceClient()
    with with_music_player(fake_context) as player:
        c = FakeChannel()
        assert await player.join_voice(c) is True
        assert fake_context['guild'].voice_client.channel == c

@pytest.mark.asyncio
async def test_music_player_voice_channel_inactive_no_voice(fake_context): #pylint:disable=redefined-outer-name
    fake_context['guild'].voice_client = FakeVoiceClient()
    with with_music_player(fake_context) as player:
        assert player.voice_channel_active() is True

@pytest.mark.asyncio
async def test_music_player_voice_channel_with_no_bot(fake_context): #pylint:disable=redefined-outer-name
    fake_context['guild'].voice_client = FakeVoiceClient()
    with with_music_player(fake_context) as player:
        assert player.voice_channel_active() is True

@pytest.mark.asyncio
async def test_music_player_voice_channel_with_only_bot(fake_context): #pylint:disable=redefined-outer-name
    fake_context['guild'].voice_client = FakeVoiceClient()
    fake_context['channel'].members = [fake_context['bot'].user]
    fake_context['guild'].voice_client.channel = fake_context['channel']
    with with_music_player(fake_context) as player:
        assert player.voice_channel_active() is False

@pytest.mark.asyncio
async def test_music_player_loop_rollover_history(fake_context): #pylint:disable=redefined-outer-name
    fake_context['guild'].voice_client = FakeVoiceClient()
    with with_music_player(fake_context) as player:
        with fake_media_download(player.file_dir, fake_context=fake_context) as sd:
            player.add_to_play_queue(sd)
            await player.player_loop()
            with fake_media_download(player.file_dir, fake_context=fake_context) as sd2:
                player.add_to_play_queue(sd2)
                await player.player_loop()
                assert player._play_queue.empty() #pylint:disable=protected-access

                assert player.get_history_items()[0] == sd
                assert not player.check_history_empty()

def test_music_get_player_messages(fake_context): #pylint:disable=redefined-outer-name
    fake_context['guild'].voice_client = FakeVoiceClient()
    with with_music_player(fake_context) as player:
        with fake_media_download(player.file_dir, fake_context=fake_context) as sd:
            player.add_to_play_queue(sd)
            result = player.get_queue_order_messages()
            assert result == [f'```Pos|| Wait Time|| Title                                           || Uploader\n-----------------------------------------------------------------------------\n1  || 00:00    || {sd.title}                                    || {sd.uploader}```'] #pylint:disable=no-member

def test_music_get_player_paths(fake_context): #pylint:disable=redefined-outer-name
    fake_context['guild'].voice_client = FakeVoiceClient()
    with with_music_player(fake_context) as player:
        with fake_media_download(player.file_dir, fake_context=fake_context) as sd:
            player.add_to_play_queue(sd)
            result = player.get_file_paths()
            assert result[0] == sd.file_path

def test_music_clear_queue_messages(fake_context): #pylint:disable=redefined-outer-name
    fake_context['guild'].voice_client = FakeVoiceClient()
    with with_music_player(fake_context) as player:
        with fake_media_download(player.file_dir, fake_context=fake_context) as sd:
            player.add_to_play_queue(sd)
            result = player.get_queue_items()
            assert len(result) == 1
            assert not player.check_queue_empty()
            player.shuffle_queue()
            player.bump_queue_item(1)
            item = player.remove_queue_item(1)
            assert item is not None

def test_music_clear_queue_messages_clear(fake_context): #pylint:disable=redefined-outer-name
    fake_context['guild'].voice_client = FakeVoiceClient()
    with with_music_player(fake_context) as player:
        with fake_media_download(player.file_dir, fake_context=fake_context) as sd:
            player.add_to_play_queue(sd)
            player.clear_queue()
            result = player.get_queue_items()
            assert len(result) == 0


# Voice channel timeout tests
def test_voice_channel_inactive_timeout_immediate_active(fake_context): #pylint:disable=redefined-outer-name
    """Test that timeout returns False immediately when channel is active"""
    with with_music_player(fake_context) as player:
        # Mock voice_channel_active to return True (channel is active)
        player.voice_channel_active = Mock(return_value=True)

        result = player.voice_channel_inactive_timeout(timeout_seconds=60)

        assert result is False
        assert player.inactive_timestamp is None

def test_voice_channel_inactive_timeout_first_check(fake_context, mocker): #pylint:disable=redefined-outer-name
    """Test that timeout sets timestamp on first inactive check"""
    with with_music_player(fake_context) as player:
        # Mock voice_channel_active to return False (channel is inactive)
        player.voice_channel_active = Mock(return_value=False)
        # Mock time to return consistent value
        mock_time = mocker.patch('discord_bot.cogs.music_helpers.music_player.time', return_value=1000)

        result = player.voice_channel_inactive_timeout(timeout_seconds=60)

        assert result is False
        assert player.inactive_timestamp == 1000
        mock_time.assert_called()

def test_voice_channel_inactive_timeout_within_limit(fake_context, mocker): #pylint:disable=redefined-outer-name
    """Test that timeout returns False when within time limit"""
    with with_music_player(fake_context) as player:
        # Mock voice_channel_active to return False (channel is inactive)
        player.voice_channel_active = Mock(return_value=False)

        # Set initial timestamp
        player.inactive_timestamp = 1000
        # Mock time to return value within timeout
        mocker.patch('discord_bot.cogs.music_helpers.music_player.time', return_value=1030)  # 30 seconds later

        result = player.voice_channel_inactive_timeout(timeout_seconds=60)

        assert result is False

def test_voice_channel_inactive_timeout_exceeded(fake_context, mocker): #pylint:disable=redefined-outer-name
    """Test that timeout returns True when time limit exceeded"""
    with with_music_player(fake_context) as player:
        # Mock voice_channel_active to return False (channel is inactive)
        player.voice_channel_active = Mock(return_value=False)

        # Set initial timestamp
        player.inactive_timestamp = 1000
        # Mock time to return value exceeding timeout
        mocker.patch('discord_bot.cogs.music_helpers.music_player.time', return_value=1070)  # 70 seconds later

        result = player.voice_channel_inactive_timeout(timeout_seconds=60)

        assert result is True

def test_voice_channel_inactive_timeout_reset_on_active(fake_context): #pylint:disable=redefined-outer-name
    """Test that timestamp gets reset when channel becomes active again"""
    with with_music_player(fake_context) as player:
        # Set initial timestamp (simulating previous inactive state)
        player.inactive_timestamp = 1000

        # Mock voice_channel_active to return True (channel became active)
        player.voice_channel_active = Mock(return_value=True)

        result = player.voice_channel_inactive_timeout(timeout_seconds=60)

        assert result is False
        assert player.inactive_timestamp is None

def test_voice_channel_active_no_voice_client(fake_context): #pylint:disable=redefined-outer-name
    """Test voice_channel_active returns True when no voice client (fail-safe)"""
    with with_music_player(fake_context) as player:
        player.guild.voice_client = None

        result = player.voice_channel_active()

        assert result is True

def test_voice_channel_active_no_channel(fake_context): #pylint:disable=redefined-outer-name
    """Test voice_channel_active returns True when voice client has no channel"""
    with with_music_player(fake_context) as player:
        # Setup voice client but no channel
        mock_voice_client = Mock()
        mock_voice_client.channel = None
        player.guild.voice_client = mock_voice_client

        result = player.voice_channel_active()

        assert result is True

def test_voice_channel_active_with_real_users(fake_context): #pylint:disable=redefined-outer-name
    """Test voice_channel_active returns True when real users are present"""
    with with_music_player(fake_context) as player:
        # Setup voice client with channel
        mock_voice_client = Mock()
        mock_channel = Mock()
        mock_voice_client.channel = mock_channel
        player.guild.voice_client = mock_voice_client

        # Create mock members - bot and real user
        # The logic checks member.id != bot.user.id
        bot_user = Mock()
        bot_user.id = player.bot.user.id  # Same as bot
        real_user = Mock()
        real_user.id = 'different_id'  # Different from bot

        mock_channel.members = [bot_user, real_user]

        result = player.voice_channel_active()

        assert result is True  # Returns True when real users present

def test_voice_channel_active_only_bots(fake_context): #pylint:disable=redefined-outer-name
    """Test voice_channel_active returns False when only bots are present"""
    with with_music_player(fake_context) as player:
        # Setup voice client with channel
        mock_voice_client = Mock()
        mock_channel = Mock()
        mock_voice_client.channel = mock_channel
        player.guild.voice_client = mock_voice_client

        # Create mock members - only bots (same ID as the player's bot)
        bot_user1 = Mock()
        bot_user1.id = player.bot.user.id  # Same as the bot
        bot_user2 = Mock()
        bot_user2.id = player.bot.user.id  # Same as the bot

        mock_channel.members = [bot_user1, bot_user2]

        result = player.voice_channel_active()

        assert result is False  # Returns False when only bots present
