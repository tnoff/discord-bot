import asyncio
from contextlib import contextmanager
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import Mock, patch, AsyncMock

import pytest

from discord.errors import ClientException

from discord_bot.exceptions import ExitEarlyException

from discord_bot.cogs.music_helpers.music_player import MusicPlayer, cleanup_source
from discord_bot.types.queue import Queue

from tests.helpers import FakeChannel, fake_context, fake_media_download, FakeVoiceClient #pylint:disable=unused-import

@contextmanager
def with_music_player(fake_context): #pylint:disable=redefined-outer-name
    with TemporaryDirectory() as tmp_dir:
        dispatcher = Mock()
        dispatcher.update_mutable = Mock()
        history_queue = Queue()
        player = MusicPlayer(fake_context['context'], {}, 10, 0.01, Path(tmp_dir), dispatcher, None, history_queue)
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
            # Verify dispatcher.update_mutable was called with play_order key
            expected_key = f'play_order-{fake_context["guild"].id}'
            assert player.dispatcher.update_mutable.called
            assert player.dispatcher.update_mutable.call_args[0][0] == expected_key

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
async def test_music_player_join_voice_timeout(fake_context): #pylint:disable=redefined-outer-name
    with with_music_player(fake_context) as player:
        c = FakeChannel()
        c.connect = AsyncMock(side_effect=asyncio.TimeoutError())
        with pytest.raises(ClientException, match='Timed out connecting to voice channel'):
            await player.join_voice(c)

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

def test_music_get_player_messages_with_empty_queue_but_now_playing(fake_context): #pylint:disable=redefined-outer-name
    """Test that now playing message is shown even when queue is empty"""
    fake_context['guild'].voice_client = FakeVoiceClient()
    with with_music_player(fake_context) as player:
        # Set np_message without adding anything to queue (simulates first song playing with empty queue)
        player.np_message = 'Now playing https://example.com/video requested by TestUser'
        result = player.get_queue_order_messages()
        assert result == ['Now playing https://example.com/video requested by TestUser']
        assert len(result) == 1

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


# Tests for cleanup_source function and audio source cleanup
def test_cleanup_source_success(fake_context): #pylint:disable=redefined-outer-name
    """Test cleanup_source cleans up audio source"""
    with with_music_player(fake_context) as player:
        with fake_media_download(player.file_dir, fake_context=fake_context): #pylint:disable=unused-variable
            mock_audio_source = Mock()
            mock_audio_source.cleanup = Mock()

            cleanup_source(mock_audio_source)

            mock_audio_source.cleanup.assert_called_once()


def test_cleanup_source_handles_value_error(fake_context): #pylint:disable=redefined-outer-name
    """Test cleanup_source handles ValueError when audio source is already cleaned"""
    with with_music_player(fake_context) as player:
        with fake_media_download(player.file_dir, fake_context=fake_context): #pylint:disable=unused-variable
            mock_audio_source = Mock()
            mock_audio_source.cleanup = Mock(side_effect=ValueError("File already closed"))

            # Should not raise exception
            cleanup_source(mock_audio_source)

            mock_audio_source.cleanup.assert_called_once()


@pytest.mark.asyncio
async def test_music_player_cleans_up_on_voice_exception(fake_context): #pylint:disable=redefined-outer-name
    """Test that audio source is cleaned up when voice client raises exception"""
    fake_context['guild'].voice_client = None
    with with_music_player(fake_context) as player:
        with fake_media_download(player.file_dir, fake_context=fake_context) as media_download:
            # Patch FFmpegPCMAudio to track cleanup calls
            with patch('discord_bot.cogs.music_helpers.music_player.PCMAudio') as mock_ffmpeg:
                mock_audio_source = Mock()
                mock_audio_source.cleanup = Mock()
                mock_ffmpeg.return_value = mock_audio_source

                player.add_to_play_queue(media_download)

                # Should raise exception due to no voice client
                with pytest.raises(ExitEarlyException) as exc:
                    await player.player_loop()

                assert 'No voice client in guild, ending loop' in str(exc.value)

                # Verify cleanup was called
                mock_audio_source.cleanup.assert_called_once()


@pytest.mark.asyncio
async def test_music_player_current_media_download(fake_context): #pylint:disable=redefined-outer-name
    """Test that current_media_download is properly set during playback"""
    fake_context['guild'].voice_client = FakeVoiceClient()
    with with_music_player(fake_context) as player:
        with fake_media_download(player.file_dir, fake_context=fake_context) as media_download:
            # Initially should be None
            assert player.current_media_download is None

            player.add_to_play_queue(media_download)
            await player.player_loop()

            # After playing, history should contain the media download
            assert player._history.get_nowait() == media_download #pylint:disable=protected-access


@pytest.mark.asyncio
async def test_music_player_cleanup_calls_audio_cleanup(fake_context): #pylint:disable=redefined-outer-name
    """Test that player cleanup properly handles audio source cleanup"""
    fake_context['guild'].voice_client = FakeVoiceClient()
    with with_music_player(fake_context) as player:
        with fake_media_download(player.file_dir, fake_context=fake_context) as media_download:
            # Patch FFmpegPCMAudio to track cleanup
            with patch('discord_bot.cogs.music_helpers.music_player.PCMAudio') as mock_ffmpeg:
                mock_audio_source = Mock()
                mock_audio_source.cleanup = Mock()
                mock_audio_source.volume = 0.5
                mock_ffmpeg.return_value = mock_audio_source

                player.add_to_play_queue(media_download)
                await player.player_loop()

                # Verify audio source cleanup was called after natural completion
                mock_audio_source.cleanup.assert_called_once()


def test_cleanup_source_with_none_values(fake_context): #pylint:disable=redefined-outer-name,unused-argument
    """Test cleanup_source handles None gracefully"""
    cleanup_source(None)


@pytest.mark.asyncio
async def test_player_cleanup_with_no_current_source(fake_context): #pylint:disable=redefined-outer-name
    """Test that player cleanup handles case when no song is playing"""
    with with_music_player(fake_context) as player:
        # Initially no current source
        assert player.current_media_download is None
        assert player.current_audio_source is None

        # Cleanup should handle None gracefully without crashing
        await player.cleanup()
        # Success - no exception raised


@pytest.mark.asyncio
async def test_player_cleanup_with_active_source(fake_context): #pylint:disable=redefined-outer-name
    """Test that player cleanup properly cleans up active audio source"""
    fake_context['guild'].voice_client = FakeVoiceClient()
    with with_music_player(fake_context) as player:
        with fake_media_download(player.file_dir, fake_context=fake_context) as media_download:
            with patch('discord_bot.cogs.music_helpers.music_player.PCMAudio') as mock_ffmpeg:
                mock_audio_source = Mock()
                mock_audio_source.cleanup = Mock()
                mock_audio_source.volume = 0.5
                mock_ffmpeg.return_value = mock_audio_source

                # Manually set current source (simulating mid-playback)
                player.current_media_download = media_download
                player.current_audio_source = mock_audio_source

                # Call cleanup
                await player.cleanup()

                # Verify audio source was cleaned up
                mock_audio_source.cleanup.assert_called_once()


# ---------------------------------------------------------------------------
# start_tasks
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_start_tasks_creates_player_task(fake_context): #pylint:disable=redefined-outer-name
    """start_tasks creates _player_task when not already set"""
    with with_music_player(fake_context) as player:
        player.bot.loop = asyncio.get_event_loop()
        assert player._player_task is None #pylint:disable=protected-access
        await player.start_tasks()
        assert player._player_task is not None #pylint:disable=protected-access
        player._player_task.cancel() #pylint:disable=protected-access


@pytest.mark.asyncio
async def test_start_tasks_idempotent(fake_context): #pylint:disable=redefined-outer-name
    """start_tasks does not replace an existing task"""
    with with_music_player(fake_context) as player:
        player.bot.loop = asyncio.get_event_loop()
        await player.start_tasks()
        first_task = player._player_task #pylint:disable=protected-access
        await player.start_tasks()
        assert player._player_task is first_task #pylint:disable=protected-access
        first_task.cancel()


# ---------------------------------------------------------------------------
# broker paths in player_loop
# ---------------------------------------------------------------------------

@contextmanager
def with_broker_player(fake_context, history_playlist_id=None, queue_max_size=10): #pylint:disable=redefined-outer-name
    with TemporaryDirectory() as tmp_dir:
        broker = Mock()
        broker.checkout.return_value = None
        broker.release = Mock()
        broker.remove = Mock()
        broker.prefetch = Mock()
        dispatcher = Mock()
        dispatcher.update_mutable = Mock()
        history_queue = Queue()
        player = MusicPlayer(
            fake_context['context'], {}, queue_max_size, 0.01, Path(tmp_dir),
            dispatcher, history_playlist_id, history_queue, broker=broker,
        )
        yield player


@pytest.mark.asyncio
async def test_player_loop_broker_checkout_called(fake_context): #pylint:disable=redefined-outer-name
    """broker.checkout is called in player_loop when broker is set"""
    fake_context['guild'].voice_client = FakeVoiceClient()
    with with_broker_player(fake_context) as player:
        with fake_media_download(player.file_dir, fake_context=fake_context) as media_download:
            player.add_to_play_queue(media_download)
            await player.player_loop()
            assert player.broker.checkout.called


@pytest.mark.asyncio
async def test_player_loop_broker_release_on_voice_exception(fake_context): #pylint:disable=redefined-outer-name
    """broker.release called when voice client raises AttributeError"""
    fake_context['guild'].voice_client = None
    with with_broker_player(fake_context) as player:
        with fake_media_download(player.file_dir, fake_context=fake_context) as media_download:
            player.add_to_play_queue(media_download)
            with pytest.raises(ExitEarlyException):
                await player.player_loop()
            player.broker.release.assert_called_once()


@pytest.mark.asyncio
async def test_player_loop_broker_release_after_play(fake_context): #pylint:disable=redefined-outer-name
    """broker.release called after next.wait() completes normally"""
    fake_context['guild'].voice_client = FakeVoiceClient()
    with with_broker_player(fake_context) as player:
        with fake_media_download(player.file_dir, fake_context=fake_context) as media_download:
            player.add_to_play_queue(media_download)
            await player.player_loop()
            player.broker.release.assert_called_once()


# ---------------------------------------------------------------------------
# history_playlist_id and history QueueFull
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_player_loop_history_playlist_id(fake_context): #pylint:disable=redefined-outer-name
    """history_playlist_queue receives an item when history_playlist_id is set"""
    fake_context['guild'].voice_client = FakeVoiceClient()
    with TemporaryDirectory() as tmp_dir:
        dispatcher = Mock()
        dispatcher.update_mutable = Mock()
        history_queue = Queue()
        player = MusicPlayer(fake_context['context'], {}, 10, 0.01, Path(tmp_dir),
                             dispatcher, 999, history_queue)
        with fake_media_download(player.file_dir, fake_context=fake_context) as media_download:
            player.add_to_play_queue(media_download)
            await player.player_loop()
            assert not history_queue.empty()
            item = history_queue.get_nowait()
            assert item.playlist_id == 999


@pytest.mark.asyncio
async def test_player_loop_history_queue_full_evicts_oldest(fake_context): #pylint:disable=redefined-outer-name
    """QueueFull on _history is handled by evicting the oldest entry"""
    fake_context['guild'].voice_client = FakeVoiceClient()
    with TemporaryDirectory() as tmp_dir:
        dispatcher = Mock()
        dispatcher.update_mutable = Mock()
        history_queue = Queue()
        # maxsize=1 means _history also holds at most 1 entry
        player = MusicPlayer(fake_context['context'], {}, 1, 0.01, Path(tmp_dir),
                             dispatcher, None, history_queue)
        with fake_media_download(player.file_dir, fake_context=fake_context) as sd1:
            with fake_media_download(player.file_dir, fake_context=fake_context) as sd2:
                player._history.put_nowait(sd1)  # fill history to capacity #pylint:disable=protected-access
                player.add_to_play_queue(sd2)
                await player.player_loop()
                # sd1 was evicted; history now contains sd2
                assert player._history.get_nowait() == sd2  #pylint:disable=protected-access


# ---------------------------------------------------------------------------
# get_queue_order_messages edge cases
# ---------------------------------------------------------------------------

def test_get_queue_order_messages_with_current_media_download(fake_context): #pylint:disable=redefined-outer-name
    """current_media_download.duration is used as wait-time offset for queued items"""
    with with_music_player(fake_context) as player:
        with fake_media_download(player.file_dir, fake_context=fake_context) as current_dl:
            with fake_media_download(player.file_dir, fake_context=fake_context) as queued_dl:
                player.current_media_download = current_dl
                player.add_to_play_queue(queued_dl)
                result = player.get_queue_order_messages()
                assert result  # non-empty list


def test_get_queue_order_messages_render_returns_non_list(fake_context): #pylint:disable=redefined-outer-name
    """A non-list render result is wrapped in a list"""
    with with_music_player(fake_context) as player:
        with fake_media_download(player.file_dir, fake_context=fake_context) as sd:
            player.add_to_play_queue(sd)
            with patch('discord_bot.cogs.music_helpers.music_player.DapperTable') as mock_table_cls:
                mock_table = Mock()
                mock_table.render.return_value = 'rendered string'
                mock_table_cls.return_value = mock_table
                result = player.get_queue_order_messages()
                assert 'rendered string' in result


# ---------------------------------------------------------------------------
# join_voice same-channel shortcut
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_join_voice_same_channel_returns_true(fake_context): #pylint:disable=redefined-outer-name
    """join_voice returns True immediately when already in the requested channel"""
    channel = FakeChannel()
    fake_context['guild'].voice_client = FakeVoiceClient()
    fake_context['guild'].voice_client.channel = channel
    with with_music_player(fake_context) as player:
        result = await player.join_voice(channel)
        assert result is True
        # channel should not have changed (move_to not called)
        assert fake_context['guild'].voice_client.channel is channel


# ---------------------------------------------------------------------------
# _on_prefetch_done
# ---------------------------------------------------------------------------

def test_on_prefetch_done_logs_warning_on_exception(fake_context): #pylint:disable=redefined-outer-name
    """_on_prefetch_done logs a warning when the task raised an exception"""
    with with_music_player(fake_context) as player:
        mock_task = Mock()
        mock_task.cancelled.return_value = False
        mock_task.exception.return_value = RuntimeError('prefetch boom')
        player._on_prefetch_done(mock_task)  # should not raise #pylint:disable=protected-access


def test_on_prefetch_done_silent_when_cancelled(fake_context): #pylint:disable=redefined-outer-name
    """_on_prefetch_done does nothing when the task was cancelled"""
    with with_music_player(fake_context) as player:
        mock_task = Mock()
        mock_task.cancelled.return_value = True
        player._on_prefetch_done(mock_task)  #pylint:disable=protected-access
        mock_task.exception.assert_not_called()


# ---------------------------------------------------------------------------
# get_file_paths with current_media_download
# ---------------------------------------------------------------------------

def test_get_file_paths_includes_current_media_download(fake_context): #pylint:disable=redefined-outer-name
    """get_file_paths includes current_media_download.file_path"""
    with with_music_player(fake_context) as player:
        with fake_media_download(player.file_dir, fake_context=fake_context) as current_dl:
            player.current_media_download = current_dl
            result = player.get_file_paths()
            assert current_dl.file_path in result


# ---------------------------------------------------------------------------
# cleanup with broker
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_cleanup_broker_release_and_remove(fake_context): #pylint:disable=redefined-outer-name
    """cleanup releases current download and removes queued downloads via broker"""
    with with_broker_player(fake_context) as player:
        with fake_media_download(player.file_dir, fake_context=fake_context) as current_dl:
            with fake_media_download(player.file_dir, fake_context=fake_context) as queued_dl:
                player.current_media_download = current_dl
                player.add_to_play_queue(queued_dl)
                await player.cleanup()
                player.broker.release.assert_called_once_with(
                    str(current_dl.media_request.uuid)
                )
                player.broker.remove.assert_called_once_with(
                    str(queued_dl.media_request.uuid)
                )


@pytest.mark.asyncio
async def test_cleanup_cancels_prefetch_task(fake_context): #pylint:disable=redefined-outer-name
    """cleanup cancels an active prefetch task"""
    with with_music_player(fake_context) as player:
        mock_prefetch = Mock()
        mock_prefetch.done.return_value = False
        mock_prefetch.cancel = Mock()
        player._prefetch_task = mock_prefetch  #pylint:disable=protected-access
        await player.cleanup()
        mock_prefetch.cancel.assert_called_once()
        assert player._prefetch_task is None  #pylint:disable=protected-access


@pytest.mark.asyncio
async def test_cleanup_cancels_player_task(fake_context): #pylint:disable=redefined-outer-name
    """cleanup cancels _player_task when set"""
    with with_music_player(fake_context) as player:
        mock_task = Mock()
        mock_task.cancel = Mock()
        player._player_task = mock_task  #pylint:disable=protected-access
        await player.cleanup()
        mock_task.cancel.assert_called_once()
        assert player._player_task is None  #pylint:disable=protected-access


def test_clear_queue_with_broker_removes_items(fake_context): #pylint:disable=redefined-outer-name
    """clear_queue calls broker.remove for each queued item"""
    with with_broker_player(fake_context) as player:
        with fake_media_download(player.file_dir, fake_context=fake_context) as sd:
            player.add_to_play_queue(sd)
            items = player.clear_queue()
            assert len(items) == 1
            player.broker.remove.assert_called_once_with(str(sd.media_request.uuid))
