import asyncio
from contextlib import contextmanager
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import Mock, patch, AsyncMock

import pytest

from discord.errors import ClientException

from discord_core.exceptions import ExitEarlyException
from discord_core.types.checkout_result import CheckoutResult
from discord_core.types.guild_queue import ClaimedDownload

from discord_gateway.cogs.music_helpers import music_player as music_player_module
from discord_gateway.cogs.music_helpers.music_player import MusicPlayer, cleanup_source
from discord_gateway.types.cleanup_reason import CleanupReason

from tests.fakes.asyncio_broker import AsyncioBroker
from tests.fakes.asyncio_broker_client import AsyncioBrokerClient
from tests.fakes.asyncio_queues import make_guild_queue_for
from tests.helpers import FakeChannel, fake_context, fake_media_download, FakeVoiceClient #pylint:disable=unused-import

GATEWAY_ID = 'gw-test'


@contextmanager
def with_music_player(fake_context, bucket_name=None, queue_max_size=10, disconnect_timeout=0.05, **kwargs): #pylint:disable=redefined-outer-name
    '''
    A player over a real broker client: the actual guild queue (on fakeredis) next to the
    AsyncioBroker double that holds the media entries. What the player claims is what a test queued.
    '''
    with TemporaryDirectory() as tmp_dir:
        engine = AsyncioBroker(bucket_name=bucket_name)
        broker = AsyncioBrokerClient(engine, guild_queue=make_guild_queue_for(engine))
        yield MusicPlayer(fake_context['bot'], fake_context['guild'], fake_context['channel'], {},
                          queue_max_size, disconnect_timeout, Path(tmp_dir), broker, GATEWAY_ID,
                          bucket_name=bucket_name, **kwargs)


@contextmanager
def with_mock_broker_player(fake_context, queue_max_size=10): #pylint:disable=redefined-outer-name
    '''
    A player over a mock broker, for asserting exactly which calls it makes. Set
    `player.broker.claim_next_track.return_value` to hand it a track.
    '''
    with TemporaryDirectory() as tmp_dir:
        broker = Mock()
        broker.claim_next_track = AsyncMock(return_value=None)
        broker.finish_track = AsyncMock()
        broker.playing_heartbeat = AsyncMock(return_value=True)
        broker.get_guild_queue = AsyncMock(return_value=Mock(items=[]))
        yield MusicPlayer(fake_context['bot'], fake_context['guild'], fake_context['channel'], {},
                          queue_max_size, 0.01, Path(tmp_dir), broker, GATEWAY_ID)


class _HoldingVoiceClient(FakeVoiceClient):
    '''A voice client whose track keeps playing until the test says it is over.'''

    def play(self, *_args, after=None, **_kwargs):
        return True


async def queue_track(player, media_download, max_size=10):
    '''Put a downloaded track in the guild's queue, as add_source_to_player does.'''
    await player.broker.register_download(media_download)
    result = await player.broker.enqueue_track(player.guild.id, str(media_download.media_request.uuid), max_size)
    assert result == 'ok'


async def history(player):
    return await player.broker.get_guild_history(player.guild.id)


async def queue_of(player):
    return await player.broker.get_guild_queue(player.guild.id)


def claimed(media_download, s3_key=None, bucket_name=None):
    '''What the broker hands the player when it claims media_download.'''
    return ClaimedDownload(download=media_download,
                           checkout=CheckoutResult(s3_key=s3_key, bucket_name=bucket_name))


async def until(condition, timeout=1.0):
    '''Wait for condition() to hold; fail the test if it does not.'''
    deadline = asyncio.get_running_loop().time() + timeout
    while not condition():
        assert asyncio.get_running_loop().time() < deadline, 'condition never became true'
        await asyncio.sleep(0.005)


@pytest.fixture(autouse=True)
def _fast_voice_client_wait():
    '''
    Shrink the voice-client wait for every test in this module.

    The production default is 60s, sized against a measured ~45s handshake. Left
    at that, any test that reaches play() without a voice client becomes a
    one-minute test -- two existing ones did exactly that the moment the wait was
    introduced, and nothing failed to say so. Scoping this to the module rather
    than patching per test means the next such test is fast by default.
    '''
    with patch('discord_gateway.cogs.music_helpers.music_player.VOICE_CLIENT_WAIT_SECONDS', 0.05), \
         patch('discord_gateway.cogs.music_helpers.music_player.VOICE_CLIENT_POLL_SECONDS', 0.01):
        yield


def test_music_player_basic(fake_context): #pylint:disable=redefined-outer-name
    with with_music_player(fake_context) as player:
        assert player is not None


@pytest.mark.asyncio
async def test_music_player_wait_for_voice_client_returns_none_on_shutdown(fake_context): #pylint:disable=redefined-outer-name
    '''Shutting the player down while it waits ends the wait immediately'''
    fake_context['guild'].voice_client = None

    with with_music_player(fake_context) as player:
        async def _shutdown_on_poll(_seconds):
            player.shutdown_called = True

        with patch('discord_gateway.cogs.music_helpers.music_player.asyncio.sleep', side_effect=_shutdown_on_poll):
            assert await player._wait_for_voice_client() is None #pylint:disable=protected-access


@pytest.mark.asyncio
async def test_music_player_wait_for_voice_client_returns_existing(fake_context): #pylint:disable=redefined-outer-name
    '''The common case does not wait at all'''
    voice_client = FakeVoiceClient()
    fake_context['guild'].voice_client = voice_client
    with with_music_player(fake_context) as player:
        with patch('discord_gateway.cogs.music_helpers.music_player.asyncio.sleep') as mock_sleep:
            assert await player._wait_for_voice_client() is voice_client #pylint:disable=protected-access
        mock_sleep.assert_not_called()


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
async def test_music_player_join_voice_connect_error(fake_context): #pylint:disable=redefined-outer-name
    '''A non-timeout connect failure is logged and re-raised unchanged'''
    with with_music_player(fake_context) as player:
        c = FakeChannel()
        c.connect = AsyncMock(side_effect=RuntimeError('boom'))
        with pytest.raises(RuntimeError, match='boom'):
            await player.join_voice(c)


@pytest.mark.asyncio
async def test_music_player_join_voice_move_error(fake_context): #pylint:disable=redefined-outer-name
    '''A move_to failure is logged and re-raised'''
    voice_client = FakeVoiceClient()
    voice_client.move_to = AsyncMock(side_effect=RuntimeError('boom'))
    fake_context['guild'].voice_client = voice_client
    with with_music_player(fake_context) as player:
        c = FakeChannel()
        with pytest.raises(RuntimeError, match='boom'):
            await player.join_voice(c)


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
        mock_time = mocker.patch('discord_gateway.cogs.music_helpers.music_player.time', return_value=1000)

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
        mocker.patch('discord_gateway.cogs.music_helpers.music_player.time', return_value=1030)  # 30 seconds later

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
        mocker.patch('discord_gateway.cogs.music_helpers.music_player.time', return_value=1070)  # 70 seconds later

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


@pytest.mark.asyncio
async def test_player_cleanup_with_active_source(fake_context): #pylint:disable=redefined-outer-name
    """Test that player cleanup properly cleans up active audio source"""
    fake_context['guild'].voice_client = FakeVoiceClient()
    with with_music_player(fake_context) as player:
        with fake_media_download(player.file_dir, fake_context=fake_context) as media_download:
            with patch('discord_gateway.cogs.music_helpers.music_player.PCMAudio') as mock_ffmpeg:
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


# ---------------------------------------------------------------------------
# The claim loop: waiting for a track
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_music_player_loop_exit_with_timeout(fake_context): #pylint:disable=redefined-outer-name
    '''An idle player shuts itself down after disconnect_timeout, as it did when it owned the queue'''
    with with_music_player(fake_context) as player:
        with pytest.raises(ExitEarlyException) as exc:
            await player.player_loop()
        assert 'timeout waiting for the next track' in str(exc.value)
        assert player.shutdown_called is True
        assert player.shutdown_reason is CleanupReason.QUEUE_TIMEOUT


@pytest.mark.asyncio
async def test_an_idle_player_claims_as_soon_as_the_gateway_queues_something(fake_context): #pylint:disable=redefined-outer-name
    '''notify_enqueued ends the wait at once, so a track does not sit out the poll interval'''
    fake_context['guild'].voice_client = FakeVoiceClient()
    with patch.object(music_player_module, 'CLAIM_POLL_SECONDS', 30):
        with with_music_player(fake_context, disconnect_timeout=30) as player:
            with fake_media_download(player.file_dir, fake_context=fake_context) as media_download:
                task = asyncio.create_task(player.player_loop())
                await asyncio.sleep(0.05)
                assert not task.done()

                await queue_track(player, media_download)
                player.notify_enqueued()
                await asyncio.wait_for(task, timeout=2)
                assert len(await history(player)) == 1


@pytest.mark.asyncio
async def test_an_idle_player_still_finds_a_track_queued_by_someone_else(fake_context): #pylint:disable=redefined-outer-name
    '''Without a wake-up it falls back to polling, so a track queued elsewhere still plays'''
    fake_context['guild'].voice_client = FakeVoiceClient()
    with patch.object(music_player_module, 'CLAIM_POLL_SECONDS', 0.01):
        with with_music_player(fake_context, disconnect_timeout=5) as player:
            with fake_media_download(player.file_dir, fake_context=fake_context) as media_download:
                task = asyncio.create_task(player.player_loop())
                await asyncio.sleep(0.05)
                await queue_track(player, media_download)
                await asyncio.wait_for(task, timeout=2)
                assert len(await history(player)) == 1


@pytest.mark.asyncio
async def test_the_player_claims_for_its_guild_and_its_own_gateway(fake_context): #pylint:disable=redefined-outer-name
    '''The claim names this gateway, which is how a replacement tells its heartbeat from its own'''
    fake_context['guild'].voice_client = FakeVoiceClient()
    with with_mock_broker_player(fake_context) as player:
        with fake_media_download(player.file_dir, fake_context=fake_context) as media_download:
            player.broker.claim_next_track.return_value = claimed(media_download)
            await player.player_loop()
            player.broker.claim_next_track.assert_awaited_once_with(fake_context['guild'].id, GATEWAY_ID)


# ---------------------------------------------------------------------------
# Playing a track, and telling the broker how it ended
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_music_player_loop_exiting_voice_client(fake_context): #pylint:disable=redefined-outer-name
    '''A voice client that never arrives still tears the player down, and the track is released'''
    fake_context['guild'].voice_client = None
    with with_music_player(fake_context) as player:
        with fake_media_download(player.file_dir, fake_context=fake_context) as media_download:
            await queue_track(player, media_download)
            with pytest.raises(ExitEarlyException) as exc:
                await player.player_loop()
            assert 'No voice client in guild, ending loop' in str(exc.value)
            # A track that never played is released, not remembered as played
            assert await history(player) == []
            assert await player.broker.local_broker.get_entry(str(media_download.media_request.uuid)) is None
            assert player.current_media_download is None


@pytest.mark.asyncio
async def test_music_player_loop_waits_for_a_late_voice_client(fake_context): #pylint:disable=redefined-outer-name
    '''
    A voice client that arrives mid-wait plays the track instead of dropping the queue.

    This is the regression: Music.get_player starts the player loop before it
    awaits join_voice, so a resumed session can fill the queue and reach play()
    while the handshake is still in flight. Prod measured ~45s between the two,
    and the player was destroyed with 15 tracks still queued.
    '''
    fake_context['guild'].voice_client = None
    voice_client = FakeVoiceClient()

    async def _connect_on_poll(_seconds):
        fake_context['guild'].voice_client = voice_client

    with with_music_player(fake_context) as player:
        with fake_media_download(player.file_dir, fake_context=fake_context) as media_download:
            await queue_track(player, media_download)
            with patch('discord_gateway.cogs.music_helpers.music_player.asyncio.sleep', side_effect=_connect_on_poll):
                await player.player_loop()
            assert player.shutdown_called is False
            assert (await queue_of(player)).items == []
            assert len(await history(player)) == 1


@pytest.mark.asyncio
async def test_music_player_loop_basic(fake_context): #pylint:disable=redefined-outer-name
    '''A played track is recorded in the broker's history and leaves nothing playing'''
    fake_context['guild'].voice_client = FakeVoiceClient()
    with with_music_player(fake_context) as player:
        with fake_media_download(player.file_dir, fake_context=fake_context) as media_download:
            await queue_track(player, media_download)
            await player.player_loop()
            [record] = await history(player)
            assert record['uuid'] == str(media_download.media_request.uuid)
            assert record['title'] == media_download.title
            assert record['webpage_url'] == media_download.webpage_url
            queue = await queue_of(player)
            assert queue.items == []
            assert queue.playing is None
            assert player.current_media_download is None


@pytest.mark.asyncio
async def test_music_player_loop_records_history_in_play_order(fake_context): #pylint:disable=redefined-outer-name
    fake_context['guild'].voice_client = FakeVoiceClient()
    with with_music_player(fake_context) as player:
        with fake_media_download(player.file_dir, fake_context=fake_context) as first:
            with fake_media_download(player.file_dir, fake_context=fake_context) as second:
                await queue_track(player, first)
                await queue_track(player, second)
                await player.player_loop()
                await player.player_loop()
                assert [record['uuid'] for record in await history(player)] == [
                    str(first.media_request.uuid), str(second.media_request.uuid)]
                assert (await queue_of(player)).items == []


@pytest.mark.asyncio
async def test_current_media_download_is_set_only_while_a_track_plays(fake_context): #pylint:disable=redefined-outer-name
    '''current_media_download is what skip and the session read, so it must not outlive the track'''
    fake_context['guild'].voice_client = _HoldingVoiceClient()
    with with_music_player(fake_context) as player:
        with fake_media_download(player.file_dir, fake_context=fake_context) as media_download:
            assert player.current_media_download is None
            await queue_track(player, media_download)
            task = asyncio.create_task(player.player_loop())
            await until(lambda: player.current_media_download is not None)
            assert player.current_media_download is media_download
            queue = await queue_of(player)
            assert queue.playing.uuid == str(media_download.media_request.uuid)
            assert queue.playing.gateway_id == GATEWAY_ID

            player.set_next()
            await asyncio.wait_for(task, timeout=1)
            assert player.current_media_download is None


@pytest.mark.asyncio
async def test_a_skipped_track_is_released_but_not_recorded(fake_context): #pylint:disable=redefined-outer-name
    '''Skipping sets video_skipped; the broker is told, and keeps the track out of history'''
    fake_context['guild'].voice_client = _HoldingVoiceClient()
    with with_music_player(fake_context) as player:
        with fake_media_download(player.file_dir, fake_context=fake_context) as media_download:
            await queue_track(player, media_download)
            task = asyncio.create_task(player.player_loop())
            await until(lambda: player.current_media_download is not None)

            player.video_skipped = True
            player.set_next()
            await asyncio.wait_for(task, timeout=1)
            assert await history(player) == []
            assert await player.broker.local_broker.get_entry(str(media_download.media_request.uuid)) is None


@pytest.mark.asyncio
async def test_a_stopped_track_is_left_with_the_broker(fake_context): #pylint:disable=redefined-outer-name
    '''
    Stopping the player ends playback through the same callback a finished track does, but the
    track did not finish: the broker must not record a play, and must still list it as playing
    so the next gateway can take it back.
    '''
    fake_context['guild'].voice_client = _HoldingVoiceClient()
    with with_music_player(fake_context) as player:
        with fake_media_download(player.file_dir, fake_context=fake_context) as media_download:
            await queue_track(player, media_download)
            task = asyncio.create_task(player.player_loop())
            await until(lambda: player.current_media_download is not None)

            player.shutdown_called = True
            player.set_next()
            await asyncio.wait_for(task, timeout=1)

            assert await history(player) == []
            queue = await queue_of(player)
            assert queue.playing.uuid == str(media_download.media_request.uuid)
            assert player.current_media_download is None


@pytest.mark.asyncio
async def test_stop_loop_cancels_the_loop_and_waits_for_it(fake_context): #pylint:disable=redefined-outer-name
    '''The loop is gone when stop_loop returns, and the track it was playing is still the broker's'''
    fake_context['guild'].voice_client = _HoldingVoiceClient()
    with with_music_player(fake_context) as player:
        with fake_media_download(player.file_dir, fake_context=fake_context) as media_download:
            await queue_track(player, media_download)
            player._player_task = asyncio.create_task(player.player_loop()) #pylint:disable=protected-access
            await until(lambda: player.current_media_download is not None)

            await player.stop_loop()

            assert player._player_task.done() #pylint:disable=protected-access
            assert player.current_media_download is None
            assert await history(player) == []
            assert (await queue_of(player)).playing.uuid == str(media_download.media_request.uuid)


@pytest.mark.asyncio
async def test_stop_loop_with_no_loop_running_is_a_noop(fake_context): #pylint:disable=redefined-outer-name
    with with_music_player(fake_context) as player:
        await player.stop_loop()
        assert player._player_task is None #pylint:disable=protected-access


@pytest.mark.asyncio
async def test_a_stopped_player_claims_nothing_more(fake_context): #pylint:disable=redefined-outer-name
    '''
    The loop runner re-enters player_loop the moment it returns. A claim made after the player
    was stopped would mark a second track as playing and hide the interrupted one.
    '''
    with with_mock_broker_player(fake_context) as player:
        player.shutdown_called = True
        with pytest.raises(ExitEarlyException):
            await player.player_loop()
        player.broker.claim_next_track.assert_not_awaited()


@pytest.mark.asyncio
async def test_a_player_stopped_while_waiting_claims_nothing_more(fake_context): #pylint:disable=redefined-outer-name
    '''Stopped between polls, the wait ends without another claim'''
    with patch.object(music_player_module, 'CLAIM_POLL_SECONDS', 0.01):
        with with_mock_broker_player(fake_context) as player:
            player.disconnect_timeout = 5
            task = asyncio.create_task(player.player_loop())
            await until(lambda: player.broker.claim_next_track.await_count >= 2)
            player.shutdown_called = True
            with pytest.raises(ExitEarlyException):
                await asyncio.wait_for(task, timeout=1)
            claims = player.broker.claim_next_track.await_count
            await asyncio.sleep(0.05)
            assert player.broker.claim_next_track.await_count == claims


@pytest.mark.asyncio
async def test_finish_tells_the_broker_how_the_track_ended(fake_context): #pylint:disable=redefined-outer-name
    '''The broker is told the track, whether it was skipped, and how much history to keep'''
    fake_context['guild'].voice_client = FakeVoiceClient()
    with with_mock_broker_player(fake_context, queue_max_size=7) as player:
        with fake_media_download(player.file_dir, fake_context=fake_context) as media_download:
            player.broker.claim_next_track.return_value = claimed(media_download)
            await player.player_loop()
            player.broker.finish_track.assert_awaited_once_with(
                fake_context['guild'].id, str(media_download.media_request.uuid), False, 7)


@pytest.mark.asyncio
async def test_finish_reports_a_skip(fake_context): #pylint:disable=redefined-outer-name
    fake_context['guild'].voice_client = _HoldingVoiceClient()
    with with_mock_broker_player(fake_context) as player:
        with fake_media_download(player.file_dir, fake_context=fake_context) as media_download:
            player.broker.claim_next_track.return_value = claimed(media_download)
            task = asyncio.create_task(player.player_loop())
            await until(lambda: player.current_media_download is not None)
            player.video_skipped = True
            player.set_next()
            await asyncio.wait_for(task, timeout=1)
            player.broker.finish_track.assert_awaited_once_with(
                fake_context['guild'].id, str(media_download.media_request.uuid), True, 10)


@pytest.mark.asyncio
async def test_music_player_cleans_up_on_voice_exception(fake_context): #pylint:disable=redefined-outer-name
    """Test that audio source is cleaned up when voice client raises exception"""
    fake_context['guild'].voice_client = None
    with with_music_player(fake_context) as player:
        with fake_media_download(player.file_dir, fake_context=fake_context) as media_download:
            # Patch the audio source to track cleanup calls
            with patch('discord_gateway.cogs.music_helpers.music_player.PCMAudio') as mock_ffmpeg:
                mock_audio_source = Mock()
                mock_audio_source.cleanup = Mock()
                mock_ffmpeg.return_value = mock_audio_source

                await queue_track(player, media_download)

                # Should raise exception due to no voice client
                with pytest.raises(ExitEarlyException) as exc:
                    await player.player_loop()

                assert 'No voice client in guild, ending loop' in str(exc.value)

                # Verify cleanup was called
                mock_audio_source.cleanup.assert_called_once()


@pytest.mark.asyncio
async def test_a_track_the_voice_client_refuses_is_released_as_skipped(fake_context): #pylint:disable=redefined-outer-name
    fake_context['guild'].voice_client = None
    with with_mock_broker_player(fake_context) as player:
        with fake_media_download(player.file_dir, fake_context=fake_context) as media_download:
            player.broker.claim_next_track.return_value = claimed(media_download)
            with pytest.raises(ExitEarlyException):
                await player.player_loop()
            player.broker.finish_track.assert_awaited_once_with(
                fake_context['guild'].id, str(media_download.media_request.uuid), True, 10)
            assert player.shutdown_reason is CleanupReason.VOICE_DISCONNECT


@pytest.mark.asyncio
async def test_music_player_cleanup_calls_audio_cleanup(fake_context): #pylint:disable=redefined-outer-name
    """Test that player cleanup properly handles audio source cleanup"""
    fake_context['guild'].voice_client = FakeVoiceClient()
    with with_music_player(fake_context) as player:
        with fake_media_download(player.file_dir, fake_context=fake_context) as media_download:
            with patch('discord_gateway.cogs.music_helpers.music_player.PCMAudio') as mock_ffmpeg:
                mock_audio_source = Mock()
                mock_audio_source.cleanup = Mock()
                mock_audio_source.volume = 0.5
                mock_ffmpeg.return_value = mock_audio_source

                await queue_track(player, media_download)
                await player.player_loop()

                # Verify audio source cleanup was called after natural completion
                mock_audio_source.cleanup.assert_called_once()


@pytest.mark.asyncio
async def test_player_loop_skips_when_file_missing(fake_context): #pylint:disable=redefined-outer-name
    """A claim whose file cannot be found (e.g. the raw S3 cache key) skips the track and
    releases it instead of crashing the loop."""
    fake_context['guild'].voice_client = FakeVoiceClient()
    with with_mock_broker_player(fake_context) as player:
        with fake_media_download(player.file_dir, fake_context=fake_context) as media_download:
            # The local file is gone, mirroring the cache-hit-before-registration race in prod.
            media_download.file_path.unlink()
            player.broker.claim_next_track.return_value = claimed(media_download)
            # Must not raise (previously FileNotFoundError killed the player loop).
            await player.player_loop()
            player.broker.finish_track.assert_awaited_once_with(
                fake_context['guild'].id, str(media_download.media_request.uuid), True, 10)


# ---------------------------------------------------------------------------
# Heartbeat: the broker's proof that this gateway is still playing
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_the_broker_hears_a_heartbeat_for_as_long_as_the_track_plays(fake_context): #pylint:disable=redefined-outer-name
    fake_context['guild'].voice_client = _HoldingVoiceClient()
    with patch.object(music_player_module, 'HEARTBEAT_INTERVAL_SECONDS', 0.01):
        with with_mock_broker_player(fake_context) as player:
            with fake_media_download(player.file_dir, fake_context=fake_context) as media_download:
                player.broker.claim_next_track.return_value = claimed(media_download)
                task = asyncio.create_task(player.player_loop())
                await until(lambda: player.broker.playing_heartbeat.await_count >= 3)
                player.broker.playing_heartbeat.assert_awaited_with(
                    fake_context['guild'].id, str(media_download.media_request.uuid))

                player.set_next()
                await asyncio.wait_for(task, timeout=1)
                beats = player.broker.playing_heartbeat.await_count
                await asyncio.sleep(0.05)
                # Once the track is over, nothing keeps claiming it is still playing
                assert player.broker.playing_heartbeat.await_count == beats


@pytest.mark.asyncio
async def test_the_heartbeat_does_not_wait_for_staging(fake_context): #pylint:disable=redefined-outer-name
    '''A slow S3 fetch can outlast the broker's 15s window, so the beat starts before staging'''
    fake_context['guild'].voice_client = FakeVoiceClient()
    with patch.object(music_player_module, 'HEARTBEAT_INTERVAL_SECONDS', 0.01):
        with with_mock_broker_player(fake_context) as player:
            with fake_media_download(player.file_dir, fake_context=fake_context) as media_download:
                player.broker.claim_next_track.return_value = claimed(
                    media_download, s3_key='track.mp3', bucket_name='my-bucket')

                def _slow_get_file(_bucket, _key, dest):
                    # Runs in a worker thread, so a real sleep here does not block the loop
                    import time  # pylint: disable=import-outside-toplevel
                    time.sleep(0.2)
                    Path(dest).write_bytes(b'audio')

                with patch('discord_gateway.cogs.music_helpers.music_player.get_file', side_effect=_slow_get_file):
                    await player.player_loop()
                assert player.broker.playing_heartbeat.await_count >= 3


@pytest.mark.asyncio
async def test_the_heartbeat_ends_when_told_to_stop(fake_context): #pylint:disable=redefined-outer-name
    '''Setting the stop event ends the heartbeat on its own, without a cancel and without another beat'''
    with with_mock_broker_player(fake_context) as player:
        stop = asyncio.Event()
        stop.set()
        await asyncio.wait_for(player._heartbeat('some-uuid', stop), timeout=1) #pylint:disable=protected-access
        player.broker.playing_heartbeat.assert_not_awaited()


@pytest.mark.asyncio
async def test_a_failing_heartbeat_does_not_stop_the_music(fake_context): #pylint:disable=redefined-outer-name
    '''The broker blipping is not a reason to cut the track off'''
    fake_context['guild'].voice_client = _HoldingVoiceClient()
    with patch.object(music_player_module, 'HEARTBEAT_INTERVAL_SECONDS', 0.01):
        with with_mock_broker_player(fake_context) as player:
            with fake_media_download(player.file_dir, fake_context=fake_context) as media_download:
                player.broker.claim_next_track.return_value = claimed(media_download)
                player.broker.playing_heartbeat.side_effect = ConnectionError('broker down')
                player.logger = Mock()
                task = asyncio.create_task(player.player_loop())
                await until(lambda: player.broker.playing_heartbeat.await_count >= 3)
                assert not task.done()
                assert player.logger.warning.called

                player.set_next()
                await asyncio.wait_for(task, timeout=1)
                player.broker.finish_track.assert_awaited_once()


@pytest.mark.asyncio
async def test_a_heartbeat_the_broker_no_longer_recognises_is_logged(fake_context): #pylint:disable=redefined-outer-name
    '''False means the broker thinks this track is not playing (the guild was closed, say)'''
    fake_context['guild'].voice_client = _HoldingVoiceClient()
    with patch.object(music_player_module, 'HEARTBEAT_INTERVAL_SECONDS', 0.01):
        with with_mock_broker_player(fake_context) as player:
            with fake_media_download(player.file_dir, fake_context=fake_context) as media_download:
                player.broker.claim_next_track.return_value = claimed(media_download)
                player.broker.playing_heartbeat.return_value = False
                player.logger = Mock()
                task = asyncio.create_task(player.player_loop())
                await until(lambda: player.logger.warning.called)
                assert 'no longer lists' in player.logger.warning.call_args.args[0]
                player.set_next()
                await asyncio.wait_for(task, timeout=1)


# ---------------------------------------------------------------------------
# Staging the claimed file
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_player_loop_claim_with_s3_key_downloads(fake_context): #pylint:disable=redefined-outer-name
    """A claim carrying an s3_key fetches the file from S3 before playback."""
    fake_context['guild'].voice_client = FakeVoiceClient()
    with with_mock_broker_player(fake_context) as player:
        with fake_media_download(player.file_dir, fake_context=fake_context) as media_download:
            player.broker.claim_next_track.return_value = claimed(
                media_download, s3_key='track.mp3', bucket_name='my-bucket')

            def _fake_get_file(_bucket, _key, dest):
                Path(dest).write_bytes(b'audio')

            with patch('discord_gateway.cogs.music_helpers.music_player.get_file',
                       side_effect=_fake_get_file) as mock_get:
                await player.player_loop()
                mock_get.assert_called_once_with('my-bucket', 'track.mp3', mock_get.call_args[0][2])


@pytest.mark.asyncio
async def test_player_loop_real_broker_claim_plays(fake_context): #pylint:disable=redefined-outer-name
    """Against the real queue and the AsyncioBroker double: the claim checks the entry out, and
    with no bucket configured the player plays the download's own path.

    Regression for the prod break where a mocked broker hid a checkout that did not return what
    the player expected; drive the real engine here."""
    fake_context['guild'].voice_client = FakeVoiceClient()
    with with_music_player(fake_context) as player:
        with fake_media_download(player.file_dir, fake_context=fake_context) as media_download:
            await queue_track(player, media_download)
            await player.player_loop()
            assert len(await history(player)) == 1


@pytest.mark.asyncio
async def test_player_loop_real_broker_claim_with_a_bucket_stages_from_s3(fake_context): #pylint:disable=redefined-outer-name
    '''With a bucket configured the claim's checkout names the S3 key and the player stages it'''
    fake_context['guild'].voice_client = FakeVoiceClient()
    with with_music_player(fake_context, bucket_name='my-bucket') as player:
        with fake_media_download(player.file_dir, fake_context=fake_context) as media_download:
            await queue_track(player, media_download)

            def _fake_get_file(_bucket, _key, dest):
                Path(dest).write_bytes(b'audio')

            with patch('discord_gateway.cogs.music_helpers.music_player.get_file',
                       side_effect=_fake_get_file) as mock_get:
                await player.player_loop()
            assert mock_get.call_args[0][0] == 'my-bucket'
            assert mock_get.call_args[0][1] == str(media_download.file_path)
            assert len(await history(player)) == 1


@pytest.mark.asyncio
async def test_player_loop_slow_staging_logs_warning(fake_context): #pylint:disable=redefined-outer-name
    """Staging past PLAY_STAGING_SLOW_SECONDS escalates the timing line to WARNING so a prod
    stall is visible and attributable."""
    fake_context['guild'].voice_client = FakeVoiceClient()
    with with_mock_broker_player(fake_context) as player:
        with fake_media_download(player.file_dir, fake_context=fake_context) as media_download:
            player.broker.claim_next_track.return_value = claimed(
                media_download, s3_key='track.mp3', bucket_name='my-bucket')
            player.logger = Mock()

            def _fake_get_file(_bucket, _key, dest):
                Path(dest).write_bytes(b'audio')

            # monotonic: the idle deadline, then the start and end of the S3 fetch
            with patch('discord_gateway.cogs.music_helpers.music_player.get_file', side_effect=_fake_get_file), \
                 patch('discord_gateway.cogs.music_helpers.music_player.monotonic', side_effect=[0.0, 0.0, 7.0]):
                await player.player_loop()
            staging = [c for c in player.logger.warning.call_args_list if 'Play staging' in c.args[0]]
            assert len(staging) == 1
            assert staging[0].args[3] == pytest.approx(7.0)


@pytest.mark.asyncio
async def test_player_loop_fast_staging_logs_debug_not_warning(fake_context): #pylint:disable=redefined-outer-name
    """Sub-threshold staging logs the timing line at DEBUG, never WARNING."""
    fake_context['guild'].voice_client = FakeVoiceClient()
    with with_mock_broker_player(fake_context) as player:
        with fake_media_download(player.file_dir, fake_context=fake_context) as media_download:
            player.broker.claim_next_track.return_value = claimed(
                media_download, s3_key='track.mp3', bucket_name='my-bucket')
            player.logger = Mock()

            def _fake_get_file(_bucket, _key, dest):
                Path(dest).write_bytes(b'audio')

            with patch('discord_gateway.cogs.music_helpers.music_player.get_file', side_effect=_fake_get_file), \
                 patch('discord_gateway.cogs.music_helpers.music_player.monotonic', side_effect=[0.0, 0.1, 0.3]):
                await player.player_loop()
            assert not [c for c in player.logger.warning.call_args_list if 'Play staging' in c.args[0]]
            assert [c for c in player.logger.debug.call_args_list if 'Play staging' in c.args[0]]


# ---------------------------------------------------------------------------
# Cleanup leaves the broker's state to the caller
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_cleanup_leaves_the_brokers_queue_alone(fake_context): #pylint:disable=redefined-outer-name
    '''A restart must not clear the queue, so releasing the player's own resources touches nothing in the broker'''
    with with_mock_broker_player(fake_context) as player:
        with fake_media_download(player.file_dir, fake_context=fake_context) as media_download:
            player.current_media_download = media_download
            await player.cleanup()
            assert player.broker.mock_calls == []


def test_discard_staged_removes_the_local_copy(fake_context): #pylint:disable=redefined-outer-name
    '''Commands call this for a track they took out of the queue without playing it'''
    with with_music_player(fake_context, bucket_name='my-bucket') as player:
        with fake_media_download(player.file_dir, fake_context=fake_context) as media_download:
            staged = player._staged_path(str(media_download.media_request.uuid), media_download.file_path) #pylint:disable=protected-access
            staged.write_bytes(b'audio')
            player.discard_staged(media_download)
            assert not staged.exists()
