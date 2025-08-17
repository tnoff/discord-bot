from contextlib import contextmanager
import logging
from pathlib import Path
from tempfile import TemporaryDirectory

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
        player = MusicPlayer(logging, fake_context['context'], [], 10, 0.01, Path(tmp_dir), message_queue, None, history_queue)
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
        assert player.voice_channel_inactive() is True

@pytest.mark.asyncio
async def test_music_player_voice_channel_with_no_bot(fake_context): #pylint:disable=redefined-outer-name
    fake_context['guild'].voice_client = FakeVoiceClient()
    with with_music_player(fake_context) as player:
        assert player.voice_channel_inactive() is True

@pytest.mark.asyncio
async def test_music_player_voice_channel_with_only_bot(fake_context): #pylint:disable=redefined-outer-name
    fake_context['guild'].voice_client = FakeVoiceClient()
    fake_context['channel'].members = [fake_context['bot'].user]
    fake_context['guild'].voice_client.channel = fake_context['channel']
    with with_music_player(fake_context) as player:
        assert player.voice_channel_inactive() is False

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
            assert result == [f'```Pos|| Wait Time|| Title /// Uploader\n--------------------------------------------------------------------------------------------------\n1  || 0:00:00  || {sd.title} /// {sd.uploader}```'] #pylint:disable=no-member

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
