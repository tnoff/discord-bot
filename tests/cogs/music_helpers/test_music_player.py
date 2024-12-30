import logging
from pathlib import Path
from tempfile import NamedTemporaryFile, TemporaryDirectory

import pytest

from discord_bot.exceptions import ExitEarlyException

from discord_bot.cogs.music_helpers.message_queue import MessageQueue, MessageType
from discord_bot.cogs.music_helpers.common import SearchType
from discord_bot.cogs.music_helpers.music_player import MusicPlayer
from discord_bot.cogs.music_helpers.source_dict import SourceDict
from discord_bot.cogs.music_helpers.source_download import SourceDownload

from tests.helpers import FakeContext, FakeVoiceClient, fake_bot_yielder, FakeGuild, FakeChannel, FakeAuthor

def test_music_player_basic():
    fake_bot = fake_bot_yielder()()
    with TemporaryDirectory() as tmp_dir:
        x = MusicPlayer(logging, FakeContext(fake_bot=fake_bot), [], 10, 0, Path(tmp_dir), MessageQueue())
        assert x is not None

@pytest.mark.asyncio
async def test_music_player_loop_exit_with_async_timeout():
    fake_bot = fake_bot_yielder()()
    with TemporaryDirectory() as tmp_dir:
        x = MusicPlayer(logging, FakeContext(fake_bot=fake_bot), [], 10, 0.01, Path(tmp_dir), MessageQueue())
        with pytest.raises(ExitEarlyException) as exc:
            await x.player_loop()
        assert 'MusicPlayer hit async timeout on player wait' in str(exc.value)

@pytest.mark.asyncio
async def test_music_player_loop_exiting_voice_client():
    fake_bot = fake_bot_yielder()()
    with TemporaryDirectory() as tmp_dir:
        with NamedTemporaryFile(dir=tmp_dir, suffix='.mp3') as tmp_file:
            x = MusicPlayer(logging, FakeContext(fake_bot=fake_bot), [], 10, 0.01, Path(tmp_dir), MessageQueue())
            file_path = Path(tmp_file.name)
            file_path.write_text('testing', encoding='utf-8')
            s = SourceDict('123', 'foo bar authr', '234', 'foo bar video', SearchType.SEARCH)
            sd = SourceDownload(file_path, {'webpage_url': 'https://foo.example'}, s)
            x.add_to_play_queue(sd)
            with pytest.raises(ExitEarlyException) as exc:
                await x.player_loop()
            assert 'No voice client in guild, ending loop' in str(exc.value)


@pytest.mark.asyncio
async def test_music_player_loop_basic():
    fake_bot = fake_bot_yielder()()
    voice = FakeVoiceClient()
    fake_guild = FakeGuild(voice=voice)
    with TemporaryDirectory() as tmp_dir:
        with NamedTemporaryFile(dir=tmp_dir, suffix='.mp3', delete=False) as tmp_file:
            q = MessageQueue()
            x = MusicPlayer(logging, FakeContext(fake_bot=fake_bot, fake_guild=fake_guild), [], 10, 0.01, Path(tmp_dir), q)
            file_path = Path(tmp_file.name)
            file_path.write_text('testing', encoding='utf-8')
            s = SourceDict('123', 'foo bar authr', '234', 'foo bar video', SearchType.SEARCH)
            sd = SourceDownload(file_path, {'webpage_url': 'https://foo.example'}, s)
            x.add_to_play_queue(sd)
            await x.player_loop()
            assert x._history.get_nowait() == sd #pylint:disable=protected-access
            assert x._play_queue.empty() #pylint:disable=protected-access
            assert q.get_next_message() == (MessageType.PLAY_ORDER, 'fake-guild-1234')


@pytest.mark.asyncio
async def test_music_player_join_already_there():
    fake_bot = fake_bot_yielder()()
    voice = FakeVoiceClient()
    fake_guild = FakeGuild(voice=voice)
    with TemporaryDirectory() as tmp_dir:
        q = MessageQueue()
        x = MusicPlayer(logging, FakeContext(fake_bot=fake_bot, fake_guild=fake_guild), [], 10, 0.01, Path(tmp_dir), q)
        c = FakeChannel()
        assert await x.join_voice(c) is True

@pytest.mark.asyncio
async def test_music_player_join_no_voice():
    fake_bot = fake_bot_yielder()()
    fake_guild = FakeGuild()
    with TemporaryDirectory() as tmp_dir:
        q = MessageQueue()
        x = MusicPlayer(logging, FakeContext(fake_bot=fake_bot, fake_guild=fake_guild), [], 10, 0.01, Path(tmp_dir), q)
        c = FakeChannel(id='new-channel-1234')
        assert await x.join_voice(c) is True

@pytest.mark.asyncio
async def test_music_player_join_move_to():
    fake_bot = fake_bot_yielder()()
    voice = FakeVoiceClient()
    fake_guild = FakeGuild(voice=voice)
    with TemporaryDirectory() as tmp_dir:
        q = MessageQueue()
        x = MusicPlayer(logging, FakeContext(fake_bot=fake_bot, fake_guild=fake_guild), [], 10, 0.01, Path(tmp_dir), q)
        c = FakeChannel(id='new-channel-1234')
        assert await x.join_voice(c) is True
        assert voice.channel == c

@pytest.mark.asyncio
async def test_music_player_voice_channel_active_no_voice():
    fake_bot = fake_bot_yielder()()
    fake_guild = FakeGuild()
    with TemporaryDirectory() as tmp_dir:
        q = MessageQueue()
        x = MusicPlayer(logging, FakeContext(fake_bot=fake_bot, fake_guild=fake_guild), [], 10, 0.01, Path(tmp_dir), q)
        assert x.voice_channel_active() is True

@pytest.mark.asyncio
async def test_music_player_voice_channel_with_no_bot():
    fake_bot = fake_bot_yielder()()
    member = FakeAuthor()
    channel = FakeChannel(members=[member])
    voice = FakeVoiceClient(channel=channel)
    fake_guild = FakeGuild(voice=voice)
    with TemporaryDirectory() as tmp_dir:
        q = MessageQueue()
        x = MusicPlayer(logging, FakeContext(fake_bot=fake_bot, fake_guild=fake_guild), [], 10, 0.01, Path(tmp_dir), q)
        assert x.voice_channel_active() is True

@pytest.mark.asyncio
async def test_music_player_voice_channel_with_only_bot():
    fake_bot = fake_bot_yielder()()
    channel = FakeChannel(members=[fake_bot.user])
    voice = FakeVoiceClient(channel=channel)
    fake_guild = FakeGuild(voice=voice)
    with TemporaryDirectory() as tmp_dir:
        q = MessageQueue()
        x = MusicPlayer(logging, FakeContext(fake_bot=fake_bot, fake_guild=fake_guild), [], 10, 0.01, Path(tmp_dir), q)
        assert x.voice_channel_active() is False

@pytest.mark.asyncio
async def test_music_player_loop_rollover_history():
    fake_bot = fake_bot_yielder()()
    voice = FakeVoiceClient()
    fake_guild = FakeGuild(voice=voice)
    with TemporaryDirectory() as tmp_dir:
        with NamedTemporaryFile(dir=tmp_dir, suffix='.mp3', delete=False) as tmp_file:
            x = MusicPlayer(logging, FakeContext(fake_bot=fake_bot, fake_guild=fake_guild), [], 1, 0.01, Path(tmp_dir), MessageQueue())
            file_path = Path(tmp_file.name)
            file_path.write_text('testing', encoding='utf-8')
            s = SourceDict('123', 'foo bar authr', '234', 'foo bar video', SearchType.SEARCH)
            sd = SourceDownload(file_path, {'webpage_url': 'https://foo.example'}, s)
            x.add_to_play_queue(sd)
            await x.player_loop()
            s = SourceDict('123', 'foo bar authr', '234', 'foo bar video', SearchType.SEARCH)
            sd = SourceDownload(file_path, {'webpage_url': 'https://foo.example'}, s)
            x.add_to_play_queue(sd)
            await x.player_loop()
            assert x._play_queue.empty() #pylint:disable=protected-access

            assert x.get_history_items()[0] == sd
            assert not x.check_history_empty()

def test_music_get_player_messages():
    fake_bot = fake_bot_yielder()()
    voice = FakeVoiceClient()
    fake_guild = FakeGuild(voice=voice)
    with TemporaryDirectory() as tmp_dir:
        with NamedTemporaryFile(dir=tmp_dir, suffix='.mp3') as tmp_file:
            x = MusicPlayer(logging, FakeContext(fake_bot=fake_bot, fake_guild=fake_guild), [], 1, 0.01, Path(tmp_dir), MessageQueue())
            file_path = Path(tmp_file.name)
            file_path.write_text('testing', encoding='utf-8')
            s = SourceDict('123', 'foo bar authr', '234', 'foo bar video', SearchType.SEARCH)
            sd = SourceDownload(file_path, {'webpage_url': 'https://foo.example', 'duration': 123, 'title': 'Foo Title', 'uploader': 'Foo Uploader'}, s)
            x.add_to_play_queue(sd)
            result = x.get_queue_order_messages()
            assert result == ['```Pos|| Wait Time|| Title /// Uploader\n--------------------------------------------------------------------------------------------------\n1  || 0:00:00  || Foo Title /// Foo Uploader```']

def test_music_get_player_paths():
    fake_bot = fake_bot_yielder()()
    voice = FakeVoiceClient()
    fake_guild = FakeGuild(voice=voice)
    with TemporaryDirectory() as tmp_dir:
        with NamedTemporaryFile(dir=tmp_dir, suffix='.mp3') as tmp_file:
            x = MusicPlayer(logging, FakeContext(fake_bot=fake_bot, fake_guild=fake_guild), [], 1, 0.01, Path(tmp_dir), MessageQueue())
            file_path = Path(tmp_file.name)
            file_path.write_text('testing', encoding='utf-8')
            s = SourceDict('123', 'foo bar authr', '234', 'foo bar video', SearchType.SEARCH)
            sd = SourceDownload(file_path, {'webpage_url': 'https://foo.example', 'duration': 123, 'title': 'Foo Title', 'uploader': 'Foo Uploader'}, s)
            x.add_to_play_queue(sd)
            result = x.get_file_paths()
            assert result[0] == file_path

def test_music_clear_queue_messages():
    fake_bot = fake_bot_yielder()()
    voice = FakeVoiceClient()
    fake_guild = FakeGuild(voice=voice)
    with TemporaryDirectory() as tmp_dir:
        with NamedTemporaryFile(dir=tmp_dir, suffix='.mp3') as tmp_file:
            x = MusicPlayer(logging, FakeContext(fake_bot=fake_bot, fake_guild=fake_guild), [], 1, 0.01, Path(tmp_dir), MessageQueue())
            file_path = Path(tmp_file.name)
            file_path.write_text('testing', encoding='utf-8')
            s = SourceDict('123', 'foo bar authr', '234', 'foo bar video', SearchType.SEARCH)
            sd = SourceDownload(file_path, {'webpage_url': 'https://foo.example', 'duration': 123, 'title': 'Foo Title', 'uploader': 'Foo Uploader'}, s)
            x.add_to_play_queue(sd)
            result = x.get_queue_items()
            assert len(result) == 1

            assert not x.check_queue_empty()

            x.shuffle_queue()
            x.bump_queue_item(1)
            item = x.remove_queue_item(1)
            assert item is not None

def test_music_clear_queue_messages_clear():
    fake_bot = fake_bot_yielder()()
    voice = FakeVoiceClient()
    fake_guild = FakeGuild(voice=voice)
    with TemporaryDirectory(ignore_cleanup_errors=True) as tmp_dir:
        with NamedTemporaryFile(dir=tmp_dir, suffix='.mp3', delete=False) as tmp_file:
            x = MusicPlayer(logging, FakeContext(fake_bot=fake_bot, fake_guild=fake_guild), [], 1, 0.01, Path(tmp_dir), MessageQueue())
            file_path = Path(tmp_file.name)
            file_path.write_text('testing', encoding='utf-8')
            s = SourceDict('123', 'foo bar authr', '234', 'foo bar video', SearchType.SEARCH)
            sd = SourceDownload(file_path, {'webpage_url': 'https://foo.example', 'duration': 123, 'title': 'Foo Title', 'uploader': 'Foo Uploader'}, s)
            x.add_to_play_queue(sd)
            x.clear_queue()
            result = x.get_queue_items()
            assert len(result) == 0

@pytest.mark.asyncio
async def test_cleanup():
    fake_bot = fake_bot_yielder()()
    voice = FakeVoiceClient()
    fake_guild = FakeGuild(voice=voice)
    with TemporaryDirectory() as tmp_dir:
        with NamedTemporaryFile(dir=tmp_dir, suffix='.mp3', delete=False) as tmp_file:
            x = MusicPlayer(logging, FakeContext(fake_bot=fake_bot, fake_guild=fake_guild), [], 1, 0.01, Path(tmp_dir), MessageQueue())
            file_path = Path(tmp_file.name)
            file_path.write_text('testing', encoding='utf-8')
            s = SourceDict('123', 'foo bar authr', '234', 'foo bar video', SearchType.SEARCH)
            sd = SourceDownload(file_path, {'webpage_url': 'https://foo.example'}, s)
            x.add_to_play_queue(sd)
            await x.player_loop()
            s = SourceDict('123', 'foo bar authr', '234', 'foo bar video', SearchType.SEARCH)
            sd = SourceDownload(file_path, {'webpage_url': 'https://foo.example'}, s)
            x.add_to_play_queue(sd)
            await x.player_loop()
            x.queue_messages = ['1234 message']
            res2 = await x.cleanup()
            assert str(res2[0]) == 'https://foo.example'
