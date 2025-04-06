from functools import partial
import logging
from pathlib import Path
from tempfile import NamedTemporaryFile, TemporaryDirectory

from discord.errors import NotFound
import pytest
from sqlalchemy import create_engine

from discord_bot.database import BASE
from discord_bot.exceptions import ExitEarlyException
from discord_bot.cogs.music import Music, match_generator

from discord_bot.cogs.music_helpers.download_client import VideoTooLong, VideoBanned, ExistingFileException
from discord_bot.cogs.music_helpers.music_player import MusicPlayer
from discord_bot.cogs.music_helpers.common import SearchType
from discord_bot.cogs.music_helpers.message_queue import SourceLifecycleStage
from discord_bot.cogs.music_helpers.video_cache_client import VideoCacheClient
from discord_bot.cogs.music_helpers.source_dict import SourceDict
from discord_bot.cogs.music_helpers.source_download import SourceDownload

from tests.helpers import mock_session, fake_bot_yielder
from tests.helpers import FakeVoiceClient, FakeChannel, FakeResponse, FakeContext, FakeMessage, FakeGuild


def test_match_generator_no_data():
    func = match_generator(None, None)
    info = {
        'duration': 100,
        'webpage_url': 'https://example.com/foo',
        'id': '1234',
        'extractor': 'foo extractor'
    }
    result = func(info, incomplete=None) #pylint:disable=assignment-from-no-return
    assert result is None

def test_match_generator_too_long():
    func = match_generator(1, None)
    info = {
        'duration': 100,
        'webpage_url': 'https://example.com/foo',
        'id': '1234',
        'extractor': 'foo extractor'
    }
    with pytest.raises(VideoTooLong) as exc:
        func(info, incomplete=None)
    assert 'Video Too Long' in str(exc.value)

def test_match_generator_banned_vidoes():
    func = match_generator(None, ['https://example.com/foo'])
    info = {
        'duration': 100,
        'webpage_url': 'https://example.com/foo',
        'id': '1234',
        'extractor': 'foo extractor'
    }
    with pytest.raises(VideoBanned) as exc:
        func(info, incomplete=None)
    assert 'Video Banned' in str(exc.value)


def test_match_generator_video_exists():
    with TemporaryDirectory() as tmp_dir:
        with NamedTemporaryFile(suffix='.sql') as temp_db:
            with NamedTemporaryFile(prefix='foo-extractor.1234', suffix='.mp3') as file_path:
                engine = create_engine(f'sqlite:///{temp_db.name}')
                BASE.metadata.create_all(engine)
                BASE.metadata.bind = engine

                x = VideoCacheClient(Path(tmp_dir), 10, partial(mock_session, engine))
                sd = SourceDict('123', 'requester name', '234', 'foo bar', SearchType.SEARCH)
                s = SourceDownload(Path(file_path.name), {
                    'webpage_url': 'https://foo.example.com',
                    'title': 'Foo title',
                    'uploader': 'Foo uploader',
                    'id': '1234',
                    'extractor': 'foo-extractor'
                }, sd)
                x.iterate_file(s)
                func = match_generator(None, None, video_cache_search=partial(x.search_existing_file))
                info = {
                    'duration': 100,
                    'webpage_url': 'https://example.com/foo',
                    'id': '1234',
                    'extractor': 'foo-extractor'
                }
                with pytest.raises(ExistingFileException) as exc:
                    func(info, incomplete=None)
                assert 'File already downloaded' in str(exc)
                assert exc.value.video_cache

@pytest.mark.asyncio
async def test_message_loop(mocker):
    with NamedTemporaryFile(suffix='.sql') as temp_db:
        engine = create_engine(f'sqlite:///{temp_db.name}')
        BASE.metadata.create_all(engine)
        BASE.metadata.bind = engine

        config = {
            'general': {
                'include': {
                    'music': True
                }
            },
        }
        fake_bot = fake_bot_yielder()()
        cog = Music(fake_bot, logging, config, engine)
        mocker.patch('discord_bot.cogs.music.sleep', return_value=True)
        assert await cog.send_messages() is True

@pytest.mark.asyncio
async def test_message_loop_bot_shutdown(mocker):
    with NamedTemporaryFile(suffix='.sql') as temp_db:
        engine = create_engine(f'sqlite:///{temp_db.name}')
        BASE.metadata.create_all(engine)
        BASE.metadata.bind = engine

        config = {
            'general': {
                'include': {
                    'music': True
                }
            },
        }
        fake_bot = fake_bot_yielder()()
        cog = Music(fake_bot, logging, config, engine)
        cog.bot_shutdown = True
        mocker.patch('discord_bot.cogs.music.sleep', return_value=True)
        with pytest.raises(ExitEarlyException) as exc:
            await cog.send_messages()
        assert 'Bot in shutdown and i dont have any more messages, exiting early' in str(exc.value)

@pytest.mark.asyncio
async def test_message_loop_send_single_message(mocker):
    with NamedTemporaryFile(suffix='.sql') as temp_db:
        engine = create_engine(f'sqlite:///{temp_db.name}')
        BASE.metadata.create_all(engine)
        BASE.metadata.bind = engine

        config = {
            'general': {
                'include': {
                    'music': True
                }
            },
        }
        fake_channel = FakeChannel()
        fake_bot = fake_bot_yielder()()
        cog = Music(fake_bot, logging, config, engine)
        mocker.patch('discord_bot.cogs.music.sleep', return_value=True)
        cog.message_queue.iterate_single_message([partial(fake_channel.send, 'test message')])
        await cog.send_messages()
        assert fake_channel.messages[1].content == 'test message'

@pytest.mark.asyncio
async def test_message_play_order(mocker):
    with NamedTemporaryFile(suffix='.sql') as temp_db:
        engine = create_engine(f'sqlite:///{temp_db.name}')
        BASE.metadata.create_all(engine)
        BASE.metadata.bind = engine

        config = {
            'general': {
                'include': {
                    'music': True
                }
            },
        }
        fake_bot = fake_bot_yielder()()
        cog = Music(fake_bot, logging, config, engine)
        mocker.patch('discord_bot.cogs.music.sleep', return_value=True)
        cog.message_queue.iterate_play_order('1234')
        result = await cog.send_messages()
        assert result is True

@pytest.mark.asyncio
async def test_message_loop_source_lifecycle(mocker):
    with NamedTemporaryFile(suffix='.sql') as temp_db:
        engine = create_engine(f'sqlite:///{temp_db.name}')
        BASE.metadata.create_all(engine)
        BASE.metadata.bind = engine

        config = {
            'general': {
                'include': {
                    'music': True
                }
            },
        }
        fake_channel = FakeChannel()
        fake_bot = fake_bot_yielder()()
        cog = Music(fake_bot, logging, config, engine)
        mocker.patch('discord_bot.cogs.music.sleep', return_value=True)
        x = SourceDict('1234', 'foobar', '2345', 'foo bar video', SearchType.SEARCH)
        cog.message_queue.iterate_source_lifecycle(x, SourceLifecycleStage.SEND, fake_channel.send, 'Original message')
        await cog.send_messages()
        assert x.message.content == 'Original message'

class FakeChannelRaise():
    def __init__(self):
        pass

    def delete_message(self, *args, **kwargs):
        raise NotFound(FakeResponse(), 'Message not found')

@pytest.mark.asyncio
async def test_message_loop_source_lifecycle_delete(mocker):
    with NamedTemporaryFile(suffix='.sql') as temp_db:
        engine = create_engine(f'sqlite:///{temp_db.name}')
        BASE.metadata.create_all(engine)
        BASE.metadata.bind = engine

        config = {
            'general': {
                'include': {
                    'music': True
                }
            },
        }
        fake_channel = FakeChannelRaise()
        fake_bot = fake_bot_yielder()()
        cog = Music(fake_bot, logging, config, engine)
        mocker.patch('discord_bot.cogs.music.sleep', return_value=True)
        x = SourceDict('1234', 'foobar', '2345', 'foo bar video', SearchType.SEARCH)
        cog.message_queue.iterate_source_lifecycle(x, SourceLifecycleStage.DELETE, fake_channel.delete_message, '')
        assert not await cog.send_messages()

@pytest.mark.asyncio
async def test_get_player(mocker):
    with NamedTemporaryFile(suffix='.sql') as temp_db:
        engine = create_engine(f'sqlite:///{temp_db.name}')
        BASE.metadata.create_all(engine)
        BASE.metadata.bind = engine

        config = {
            'general': {
                'include': {
                    'music': True
                }
            },
        }
        fake_bot = fake_bot_yielder()()
        cog = Music(fake_bot, logging, config, engine)
        mocker.patch.object(MusicPlayer, 'start_tasks')
        await cog.get_player('1234', ctx=FakeContext())
        assert '1234' in cog.players

@pytest.mark.asyncio
async def test_get_player_and_then_check_voice(mocker):
    with NamedTemporaryFile(suffix='.sql') as temp_db:
        engine = create_engine(f'sqlite:///{temp_db.name}')
        BASE.metadata.create_all(engine)
        BASE.metadata.bind = engine

        config = {
            'general': {
                'include': {
                    'music': True
                }
            },
        }
        fake_bot = fake_bot_yielder()()
        cog = Music(fake_bot, logging, config, engine)
        mocker.patch.object(MusicPlayer, 'start_tasks')
        await cog.get_player('1234', ctx=FakeContext())
        assert '1234' in cog.players
        result = await cog.get_player('1234', check_voice_client_active=True)
        assert result is None

@pytest.mark.asyncio
async def test_get_player_join_channel(mocker):
    with NamedTemporaryFile(suffix='.sql') as temp_db:
        engine = create_engine(f'sqlite:///{temp_db.name}')
        BASE.metadata.create_all(engine)
        BASE.metadata.bind = engine

        config = {
            'general': {
                'include': {
                    'music': True
                }
            },
        }
        fake_channel = FakeChannel()
        fake_bot = fake_bot_yielder()()
        cog = Music(fake_bot, logging, config, engine)
        mocker.patch.object(MusicPlayer, 'start_tasks')
        await cog.get_player('1234', ctx=FakeContext(), join_channel=fake_channel)
        assert '1234' in cog.players

@pytest.mark.asyncio
async def test_get_player_no_create(mocker):
    with NamedTemporaryFile(suffix='.sql') as temp_db:
        engine = create_engine(f'sqlite:///{temp_db.name}')
        BASE.metadata.create_all(engine)
        BASE.metadata.bind = engine

        config = {
            'general': {
                'include': {
                    'music': True
                }
            },
        }
        fake_bot = fake_bot_yielder()()
        cog = Music(fake_bot, logging, config, engine)
        mocker.patch.object(MusicPlayer, 'start_tasks')
        assert await cog.get_player('1234', ctx=FakeContext(), create_player=False) is None

@pytest.mark.asyncio
async def test_player_should_update_player_queue_false(mocker):
    with NamedTemporaryFile(suffix='.sql') as temp_db:
        engine = create_engine(f'sqlite:///{temp_db.name}')
        BASE.metadata.create_all(engine)
        BASE.metadata.bind = engine

        config = {
            'general': {
                'include': {
                    'music': True
                }
            },
        }
        fake_bot = fake_bot_yielder()()
        fake_message = FakeMessage(id='foo-bar-1234')
        fake_channel = FakeChannel(fake_message=fake_message)
        cog = Music(fake_bot, logging, config, engine)
        mocker.patch.object(MusicPlayer, 'start_tasks')
        player = await cog.get_player('1234', ctx=FakeContext(channel=fake_channel))
        cog.player_messages[player.guild.id] = [
            fake_message,
        ]
        result = await cog.player_should_update_queue_order(player)
        assert not result

@pytest.mark.asyncio
async def test_player_should_update_player_queue_true(mocker):
    with NamedTemporaryFile(suffix='.sql') as temp_db:
        engine = create_engine(f'sqlite:///{temp_db.name}')
        BASE.metadata.create_all(engine)
        BASE.metadata.bind = engine

        config = {
            'general': {
                'include': {
                    'music': True
                }
            },
        }
        fake_bot = fake_bot_yielder()()
        fake_message = FakeMessage(id='foo-bar-1234')
        fake_message_dos = FakeMessage(id='bar-foo-234')
        fake_channel = FakeChannel(fake_message=fake_message)
        cog = Music(fake_bot, logging, config, engine)
        mocker.patch.object(MusicPlayer, 'start_tasks')
        player = await cog.get_player('1234', ctx=FakeContext(channel=fake_channel))
        cog.player_messages[player.guild.id] = [
            fake_message_dos,
        ]
        result = await cog.player_should_update_queue_order(player)
        assert result

@pytest.mark.asyncio
async def test_player_clear_queue(mocker):
    with NamedTemporaryFile(suffix='.sql') as temp_db:
        engine = create_engine(f'sqlite:///{temp_db.name}')
        BASE.metadata.create_all(engine)
        BASE.metadata.bind = engine

        config = {
            'general': {
                'include': {
                    'music': True
                }
            },
        }
        fake_bot = fake_bot_yielder()()
        cog = Music(fake_bot, logging, config, engine)
        mocker.patch.object(MusicPlayer, 'start_tasks')
        player = await cog.get_player('1234', ctx=FakeContext())
        cog.player_messages[player.guild.id] = [
            FakeMessage(id='fake-message-1234', content='```Num|Wait|Message\n01|02:00|Foo Song ///Uploader```')
        ]
        result = await cog.clear_player_queue(player.guild.id)
        assert not cog.player_messages[player.guild.id]
        assert result is True

@pytest.mark.asyncio
async def test_player_update_queue_order_only_new(mocker):
    with NamedTemporaryFile(suffix='.sql') as temp_db:
        engine = create_engine(f'sqlite:///{temp_db.name}')
        BASE.metadata.create_all(engine)
        BASE.metadata.bind = engine

        config = {
            'general': {
                'include': {
                    'music': True
                }
            },
        }
        fake_bot = fake_bot_yielder()()
        cog = Music(fake_bot, logging, config, engine)
        guild = FakeGuild()
        mocker.patch.object(MusicPlayer, 'start_tasks')
        player = await cog.get_player(guild.id, ctx=FakeContext(fake_guild=guild))
        with TemporaryDirectory() as tmp_dir:
            with NamedTemporaryFile(dir=tmp_dir, suffix='.mp3') as tmp_file:
                file_path = Path(tmp_file.name)
                file_path.write_text('testing', encoding='utf-8')
                s = SourceDict('123', 'foo bar authr', '234', 'foo bar video', SearchType.SEARCH)
                sd = SourceDownload(file_path, {'webpage_url': 'https://foo.example', 'duration': 123, 'title': 'Foo Title', 'uploader': 'Foo Uploader'}, s)
            player.add_to_play_queue(sd)
            await cog.player_update_queue_order(player.guild.id)
            assert cog.player_messages[player.guild.id][0].content == '```Pos|| Wait Time|| Title /// Uploader\n--------------------------------------------------------------------------------------------------\n1  || 0:00:00  || Foo Title /// Foo Uploader```'

@pytest.mark.asyncio
async def test_player_update_queue_order_delete_and_edit(mocker):
    with NamedTemporaryFile(suffix='.sql') as temp_db:
        engine = create_engine(f'sqlite:///{temp_db.name}')
        BASE.metadata.create_all(engine)
        BASE.metadata.bind = engine

        config = {
            'general': {
                'include': {
                    'music': True
                }
            },
        }
        fake_bot = fake_bot_yielder()()
        cog = Music(fake_bot, logging, config, engine)
        guild = FakeGuild()
        mocker.patch.object(MusicPlayer, 'start_tasks')
        player = await cog.get_player(guild.id, ctx=FakeContext(fake_guild=guild))
        cog.player_messages[player.guild.id] = [
            FakeMessage(id='first-123', content='foo bar'),
            FakeMessage(id='second-234', content='second message')
        ]
        with TemporaryDirectory() as tmp_dir:
            with NamedTemporaryFile(dir=tmp_dir, suffix='.mp3') as tmp_file:
                file_path = Path(tmp_file.name)
                file_path.write_text('testing', encoding='utf-8')
                s = SourceDict('123', 'foo bar authr', '234', 'foo bar video', SearchType.SEARCH)
                sd = SourceDownload(file_path, {'webpage_url': 'https://foo.example', 'duration': 123, 'title': 'Foo Title', 'uploader': 'Foo Uploader'}, s)
            player.add_to_play_queue(sd)
            await cog.player_update_queue_order(player.guild.id)
            assert cog.player_messages[player.guild.id][0].content == '```Pos|| Wait Time|| Title /// Uploader\n--------------------------------------------------------------------------------------------------\n1  || 0:00:00  || Foo Title /// Foo Uploader```'
            assert len(cog.player_messages[player.guild.id]) == 1

@pytest.mark.asyncio
async def test_player_update_queue_order_no_edit(mocker):
    with NamedTemporaryFile(suffix='.sql') as temp_db:
        engine = create_engine(f'sqlite:///{temp_db.name}')
        BASE.metadata.create_all(engine)
        BASE.metadata.bind = engine

        config = {
            'general': {
                'include': {
                    'music': True
                }
            },
        }
        fake_bot = fake_bot_yielder()()
        cog = Music(fake_bot, logging, config, engine)
        guild = FakeGuild()
        fake_message = FakeMessage(id='first-123', content='```Pos|| Wait Time|| Title /// Uploader\n--------------------------------------------------------------------------------------------------\n1  || 0:00:00  || Foo Title /// Foo Uploader```')
        fake_channel = FakeChannel(fake_message=fake_message)
        mocker.patch.object(MusicPlayer, 'start_tasks')
        player = await cog.get_player(guild.id, ctx=FakeContext(fake_guild=guild, channel=fake_channel))
        cog.player_messages[player.guild.id] = [
            fake_message,
        ]
        with TemporaryDirectory() as tmp_dir:
            with NamedTemporaryFile(dir=tmp_dir, suffix='.mp3') as tmp_file:
                file_path = Path(tmp_file.name)
                file_path.write_text('testing', encoding='utf-8')
                s = SourceDict('123', 'foo bar authr', '234', 'foo bar video', SearchType.SEARCH)
                sd = SourceDownload(file_path, {'webpage_url': 'https://foo.example', 'duration': 123, 'title': 'Foo Title', 'uploader': 'Foo Uploader'}, s)
            player.add_to_play_queue(sd)
            await cog.player_update_queue_order(player.guild.id)
            assert cog.player_messages[player.guild.id][0].id == 'first-123'

@pytest.mark.asyncio
async def test_get_player_check_voice_client_active(mocker):
    with NamedTemporaryFile(suffix='.sql') as temp_db:
        engine = create_engine(f'sqlite:///{temp_db.name}')
        BASE.metadata.create_all(engine)
        BASE.metadata.bind = engine

        config = {
            'general': {
                'include': {
                    'music': True
                }
            },
        }
        fake_bot = fake_bot_yielder()()
        cog = Music(fake_bot, logging, config, engine)
        mocker.patch.object(MusicPlayer, 'start_tasks')
        assert await cog.get_player('1234', ctx=FakeContext(), check_voice_client_active=True) is None

@pytest.mark.asyncio
async def test_youtube_backoff_time_doesnt_exist_yet():
    with NamedTemporaryFile(suffix='.sql') as temp_db:
        engine = create_engine(f'sqlite:///{temp_db.name}')
        BASE.metadata.create_all(engine)
        BASE.metadata.bind = engine

        config = {
            'general': {
                'include': {
                    'music': True
                }
            },
        }
        fake_bot = fake_bot_yielder()()
        cog = Music(fake_bot, logging, config, engine)
        assert await cog.youtube_backoff_time(10, 10)

@pytest.mark.asyncio
@pytest.mark.freeze_time
async def test_youtube_backoff_time(freezer):
    with NamedTemporaryFile(suffix='.sql') as temp_db:
        engine = create_engine(f'sqlite:///{temp_db.name}')
        BASE.metadata.create_all(engine)
        BASE.metadata.bind = engine

        config = {
            'general': {
                'include': {
                    'music': True
                }
            },
        }
        sd = SourceDownload(None, {
            'extractor': 'youtube'
        }, None)
        fake_bot = fake_bot_yielder()()
        cog = Music(fake_bot, logging, config, engine)
        freezer.move_to('2025-01-01 12:00:00 UTC')
        cog.update_download_lockfile(sd)
        freezer.move_to('2025-01-01 16:00:00 UTC')
        await cog.youtube_backoff_time(cog.youtube_wait_period_min, cog.youtube_wait_period_max_variance)

@pytest.mark.asyncio
@pytest.mark.freeze_time
async def test_youtube_backoff_time_with_bot_shutdown(freezer):
    with NamedTemporaryFile(suffix='.sql') as temp_db:
        engine = create_engine(f'sqlite:///{temp_db.name}')
        BASE.metadata.create_all(engine)
        BASE.metadata.bind = engine

        config = {
            'general': {
                'include': {
                    'music': True
                }
            },
        }
        sd = SourceDownload(None, {
            'extractor': 'youtube'
        }, None)
        fake_bot = fake_bot_yielder()()
        cog = Music(fake_bot, logging, config, engine)
        freezer.move_to('2025-01-01 12:00:00 UTC')
        cog.update_download_lockfile(sd)
        cog.bot_shutdown = True
        freezer.move_to('2025-01-01 16:00:00 UTC')
        with pytest.raises(ExitEarlyException) as exc:
            await cog.youtube_backoff_time(cog.youtube_wait_period_min, cog.youtube_wait_period_max_variance)
        assert 'Exiting bot wait loop' in str(exc.value)

@pytest.mark.asyncio
@pytest.mark.freeze_time
async def test_youtube_last_update_time_with_more_backoff(freezer):
    with NamedTemporaryFile(suffix='.sql') as temp_db:
        engine = create_engine(f'sqlite:///{temp_db.name}')
        BASE.metadata.create_all(engine)
        BASE.metadata.bind = engine

        config = {
            'general': {
                'include': {
                    'music': True
                }
            },
        }
        sd = SourceDownload(None, {
            'extractor': 'youtube'
        }, None)
        fake_bot = fake_bot_yielder()()
        cog = Music(fake_bot, logging, config, engine)
        freezer.move_to('2025-01-01 12:00:00 UTC')
        cog.update_download_lockfile(sd, add_additional_backoff=60)
        assert cog.last_download_lockfile.read_text(encoding='utf-8') == '1735732860'
        cog.update_download_lockfile(sd)
        assert cog.last_download_lockfile.read_text(encoding='utf-8') == '1735732800'


@pytest.mark.asyncio
async def test_cleanup_players_just_bot(mocker):
    with NamedTemporaryFile(suffix='.sql') as temp_db:
        engine = create_engine(f'sqlite:///{temp_db.name}')
        BASE.metadata.create_all(engine)
        BASE.metadata.bind = engine

        config = {
            'general': {
                'include': {
                    'music': True
                }
            },
        }
        fake_bot = fake_bot_yielder()()
        cog = Music(fake_bot, logging, config, engine)
        mocker.patch('discord_bot.cogs.music.sleep', return_value=True)
        mocker.patch.object(MusicPlayer, 'start_tasks')
        fake_channel = FakeChannel(members=[fake_bot.user])
        fake_voice = FakeVoiceClient(channel=fake_channel)
        fake_guild = FakeGuild(voice=fake_voice)
        await cog.get_player(fake_guild.id, ctx=FakeContext(fake_guild=fake_guild, fake_bot=fake_bot))
        await cog.cleanup_players()
        assert fake_guild.id not in cog.players

# TODO test cleanup with player history bits in particular

@pytest.mark.asyncio
async def test_cache_cleanup_no_op(mocker):
    with NamedTemporaryFile(suffix='.sql') as temp_db:
        engine = create_engine(f'sqlite:///{temp_db.name}')
        BASE.metadata.create_all(engine)
        BASE.metadata.bind = engine

        config = {
            'general': {
                'include': {
                    'music': True
                },
            },
            'music': {
                'enable_cache_files': True,
                'max_cache_files': 0,
            }
        }
        fake_bot = fake_bot_yielder()()
        cog = Music(fake_bot, logging, config, engine)
        mocker.patch('discord_bot.cogs.music.sleep', return_value=True)
        mocker.patch.object(MusicPlayer, 'start_tasks')
        fake_channel = FakeChannel(members=[fake_bot.user])
        fake_voice = FakeVoiceClient(channel=fake_channel)
        fake_guild = FakeGuild(voice=fake_voice)
        await cog.get_player(fake_guild.id, ctx=FakeContext(fake_guild=fake_guild, fake_bot=fake_bot))
        with TemporaryDirectory() as tmp_dir:
            with NamedTemporaryFile(dir=tmp_dir, suffix='.mp3', delete=False) as tmp_file:
                file_path = Path(tmp_file.name)
                file_path.write_text('testing', encoding='utf-8')
                s = SourceDict('123', 'foo bar authr', '234', 'https://foo.example', SearchType.SEARCH)
                sd = SourceDownload(file_path, {'webpage_url': 'https://foo.example'}, s)
                cog.players[fake_guild.id].add_to_play_queue(sd)
                cog.video_cache.iterate_file(sd)
                cog.video_cache.ready_remove()
                await cog.cache_cleanup()
                assert cog.video_cache.get_webpage_url_item(s)

@pytest.mark.asyncio
async def test_cache_cleanup_removes(mocker):
    with NamedTemporaryFile(suffix='.sql') as temp_db:
        engine = create_engine(f'sqlite:///{temp_db.name}')
        BASE.metadata.create_all(engine)
        BASE.metadata.bind = engine

        config = {
            'general': {
                'include': {
                    'music': True
                },
            },
            'music': {
                'enable_cache_files': True,
                'max_cache_files': 0,
            }
        }
        fake_bot = fake_bot_yielder()()
        cog = Music(fake_bot, logging, config, engine)
        mocker.patch('discord_bot.cogs.music.sleep', return_value=True)
        mocker.patch.object(MusicPlayer, 'start_tasks')
        fake_channel = FakeChannel(members=[fake_bot.user])
        fake_voice = FakeVoiceClient(channel=fake_channel)
        fake_guild = FakeGuild(voice=fake_voice)
        await cog.get_player(fake_guild.id, ctx=FakeContext(fake_guild=fake_guild, fake_bot=fake_bot))
        with TemporaryDirectory() as tmp_dir:
            with NamedTemporaryFile(dir=tmp_dir, suffix='.mp3', delete=False) as tmp_file:
                file_path = Path(tmp_file.name)
                file_path.write_text('testing', encoding='utf-8')
                s = SourceDict('123', 'foo bar authr', '234', 'https://foo.example', SearchType.SEARCH)
                sd = SourceDownload(file_path, {'webpage_url': 'https://foo.example'}, s)
                cog.video_cache.iterate_file(sd)
                cog.video_cache.ready_remove()
                await cog.cache_cleanup()
                assert not cog.video_cache.get_webpage_url_item(s)
