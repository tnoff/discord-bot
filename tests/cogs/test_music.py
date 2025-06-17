from functools import partial
from pathlib import Path
from tempfile import NamedTemporaryFile, TemporaryDirectory
from typing import List

from discord.errors import NotFound
import pytest
from sqlalchemy import create_engine

from discord_bot.database import BASE, Playlist, PlaylistItem, VideoCache
from discord_bot.exceptions import ExitEarlyException
from discord_bot.cogs.music import Music, match_generator

from discord_bot.cogs.music_helpers.history_playlist_item import HistoryPlaylistItem
from discord_bot.cogs.music_helpers.download_client import VideoTooLong, VideoBanned
from discord_bot.cogs.music_helpers.download_client import ExistingFileException, BotDownloadFlagged, DownloadClientException, DownloadError
from discord_bot.cogs.music_helpers.music_player import MusicPlayer
from discord_bot.cogs.music_helpers.common import SearchType
from discord_bot.cogs.music_helpers.message_queue import SourceLifecycleStage
from discord_bot.cogs.music_helpers.search_client import SearchException
from discord_bot.cogs.music_helpers.video_cache_client import VideoCacheClient
from discord_bot.cogs.music_helpers.source_dict import SourceDict
from discord_bot.cogs.music_helpers.source_download import SourceDownload

from tests.helpers import mock_session, fake_bot_yielder
from tests.helpers import FakeAuthor, FakeVoiceClient, FakeChannel, FakeResponse, FakeContext, FakeMessage, FakeGuild


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
    config = {
        'general': {
            'include': {
                'music': True
            }
        },
    }
    fake_bot = fake_bot_yielder()()
    cog = Music(fake_bot, config, None)
    mocker.patch('discord_bot.cogs.music.sleep', return_value=True)
    assert await cog.send_messages() is True

@pytest.mark.asyncio
async def test_message_loop_bot_shutdown(mocker):
    config = {
        'general': {
            'include': {
                'music': True
            }
        },
    }
    fake_bot = fake_bot_yielder()()
    cog = Music(fake_bot, config, None)
    cog.bot_shutdown = True
    mocker.patch('discord_bot.cogs.music.sleep', return_value=True)
    with pytest.raises(ExitEarlyException) as exc:
        await cog.send_messages()
    assert 'Bot in shutdown and i dont have any more messages, exiting early' in str(exc.value)

@pytest.mark.asyncio
async def test_message_loop_send_single_message(mocker):
    config = {
        'general': {
            'include': {
                'music': True
            }
        },
    }
    fake_channel = FakeChannel()
    fake_bot = fake_bot_yielder()()
    cog = Music(fake_bot, config, None)
    mocker.patch('discord_bot.cogs.music.sleep', return_value=True)
    cog.message_queue.iterate_single_message([partial(fake_channel.send, 'test message')])
    await cog.send_messages()
    assert fake_channel.messages[1].content == 'test message'

@pytest.mark.asyncio
async def test_message_play_order(mocker):
    config = {
        'general': {
            'include': {
                'music': True
            }
        },
    }
    fake_bot = fake_bot_yielder()()
    cog = Music(fake_bot, config, None)
    mocker.patch('discord_bot.cogs.music.sleep', return_value=True)
    cog.message_queue.iterate_play_order('1234')
    result = await cog.send_messages()
    assert result is True

@pytest.mark.asyncio
async def test_message_loop_source_lifecycle(mocker):
    config = {
        'general': {
            'include': {
                'music': True
            }
        },
    }
    fake_channel = FakeChannel()
    fake_bot = fake_bot_yielder()()
    cog = Music(fake_bot, config, None)
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
    config = {
        'general': {
            'include': {
                'music': True
            }
        },
    }
    fake_channel = FakeChannelRaise()
    fake_bot = fake_bot_yielder()()
    cog = Music(fake_bot, config, None)
    mocker.patch('discord_bot.cogs.music.sleep', return_value=True)
    x = SourceDict('1234', 'foobar', '2345', 'foo bar video', SearchType.SEARCH)
    cog.message_queue.iterate_source_lifecycle(x, SourceLifecycleStage.DELETE, fake_channel.delete_message, '')
    assert not await cog.send_messages()

@pytest.mark.asyncio
async def test_get_player(mocker):
    config = {
        'general': {
            'include': {
                'music': True
            }
        },
    }
    fake_bot = fake_bot_yielder()()
    cog = Music(fake_bot, config, None)
    mocker.patch.object(MusicPlayer, 'start_tasks')
    await cog.get_player('1234', ctx=FakeContext())
    assert '1234' in cog.players

@pytest.mark.asyncio
async def test_get_player_and_then_check_voice(mocker):
    config = {
        'general': {
            'include': {
                'music': True
            }
        },
    }
    fake_bot = fake_bot_yielder()()
    cog = Music(fake_bot, config, None)
    mocker.patch.object(MusicPlayer, 'start_tasks')
    await cog.get_player('1234', ctx=FakeContext())
    assert '1234' in cog.players
    result = await cog.get_player('1234', check_voice_client_active=True)
    assert result is None

@pytest.mark.asyncio
async def test_get_player_join_channel(mocker):
    config = {
        'general': {
            'include': {
                'music': True
            }
        },
    }
    fake_channel = FakeChannel()
    fake_bot = fake_bot_yielder()()
    cog = Music(fake_bot, config, None)
    mocker.patch.object(MusicPlayer, 'start_tasks')
    await cog.get_player('1234', ctx=FakeContext(), join_channel=fake_channel)
    assert '1234' in cog.players

@pytest.mark.asyncio
async def test_get_player_no_create(mocker):
    config = {
        'general': {
            'include': {
                'music': True
            }
        },
    }
    fake_bot = fake_bot_yielder()()
    cog = Music(fake_bot, config, None)
    mocker.patch.object(MusicPlayer, 'start_tasks')
    assert await cog.get_player('1234', ctx=FakeContext(), create_player=False) is None

@pytest.mark.asyncio
async def test_player_should_update_player_queue_false(mocker):
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
    cog = Music(fake_bot, config, None)
    mocker.patch.object(MusicPlayer, 'start_tasks')
    player = await cog.get_player('1234', ctx=FakeContext(channel=fake_channel))
    cog.player_messages[player.guild.id] = [
        fake_message,
    ]
    result = await cog.player_should_update_queue_order(player)
    assert not result

@pytest.mark.asyncio
async def test_player_should_update_player_queue_true(mocker):
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
    cog = Music(fake_bot, config, None)
    mocker.patch.object(MusicPlayer, 'start_tasks')
    player = await cog.get_player('1234', ctx=FakeContext(channel=fake_channel))
    cog.player_messages[player.guild.id] = [
        fake_message_dos,
    ]
    result = await cog.player_should_update_queue_order(player)
    assert result

@pytest.mark.asyncio
async def test_player_clear_queue(mocker):
    config = {
        'general': {
            'include': {
                'music': True
            }
        },
    }
    fake_bot = fake_bot_yielder()()
    cog = Music(fake_bot, config, None)
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
    config = {
        'general': {
            'include': {
                'music': True
            }
        },
    }
    fake_bot = fake_bot_yielder()()
    cog = Music(fake_bot, config, None)
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
    config = {
        'general': {
            'include': {
                'music': True
            }
        },
    }
    fake_bot = fake_bot_yielder()()
    cog = Music(fake_bot, config, None)
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
    config = {
        'general': {
            'include': {
                'music': True
            }
        },
    }
    fake_bot = fake_bot_yielder()()
    cog = Music(fake_bot, config, None)
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
    config = {
        'general': {
            'include': {
                'music': True
            }
        },
    }
    fake_bot = fake_bot_yielder()()
    cog = Music(fake_bot, config, None)
    mocker.patch.object(MusicPlayer, 'start_tasks')
    assert await cog.get_player('1234', ctx=FakeContext(), check_voice_client_active=True) is None

@pytest.mark.asyncio
async def test_youtube_backoff_time_doesnt_exist_yet():
    config = {
        'general': {
            'include': {
                'music': True
            }
        },
    }
    fake_bot = fake_bot_yielder()()
    cog = Music(fake_bot, config, None)
    assert await cog.youtube_backoff_time(10, 10)

@pytest.mark.asyncio
@pytest.mark.freeze_time
async def test_youtube_backoff_time(freezer):
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
    cog = Music(fake_bot, config, None)
    freezer.move_to('2025-01-01 12:00:00 UTC')
    cog.update_download_lockfile(sd)
    freezer.move_to('2025-01-01 16:00:00 UTC')
    await cog.youtube_backoff_time(cog.youtube_wait_period_min, cog.youtube_wait_period_max_variance)

@pytest.mark.asyncio
@pytest.mark.freeze_time
async def test_youtube_backoff_time_with_bot_shutdown(freezer):
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
    cog = Music(fake_bot, config, None)
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
    cog = Music(fake_bot, config, None)
    freezer.move_to('2025-01-01 12:00:00 UTC')
    cog.update_download_lockfile(sd, add_additional_backoff=60)
    assert cog.last_download_lockfile.read_text(encoding='utf-8') == '1735732860'
    cog.update_download_lockfile(sd)
    assert cog.last_download_lockfile.read_text(encoding='utf-8') == '1735732800'


@pytest.mark.asyncio
async def test_cleanup_players_just_bot(mocker, freezer):
    config = {
        'general': {
            'include': {
                'music': True
            }
        },
    }
    fake_bot = fake_bot_yielder()()
    cog = Music(fake_bot, config, None)
    mocker.patch('discord_bot.cogs.music.sleep', return_value=True)
    mocker.patch.object(MusicPlayer, 'start_tasks')
    fake_channel = FakeChannel(members=[fake_bot.user])
    fake_voice = FakeVoiceClient(channel=fake_channel)
    fake_guild = FakeGuild(voice=fake_voice)
    await cog.get_player(fake_guild.id, ctx=FakeContext(fake_guild=fake_guild, fake_bot=fake_bot))
    # Run twice cause first time needs to hit heartbeat
    freezer.move_to('2024-12-01 12:00:00')
    await cog.cleanup_players()
    freezer.move_to('2024-12-02 12:00:00')
    await cog.cleanup_players()
    assert fake_guild.id not in cog.players


@pytest.mark.asyncio
async def test_history_playlist_update(mocker):
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
        cog = Music(fake_bot, config, engine)
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
                s = SourceDict('123', 'foo bar authr', '234', 'https://foo.example', SearchType.DIRECT)
                sd = SourceDownload(file_path, {'webpage_url': 'https://foo.example'}, s)

                cog.history_playlist_queue.put_nowait(HistoryPlaylistItem(cog.players[fake_guild.id].history_playlist_id, sd))
                await cog.playlist_history_update()

                with mock_session(engine) as session:
                    assert session.query(Playlist).count() == 1
                    assert session.query(PlaylistItem).count() == 1

                # Run twice to exercise dupes aren't created
                cog.history_playlist_queue.put_nowait(HistoryPlaylistItem(cog.players[fake_guild.id].history_playlist_id, sd))
                await cog.playlist_history_update()

                with mock_session(engine) as session:
                    assert session.query(Playlist).count() == 1
                    assert session.query(PlaylistItem).count() == 1

@pytest.mark.asyncio
async def test_history_playlist_update_delete_extra_items(mocker):
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
            'music': {
                'playlist': {
                    'server_playlist_max_size': 1,
                }
            }
        }
        fake_bot = fake_bot_yielder()()
        cog = Music(fake_bot, config, engine)
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
                s = SourceDict('123', 'foo bar authr', '234', 'https://foo.example', SearchType.DIRECT)
                sd = SourceDownload(file_path, {'webpage_url': 'https://foo.example'}, s)

                cog.history_playlist_queue.put_nowait(HistoryPlaylistItem(cog.players[fake_guild.id].history_playlist_id, sd))
                await cog.playlist_history_update()

                sd2 = SourceDownload(file_path, {'webpage_url': 'https://foo.example.dos'}, s)
                cog.history_playlist_queue.put_nowait(HistoryPlaylistItem(cog.players[fake_guild.id].history_playlist_id, sd2))
                await cog.playlist_history_update()

                with mock_session(engine) as session:
                    assert session.query(Playlist).count() == 1
                    assert session.query(PlaylistItem).count() == 1

@pytest.mark.asyncio
async def test_guild_cleanup(mocker):
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
        cog = Music(fake_bot, config, engine)
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
                s = SourceDict('123', 'foo bar authr', '234', 'https://foo.example', SearchType.DIRECT)
                sd = SourceDownload(file_path, {'webpage_url': 'https://foo.example'}, s)
                await cog.players[fake_guild.id]._history.put(sd) #pylint:disable=protected-access
                await cog.cleanup(fake_guild, external_shutdown_called=True)
                assert fake_guild.id not in cog.players
                assert fake_guild.id not in cog.download_queue.queues

@pytest.mark.asyncio
async def test_guild_hanging_downloads(mocker):
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
        cog = Music(fake_bot, config, engine)
        mocker.patch('discord_bot.cogs.music.sleep', return_value=True)
        mocker.patch.object(MusicPlayer, 'start_tasks')
        fake_channel = FakeChannel(members=[fake_bot.user])
        fake_voice = FakeVoiceClient(channel=fake_channel)
        fake_guild = FakeGuild(voice=fake_voice)
        await cog.get_player(fake_guild.id, ctx=FakeContext(fake_guild=fake_guild, fake_bot=fake_bot))
        s = SourceDict('123', 'foo bar authr', '234', 'https://foo.example', SearchType.DIRECT)
        cog.download_queue.put_nowait(fake_guild.id, s)
        await cog.cleanup(fake_guild, external_shutdown_called=True)
        assert fake_guild.id not in cog.download_queue.queues

def yield_fake_search_client(source_dict: SourceDict = None):
    class FakeSearchClient():
        def __init__(self, *_args, **_kwargs):
            pass

        async def check_source(self, *_args, **_kwargs):
            return [source_dict]

    return FakeSearchClient

def yield_fake_download_client(source_download: SourceDownload):

    class FakeDownloadClient():
        def __init__(self, *_args, **_kwargs):
            pass

        async def create_source(self, *_args, **_kwargs):
            return source_download

    return FakeDownloadClient

@pytest.mark.asyncio()
async def test_download_queue_no_download(mocker):

    async def fake_callback(source_download: SourceDownload):
        source_download.i_was_called = True

    config = {
        'general': {
            'include': {
                'music': True
            }
        },
    }
    fake_bot = fake_bot_yielder()()
    s = SourceDict('123', 'foo bar authr', '234', 'https://foo.example', SearchType.DIRECT, download_file=False, post_download_callback_functions=[fake_callback])
    sd = SourceDownload(None, {'webpage_url': 'https://foo.example'}, s)
    mocker.patch('discord_bot.cogs.music.DownloadClient', side_effect=yield_fake_download_client(sd))
    cog = Music(fake_bot, config, None)
    mocker.patch('discord_bot.cogs.music.sleep', return_value=True)
    mocker.patch.object(MusicPlayer, 'start_tasks')
    fake_channel = FakeChannel(members=[fake_bot.user])
    fake_voice = FakeVoiceClient(channel=fake_channel)
    fake_guild = FakeGuild(voice=fake_voice)
    await cog.get_player(fake_guild.id, ctx=FakeContext(fake_guild=fake_guild, fake_bot=fake_bot))
    cog.download_queue.put_nowait(fake_guild.id, s)
    await cog.download_files()
    assert sd.i_was_called #pylint:disable=no-member

@pytest.mark.asyncio()
async def test_download_queue(mocker):
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
            'music': {
                'download': {
                    'cache': {
                        'enable_cache_files': True,
                    }
                }
            }
        }
        fake_bot = fake_bot_yielder()()
        with TemporaryDirectory() as tmp_dir:
            with NamedTemporaryFile(dir=tmp_dir, suffix='.mp3', delete=False) as tmp_file:
                file_path = Path(tmp_file.name)
                file_path.write_text('testing', encoding='utf-8')
                mocker.patch('discord_bot.cogs.music.sleep', return_value=True)
                mocker.patch.object(MusicPlayer, 'start_tasks')
                fake_channel = FakeChannel(members=[fake_bot.user])
                fake_voice = FakeVoiceClient(channel=fake_channel)
                fake_guild = FakeGuild(voice=fake_voice)
                s = SourceDict(fake_guild.id, 'foo bar authr', '234', 'https://foo.example', SearchType.DIRECT)
                sd = SourceDownload(file_path, {'webpage_url': 'https://foo.example'}, s)
                mocker.patch('discord_bot.cogs.music.DownloadClient', side_effect=yield_fake_download_client(sd))
                cog = Music(fake_bot, config, engine)
                await cog.get_player(fake_guild.id, ctx=FakeContext(fake_guild=fake_guild, fake_bot=fake_bot))
                cog.download_queue.put_nowait(fake_guild.id, s)
                await cog.download_files()
                assert cog.players[fake_guild.id].get_queue_items()

def yield_fake_download_client_from_cache(video_cache: VideoCache):

    class FakeDownloadClient():
        def __init__(self, *_args, **_kwargs):
            pass

        async def create_source(self, *_args, **_kwargs):
            raise ExistingFileException('foo', video_cache=video_cache)

    return FakeDownloadClient

@pytest.mark.asyncio()
async def test_download_queue_hits_cache(mocker):
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
            'music': {
                'download': {
                    'cache': {
                        'enable_cache_files': True,
                    }
                }
            }
        }
        fake_bot = fake_bot_yielder()()
        with TemporaryDirectory() as tmp_dir:
            with NamedTemporaryFile(dir=tmp_dir, suffix='.mp3', delete=False) as tmp_file:
                file_path = Path(tmp_file.name)
                file_path.write_text('testing', encoding='utf-8')
                mocker.patch('discord_bot.cogs.music.sleep', return_value=True)
                mocker.patch.object(MusicPlayer, 'start_tasks')
                fake_channel = FakeChannel(members=[fake_bot.user])
                fake_voice = FakeVoiceClient(channel=fake_channel)
                fake_guild = FakeGuild(voice=fake_voice)
                s = SourceDict(fake_guild.id, 'foo bar authr', '234', 'https://foo.example', SearchType.DIRECT)
                sd = SourceDownload(file_path, {'webpage_url': 'https://foo.example'}, s)
                cog = Music(fake_bot, config, engine)
                cog.video_cache.iterate_file(sd)
                await cog.get_player(fake_guild.id, ctx=FakeContext(fake_guild=fake_guild, fake_bot=fake_bot))
                cog.download_queue.put_nowait(fake_guild.id, s)
                await cog.download_files()
                assert cog.players[fake_guild.id].get_queue_items()

@pytest.mark.asyncio()
async def test_download_queue_existing_video(mocker):
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
            'music': {
                'download': {
                    'cache': {
                        'enable_cache_files': True,
                    }
                }
            }
        }
        fake_bot = fake_bot_yielder()()
        with TemporaryDirectory() as tmp_dir:
            with NamedTemporaryFile(dir=tmp_dir, suffix='.mp3', delete=False) as tmp_file:
                file_path = Path(tmp_file.name)
                file_path.write_text('testing', encoding='utf-8')
                mocker.patch('discord_bot.cogs.music.sleep', return_value=True)
                mocker.patch.object(MusicPlayer, 'start_tasks')
                fake_channel = FakeChannel(members=[fake_bot.user])
                fake_voice = FakeVoiceClient(channel=fake_channel)
                fake_guild = FakeGuild(voice=fake_voice)
                s = SourceDict(fake_guild.id, 'foo bar authr', '234', 'https://foo.example', SearchType.DIRECT)
                sd = SourceDownload(file_path, {'webpage_url': 'https://foo.example'}, s)
                with mock_session(engine) as db_session:
                    video_cache = VideoCache(base_path=str(sd.base_path), video_url='https://foo.bar.example.com', count=0)
                    db_session.add(video_cache)
                    db_session.commit()
                    mocker.patch('discord_bot.cogs.music.DownloadClient', side_effect=yield_fake_download_client_from_cache(video_cache))
                    cog = Music(fake_bot, config, engine)
                    await cog.get_player(fake_guild.id, ctx=FakeContext(fake_guild=fake_guild, fake_bot=fake_bot))
                    cog.download_queue.put_nowait(fake_guild.id, s)
                    await cog.download_files()
                    assert cog.players[fake_guild.id].get_queue_items()

def yield_download_client_bot_flagged():
    class FakeDownloadClient():
        def __init__(self, *_args, **_kwargs):
            pass

        async def create_source(self, *_args, **_kwargs):
            raise BotDownloadFlagged('foo', user_message='woopsie')

    return FakeDownloadClient

@pytest.mark.asyncio()
async def test_download_queue_bot_warning(mocker):
    config = {
        'general': {
            'include': {
                'music': True
            }
        },
    }
    fake_bot = fake_bot_yielder()()
    mocker.patch('discord_bot.cogs.music.sleep', return_value=True)
    mocker.patch.object(MusicPlayer, 'start_tasks')
    fake_channel = FakeChannel(members=[fake_bot.user])
    fake_voice = FakeVoiceClient(channel=fake_channel)
    fake_guild = FakeGuild(voice=fake_voice)
    mocker.patch('discord_bot.cogs.music.DownloadClient', side_effect=yield_download_client_bot_flagged())
    cog = Music(fake_bot, config, None)
    s = SourceDict(fake_guild.id, 'foo bar authr', '234', 'https://foo.example', SearchType.DIRECT)
    await cog.get_player(fake_guild.id, ctx=FakeContext(fake_guild=fake_guild, fake_bot=fake_bot))
    cog.download_queue.put_nowait(fake_guild.id, s)
    await cog.download_files()
    assert not cog.players[fake_guild.id].get_queue_items()

def yield_download_client_download_exception():
    class FakeDownloadClient():
        def __init__(self, *_args, **_kwargs):
            pass

        async def create_source(self, *_args, **_kwargs):
            raise DownloadClientException('foo', user_message='whoopsie')

    return FakeDownloadClient

@pytest.mark.asyncio()
async def test_download_queue_download_exception(mocker):
    async def bump_value():
        return True

    config = {
        'general': {
            'include': {
                'music': True
            }
        },
    }
    fake_bot = fake_bot_yielder()()
    mocker.patch('discord_bot.cogs.music.sleep', return_value=True)
    mocker.patch.object(MusicPlayer, 'start_tasks')
    fake_channel = FakeChannel(members=[fake_bot.user])
    fake_voice = FakeVoiceClient(channel=fake_channel)
    fake_guild = FakeGuild(voice=fake_voice)

    mocker.patch('discord_bot.cogs.music.DownloadClient', side_effect=yield_download_client_download_exception())
    cog = Music(fake_bot, config, None)
    s = SourceDict(fake_guild.id, 'foo bar authr', '234', 'https://foo.example', SearchType.DIRECT,
                    video_non_exist_callback_functions=[partial(bump_value)])
    await cog.get_player(fake_guild.id, ctx=FakeContext(fake_guild=fake_guild, fake_bot=fake_bot))
    cog.download_queue.put_nowait(fake_guild.id, s)
    await cog.download_files()
    assert not cog.players[fake_guild.id].get_queue_items()

def yield_download_client_download_error():
    class FakeDownloadClient():
        def __init__(self, *_args, **_kwargs):
            pass

        async def create_source(self, *_args, **_kwargs):
            raise DownloadError('foo')

    return FakeDownloadClient

@pytest.mark.asyncio()
async def test_download_queue_download_error(mocker):
    async def bump_value():
        return True
    config = {
        'general': {
            'include': {
                'music': True
            }
        },
    }
    fake_bot = fake_bot_yielder()()
    mocker.patch('discord_bot.cogs.music.sleep', return_value=True)
    mocker.patch.object(MusicPlayer, 'start_tasks')
    fake_channel = FakeChannel(members=[fake_bot.user])
    fake_voice = FakeVoiceClient(channel=fake_channel)
    fake_guild = FakeGuild(voice=fake_voice)

    mocker.patch('discord_bot.cogs.music.DownloadClient', side_effect=yield_download_client_download_error())
    cog = Music(fake_bot, config, None)
    s = SourceDict(fake_guild.id, 'foo bar authr', '234', 'https://foo.example', SearchType.DIRECT,
                    video_non_exist_callback_functions=[partial(bump_value)])
    await cog.get_player(fake_guild.id, ctx=FakeContext(fake_guild=fake_guild, fake_bot=fake_bot))
    cog.download_queue.put_nowait(fake_guild.id, s)
    await cog.download_files()
    assert not cog.players[fake_guild.id].get_queue_items()

@pytest.mark.asyncio()
async def test_download_queue_no_result(mocker):
    config = {
        'general': {
            'include': {
                'music': True
            }
        },
    }
    fake_bot = fake_bot_yielder()()
    mocker.patch('discord_bot.cogs.music.sleep', return_value=True)
    mocker.patch.object(MusicPlayer, 'start_tasks')
    fake_channel = FakeChannel(members=[fake_bot.user])
    fake_voice = FakeVoiceClient(channel=fake_channel)
    fake_guild = FakeGuild(voice=fake_voice)
    s = SourceDict(fake_guild.id, 'foo bar authr', '234', 'https://foo.example', SearchType.DIRECT)
    mocker.patch('discord_bot.cogs.music.DownloadClient', side_effect=yield_fake_download_client(None))
    cog = Music(fake_bot, config, None)
    await cog.get_player(fake_guild.id, ctx=FakeContext(fake_guild=fake_guild, fake_bot=fake_bot))
    cog.download_queue.put_nowait(fake_guild.id, s)
    await cog.download_files()
    assert not cog.players[fake_guild.id].get_queue_items()

@pytest.mark.asyncio()
async def test_download_queue_player_shutdown(mocker):
    config = {
        'general': {
            'include': {
                'music': True
            }
        },
    }
    fake_bot = fake_bot_yielder()()
    mocker.patch('discord_bot.cogs.music.sleep', return_value=True)
    mocker.patch.object(MusicPlayer, 'start_tasks')
    fake_channel = FakeChannel(members=[fake_bot.user])
    fake_voice = FakeVoiceClient(channel=fake_channel)
    fake_guild = FakeGuild(voice=fake_voice)
    s = SourceDict(fake_guild.id, 'foo bar authr', '234', 'https://foo.example', SearchType.DIRECT)
    cog = Music(fake_bot, config, None)
    await cog.get_player(fake_guild.id, ctx=FakeContext(fake_guild=fake_guild, fake_bot=fake_bot))
    cog.download_queue.put_nowait(fake_guild.id, s)
    cog.players[fake_guild.id].shutdown_called = True
    await cog.download_files()
    assert not cog.players[fake_guild.id].get_queue_items()

@pytest.mark.asyncio()
async def test_download_queue_no_player_queue(mocker):
    config = {
        'general': {
            'include': {
                'music': True
            }
        },
    }
    fake_bot = fake_bot_yielder()()
    mocker.patch('discord_bot.cogs.music.sleep', return_value=True)
    mocker.patch.object(MusicPlayer, 'start_tasks')
    fake_channel = FakeChannel(members=[fake_bot.user])
    fake_voice = FakeVoiceClient(channel=fake_channel)
    fake_guild = FakeGuild(voice=fake_voice)
    s = SourceDict(fake_guild.id, 'foo bar authr', '234', 'https://foo.example', SearchType.DIRECT)
    cog = Music(fake_bot, config, None)
    cog.download_queue.put_nowait(fake_guild.id, s)
    await cog.download_files()
    assert fake_guild.id not in cog.players

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
                'download': {
                    'cache': {
                        'enable_cache_files': True,
                    }
                }
            }
        }
        fake_bot = fake_bot_yielder()()
        cog = Music(fake_bot, config, engine)
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
                s = SourceDict('123', 'foo bar authr', '234', 'https://foo.example', SearchType.DIRECT)
                sd = SourceDownload(file_path, {'webpage_url': 'https://foo.example'}, s)
                cog.players[fake_guild.id].add_to_play_queue(sd)
                cog.video_cache.iterate_file(sd)
                cog.video_cache.ready_remove()
                await cog.cache_cleanup()
                assert cog.video_cache.get_webpage_url_item(s)

@pytest.mark.asyncio
async def test_cache_cleanup_removes(mocker, freezer):
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
                'download': {
                    'cache': {
                        'enable_cache_files': True,
                        'max_cache_files': 1,
                    }
                }
            }
        }
        fake_bot = fake_bot_yielder()()
        cog = Music(fake_bot, config, engine)
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
                with NamedTemporaryFile(dir=tmp_dir, suffix='.mp3', delete=False) as tmp_file2:
                    file_path2 = Path(tmp_file2.name)
                    file_path2.write_text('testing', encoding='utf-8')
                    s = SourceDict('123', 'foo bar authr', '234', 'https://foo.example', SearchType.DIRECT)
                    sd = SourceDownload(file_path, {'webpage_url': 'https://foo.example'}, s)
                    s2 = SourceDict('123', 'foo bar authr', '234', 'https://foo.example/bar', SearchType.DIRECT)
                    sd2 = SourceDownload(file_path2, {'webpage_url': 'https://foo.example/bar'}, s2)
                    cog.video_cache.iterate_file(sd)
                    cog.video_cache.iterate_file(sd2)
                    cog.video_cache.ready_remove()
                    # Run twice so heartbeat passses
                    freezer.move_to('2024-12-01 12:00:00')
                    await cog.cache_cleanup()
                    freezer.move_to('2024-12-02 12:00:00')
                    await cog.cache_cleanup()
                    assert not cog.video_cache.get_webpage_url_item(s)

@pytest.mark.asyncio
async def test_cache_cleanup_skips_source_in_transit(mocker):
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
                'download': {
                    'cache': {
                        'enable_cache_files': True,
                        'max_cache_files': 1,
                    }
                }
            }
        }
        fake_bot = fake_bot_yielder()()
        cog = Music(fake_bot, config, engine)
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
                with NamedTemporaryFile(dir=tmp_dir, suffix='.mp3', delete=False) as tmp_file2:
                    file_path2 = Path(tmp_file2.name)
                    file_path2.write_text('testing', encoding='utf-8')
                    s = SourceDict('123', 'foo bar authr', '234', 'https://foo.example', SearchType.DIRECT)
                    sd = SourceDownload(file_path, {'webpage_url': 'https://foo.example'}, s)
                    s2 = SourceDict('123', 'foo bar authr', '234', 'https://foo.example/bar', SearchType.DIRECT)
                    sd2 = SourceDownload(file_path2, {'webpage_url': 'https://foo.example/bar'}, s2)
                    cog.video_cache.iterate_file(sd)
                    cog.video_cache.iterate_file(sd2)
                    cog.video_cache.ready_remove()
                    cog.sources_in_transit[sd.source_dict.uuid] = str(sd.base_path)
                    await cog.cache_cleanup()
                    assert cog.video_cache.get_webpage_url_item(s)

@pytest.mark.asyncio
async def test_add_source_to_player_caches_video(mocker):
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
                'download': {
                    'cache': {
                        'enable_cache_files': True,
                    }
                }
            }
        }
        fake_bot = fake_bot_yielder()()
        cog = Music(fake_bot, config, engine)
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
                s = SourceDict('123', 'foo bar authr', '234', 'https://foo.example', SearchType.DIRECT)
                sd = SourceDownload(file_path, {'webpage_url': 'https://foo.example'}, s)
                await cog.add_source_to_player(sd, cog.players[fake_guild.id])
                assert cog.players[fake_guild.id].get_queue_items()
                assert cog.video_cache.get_webpage_url_item(s)

@pytest.mark.asyncio
async def test_add_source_to_player_caches_search(mocker):
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
                'download': {
                    'cache': {
                        'enable_cache_files': True,
                    }
                }
            }
        }
        fake_bot = fake_bot_yielder()()
        cog = Music(fake_bot, config, engine)
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
                s = SourceDict('123', 'foo bar author', '234', 'foo artist foo title', SearchType.SPOTIFY)
                sd = SourceDownload(file_path, {'webpage_url': 'https://foo.example'}, s)
                await cog.add_source_to_player(sd, cog.players[fake_guild.id])
                assert cog.players[fake_guild.id].get_queue_items()
                assert not cog.video_cache.get_webpage_url_item(s)
                assert cog.search_string_cache.check_cache(s)

@pytest.mark.asyncio
async def test_add_source_to_player_puts_blocked(mocker):
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
                'download': {
                    'cache': {
                        'enable_cache_files': True,
                    }
                }
            }
        }
        fake_bot = fake_bot_yielder()()
        cog = Music(fake_bot, config, engine)
        mocker.patch('discord_bot.cogs.music.sleep', return_value=True)
        mocker.patch.object(MusicPlayer, 'start_tasks')
        fake_channel = FakeChannel(members=[fake_bot.user])
        fake_voice = FakeVoiceClient(channel=fake_channel)
        fake_guild = FakeGuild(voice=fake_voice)
        await cog.get_player(fake_guild.id, ctx=FakeContext(fake_guild=fake_guild, fake_bot=fake_bot))
        cog.players[fake_guild.id]._play_queue.block() #pylint:disable=protected-access
        with TemporaryDirectory() as tmp_dir:
            with NamedTemporaryFile(dir=tmp_dir, suffix='.mp3', delete=False) as tmp_file:
                file_path = Path(tmp_file.name)
                file_path.write_text('testing', encoding='utf-8')
                s = SourceDict('123', 'foo bar author', '234', 'foo artist foo title', SearchType.SPOTIFY)
                sd = SourceDownload(file_path, {'webpage_url': 'https://foo.example'}, s)
                result = await cog.add_source_to_player(sd, cog.players[fake_guild.id])
                assert not result

@pytest.mark.asyncio
async def test_awaken(mocker):

    config = {
        'general': {
            'include': {
                'music': True
            },
        },
    }
    fake_bot = fake_bot_yielder()()
    cog = Music(fake_bot, config, None)
    mocker.patch('discord_bot.cogs.music.sleep', return_value=True)
    mocker.patch.object(MusicPlayer, 'start_tasks')
    fake_voice_channel = FakeChannel(id='fake-voice-123')
    fake_channel = FakeChannel(members=[fake_bot.user])
    fake_voice = FakeVoiceClient(channel=fake_voice_channel)
    fake_author = FakeAuthor(voice=fake_voice)
    fake_guild = FakeGuild(voice=fake_voice)
    fake_context = FakeContext(fake_bot=fake_bot, fake_guild=fake_guild, author=fake_author, channel=fake_channel)
    await cog.connect_(cog, fake_context)
    assert fake_guild.id in cog.players

@pytest.mark.asyncio
async def test_awaken_user_not_joined(mocker):

    config = {
        'general': {
            'include': {
                'music': True
            },
        },
    }
    fake_bot = fake_bot_yielder()()
    cog = Music(fake_bot, config, None)
    mocker.patch('discord_bot.cogs.music.sleep', return_value=True)
    mocker.patch.object(MusicPlayer, 'start_tasks')
    fake_voice_channel = FakeChannel(id='fake-voice-123')
    fake_channel = FakeChannel(members=[fake_bot.user])
    fake_voice = FakeVoiceClient(channel=fake_voice_channel)
    fake_author = FakeAuthor(voice=None)
    fake_guild = FakeGuild(voice=fake_voice)
    fake_context = FakeContext(fake_bot=fake_bot, fake_guild=fake_guild, author=fake_author, channel=fake_channel)
    await cog.connect_(cog, fake_context)
    assert fake_guild.id not in cog.players

def yield_search_client_check_source(source_dict_list: List[SourceDict]):
    class FakeSearchClient():
        def __init__(self, *_args, **_kwargs):
            pass

        async def check_source(self, *_args, **_kwargs):
            return source_dict_list

    return FakeSearchClient

def yield_search_client_check_source_raises():
    class FakeSearchClient():
        def __init__(self, *_args, **_kwargs):
            pass

        async def check_source(self, *_args, **_kwargs):
            raise SearchException('foo', user_message='woopsie')

    return FakeSearchClient

@pytest.mark.asyncio()
async def test_play_called_basic(mocker):
    config = {
        'general': {
            'include': {
                'music': True
            }
        },
    }
    fake_bot = fake_bot_yielder()()
    mocker.patch('discord_bot.cogs.music.sleep', return_value=True)
    mocker.patch.object(MusicPlayer, 'start_tasks')
    fake_voice_channel = FakeChannel(id='fake-voice-123')
    fake_channel = FakeChannel(members=[fake_bot.user])
    fake_voice = FakeVoiceClient(channel=fake_voice_channel)
    fake_author = FakeAuthor(voice=fake_voice)
    fake_guild = FakeGuild(voice=fake_voice)
    fake_context = FakeContext(fake_bot=fake_bot, fake_guild=fake_guild, author=fake_author, channel=fake_channel)
    s = SourceDict(fake_guild.id, 'foo bar author', '234', 'https://foo.example', SearchType.DIRECT)
    s1 = SourceDict(fake_guild.id, 'foo bar author', '234', 'https://foo.example', SearchType.DIRECT)
    mocker.patch('discord_bot.cogs.music.SearchClient', side_effect=yield_search_client_check_source([s, s1]))
    cog = Music(fake_bot, config, None)
    await cog.play_(cog, fake_context, search='foo bar')
    item0 = cog.download_queue.get_nowait()
    item1 = cog.download_queue.get_nowait()
    assert item0 == s
    assert item1 == s1

@pytest.mark.asyncio()
async def test_skip(mocker):
    config = {
        'general': {
            'include': {
                'music': True
            }
        },
    }
    fake_bot = fake_bot_yielder()()
    mocker.patch('discord_bot.cogs.music.sleep', return_value=True)
    mocker.patch.object(MusicPlayer, 'start_tasks')
    with TemporaryDirectory() as tmp_dir:
        with NamedTemporaryFile(dir=tmp_dir, suffix='.mp3', delete=False) as tmp_file:
            file_path = Path(tmp_file.name)
            file_path.write_text('testing', encoding='utf-8')
            mocker.patch('discord_bot.cogs.music.sleep', return_value=True)
            mocker.patch.object(MusicPlayer, 'start_tasks')
            fake_voice_channel = FakeChannel(id='fake-voice-123')
            fake_channel = FakeChannel(members=[fake_bot.user])
            fake_voice = FakeVoiceClient(channel=fake_voice_channel)
            fake_author = FakeAuthor(voice=fake_voice)
            fake_guild = FakeGuild(voice=fake_voice)
            fake_context = FakeContext(fake_bot=fake_bot, fake_guild=fake_guild, author=fake_author, channel=fake_channel)
            s = SourceDict(fake_guild.id, 'foo bar authr', '234', 'https://foo.example', SearchType.DIRECT)
            sd = SourceDownload(file_path, {'webpage_url': 'https://foo.example', 'title': 'foo bar song'}, s)
            mocker.patch('discord_bot.cogs.music.DownloadClient', side_effect=yield_fake_download_client(sd))
            mocker.patch('discord_bot.cogs.music.SearchClient', side_effect=yield_fake_search_client(s))
            cog = Music(fake_bot, config, None)
            await cog.play_(cog, fake_context, search='foo bar')
            await cog.download_files()
            # Mock current playing
            cog.players[fake_guild.id].current_source = sd
            await cog.skip_(cog, fake_context)
            assert cog.players[fake_guild.id].video_skipped

@pytest.mark.asyncio()
async def test_clear(mocker):
    config = {
        'general': {
            'include': {
                'music': True
            }
        },
    }
    fake_bot = fake_bot_yielder()()
    mocker.patch('discord_bot.cogs.music.sleep', return_value=True)
    mocker.patch.object(MusicPlayer, 'start_tasks')
    with TemporaryDirectory() as tmp_dir:
        with NamedTemporaryFile(dir=tmp_dir, suffix='.mp3', delete=False) as tmp_file:
            file_path = Path(tmp_file.name)
            file_path.write_text('testing', encoding='utf-8')
            mocker.patch('discord_bot.cogs.music.sleep', return_value=True)
            mocker.patch.object(MusicPlayer, 'start_tasks')
            fake_voice_channel = FakeChannel(id='fake-voice-123')
            fake_channel = FakeChannel(members=[fake_bot.user])
            fake_voice = FakeVoiceClient(channel=fake_voice_channel)
            fake_author = FakeAuthor(voice=fake_voice)
            fake_guild = FakeGuild(voice=fake_voice)
            fake_context = FakeContext(fake_bot=fake_bot, fake_guild=fake_guild, author=fake_author, channel=fake_channel)
            s = SourceDict(fake_guild.id, 'foo bar authr', '234', 'https://foo.example', SearchType.DIRECT)
            sd = SourceDownload(file_path, {'webpage_url': 'https://foo.example', 'title': 'foo bar song'}, s)
            mocker.patch('discord_bot.cogs.music.DownloadClient', side_effect=yield_fake_download_client(sd))
            mocker.patch('discord_bot.cogs.music.SearchClient', side_effect=yield_fake_search_client(s))
            cog = Music(fake_bot, config, None)
            await cog.play_(cog, fake_context, search='foo bar')
            await cog.download_files()
            await cog.clear(cog, fake_context)
            assert not cog.players[fake_guild.id].get_queue_items()

@pytest.mark.asyncio()
async def test_history(mocker):
    config = {
        'general': {
            'include': {
                'music': True
            }
        },
    }
    fake_bot = fake_bot_yielder()()
    mocker.patch('discord_bot.cogs.music.sleep', return_value=True)
    mocker.patch.object(MusicPlayer, 'start_tasks')
    with TemporaryDirectory() as tmp_dir:
        with NamedTemporaryFile(dir=tmp_dir, suffix='.mp3', delete=False) as tmp_file:
            file_path = Path(tmp_file.name)
            file_path.write_text('testing', encoding='utf-8')
            mocker.patch('discord_bot.cogs.music.sleep', return_value=True)
            mocker.patch.object(MusicPlayer, 'start_tasks')
            fake_voice_channel = FakeChannel(id='fake-voice-123')
            fake_channel = FakeChannel(members=[fake_bot.user])
            fake_voice = FakeVoiceClient(channel=fake_voice_channel)
            fake_author = FakeAuthor(voice=fake_voice)
            fake_guild = FakeGuild(voice=fake_voice)
            fake_context = FakeContext(fake_bot=fake_bot, fake_guild=fake_guild, author=fake_author, channel=fake_channel)
            s = SourceDict(fake_guild.id, 'foo bar authr', '234', 'https://foo.example', SearchType.DIRECT)
            sd = SourceDownload(file_path, {'webpage_url': 'https://foo.example', 'title': 'foo bar song', 'uploader': 'foo bar artist'}, s)
            cog = Music(fake_bot, config, None)
            await cog.get_player(fake_guild.id, ctx=FakeContext(fake_guild=fake_guild, fake_bot=fake_bot))
            cog.players[fake_guild.id]._history.put_nowait(sd) #pylint:disable=protected-access
            await cog.history_(cog, fake_context)
            m0 = cog.message_queue.get_next_message()
            assert m0[1][0].args[0] == '```Pos|| Title /// Uploader\n--------------------------------------------------------------------------------------\n1  || foo bar song /// foo bar artist```'

@pytest.mark.asyncio()
async def test_shuffle(mocker):
    config = {
        'general': {
            'include': {
                'music': True
            }
        },
    }
    fake_bot = fake_bot_yielder()()
    mocker.patch('discord_bot.cogs.music.sleep', return_value=True)
    mocker.patch.object(MusicPlayer, 'start_tasks')
    with TemporaryDirectory() as tmp_dir:
        with NamedTemporaryFile(dir=tmp_dir, suffix='.mp3', delete=False) as tmp_file:
            file_path = Path(tmp_file.name)
            file_path.write_text('testing', encoding='utf-8')
            mocker.patch('discord_bot.cogs.music.sleep', return_value=True)
            mocker.patch.object(MusicPlayer, 'start_tasks')
            fake_voice_channel = FakeChannel(id='fake-voice-123')
            fake_channel = FakeChannel(members=[fake_bot.user])
            fake_voice = FakeVoiceClient(channel=fake_voice_channel)
            fake_author = FakeAuthor(voice=fake_voice)
            fake_guild = FakeGuild(voice=fake_voice)
            fake_context = FakeContext(fake_bot=fake_bot, fake_guild=fake_guild, author=fake_author, channel=fake_channel)
            s = SourceDict(fake_guild.id, 'foo bar authr', '234', 'https://foo.example', SearchType.DIRECT)
            sd = SourceDownload(file_path, {'webpage_url': 'https://foo.example', 'title': 'foo bar song'}, s)
            mocker.patch('discord_bot.cogs.music.DownloadClient', side_effect=yield_fake_download_client(sd))
            mocker.patch('discord_bot.cogs.music.SearchClient', side_effect=yield_fake_search_client(s))
            cog = Music(fake_bot, config, None)
            await cog.play_(cog, fake_context, search='foo bar')
            await cog.download_files()
            await cog.shuffle_(cog, fake_context)
            assert cog.players[fake_guild.id].get_queue_items()

@pytest.mark.asyncio()
async def test_remove_item(mocker):
    config = {
        'general': {
            'include': {
                'music': True
            }
        },
    }
    fake_bot = fake_bot_yielder()()
    mocker.patch('discord_bot.cogs.music.sleep', return_value=True)
    mocker.patch.object(MusicPlayer, 'start_tasks')
    with TemporaryDirectory() as tmp_dir:
        with NamedTemporaryFile(dir=tmp_dir, suffix='.mp3', delete=False) as tmp_file:
            file_path = Path(tmp_file.name)
            file_path.write_text('testing', encoding='utf-8')
            mocker.patch('discord_bot.cogs.music.sleep', return_value=True)
            mocker.patch.object(MusicPlayer, 'start_tasks')
            fake_voice_channel = FakeChannel(id='fake-voice-123')
            fake_channel = FakeChannel(members=[fake_bot.user])
            fake_voice = FakeVoiceClient(channel=fake_voice_channel)
            fake_author = FakeAuthor(voice=fake_voice)
            fake_guild = FakeGuild(voice=fake_voice)
            fake_context = FakeContext(fake_bot=fake_bot, fake_guild=fake_guild, author=fake_author, channel=fake_channel)
            s = SourceDict(fake_guild.id, 'foo bar authr', '234', 'https://foo.example', SearchType.DIRECT)
            sd = SourceDownload(file_path, {'webpage_url': 'https://foo.example', 'title': 'foo bar song'}, s)
            mocker.patch('discord_bot.cogs.music.DownloadClient', side_effect=yield_fake_download_client(sd))
            mocker.patch('discord_bot.cogs.music.SearchClient', side_effect=yield_fake_search_client(s))
            cog = Music(fake_bot, config, None)
            await cog.play_(cog, fake_context, search='foo bar')
            await cog.download_files()
            await cog.remove_item(cog, fake_context, 1)
            assert not cog.players[fake_guild.id].get_queue_items()

@pytest.mark.asyncio()
async def test_bump_item(mocker):
    config = {
        'general': {
            'include': {
                'music': True
            }
        },
    }
    fake_bot = fake_bot_yielder()()
    mocker.patch('discord_bot.cogs.music.sleep', return_value=True)
    mocker.patch.object(MusicPlayer, 'start_tasks')
    with TemporaryDirectory() as tmp_dir:
        with NamedTemporaryFile(dir=tmp_dir, suffix='.mp3', delete=False) as tmp_file:
            file_path = Path(tmp_file.name)
            file_path.write_text('testing', encoding='utf-8')
            mocker.patch('discord_bot.cogs.music.sleep', return_value=True)
            mocker.patch.object(MusicPlayer, 'start_tasks')
            fake_voice_channel = FakeChannel(id='fake-voice-123')
            fake_channel = FakeChannel(members=[fake_bot.user])
            fake_voice = FakeVoiceClient(channel=fake_voice_channel)
            fake_author = FakeAuthor(voice=fake_voice)
            fake_guild = FakeGuild(voice=fake_voice)
            fake_context = FakeContext(fake_bot=fake_bot, fake_guild=fake_guild, author=fake_author, channel=fake_channel)
            s = SourceDict(fake_guild.id, 'foo bar authr', '234', 'https://foo.example', SearchType.DIRECT)
            sd = SourceDownload(file_path, {'webpage_url': 'https://foo.example', 'title': 'foo bar song'}, s)
            mocker.patch('discord_bot.cogs.music.DownloadClient', side_effect=yield_fake_download_client(sd))
            mocker.patch('discord_bot.cogs.music.SearchClient', side_effect=yield_fake_search_client(s))
            cog = Music(fake_bot, config, None)
            await cog.play_(cog, fake_context, search='foo bar')
            await cog.download_files()
            await cog.bump_item(cog, fake_context, 1)
            assert cog.players[fake_guild.id].get_queue_items()

@pytest.mark.asyncio
async def test_stop(mocker):
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
        cog = Music(fake_bot, config, engine)
        mocker.patch('discord_bot.cogs.music.sleep', return_value=True)
        mocker.patch.object(MusicPlayer, 'start_tasks')
        fake_voice_channel = FakeChannel(id='fake-voice-123')
        fake_channel = FakeChannel(members=[fake_bot.user])
        fake_voice = FakeVoiceClient(channel=fake_voice_channel)
        fake_author = FakeAuthor(voice=fake_voice)
        fake_guild = FakeGuild(voice=fake_voice)
        fake_context = FakeContext(fake_bot=fake_bot, fake_guild=fake_guild, author=fake_author, channel=fake_channel)
        await cog.get_player(fake_guild.id, ctx=FakeContext(fake_guild=fake_guild, fake_bot=fake_bot))
        with TemporaryDirectory() as tmp_dir:
            with NamedTemporaryFile(dir=tmp_dir, suffix='.mp3', delete=False) as tmp_file:
                file_path = Path(tmp_file.name)
                file_path.write_text('testing', encoding='utf-8')
                s = SourceDict('123', 'foo bar authr', '234', 'https://foo.example', SearchType.DIRECT)
                sd = SourceDownload(file_path, {'webpage_url': 'https://foo.example'}, s)
                await cog.players[fake_guild.id]._history.put(sd) #pylint:disable=protected-access
                await cog.stop_(cog, fake_context)
                assert fake_guild.id not in cog.players
                assert fake_guild.id not in cog.download_queue.queues

@pytest.mark.asyncio()
async def test_move_messages(mocker):
    config = {
        'general': {
            'include': {
                'music': True
            }
        },
    }
    fake_bot = fake_bot_yielder()()
    mocker.patch('discord_bot.cogs.music.sleep', return_value=True)
    mocker.patch.object(MusicPlayer, 'start_tasks')
    with TemporaryDirectory() as tmp_dir:
        with NamedTemporaryFile(dir=tmp_dir, suffix='.mp3', delete=False) as tmp_file:
            file_path = Path(tmp_file.name)
            file_path.write_text('testing', encoding='utf-8')
            mocker.patch('discord_bot.cogs.music.sleep', return_value=True)
            mocker.patch.object(MusicPlayer, 'start_tasks')
            fake_voice_channel = FakeChannel(id='fake-voice-123')
            fake_channel = FakeChannel(members=[fake_bot.user])
            fake_channel2 = FakeChannel(id='fakechannel2')
            fake_voice = FakeVoiceClient(channel=fake_voice_channel)
            fake_author = FakeAuthor(voice=fake_voice)
            fake_guild = FakeGuild(voice=fake_voice)
            fake_context = FakeContext(fake_bot=fake_bot, fake_guild=fake_guild, author=fake_author, channel=fake_channel)
            fake_context2 = FakeContext(fake_bot=fake_bot, fake_guild=fake_guild, author=fake_author, channel=fake_channel2)
            s = SourceDict(fake_guild.id, 'foo bar authr', '234', 'https://foo.example', SearchType.DIRECT)
            sd = SourceDownload(file_path, {'webpage_url': 'https://foo.example', 'title': 'foo bar song'}, s)
            mocker.patch('discord_bot.cogs.music.DownloadClient', side_effect=yield_fake_download_client(sd))
            mocker.patch('discord_bot.cogs.music.SearchClient', side_effect=yield_fake_search_client(s))
            cog = Music(fake_bot, config, None)
            await cog.play_(cog, fake_context, search='foo bar')
            await cog.download_files()
            await cog.move_messages_here(cog, fake_context2)
            assert cog.players[fake_guild.id].text_channel.id == 'fakechannel2'

@pytest.mark.asyncio()
async def test_play_called_downloads_blocked(mocker):
    config = {
        'general': {
            'include': {
                'music': True
            }
        },
    }
    fake_bot = fake_bot_yielder()()
    mocker.patch('discord_bot.cogs.music.sleep', return_value=True)
    mocker.patch.object(MusicPlayer, 'start_tasks')
    fake_voice_channel = FakeChannel(id='fake-voice-123')
    fake_channel = FakeChannel(members=[fake_bot.user])
    fake_voice = FakeVoiceClient(channel=fake_voice_channel)
    fake_author = FakeAuthor(voice=fake_voice)
    fake_guild = FakeGuild(voice=fake_voice)
    fake_context = FakeContext(fake_bot=fake_bot, fake_guild=fake_guild, author=fake_author, channel=fake_channel)
    s = SourceDict(fake_guild.id, 'foo bar authr', '234', 'https://foo.example', SearchType.DIRECT)
    s1 = SourceDict(fake_guild.id, 'foo bar authr', '234', 'https://foo.example', SearchType.DIRECT)
    mocker.patch('discord_bot.cogs.music.SearchClient', side_effect=yield_search_client_check_source([s, s1]))
    cog = Music(fake_bot, config, None)
    # Put source dict so we can a download queue to block
    cog.download_queue.put_nowait(fake_guild.id, s)
    cog.download_queue.block(fake_guild.id)
    await cog.play_(cog, fake_context, search='foo bar')

@pytest.mark.asyncio()
async def test_play_hits_max_items(mocker):
    config = {
        'general': {
            'include': {
                'music': True
            }
        },
        'music': {
            'player': {
                'queue_max_size': 1,
            }
        }
    }
    fake_bot = fake_bot_yielder()()
    mocker.patch('discord_bot.cogs.music.sleep', return_value=True)
    mocker.patch.object(MusicPlayer, 'start_tasks')
    fake_voice_channel = FakeChannel(id='fake-voice-123')
    fake_channel = FakeChannel(members=[fake_bot.user])
    fake_voice = FakeVoiceClient(channel=fake_voice_channel)
    fake_author = FakeAuthor(voice=fake_voice)
    fake_guild = FakeGuild(voice=fake_voice)
    fake_context = FakeContext(fake_bot=fake_bot, fake_guild=fake_guild, author=fake_author, channel=fake_channel)
    s = SourceDict(fake_guild.id, 'foo bar authr', '234', 'https://foo.example', SearchType.DIRECT)
    s1 = SourceDict(fake_guild.id, 'foo bar authr', '234', 'https://foo.example', SearchType.DIRECT)
    mocker.patch('discord_bot.cogs.music.SearchClient', side_effect=yield_search_client_check_source([s, s1]))
    cog = Music(fake_bot, config, None)
    await cog.play_(cog, fake_context, search='foo bar')
    cog.message_queue.get_next_message()
    m1 = cog.message_queue.get_next_message()
    assert m1[1].source_dict == s1
    assert m1[1].message_content == 'Unable to add "<https://foo.example>" to queue, download queue is full'

@pytest.mark.asyncio()
async def test_play_called_raises_exception(mocker):
    config = {
        'general': {
            'include': {
                'music': True
            }
        },
    }
    fake_bot = fake_bot_yielder()()
    mocker.patch('discord_bot.cogs.music.sleep', return_value=True)
    mocker.patch.object(MusicPlayer, 'start_tasks')
    fake_voice_channel = FakeChannel(id='fake-voice-123')
    fake_channel = FakeChannel(members=[fake_bot.user])
    fake_voice = FakeVoiceClient(channel=fake_voice_channel)
    fake_author = FakeAuthor(voice=fake_voice)
    fake_guild = FakeGuild(voice=fake_voice)
    fake_context = FakeContext(fake_bot=fake_bot, fake_guild=fake_guild, author=fake_author, channel=fake_channel)
    mocker.patch('discord_bot.cogs.music.SearchClient', side_effect=yield_search_client_check_source_raises())
    cog = Music(fake_bot, config, None)
    await cog.play_(cog, fake_context, search='foo bar')
    m0 = cog.message_queue.get_next_message()
    assert m0[1][0].args[0] == 'woopsie'

@pytest.mark.asyncio()
async def test_play_called_basic_hits_cache(mocker):
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
            'music': {
                'download': {
                    'cache': {
                        'enable_cache_files': True,
                    }
                }
            }
        }
        fake_bot = fake_bot_yielder()()
        with TemporaryDirectory() as tmp_dir:
            with NamedTemporaryFile(dir=tmp_dir, suffix='.mp3', delete=False) as tmp_file:
                file_path = Path(tmp_file.name)
                file_path.write_text('testing', encoding='utf-8')
                mocker.patch('discord_bot.cogs.music.sleep', return_value=True)
                mocker.patch.object(MusicPlayer, 'start_tasks')
                fake_voice_channel = FakeChannel(id='fake-voice-123')
                fake_channel = FakeChannel(members=[fake_bot.user])
                fake_voice = FakeVoiceClient(channel=fake_voice_channel)
                fake_author = FakeAuthor(voice=fake_voice)
                fake_guild = FakeGuild(voice=fake_voice)
                fake_context = FakeContext(fake_bot=fake_bot, fake_guild=fake_guild, author=fake_author, channel=fake_channel)
                s = SourceDict(fake_guild.id, 'foo bar authr', '234', 'https://foo.example', SearchType.DIRECT)
                sd = SourceDownload(file_path, {'webpage_url': 'https://foo.example'}, s)
                mocker.patch('discord_bot.cogs.music.SearchClient', side_effect=yield_search_client_check_source([s]))
                cog = Music(fake_bot, config, engine)
                cog.video_cache.iterate_file(sd)
                await cog.play_(cog, fake_context, search='foo bar')
                assert cog.players[fake_guild.id].get_queue_items()

@pytest.mark.asyncio
async def test_create_playlist(mocker):
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
        }
        fake_bot = fake_bot_yielder()()
        cog = Music(fake_bot, config, engine)
        mocker.patch('discord_bot.cogs.music.sleep', return_value=True)
        await cog.playlist_create(cog, FakeContext(), name='new-playlist')
        with mock_session(engine) as db_session:
            assert db_session.query(Playlist).count()

@pytest.mark.asyncio
async def test_create_playlist_invalid_name(mocker):
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
        }
        fake_bot = fake_bot_yielder()()
        cog = Music(fake_bot, config, engine)
        mocker.patch('discord_bot.cogs.music.sleep', return_value=True)
        await cog.playlist_create(cog, FakeContext(), name='__playhistory__derp')
        with mock_session(engine) as db_session:
            assert not db_session.query(Playlist).count()

@pytest.mark.asyncio
async def test_create_playlist_same_name_twice(mocker):
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
        }
        fake_bot = fake_bot_yielder()()
        cog = Music(fake_bot, config, engine)
        mocker.patch('discord_bot.cogs.music.sleep', return_value=True)
        await cog.playlist_create(cog, FakeContext(), name='new-playlist')
        await cog.playlist_create(cog, FakeContext(), name='new-playlist')
        with mock_session(engine) as db_session:
            assert db_session.query(Playlist).count() == 1

@pytest.mark.asyncio
async def test_list_playlist(mocker):
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
        }
        fake_bot = fake_bot_yielder()()
        cog = Music(fake_bot, config, engine)
        mocker.patch('discord_bot.cogs.music.sleep', return_value=True)
        fake_guild = FakeGuild()
        await cog.playlist_create(cog, FakeContext(fake_guild=fake_guild), name='new-playlist')
        await cog.playlist_list(cog, FakeContext(fake_guild=fake_guild))

        _result0 = cog.message_queue.get_single_message()
        result1 = cog.message_queue.get_single_message()
        assert result1[0].args[0] == '```ID || Playlist Name                                                   || Last Queued\n---------------------------------------------------------------------------------------------\n1  || new-playlist                                                    || N/A```'


@pytest.mark.asyncio()
async def test_playlsit_add_item_function(mocker):

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
        s = SourceDict('123', 'foo bar authr', '234', 'https://foo.example', SearchType.DIRECT, download_file=False)
        sd = SourceDownload(None, {'webpage_url': 'https://foo.example'}, s)
        mocker.patch('discord_bot.cogs.music.DownloadClient', side_effect=yield_fake_download_client(sd))
        mocker.patch('discord_bot.cogs.music.SearchClient', side_effect=yield_fake_search_client(s))
        cog = Music(fake_bot, config, engine)
        mocker.patch('discord_bot.cogs.music.sleep', return_value=True)
        mocker.patch.object(MusicPlayer, 'start_tasks')
        fake_channel = FakeChannel(members=[fake_bot.user])
        fake_voice = FakeVoiceClient(channel=fake_channel)
        fake_guild = FakeGuild(voice=fake_voice)
        await cog.playlist_create(cog, FakeContext(fake_guild=fake_guild), name='new-playlist')
        await cog.playlist_item_add(cog, FakeContext(fake_guild=fake_guild), 1, search='https://foo.example')
        await cog.download_files()
        with mock_session(engine) as db_session:
            assert db_session.query(PlaylistItem).count() == 1

@pytest.mark.asyncio()
async def test_playlist_remove_item(mocker):

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
        s = SourceDict('123', 'foo bar authr', '234', 'https://foo.example', SearchType.DIRECT, download_file=False)
        sd = SourceDownload(None, {'webpage_url': 'https://foo.example'}, s)
        mocker.patch('discord_bot.cogs.music.DownloadClient', side_effect=yield_fake_download_client(sd))
        mocker.patch('discord_bot.cogs.music.SearchClient', side_effect=yield_fake_search_client(s))
        cog = Music(fake_bot, config, engine)
        mocker.patch('discord_bot.cogs.music.sleep', return_value=True)
        mocker.patch.object(MusicPlayer, 'start_tasks')
        fake_channel = FakeChannel(members=[fake_bot.user])
        fake_voice = FakeVoiceClient(channel=fake_channel)
        fake_guild = FakeGuild(voice=fake_voice)
        await cog.playlist_create(cog, FakeContext(fake_guild=fake_guild), name='new-playlist')
        await cog.playlist_item_add(cog, FakeContext(fake_guild=fake_guild), 1, search='https://foo.example')
        await cog.download_files()
        await cog.playlist_item_remove(cog, FakeContext(fake_guild=fake_guild), 1, 1)
        with mock_session(engine) as db_session:
            assert db_session.query(PlaylistItem).count() == 0

@pytest.mark.asyncio()
async def test_playlist_show(mocker):

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
        s = SourceDict('123', 'foo bar authr', '234', 'https://foo.example', SearchType.DIRECT, download_file=False)
        sd = SourceDownload(None, {'webpage_url': 'https://foo.example', 'title': 'foo', 'uploader': 'foobar'}, s)
        mocker.patch('discord_bot.cogs.music.DownloadClient', side_effect=yield_fake_download_client(sd))
        mocker.patch('discord_bot.cogs.music.SearchClient', side_effect=yield_fake_search_client(s))
        cog = Music(fake_bot, config, engine)
        mocker.patch('discord_bot.cogs.music.sleep', return_value=True)
        mocker.patch.object(MusicPlayer, 'start_tasks')
        fake_channel = FakeChannel(members=[fake_bot.user])
        fake_voice = FakeVoiceClient(channel=fake_channel)
        fake_guild = FakeGuild(voice=fake_voice)
        await cog.playlist_create(cog, FakeContext(fake_guild=fake_guild), name='new-playlist')
        await cog.playlist_item_add(cog, FakeContext(fake_guild=fake_guild), 1, search='https://foo.example')
        await cog.download_files()

        await cog.playlist_show(cog, FakeContext(fake_guild=fake_guild), 1)
        cog.message_queue.get_next_message()
        cog.message_queue.get_next_message()
        m2 = cog.message_queue.get_next_message()
        assert m2[1][0].args[0] == '```Pos|| Title /// Uploader\n----------------------------------------------------------------------\n1  || foo /// foobar```'

@pytest.mark.asyncio()
async def test_playlist_delete(mocker):

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
        s = SourceDict('123', 'foo bar authr', '234', 'https://foo.example', SearchType.DIRECT, download_file=False)
        sd = SourceDownload(None, {'webpage_url': 'https://foo.example', 'title': 'foo', 'uploader': 'foobar'}, s)
        mocker.patch('discord_bot.cogs.music.DownloadClient', side_effect=yield_fake_download_client(sd))
        mocker.patch('discord_bot.cogs.music.SearchClient', side_effect=yield_fake_search_client(s))
        cog = Music(fake_bot, config, engine)
        mocker.patch('discord_bot.cogs.music.sleep', return_value=True)
        mocker.patch.object(MusicPlayer, 'start_tasks')
        fake_channel = FakeChannel(members=[fake_bot.user])
        fake_voice = FakeVoiceClient(channel=fake_channel)
        fake_guild = FakeGuild(voice=fake_voice)
        await cog.playlist_create(cog, FakeContext(fake_guild=fake_guild), name='new-playlist')
        await cog.playlist_item_add(cog, FakeContext(fake_guild=fake_guild), 1, search='https://foo.example')
        await cog.download_files()

        await cog.playlist_delete(cog, FakeContext(fake_guild=fake_guild), 1)
        with mock_session(engine) as db_session:
            assert db_session.query(PlaylistItem).count() == 0
            assert db_session.query(Playlist).count() == 0

@pytest.mark.asyncio
async def test_playlist_rename(mocker):
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
        }
        fake_bot = fake_bot_yielder()()
        cog = Music(fake_bot, config, engine)
        mocker.patch('discord_bot.cogs.music.sleep', return_value=True)
        fake_guild = FakeGuild()
        await cog.playlist_create(cog, FakeContext(fake_guild=fake_guild), name='new-playlist')
        await cog.playlist_rename(cog, FakeContext(fake_guild=fake_guild), 1, playlist_name='foo-bar-playlist')
        with mock_session(engine) as db_session:
            assert db_session.query(Playlist).count() == 1
            item = db_session.query(Playlist).first()
            assert item.name == 'foo-bar-playlist'

@pytest.mark.asyncio
async def test_history_save(mocker):
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
        cog = Music(fake_bot, config, engine)
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
                s = SourceDict(fake_guild.id, 'foo bar authr', '234', 'https://foo.example', SearchType.DIRECT)
                sd = SourceDownload(file_path, {'webpage_url': 'https://foo.example'}, s)
                await cog.players[fake_guild.id]._history.put(sd) #pylint:disable=protected-access

                await cog.playlist_history_save(cog, FakeContext(fake_guild=fake_guild), name='foobar')
                with mock_session(engine) as db_session:
                    # 2 since history playlist will have been created
                    assert db_session.query(Playlist).count() == 2
                    assert db_session.query(PlaylistItem).count() == 1

@pytest.mark.asyncio
async def test_queue_save(mocker):
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
        cog = Music(fake_bot, config, engine)
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
                s = SourceDict('123', 'foo bar authr', '234', 'https://foo.example', SearchType.DIRECT)
                sd = SourceDownload(file_path, {'webpage_url': 'https://foo.example'}, s)
                await cog.players[fake_guild.id]._play_queue.put(sd) #pylint:disable=protected-access

                await cog.playlist_queue_save(cog, FakeContext(fake_guild=fake_guild), name='foobar')
                with mock_session(engine) as db_session:
                    # 2 since history playlist will have been created
                    assert db_session.query(Playlist).count() == 2
                    assert db_session.query(PlaylistItem).count() == 1



@pytest.mark.asyncio()
async def test_play_queue(mocker):
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
        mocker.patch('discord_bot.cogs.music.sleep', return_value=True)
        mocker.patch.object(MusicPlayer, 'start_tasks')
        fake_voice_channel = FakeChannel(id='fake-voice-123')
        fake_channel = FakeChannel(members=[fake_bot.user])
        fake_voice = FakeVoiceClient(channel=fake_voice_channel)
        fake_author = FakeAuthor(voice=fake_voice)
        fake_guild = FakeGuild(voice=fake_voice)
        fake_context = FakeContext(fake_bot=fake_bot, fake_guild=fake_guild, author=fake_author, channel=fake_channel)
        s = SourceDict('123', 'foo bar authr', '234', 'https://foo.example', SearchType.DIRECT, download_file=False)
        sd = SourceDownload(None, {'webpage_url': 'https://foo.example', 'title': 'foo', 'uploader': 'foobar'}, s)
        mocker.patch('discord_bot.cogs.music.DownloadClient', side_effect=yield_fake_download_client(sd))
        mocker.patch('discord_bot.cogs.music.SearchClient', side_effect=yield_fake_search_client(s))
        cog = Music(fake_bot, config, engine)
        await cog.playlist_create(cog, FakeContext(fake_guild=fake_guild), name='new-playlist')
        await cog.playlist_item_add(cog, FakeContext(fake_guild=fake_guild), 1, search='https://foo.example')
        await cog.download_files()

        await cog.playlist_queue(cog, fake_context, 1)
        assert cog.download_queue.queues[fake_guild.id]


@pytest.mark.asyncio
async def test_random_play(mocker):
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
        cog = Music(fake_bot, config, engine)
        mocker.patch('discord_bot.cogs.music.sleep', return_value=True)
        mocker.patch.object(MusicPlayer, 'start_tasks')
        fake_voice_channel = FakeChannel(id='fake-voice-123')
        fake_channel = FakeChannel(members=[fake_bot.user])
        fake_voice = FakeVoiceClient(channel=fake_voice_channel)
        fake_author = FakeAuthor(voice=fake_voice)
        fake_guild = FakeGuild(voice=fake_voice)
        fake_context = FakeContext(fake_bot=fake_bot, fake_guild=fake_guild, author=fake_author, channel=fake_channel)
        with TemporaryDirectory() as tmp_dir:
            with NamedTemporaryFile(dir=tmp_dir, suffix='.mp3', delete=False) as tmp_file:
                file_path = Path(tmp_file.name)
                file_path.write_text('testing', encoding='utf-8')
                s = SourceDict(fake_guild.id, 'foo bar authr', '234', 'https://foo.example', SearchType.DIRECT)
                sd = SourceDownload(file_path, {'webpage_url': 'https://foo.example'}, s)
                cog = Music(fake_bot, config, engine)
                await cog.get_player(fake_guild.id, ctx=FakeContext(fake_guild=fake_guild, fake_bot=fake_bot))
                cog.history_playlist_queue.put_nowait(HistoryPlaylistItem(cog.players[fake_guild.id].history_playlist_id, sd))
                await cog.playlist_history_update()

                await cog.playlist_random_play(cog, fake_context)
                assert cog.download_queue.queues[fake_guild.id]

@pytest.mark.asyncio
async def test_random_play_deletes_no_existent_video(mocker):
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
        mocker.patch('discord_bot.cogs.music.sleep', return_value=True)
        mocker.patch.object(MusicPlayer, 'start_tasks')
        fake_voice_channel = FakeChannel(id='fake-voice-123')
        fake_channel = FakeChannel(members=[fake_bot.user])
        fake_voice = FakeVoiceClient(channel=fake_voice_channel)
        fake_author = FakeAuthor(voice=fake_voice)
        fake_guild = FakeGuild(voice=fake_voice)
        fake_context = FakeContext(fake_bot=fake_bot, fake_guild=fake_guild, author=fake_author, channel=fake_channel)
        with TemporaryDirectory() as tmp_dir:
            with NamedTemporaryFile(dir=tmp_dir, suffix='.mp3', delete=False) as tmp_file:
                file_path = Path(tmp_file.name)
                file_path.write_text('testing', encoding='utf-8')
                s = SourceDict('123', 'foo bar authr', '234', 'https://foo.example', SearchType.DIRECT)
                sd = SourceDownload(file_path, {'webpage_url': 'https://foo.example'}, s)
                mocker.patch('discord_bot.cogs.music.DownloadClient', side_effect=yield_download_client_download_exception())
                cog = Music(fake_bot, config, engine)
                await cog.get_player(fake_guild.id, ctx=FakeContext(fake_guild=fake_guild, fake_bot=fake_bot))
                cog.history_playlist_queue.put_nowait(HistoryPlaylistItem(cog.players[fake_guild.id].history_playlist_id, sd))
                await cog.playlist_history_update()

                await cog.playlist_random_play(cog, fake_context)
                await cog.download_files()
                with mock_session(engine) as db_session:
                    assert db_session.query(Playlist).count() == 1
                    assert db_session.query(PlaylistItem).count() == 0

@pytest.mark.asyncio()
async def test_random_play_cache(mocker):
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
            'music': {
                'download': {
                    'cache': {
                        'enable_cache_files': True,
                    }
                }
            }
        }
        fake_bot = fake_bot_yielder()()
        with TemporaryDirectory() as tmp_dir:
            with NamedTemporaryFile(dir=tmp_dir, suffix='.mp3', delete=False) as tmp_file:
                file_path = Path(tmp_file.name)
                file_path.write_text('testing', encoding='utf-8')
                mocker.patch('discord_bot.cogs.music.sleep', return_value=True)
                mocker.patch.object(MusicPlayer, 'start_tasks')
                fake_voice_channel = FakeChannel(id='fake-voice-123')
                fake_channel = FakeChannel(members=[fake_bot.user])
                fake_voice = FakeVoiceClient(channel=fake_voice_channel)
                fake_author = FakeAuthor(voice=fake_voice)
                fake_guild = FakeGuild(voice=fake_voice)
                fake_context = FakeContext(fake_bot=fake_bot, fake_guild=fake_guild, author=fake_author, channel=fake_channel)
                s = SourceDict(fake_guild.id, 'foo bar authr', '234', 'https://foo.example', SearchType.DIRECT)
                sd = SourceDownload(file_path, {'webpage_url': 'https://foo.example'}, s)
                cog = Music(fake_bot, config, engine)
                cog.video_cache.iterate_file(sd)

                await cog.get_player(fake_guild.id, ctx=FakeContext(fake_guild=fake_guild, fake_bot=fake_bot))
                await cog.playlist_random_play(cog, fake_context, 'cache')
                assert cog.players[fake_guild.id].get_queue_items()

@pytest.mark.asyncio()
async def test_playlist_merge(mocker):

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
        s = SourceDict('123', 'foo bar authr', '234', 'https://foo.example', SearchType.DIRECT, download_file=False)
        sd = SourceDownload(None, {'webpage_url': 'https://foo.example', 'title': 'foo', 'uploader': 'foobar'}, s)
        mocker.patch('discord_bot.cogs.music.DownloadClient', side_effect=yield_fake_download_client(sd))
        mocker.patch('discord_bot.cogs.music.SearchClient', side_effect=yield_fake_search_client(s))
        cog = Music(fake_bot, config, engine)
        mocker.patch('discord_bot.cogs.music.sleep', return_value=True)
        mocker.patch.object(MusicPlayer, 'start_tasks')
        fake_channel = FakeChannel(members=[fake_bot.user])
        fake_voice = FakeVoiceClient(channel=fake_channel)
        fake_guild = FakeGuild(voice=fake_voice)
        await cog.playlist_create(cog, FakeContext(fake_guild=fake_guild), name='new-playlist')
        await cog.playlist_create(cog, FakeContext(fake_guild=fake_guild), name='delete-me')
        await cog.playlist_item_add(cog, FakeContext(fake_guild=fake_guild), 2, search='https://foo.example')
        await cog.download_files()
        await cog.playlist_merge(cog, FakeContext(fake_guild=fake_guild), 1, 2)
        with mock_session(engine) as db_session:
            assert db_session.query(Playlist).count() == 1
            assert db_session.query(PlaylistItem).count() == 1
