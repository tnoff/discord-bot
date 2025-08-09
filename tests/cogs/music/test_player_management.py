from tempfile import TemporaryDirectory

import pytest

from discord_bot.cogs.music import Music
from discord_bot.cogs.music_helpers.music_player import MusicPlayer

from tests.helpers import fake_source_download, fake_source_dict
from tests.helpers import fake_engine, fake_context #pylint:disable=unused-import
from tests.helpers import FakeMessage

BASE_MUSIC_CONFIG = {
    'general': {
        'include': {
            'music': True
        }
    },
}

@pytest.mark.asyncio
async def test_get_player(mocker, fake_context):  #pylint:disable=redefined-outer-name
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, None)
    mocker.patch.object(MusicPlayer, 'start_tasks')
    await cog.get_player(fake_context['guild'].id, ctx=fake_context['context'])
    assert fake_context['guild'].id in cog.players

@pytest.mark.asyncio
async def test_get_player_and_then_check_voice(mocker, fake_context):  #pylint:disable=redefined-outer-name
    fake_context['guild'].voice_client = None
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, None)
    mocker.patch.object(MusicPlayer, 'start_tasks')
    await cog.get_player(fake_context['guild'].id, ctx=fake_context['context'])
    assert fake_context['guild'].id in cog.players
    result = await cog.get_player(fake_context['guild'].id, check_voice_client_active=True)
    assert result is None

@pytest.mark.asyncio
async def test_get_player_join_channel(mocker, fake_context):  #pylint:disable=redefined-outer-name
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, None)
    mocker.patch.object(MusicPlayer, 'start_tasks')
    await cog.get_player(fake_context['guild'].id, ctx=fake_context['context'], join_channel=fake_context['channel'])
    assert fake_context['guild'].id in cog.players

@pytest.mark.asyncio
async def test_get_player_no_create(mocker, fake_context):  #pylint:disable=redefined-outer-name
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, None)
    mocker.patch.object(MusicPlayer, 'start_tasks')
    assert await cog.get_player(fake_context['guild'].id, ctx=fake_context['context'], create_player=False) is None

@pytest.mark.asyncio
async def test_get_player_check_voice_client_active(mocker, fake_context):  #pylint:disable=redefined-outer-name
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, None)
    mocker.patch.object(MusicPlayer, 'start_tasks')
    assert await cog.get_player(fake_context['guild'].id, ctx=fake_context['context'], check_voice_client_active=True) is None

@pytest.mark.asyncio
async def test_player_should_update_player_queue_false(mocker, fake_context):  #pylint:disable=redefined-outer-name
    fake_message = FakeMessage()
    fake_context['channel'].messages = [fake_message]
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, None)
    mocker.patch.object(MusicPlayer, 'start_tasks')
    player = await cog.get_player(fake_context['guild'].id, ctx=fake_context['context'])
    cog.player_messages[player.guild.id] = [
        fake_message,
    ]
    result = await cog.player_should_update_queue_order(player)
    assert not result

@pytest.mark.asyncio
async def test_player_should_update_player_queue_true(mocker, fake_context):  #pylint:disable=redefined-outer-name
    fake_message = FakeMessage()
    fake_message_dos = FakeMessage()
    fake_context['channel'].messages = [fake_message]
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, None)
    mocker.patch.object(MusicPlayer, 'start_tasks')
    player = await cog.get_player(fake_context['guild'].id, ctx=fake_context['context'])
    cog.player_messages[player.guild.id] = [
        fake_message_dos,
    ]
    result = await cog.player_should_update_queue_order(player)
    assert result

@pytest.mark.asyncio
async def test_player_clear_queue(mocker, fake_context):  #pylint:disable=redefined-outer-name
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, None)
    mocker.patch.object(MusicPlayer, 'start_tasks')
    player = await cog.get_player(fake_context['guild'].id, ctx=fake_context['context'])
    cog.player_messages[player.guild.id] = [
        FakeMessage(content='```Num|Wait|Message\n01|02:00|Foo Song ///Uploader```')
    ]
    result = await cog.clear_player_queue(player.guild.id)
    assert not cog.player_messages[player.guild.id]
    assert result is True

@pytest.mark.asyncio
async def test_player_update_queue_order_only_new(mocker, fake_context):  #pylint:disable=redefined-outer-name
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, None)
    mocker.patch.object(MusicPlayer, 'start_tasks')
    player = await cog.get_player(fake_context['guild'].id, ctx=fake_context['context'])
    with TemporaryDirectory() as tmp_dir:
        with fake_source_download(tmp_dir, fake_context=fake_context) as sd:
            player.add_to_play_queue(sd)
            await cog.player_update_queue_order(player.guild.id)
            assert cog.player_messages[player.guild.id][0].content == f'```Pos|| Wait Time|| Title /// Uploader\n--------------------------------------------------------------------------------------------------\n1  || 0:00:00  || {sd.title} /// {sd.uploader}```' #pylint:disable=no-member

@pytest.mark.asyncio
async def test_player_update_queue_order_delete_and_edit(mocker, fake_context):  #pylint:disable=redefined-outer-name
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, None)
    mocker.patch.object(MusicPlayer, 'start_tasks')
    player = await cog.get_player(fake_context['guild'].id, ctx=fake_context['context'])
    cog.player_messages[player.guild.id] = [
        FakeMessage(content='foo bar'),
        FakeMessage(content='second message')
    ]
    with TemporaryDirectory() as tmp_dir:
        with fake_source_download(tmp_dir, fake_context=fake_context) as sd:
            player.add_to_play_queue(sd)
            await cog.player_update_queue_order(player.guild.id)
            assert cog.player_messages[player.guild.id][0].content == f'```Pos|| Wait Time|| Title /// Uploader\n--------------------------------------------------------------------------------------------------\n1  || 0:00:00  || {sd.title} /// {sd.uploader}```' #pylint:disable=no-member
            assert len(cog.player_messages[player.guild.id]) == 1

@pytest.mark.asyncio
async def test_player_update_queue_order_no_edit(mocker, fake_context):  #pylint:disable=redefined-outer-name
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, None)
    fake_message = FakeMessage(id='first-123', content='```Pos|| Wait Time|| Title /// Uploader\n--------------------------------------------------------------------------------------------------\n1  || 0:00:00  || Foo Title /// Foo Uploader```') #pylint:disable=no-member
    mocker.patch.object(MusicPlayer, 'start_tasks')
    player = await cog.get_player(fake_context['guild'].id, ctx=fake_context['context'])
    cog.player_messages[player.guild.id] = [
        fake_message,
    ]
    with TemporaryDirectory() as tmp_dir:
        with fake_source_download(tmp_dir, fake_context=fake_context) as sd:
            player.add_to_play_queue(sd)
            await cog.player_update_queue_order(player.guild.id)
            assert cog.player_messages[player.guild.id][0].id == 'first-123'

@pytest.mark.asyncio
async def test_cleanup_players_just_bot(mocker, fake_context):  #pylint:disable=redefined-outer-name
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, None)
    mocker.patch('discord_bot.cogs.music.sleep', return_value=True)
    mocker.patch.object(MusicPlayer, 'start_tasks')
    await cog.get_player(fake_context['guild'].id, ctx=fake_context['context'], create_player=True, join_channel=fake_context['channel'])
    fake_context['channel'].members = [fake_context['bot'].user]
    await cog.cleanup_players()
    assert fake_context['guild'].id not in cog.players

@pytest.mark.asyncio
async def test_guild_cleanup(mocker, fake_engine, fake_context):  #pylint:disable=redefined-outer-name
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, fake_engine)
    mocker.patch('discord_bot.cogs.music.sleep', return_value=True)
    mocker.patch.object(MusicPlayer, 'start_tasks')
    await cog.get_player(fake_context['guild'].id, ctx=fake_context['context'])
    with TemporaryDirectory() as tmp_dir:
        with fake_source_download(tmp_dir, fake_context=fake_context) as sd:
            await cog.players[fake_context['guild'].id]._history.put(sd) #pylint:disable=protected-access
            await cog.cleanup(fake_context['guild'], external_shutdown_called=True)
            assert fake_context['guild'].id not in cog.players
            assert fake_context['guild'].id not in cog.download_queue.queues

@pytest.mark.asyncio
async def test_guild_hanging_downloads(mocker, fake_engine, fake_context):  #pylint:disable=redefined-outer-name
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, fake_engine)
    mocker.patch('discord_bot.cogs.music.sleep', return_value=True)
    mocker.patch.object(MusicPlayer, 'start_tasks')
    await cog.get_player(fake_context['guild'].id, ctx=fake_context['context'])
    s = fake_source_dict(fake_context)
    cog.download_queue.put_nowait(fake_context['guild'].id, s)
    await cog.cleanup(fake_context['guild'], external_shutdown_called=True)
    assert fake_context['guild'].id not in cog.download_queue.queues