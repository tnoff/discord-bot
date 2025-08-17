from tempfile import TemporaryDirectory

import pytest

from discord_bot.cogs.music import Music
from discord_bot.cogs.music_helpers.music_player import MusicPlayer

from tests.cogs.test_music import BASE_MUSIC_CONFIG
from tests.helpers import fake_media_download
from tests.helpers import fake_engine, fake_context  # pylint: disable=unused-import


@pytest.mark.asyncio
async def test_get_player(mocker, fake_context):  # pylint: disable=redefined-outer-name
    """Test basic player creation"""
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, None)
    mocker.patch.object(MusicPlayer, 'start_tasks')
    await cog.get_player(fake_context['guild'].id, ctx=fake_context['context'])
    assert fake_context['guild'].id in cog.players


@pytest.mark.asyncio
async def test_get_player_and_then_check_voice(mocker, fake_context):  # pylint: disable=redefined-outer-name
    """Test player creation and voice client check"""
    fake_context['guild'].voice_client = None
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, None)
    mocker.patch.object(MusicPlayer, 'start_tasks')
    await cog.get_player(fake_context['guild'].id, ctx=fake_context['context'])
    assert fake_context['guild'].id in cog.players
    result = await cog.get_player(fake_context['guild'].id, check_voice_client_active=True)
    assert result is None


@pytest.mark.asyncio
async def test_get_player_join_channel(mocker, fake_context):  # pylint: disable=redefined-outer-name
    """Test player creation with join channel"""
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, None)
    mocker.patch.object(MusicPlayer, 'start_tasks')
    await cog.get_player(fake_context['guild'].id, ctx=fake_context['context'], join_channel=fake_context['channel'])
    assert fake_context['guild'].id in cog.players


@pytest.mark.asyncio
async def test_get_player_no_create(mocker, fake_context):  # pylint: disable=redefined-outer-name
    """Test get_player with create_player=False returns None when player doesn't exist"""
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, None)
    mocker.patch.object(MusicPlayer, 'start_tasks')
    assert await cog.get_player(fake_context['guild'].id, ctx=fake_context['context'], create_player=False) is None


@pytest.mark.asyncio
async def test_get_player_check_voice_client_active(mocker, fake_context):  # pylint: disable=redefined-outer-name
    """Test get_player with check_voice_client_active when no voice client"""
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, None)
    mocker.patch.object(MusicPlayer, 'start_tasks')
    assert await cog.get_player(fake_context['guild'].id, ctx=fake_context['context'], check_voice_client_active=True) is None


@pytest.mark.asyncio
async def test_add_source_to_player_caches_video(fake_engine, mocker, fake_context):  # pylint: disable=redefined-outer-name
    """Test adding source to player with caching enabled"""
    config = {
        'music': {
            'download': {
                'cache': {
                    'enable_cache_files': True,
                }
            }
        }
    } | BASE_MUSIC_CONFIG
    cog = Music(fake_context['bot'], config, fake_engine)
    mocker.patch('discord_bot.cogs.music.sleep', return_value=True)
    mocker.patch.object(MusicPlayer, 'start_tasks')
    await cog.get_player(fake_context['guild'].id, ctx=fake_context['context'])
    with TemporaryDirectory() as tmp_dir:
        with fake_media_download(tmp_dir, fake_context=fake_context, is_direct_search=True) as media_download:
            await cog.add_source_to_player(media_download, cog.players[fake_context['guild'].id])
            assert cog.players[fake_context['guild'].id].get_queue_items()
            assert cog.video_cache.get_webpage_url_item(media_download.media_request)


@pytest.mark.asyncio
async def test_add_source_to_player_puts_blocked(fake_engine, mocker, fake_context):  # pylint: disable=redefined-outer-name
    """Test adding source to player when queue is blocked"""
    config = {
        'music': {
            'download': {
                'cache': {
                    'enable_cache_files': True,
                }
            }
        }
    } | BASE_MUSIC_CONFIG
    cog = Music(fake_context['bot'], config, fake_engine)
    mocker.patch('discord_bot.cogs.music.sleep', return_value=True)
    mocker.patch.object(MusicPlayer, 'start_tasks')
    await cog.get_player(fake_context['guild'].id, ctx=fake_context['context'])
    cog.players[fake_context['guild'].id]._play_queue.block()  # pylint: disable=protected-access
    with TemporaryDirectory() as tmp_dir:
        with fake_media_download(tmp_dir, fake_context=fake_context) as media_download:
            result = await cog.add_source_to_player(media_download, cog.players[fake_context['guild'].id])
            assert not result


@pytest.mark.asyncio
async def test_player_message_queue_integration(mocker, fake_context):  # pylint: disable=redefined-outer-name
    """Test that player operations trigger message queue updates"""
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, None)
    mocker.patch('discord_bot.cogs.music.sleep', return_value=True)
    mocker.patch.object(MusicPlayer, 'start_tasks')
    player = await cog.get_player(fake_context['guild'].id, ctx=fake_context['context'])
    with TemporaryDirectory() as tmp_dir:
        with fake_media_download(tmp_dir, fake_context=fake_context) as media_download:
            await cog.add_source_to_player(media_download, player)

            # Check that message queue has been updated for this player
            msg_type, msg_data = cog.message_queue.get_next_message()
            expected_key = f'play_order-{fake_context["guild"].id}'
            assert msg_type is not None
            assert msg_data == expected_key


@pytest.mark.asyncio
async def test_player_queue_management(mocker, fake_context):  # pylint: disable=redefined-outer-name
    """Test basic player queue management functionality"""
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, None)
    mocker.patch('discord_bot.cogs.music.sleep', return_value=True)
    mocker.patch.object(MusicPlayer, 'start_tasks')
    player = await cog.get_player(fake_context['guild'].id, ctx=fake_context['context'])

    with TemporaryDirectory() as tmp_dir:
        with fake_media_download(tmp_dir, fake_context=fake_context) as media_download:
            # Initially queue should be empty
            assert player._play_queue.empty()  # pylint: disable=protected-access
            assert not player.get_queue_items()

            # Add a source
            player.add_to_play_queue(media_download)
            assert not player._play_queue.empty()  # pylint: disable=protected-access
            assert len(player.get_queue_items()) == 1

            # Check that the source is retrievable
            queue_items = player.get_queue_items()
            assert queue_items[0].webpage_url == media_download.webpage_url  # pylint: disable=no-member
