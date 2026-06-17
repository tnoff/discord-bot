from asyncio import QueueFull
from tempfile import TemporaryDirectory
from unittest.mock import MagicMock

import pytest

from discord_bot.cogs.music import Music
from discord_bot.cogs.music_helpers.music_player import MusicPlayer

from tests.cogs.test_music import BASE_MUSIC_CONFIG
from tests.helpers import fake_media_download
from tests.helpers import fake_engine, fake_context  # pylint: disable=unused-import


@pytest.mark.asyncio
async def test_get_player(mocker, fake_context):  # pylint: disable=redefined-outer-name
    """Test basic player creation"""
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, fake_context['dispatcher'])
    mocker.patch.object(MusicPlayer, 'start_tasks')
    await cog.get_player(fake_context['guild'].id, ctx=fake_context['context'])
    assert fake_context['guild'].id in cog.players


@pytest.mark.asyncio
async def test_get_player_and_then_check_voice(mocker, fake_context):  # pylint: disable=redefined-outer-name
    """Test player creation and voice client check"""
    fake_context['guild'].voice_client = None
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, fake_context['dispatcher'])
    cog.dispatcher = MagicMock()
    mocker.patch.object(MusicPlayer, 'start_tasks')
    await cog.get_player(fake_context['guild'].id, ctx=fake_context['context'])
    assert fake_context['guild'].id in cog.players
    result = await cog.get_player(fake_context['guild'].id, check_voice_client_active=True)
    assert result is None


@pytest.mark.asyncio
async def test_get_player_join_channel(mocker, fake_context):  # pylint: disable=redefined-outer-name
    """Test player creation with join channel"""
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, fake_context['dispatcher'])
    mocker.patch.object(MusicPlayer, 'start_tasks')
    await cog.get_player(fake_context['guild'].id, ctx=fake_context['context'], join_channel=fake_context['channel'])
    assert fake_context['guild'].id in cog.players


@pytest.mark.asyncio
async def test_get_player_no_create(mocker, fake_context):  # pylint: disable=redefined-outer-name
    """Test get_player with create_player=False returns None when player doesn't exist"""
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, fake_context['dispatcher'])
    mocker.patch.object(MusicPlayer, 'start_tasks')
    assert await cog.get_player(fake_context['guild'].id, ctx=fake_context['context'], create_player=False) is None


@pytest.mark.asyncio
async def test_get_player_check_voice_client_active(mocker, fake_context):  # pylint: disable=redefined-outer-name
    """Test get_player with check_voice_client_active when no voice client"""
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, fake_context['dispatcher'])
    cog.dispatcher = MagicMock()
    mocker.patch.object(MusicPlayer, 'start_tasks')
    assert await cog.get_player(fake_context['guild'].id, ctx=fake_context['context'], check_voice_client_active=True) is None


@pytest.mark.asyncio
async def test_add_source_to_player_caches_video(fake_engine, mocker, fake_context):  # pylint: disable=redefined-outer-name
    """Test adding source to player with S3 caching enabled"""
    config = {
        'music': {
            'download': {
                'cache': {
                    'enable_cache_files': True,
                },
                'storage': {
                    'bucket_name': 'test-bucket',
                }
            }
        }
    } | BASE_MUSIC_CONFIG
    cog = Music(fake_context['bot'], config, fake_context['dispatcher'], fake_engine)
    cog.dispatcher = MagicMock()
    mocker.patch('discord_bot.cogs.music.sleep', return_value=True)
    mocker.patch.object(MusicPlayer, 'start_tasks')
    mocker.patch('discord_bot.cogs.music_helpers.media_broker.get_file', return_value=True)
    await cog.get_player(fake_context['guild'].id, ctx=fake_context['context'])
    with TemporaryDirectory() as tmp_dir:
        with fake_media_download(tmp_dir, fake_context=fake_context, is_direct_search=True) as media_download:
            await cog.add_source_to_player(media_download, cog.players[fake_context['guild'].id])
            assert cog.players[fake_context['guild'].id].get_queue_items()
            assert await cog.video_cache.get_webpage_url_item(media_download.media_request)


@pytest.mark.asyncio
async def test_add_source_to_player_puts_blocked(fake_engine, mocker, fake_context):  # pylint: disable=redefined-outer-name
    """Test adding source to player when queue is blocked"""
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, fake_context['dispatcher'], fake_engine)
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
    """Test that player operations trigger dispatcher.update_mutable calls"""
    from unittest.mock import Mock  # pylint: disable=import-outside-toplevel
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, fake_context['dispatcher'])
    cog.dispatcher = Mock()
    mocker.patch('discord_bot.cogs.music.sleep', return_value=True)
    mocker.patch.object(MusicPlayer, 'start_tasks')
    player = await cog.get_player(fake_context['guild'].id, ctx=fake_context['context'])
    with TemporaryDirectory() as tmp_dir:
        with fake_media_download(tmp_dir, fake_context=fake_context) as media_download:
            await cog.add_source_to_player(media_download, player)

            # Check that dispatcher.update_mutable was called with play_order key
            expected_key = f'play_order-{fake_context["guild"].id}'
            assert cog.dispatcher.update_mutable.called
            assert cog.dispatcher.update_mutable.call_args[0][0] == expected_key


@pytest.mark.asyncio
async def test_player_queue_management(mocker, fake_context):  # pylint: disable=redefined-outer-name
    """Test basic player queue management functionality"""
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, fake_context['dispatcher'])
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


@pytest.mark.asyncio
async def test_add_source_to_player_queue_full(fake_engine, mocker, fake_context):  # pylint: disable=redefined-outer-name
    """add_source_to_player returns False when the play queue is full."""
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, fake_context['dispatcher'], fake_engine)
    cog.dispatcher = MagicMock()
    mocker.patch('discord_bot.cogs.music.sleep', return_value=True)
    mocker.patch.object(MusicPlayer, 'start_tasks')
    player = await cog.get_player(fake_context['guild'].id, ctx=fake_context['context'])
    mocker.patch.object(player, 'add_to_play_queue', side_effect=QueueFull())
    with TemporaryDirectory() as tmp_dir:
        with fake_media_download(tmp_dir, fake_context=fake_context) as media_download:
            result = await cog.add_source_to_player(media_download, player)
            assert result is False


@pytest.mark.asyncio
async def test_cog_load_creates_background_tasks(fake_context):  # pylint: disable=redefined-outer-name
    """cog_load sets up the dispatcher and creates background loop tasks."""
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, fake_context['dispatcher'])
    cog.dispatcher = MagicMock()
    # Provide a real (or mock) event loop so create_task doesn't fail
    loop_mock = MagicMock()
    loop_mock.create_task = MagicMock(return_value=MagicMock())
    fake_context['bot'].loop = loop_mock
    # get_cog returns a fake dispatcher
    fake_context['bot'].get_cog = MagicMock(return_value=MagicMock())

    await cog.cog_load()

    assert loop_mock.create_task.call_count >= 3  # cleanup, download, youtube_search


@pytest.mark.asyncio
async def test_cog_load_with_db_engine_creates_post_play_task(fake_engine, fake_context):  # pylint: disable=redefined-outer-name
    """cog_load creates the post_play_processing task when db_engine is set."""
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, fake_context['dispatcher'], fake_engine)
    cog.dispatcher = MagicMock()
    loop_mock = MagicMock()
    loop_mock.create_task = MagicMock(return_value=MagicMock())
    fake_context['bot'].loop = loop_mock
    fake_context['bot'].get_cog = MagicMock(return_value=MagicMock())

    await cog.cog_load()

    # With db_engine set, 4 tasks: cleanup, download, youtube_search, post_play_processing
    assert loop_mock.create_task.call_count >= 4


@pytest.mark.asyncio
async def test_add_source_triggers_prefetch(mocker, fake_context):  # pylint: disable=redefined-outer-name
    """add_source_to_player calls trigger_prefetch on the player after register_download"""
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, fake_context['dispatcher'])
    cog.dispatcher = MagicMock()
    mocker.patch('discord_bot.cogs.music.sleep', return_value=True)
    mocker.patch.object(MusicPlayer, 'start_tasks')
    player = await cog.get_player(fake_context['guild'].id, ctx=fake_context['context'])
    prefetch_mock = mocker.patch.object(player, 'trigger_prefetch')
    with TemporaryDirectory() as tmp_dir:
        with fake_media_download(tmp_dir, fake_context=fake_context) as media_download:
            await cog.add_source_to_player(media_download, player)
            prefetch_mock.assert_called_once()


@pytest.mark.asyncio
async def test_add_source_to_player_queue_full_with_bundle(fake_engine, mocker, fake_context):  # pylint: disable=redefined-outer-name
    """add_source_to_player sets failure_reason on the bundle when queue is full."""
    from discord_bot.types.media_request import MultiMediaRequestBundle  # pylint: disable=import-outside-toplevel
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, fake_context['dispatcher'], fake_engine)
    cog.dispatcher = MagicMock()
    mocker.patch('discord_bot.cogs.music.sleep', return_value=True)
    mocker.patch.object(MusicPlayer, 'start_tasks')
    player = await cog.get_player(fake_context['guild'].id, ctx=fake_context['context'])
    mocker.patch.object(player, 'add_to_play_queue', side_effect=QueueFull())
    with TemporaryDirectory() as tmp_dir:
        with fake_media_download(tmp_dir, fake_context=fake_context) as media_download:
            # Register a bundle and link it to the media request
            bundle = MultiMediaRequestBundle(fake_context['guild'].id, fake_context['channel'].id)
            bundle.set_initial_search('test')
            cog.multirequest_bundles[bundle.uuid] = bundle
            bundle.add_media_request(media_download.media_request)
            # bundle_uuid is now set on the media_request
            result = await cog.add_source_to_player(media_download, player)
            assert result is False
            assert media_download.media_request.failure_reason is not None
