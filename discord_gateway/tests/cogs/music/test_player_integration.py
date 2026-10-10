from tempfile import TemporaryDirectory
from unittest.mock import MagicMock

import pytest

from discord_gateway.cogs.music import Music
from discord_gateway.cogs.music_helpers.music_player import MusicPlayer

from discord_gateway.tests.cogs.test_music import music_config, BASE_MUSIC_CONFIG
from tests.helpers import fake_media_download
from tests.helpers import fake_engine, fake_context, fake_stores  # pylint: disable=unused-import
from tests.helpers import attach_in_process_broker


@pytest.mark.asyncio
async def test_get_player(mocker, fake_context):  # pylint: disable=redefined-outer-name
    """Test basic player creation"""
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, fake_context['dispatcher'])
    attach_in_process_broker(cog)
    mocker.patch.object(MusicPlayer, 'start_tasks')
    await cog.get_player(fake_context['guild'].id, ctx=fake_context['context'])
    assert fake_context['guild'].id in cog.players


@pytest.mark.asyncio
async def test_get_player_creates_the_history_playlist(mocker, fake_context, fake_stores):  # pylint: disable=redefined-outer-name
    """The playlist list leads with history, so it has to exist before the first play is recorded"""
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, fake_context['dispatcher'], fake_stores)
    attach_in_process_broker(cog)
    mocker.patch.object(MusicPlayer, 'start_tasks')
    assert await cog.playlist_store.get_history_playlist(fake_context['guild'].id) is None

    await cog.get_player(fake_context['guild'].id, ctx=fake_context['context'])

    history = await cog.playlist_store.get_history_playlist(fake_context['guild'].id)
    assert history is not None and history.is_history


@pytest.mark.asyncio
async def test_get_player_and_then_check_voice(mocker, fake_context):  # pylint: disable=redefined-outer-name
    """Test player creation and voice client check"""
    fake_context['guild'].voice_client = None
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, fake_context['dispatcher'])
    attach_in_process_broker(cog)
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
    attach_in_process_broker(cog)
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
    attach_in_process_broker(cog)
    cog.dispatcher = MagicMock()
    mocker.patch.object(MusicPlayer, 'start_tasks')
    assert await cog.get_player(fake_context['guild'].id, ctx=fake_context['context'], check_voice_client_active=True) is None


@pytest.mark.asyncio
async def test_add_source_to_player_caches_video(fake_engine, mocker, fake_context, fake_stores):  # pylint: disable=redefined-outer-name
    """Test adding source to player with S3 caching enabled"""
    config = music_config({
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
    })
    cog = Music(fake_context['bot'], config, fake_context['dispatcher'], fake_stores)
    attach_in_process_broker(cog, db_engine=fake_engine)
    cog.dispatcher = MagicMock()
    mocker.patch('discord_gateway.cogs.music.sleep', return_value=True)
    mocker.patch.object(MusicPlayer, 'start_tasks')
    mocker.patch('tests.fakes.asyncio_broker.get_file', return_value=True)
    await cog.get_player(fake_context['guild'].id, ctx=fake_context['context'])
    with TemporaryDirectory() as tmp_dir:
        with fake_media_download(tmp_dir, fake_context=fake_context, is_direct_search=True) as media_download:
            await cog.add_source_to_player(media_download, cog.players[fake_context['guild'].id])
            assert (await cog.broker_client.get_guild_queue(fake_context['guild'].id)).items
            assert await cog.broker_client.local_broker.video_cache.get_webpage_url_item(media_download.media_request)


@pytest.mark.asyncio
async def test_add_source_to_player_queues_for_the_players_guild(mocker, fake_context, fake_stores):  # pylint: disable=redefined-outer-name
    """A downloaded track goes into the broker's queue for its guild, ready for the player to claim."""
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, fake_context['dispatcher'], fake_stores)
    attach_in_process_broker(cog)
    cog.dispatcher = MagicMock()
    mocker.patch('discord_gateway.cogs.music.sleep', return_value=True)
    mocker.patch.object(MusicPlayer, 'start_tasks')
    player = await cog.get_player(fake_context['guild'].id, ctx=fake_context['context'])
    assert (await cog.broker_client.get_guild_queue(fake_context['guild'].id)).items == []
    with TemporaryDirectory() as tmp_dir:
        with fake_media_download(tmp_dir, fake_context=fake_context) as media_download:
            assert await cog.add_source_to_player(media_download, player) is True
            items = (await cog.broker_client.get_guild_queue(fake_context['guild'].id)).items
            assert [item.webpage_url for item in items] == [media_download.webpage_url]


@pytest.mark.asyncio
async def test_add_source_to_player_wakes_an_idle_player(mocker, fake_context):  # pylint: disable=redefined-outer-name
    """The player is told straight away, so a track does not wait out the claim poll"""
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, fake_context['dispatcher'])
    attach_in_process_broker(cog)
    cog.dispatcher = MagicMock()
    mocker.patch('discord_gateway.cogs.music.sleep', return_value=True)
    mocker.patch.object(MusicPlayer, 'start_tasks')
    player = await cog.get_player(fake_context['guild'].id, ctx=fake_context['context'])
    notified = mocker.patch.object(player, 'notify_enqueued')
    with TemporaryDirectory() as tmp_dir:
        with fake_media_download(tmp_dir, fake_context=fake_context) as media_download:
            await cog.add_source_to_player(media_download, player)
            notified.assert_called_once()


@pytest.mark.asyncio
async def test_add_source_to_player_refuses_a_closed_queue(mocker, fake_context, fake_stores):  # pylint: disable=redefined-outer-name
    """A guild closed while a download was in flight (the bot is going away) does not take new tracks"""
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, fake_context['dispatcher'], fake_stores)
    attach_in_process_broker(cog)
    cog.dispatcher = MagicMock()
    mocker.patch('discord_gateway.cogs.music.sleep', return_value=True)
    mocker.patch.object(MusicPlayer, 'start_tasks')
    player = await cog.get_player(fake_context['guild'].id, ctx=fake_context['context'])
    await cog.broker_client.close_guild(fake_context['guild'].id)
    with TemporaryDirectory() as tmp_dir:
        with fake_media_download(tmp_dir, fake_context=fake_context) as media_download:
            result = await cog.add_source_to_player(media_download, player)
            assert result is False
            assert (await cog.broker_client.get_guild_queue(fake_context['guild'].id)).items == []
            # The registration is undone, so the entry is not left behind in the broker
            assert await cog.broker_client.local_broker.get_entry(str(media_download.media_request.uuid)) is None


@pytest.mark.asyncio
async def test_add_source_to_player_tolerates_a_track_already_queued(mocker, fake_context):  # pylint: disable=redefined-outer-name
    """Queued twice (a retried result, say) plays once, and the second add still counts as added"""
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, fake_context['dispatcher'])
    attach_in_process_broker(cog)
    cog.dispatcher = MagicMock()
    mocker.patch('discord_gateway.cogs.music.sleep', return_value=True)
    mocker.patch.object(MusicPlayer, 'start_tasks')
    player = await cog.get_player(fake_context['guild'].id, ctx=fake_context['context'])
    with TemporaryDirectory() as tmp_dir:
        with fake_media_download(tmp_dir, fake_context=fake_context) as media_download:
            assert await cog.add_source_to_player(media_download, player) is True
            assert await cog.add_source_to_player(media_download, player) is True
            assert len((await cog.broker_client.get_guild_queue(fake_context['guild'].id)).items) == 1


@pytest.mark.asyncio
async def test_player_queue_management(mocker, fake_context):  # pylint: disable=redefined-outer-name
    """The queue the broker holds is the one the cog reads back, in order"""
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, fake_context['dispatcher'])
    attach_in_process_broker(cog)
    cog.dispatcher = MagicMock()
    mocker.patch('discord_gateway.cogs.music.sleep', return_value=True)
    mocker.patch.object(MusicPlayer, 'start_tasks')
    player = await cog.get_player(fake_context['guild'].id, ctx=fake_context['context'])

    with TemporaryDirectory() as tmp_dir:
        with fake_media_download(tmp_dir, fake_context=fake_context) as first:
            with fake_media_download(tmp_dir, fake_context=fake_context) as second:
                await cog.add_source_to_player(first, player)
                await cog.add_source_to_player(second, player)

                items = (await cog.broker_client.get_guild_queue(fake_context['guild'].id)).items
                assert [item.webpage_url for item in items] == [first.webpage_url, second.webpage_url]


@pytest.mark.asyncio
async def test_add_source_to_player_queue_full(mocker, fake_context, fake_stores):  # pylint: disable=redefined-outer-name
    """add_source_to_player returns False when the play queue is full."""
    config = music_config({'music': {'player': {'queue_max_size': 1}}})
    cog = Music(fake_context['bot'], config, fake_context['dispatcher'], fake_stores)
    attach_in_process_broker(cog)
    cog.dispatcher = MagicMock()
    mocker.patch('discord_gateway.cogs.music.sleep', return_value=True)
    mocker.patch.object(MusicPlayer, 'start_tasks')
    player = await cog.get_player(fake_context['guild'].id, ctx=fake_context['context'])
    with TemporaryDirectory() as tmp_dir:
        with fake_media_download(tmp_dir, fake_context=fake_context) as queued:
            with fake_media_download(tmp_dir, fake_context=fake_context) as refused:
                assert await cog.add_source_to_player(queued, player) is True
                assert await cog.add_source_to_player(refused, player) is False
                items = (await cog.broker_client.get_guild_queue(fake_context['guild'].id)).items
                assert [item.webpage_url for item in items] == [queued.webpage_url]
                assert await cog.broker_client.local_broker.get_entry(str(refused.media_request.uuid)) is None


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
async def test_cog_load_starts_the_resume_task(fake_context, fake_stores):  # pylint: disable=redefined-outer-name
    """cog_load starts the one-shot that picks guilds back up after a restart."""
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, fake_context['dispatcher'], fake_stores)
    cog.dispatcher = MagicMock()
    loop_mock = MagicMock()
    loop_mock.create_task = MagicMock(return_value=MagicMock())
    fake_context['bot'].loop = loop_mock
    fake_context['bot'].get_cog = MagicMock(return_value=MagicMock())

    await cog.cog_load()

    # cleanup, resume, download results, search results: no loop records plays any more
    assert loop_mock.create_task.call_count == 4
    assert cog._init_task is loop_mock.create_task.return_value  # pylint: disable=protected-access


@pytest.mark.asyncio
async def test_add_source_triggers_prefetch(mocker, fake_context):  # pylint: disable=redefined-outer-name
    """add_source_to_player calls trigger_prefetch on the player after register_download"""
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, fake_context['dispatcher'])
    attach_in_process_broker(cog)
    cog.dispatcher = MagicMock()
    mocker.patch('discord_gateway.cogs.music.sleep', return_value=True)
    mocker.patch.object(MusicPlayer, 'start_tasks')
    player = await cog.get_player(fake_context['guild'].id, ctx=fake_context['context'])
    prefetch_mock = mocker.patch.object(player, 'trigger_prefetch')
    with TemporaryDirectory() as tmp_dir:
        with fake_media_download(tmp_dir, fake_context=fake_context) as media_download:
            await cog.add_source_to_player(media_download, player)
            prefetch_mock.assert_called_once()


@pytest.mark.asyncio
async def test_add_source_to_player_queue_full_with_bundle(mocker, fake_context, fake_stores):  # pylint: disable=redefined-outer-name
    """add_source_to_player sets failure_reason on the bundle when queue is full."""
    config = music_config({'music': {'player': {'queue_max_size': 1}}})
    cog = Music(fake_context['bot'], config, fake_context['dispatcher'], fake_stores)
    attach_in_process_broker(cog)
    mocker.patch('discord_gateway.cogs.music.sleep', return_value=True)
    mocker.patch.object(MusicPlayer, 'start_tasks')
    player = await cog.get_player(fake_context['guild'].id, ctx=fake_context['context'])
    with TemporaryDirectory() as tmp_dir:
        with fake_media_download(tmp_dir, fake_context=fake_context) as already_queued:
            assert await cog.add_source_to_player(already_queued, player) is True
        with fake_media_download(tmp_dir, fake_context=fake_context) as media_download:
            # Register a broker bundle and link it to the media request
            bundle_uuid = await cog.create_bundle(
                fake_context['guild'].id, fake_context['channel'].id, input_string='test',
            )
            media_download.media_request.bundle_uuid = bundle_uuid
            await cog.broker_client.register_request(media_download.media_request)
            result = await cog.add_source_to_player(media_download, player)
            assert result is False
            assert media_download.media_request.failure_reason is not None
