from tempfile import TemporaryDirectory
from unittest.mock import MagicMock

import pytest

from discord_bot.cogs.music import Music

from discord_bot.cogs.music_helpers.music_player import MusicPlayer
from discord_bot.types.download import DownloadErrorType, DownloadResult, DownloadStatus

from tests.cogs.test_music import BASE_MUSIC_CONFIG, yield_download_client_download_exception, yield_fake_download_client, yield_download_client_download_error
from tests.helpers import fake_source_dict, fake_media_download
from tests.helpers import fake_engine, fake_context #pylint:disable=unused-import

@pytest.mark.asyncio()
async def test_download_queue(mocker, fake_engine, fake_context):  #pylint:disable=redefined-outer-name
    config = {
        'music': {
            'download': {
                'cache': {
                    'enable_cache_files': True,
                }
            }
        }
    } | BASE_MUSIC_CONFIG
    with TemporaryDirectory() as tmp_dir:
        with fake_media_download(tmp_dir, fake_context=fake_context) as sd:
            mocker.patch('discord_bot.cogs.music.sleep', return_value=True)
            mocker.patch.object(MusicPlayer, 'start_tasks')
            mocker.patch('discord_bot.cogs.music.DownloadClient', side_effect=yield_fake_download_client(sd))
            cog = Music(fake_context['bot'], config, fake_engine)
            cog.dispatcher = MagicMock()
            await cog.get_player(fake_context['guild'].id, ctx=fake_context['context'])
            await cog.download_queue.put_nowait(fake_context['guild'].id, sd.media_request)
            await cog.download_files()
            assert cog.players[fake_context['guild'].id].get_queue_items()

@pytest.mark.asyncio()
async def test_download_queue_hits_cache(mocker, fake_engine, fake_context):  #pylint:disable=redefined-outer-name
    config = {
        'music': {
            'download': {
                'cache': {
                    'enable_cache_files': True,
                }
            }
        }
    } | BASE_MUSIC_CONFIG
    with TemporaryDirectory() as tmp_dir:
        with fake_media_download(tmp_dir, fake_context=fake_context, is_direct_search=True) as sd:
            mocker.patch('discord_bot.cogs.music.sleep', return_value=True)
            mocker.patch.object(MusicPlayer, 'start_tasks')
            cog = Music(fake_context['bot'], config, fake_engine)
            cog.dispatcher = MagicMock()
            cog.media_broker.register_download(sd)
            await cog.get_player(fake_context['guild'].id, ctx=fake_context['context'])
            await cog.download_queue.put_nowait(fake_context['guild'].id, sd.media_request)
            await cog.download_files()
            assert cog.players[fake_context['guild'].id].get_queue_items()

def yield_download_client_bot_flagged():
    class FakeDownloadClient():
        def __init__(self, *_args, **_kwargs):
            pass

        async def create_source(self, media_request, *_args, **_kwargs):
            return DownloadResult(status=DownloadStatus(success=False, error_type=DownloadErrorType.BOT_FLAGGED, error_detail='foo'), media_request=media_request, ytdlp_data=None, file_name=None)

    return FakeDownloadClient

@pytest.mark.asyncio()
async def test_download_queue_bot_warning(mocker, fake_context):  #pylint:disable=redefined-outer-name
    mocker.patch('discord_bot.cogs.music.sleep', return_value=True)
    mocker.patch.object(MusicPlayer, 'start_tasks')
    mocker.patch('discord_bot.cogs.music.DownloadClient', side_effect=yield_download_client_bot_flagged())
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, None)
    s = fake_source_dict(fake_context)
    await cog.get_player(fake_context['guild'].id, ctx=fake_context['context'])
    await cog.download_queue.put_nowait(fake_context['guild'].id, s)
    await cog.download_files()
    assert not cog.players[fake_context['guild'].id].get_queue_items()

@pytest.mark.asyncio()
async def test_download_queue_download_exception(mocker, fake_context):  #pylint:disable=redefined-outer-name
    async def _bump_value():
        return True

    mocker.patch('discord_bot.cogs.music.sleep', return_value=True)
    mocker.patch.object(MusicPlayer, 'start_tasks')

    mocker.patch('discord_bot.cogs.music.DownloadClient', side_effect=yield_download_client_download_exception())
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, None)
    s = fake_source_dict(fake_context)
    await cog.get_player(fake_context['guild'].id, ctx=fake_context['context'])
    await cog.download_queue.put_nowait(fake_context['guild'].id, s)
    await cog.download_files()
    assert not cog.players[fake_context['guild'].id].get_queue_items()

@pytest.mark.asyncio()
async def test_download_queue_download_error(mocker, fake_context):  #pylint:disable=redefined-outer-name
    async def _bump_value():
        return True
    mocker.patch('discord_bot.cogs.music.sleep', return_value=True)
    mocker.patch.object(MusicPlayer, 'start_tasks')

    mocker.patch('discord_bot.cogs.music.DownloadClient', side_effect=yield_download_client_download_error())
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, None)
    s = fake_source_dict(fake_context)
    await cog.get_player(fake_context['guild'].id, ctx=fake_context['context'])
    await cog.download_queue.put_nowait(fake_context['guild'].id, s)
    await cog.download_files()
    assert not cog.players[fake_context['guild'].id].get_queue_items()

@pytest.mark.asyncio()
async def test_download_queue_no_result(mocker, fake_context):  #pylint:disable=redefined-outer-name
    mocker.patch('discord_bot.cogs.music.sleep', return_value=True)
    mocker.patch.object(MusicPlayer, 'start_tasks')
    s = fake_source_dict(fake_context)
    mocker.patch('discord_bot.cogs.music.DownloadClient', side_effect=yield_fake_download_client(None))
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, None)
    await cog.get_player(fake_context['guild'].id, ctx=fake_context['context'])
    await cog.download_queue.put_nowait(fake_context['guild'].id, s)
    await cog.download_files()
    assert not cog.players[fake_context['guild'].id].get_queue_items()

@pytest.mark.asyncio()
async def test_download_queue_player_shutdown(mocker, fake_context):  #pylint:disable=redefined-outer-name
    mocker.patch('discord_bot.cogs.music.sleep', return_value=True)
    mocker.patch.object(MusicPlayer, 'start_tasks')
    s = fake_source_dict(fake_context)
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, None)
    await cog.get_player(fake_context['guild'].id, ctx=fake_context['context'])
    await cog.download_queue.put_nowait(fake_context['guild'].id, s)
    cog.players[fake_context['guild'].id].shutdown_called = True
    await cog.download_files()
    assert not cog.players[fake_context['guild'].id].get_queue_items()

@pytest.mark.asyncio()
async def test_download_queue_no_player_queue(mocker, fake_context):  #pylint:disable=redefined-outer-name
    mocker.patch('discord_bot.cogs.music.sleep', return_value=True)
    mocker.patch.object(MusicPlayer, 'start_tasks')
    s = fake_source_dict(fake_context)
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, None)
    await cog.download_queue.put_nowait(fake_context['guild'].id, s)
    await cog.download_files()
    assert fake_context['guild'].id not in cog.players
