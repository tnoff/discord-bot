from tempfile import TemporaryDirectory

import pytest

from discord_bot.database import  VideoCache, VideoCacheBackup
from discord_bot.cogs.music import Music

from discord_bot.cogs.music_helpers.music_player import MusicPlayer

from tests.cogs.test_music import BASE_MUSIC_CONFIG
from tests.helpers import mock_session, fake_media_download
from tests.helpers import fake_engine, fake_context #pylint:disable=unused-import


@pytest.mark.asyncio
async def test_cache_cleanup_no_op(mocker, fake_engine, fake_context):  #pylint:disable=redefined-outer-name
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
        with fake_media_download(tmp_dir, fake_context=fake_context, is_direct_search=True) as sd:
            cog.players[fake_context['guild'].id].add_to_play_queue(sd)
            cog.video_cache.iterate_file(sd)
            cog.video_cache.ready_remove()
            await cog.cache_cleanup()
            assert cog.video_cache.get_webpage_url_item(sd.media_request)

@pytest.mark.asyncio
async def test_cache_cleanup_uploads_object_storage(fake_engine, mocker, fake_context):  #pylint:disable=redefined-outer-name
    config = {
        'music': {
            'download': {
                'cache': {
                    'enable_cache_files': True,
                },
                'storage': {
                    'backend': 's3',
                    'bucket_name': 'foo',
                }
            }
        }
    } | BASE_MUSIC_CONFIG
    cog = Music(fake_context['bot'], config, fake_engine)
    mocker.patch('discord_bot.cogs.music.sleep', return_value=True)
    mocker.patch.object(MusicPlayer, 'start_tasks')
    await cog.get_player(fake_context['guild'].id, ctx=fake_context['context'])
    with TemporaryDirectory() as tmp_dir:
        with fake_media_download(tmp_dir, fake_context=fake_context) as sd:
            cog.players[fake_context['guild'].id].add_to_play_queue(sd)
            mocker.patch('discord_bot.cogs.music_helpers.video_cache_client.upload_file', return_value=True)
            cog.video_cache.iterate_file(sd)
            await cog.cache_cleanup()
            with mock_session(fake_engine) as session:
                assert session.query(VideoCache).count() == 1
                assert session.query(VideoCacheBackup).count() == 1

@pytest.mark.asyncio
async def test_cache_cleanup_removes(fake_engine, mocker, fake_context):  #pylint:disable=redefined-outer-name
    config = {
        'music': {
            'download': {
                'cache': {
                    'enable_cache_files': True,
                    'max_cache_files': 1,
                },
                'storage': {
                    'backend': 's3',
                    'bucket_name': 'foo',
                }
            }
        }
    } | BASE_MUSIC_CONFIG
    cog = Music(fake_context['bot'], config, fake_engine)
    mocker.patch('discord_bot.cogs.music.sleep', return_value=True)
    mocker.patch.object(MusicPlayer, 'start_tasks')
    await cog.get_player(fake_context['guild'].id, ctx=fake_context['context'])
    with TemporaryDirectory() as tmp_dir:
        with fake_media_download(tmp_dir, fake_context=fake_context) as sd:
            with fake_media_download(tmp_dir, fake_context=fake_context) as sd2:
                mocker.patch('discord_bot.cogs.music_helpers.video_cache_client.upload_file', return_value=True)
                mocker.patch('discord_bot.cogs.music_helpers.video_cache_client.delete_file', return_value=True)
                cog.video_cache.iterate_file(sd)
                cog.video_cache.iterate_file(sd2)
                cog.video_cache.ready_remove()
                await cog.cache_cleanup()
                assert not cog.video_cache.get_webpage_url_item(sd.media_request)

@pytest.mark.asyncio
async def test_cache_cleanup_skips_source_in_transit(fake_engine, mocker, fake_context):  #pylint:disable=redefined-outer-name
    config = {
        'music': {
            'download': {
                'cache': {
                    'enable_cache_files': True,
                    'max_cache_files': 1,
                }
            }
        }
    } | BASE_MUSIC_CONFIG
    cog = Music(fake_context['bot'], config, fake_engine)
    mocker.patch('discord_bot.cogs.music.sleep', return_value=True)
    mocker.patch.object(MusicPlayer, 'start_tasks')
    await cog.get_player(fake_context['guild'].id, ctx=fake_context['context'])
    with TemporaryDirectory() as tmp_dir:
        with fake_media_download(tmp_dir, fake_context=fake_context, is_direct_search=True) as sd:
            with fake_media_download(tmp_dir, fake_context=fake_context, is_direct_search=True) as sd2:
                cog.video_cache.iterate_file(sd)
                cog.video_cache.iterate_file(sd2)
                cog.video_cache.ready_remove()
                cog.sources_in_transit[sd.media_request.uuid] = str(sd.base_path)
                await cog.cache_cleanup()
                assert cog.video_cache.get_webpage_url_item(sd.media_request)
