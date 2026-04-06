from pathlib import Path
from tempfile import TemporaryDirectory

import pytest
from sqlalchemy import select
from sqlalchemy.sql.functions import count as sql_count

from discord_bot.database import VideoCache
from discord_bot.cogs.music import Music
from discord_bot.exceptions import CogMissingRequiredArg

from discord_bot.cogs.music_helpers.music_player import MusicPlayer

from tests.cogs.test_music import BASE_MUSIC_CONFIG
from tests.helpers import async_mock_session, fake_media_download
from tests.helpers import fake_engine, fake_context #pylint:disable=unused-import


def test_cache_cleanup_enable_cache_files_requires_storage(fake_context):  #pylint:disable=redefined-outer-name
    '''enable_cache_files without storage raises CogMissingRequiredArg at construction time'''
    config = {
        'music': {
            'download': {
                'cache': {
                    'enable_cache_files': True,
                }
            }
        }
    } | BASE_MUSIC_CONFIG
    with pytest.raises(CogMissingRequiredArg, match='enable_cache_files requires storage'):
        Music(fake_context['bot'], config, fake_context['bot'])

@pytest.mark.asyncio
async def test_cache_cleanup_s3_upload_in_download_client(fake_engine, mocker, fake_context):  #pylint:disable=redefined-outer-name
    '''In S3 mode, upload_file is called by DownloadClient during create_source.
    cache_cleanup is a no-op while the entry is still in the broker registry.'''
    config = {
        'music': {
            'download': {
                'cache': {
                    'enable_cache_files': True,
                },
                'storage': {
                    'bucket_name': 'foo',
                }
            }
        }
    } | BASE_MUSIC_CONFIG
    cog = Music(fake_context['bot'], config, fake_engine)
    mocker.patch('discord_bot.cogs.music.sleep', return_value=True)
    mocker.patch.object(MusicPlayer, 'start_tasks')
    await cog.get_player(fake_context['guild'].id, ctx=fake_context['context'])
    upload_mock = mocker.patch('discord_bot.cogs.music_helpers.download_client.upload_file', return_value=True)
    with TemporaryDirectory() as tmp_dir:
        with fake_media_download(tmp_dir, fake_context=fake_context) as sd:
            # Simulate what DownloadClient does: upload then register with S3 key
            s3_key = f'cache/{sd.media_request.uuid}.mp3'
            upload_mock(cog.media_broker.bucket_name, sd.file_path, s3_key)
            sd.file_path = Path(s3_key)
            await cog.media_broker.register_download(sd)
            upload_mock.assert_called_once()
            async with async_mock_session(fake_engine) as session:
                assert (await session.execute(select(sql_count()).select_from(VideoCache))).scalar() == 1
            # cleanup is a no-op when entry is still in broker registry (AVAILABLE)
            result = await cog.media_broker.cache_cleanup()
            assert result is False

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
                delete_mock = mocker.patch('discord_bot.cogs.music_helpers.media_broker.delete_file', return_value=True)
                # Register via iterate_file only (no S3 upload — simulates pre-existing cache rows)
                await cog.video_cache.iterate_file(sd)
                await cog.video_cache.iterate_file(sd2)
                # Neither is in the broker registry, so both are evictable
                await cog.media_broker.cache_cleanup()
                delete_mock.assert_called_once()
                assert not await cog.media_broker.check_cache(sd.media_request)
