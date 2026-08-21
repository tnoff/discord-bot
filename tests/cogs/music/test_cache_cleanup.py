from pathlib import Path
from tempfile import TemporaryDirectory

import pytest
from sqlalchemy import select
from sqlalchemy.sql.functions import count as sql_count

from discord_bot.database import VideoCache
from discord_bot.cogs.music import Music
from discord_bot.exceptions import DiscordBotException

from discord_bot.cogs.music_helpers.music_player import MusicPlayer

from tests.cogs.test_music import music_config
from tests.helpers import async_mock_session, fake_media_download
from tests.helpers import fake_engine, fake_context #pylint:disable=unused-import
from tests.helpers import attach_in_process_broker


def test_cache_cleanup_enable_cache_files_requires_storage(fake_context):  #pylint:disable=redefined-outer-name
    '''enable_cache_files without storage is fatal at construction time.

    Raises DiscordBotException rather than CogMissingRequiredArg on purpose:
    load_cogs skips a cog that raises the latter, so a music section that IS
    present and does not validate would silently start a music-less bot.'''
    config = music_config({
        'music': {
            'download': {
                'cache': {
                    'enable_cache_files': True,
                }
            }
        }
    })
    with pytest.raises(DiscordBotException, match='enable_cache_files requires storage'):
        Music(fake_context['bot'], config, fake_context['bot'])

@pytest.mark.asyncio
async def test_cache_cleanup_s3_upload_in_download_client(fake_engine, mocker, fake_context):  #pylint:disable=redefined-outer-name
    '''In S3 mode, upload_file is called by InMemoryDownloadClient during create_source.
    cache_cleanup is a no-op while the entry is still in the broker registry.'''
    config = music_config({
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
    })
    cog = Music(fake_context['bot'], config, fake_context['dispatcher'], fake_engine)
    attach_in_process_broker(cog)
    mocker.patch('discord_bot.cogs.music.sleep', return_value=True)
    mocker.patch.object(MusicPlayer, 'start_tasks')
    await cog.get_player(fake_context['guild'].id, ctx=fake_context['context'])
    upload_mock = mocker.patch('discord_bot.interfaces.download_protocols.upload_file', return_value=True)
    with TemporaryDirectory() as tmp_dir:
        with fake_media_download(tmp_dir, fake_context=fake_context) as sd:
            # Simulate what InMemoryDownloadClient does: upload then register with S3 key
            s3_key = f'cache/{sd.media_request.uuid}.mp3'
            upload_mock(cog.broker_client.local_broker.bucket_name, sd.file_path, s3_key)
            sd.file_path = Path(s3_key)
            await cog.broker_client.register_download(sd)
            upload_mock.assert_called_once()
            async with async_mock_session(fake_engine) as session:
                assert (await session.execute(select(sql_count()).select_from(VideoCache))).scalar() == 1
            # cleanup is a no-op when entry is still in broker registry (AVAILABLE)
            result = await cog.broker_client.cache_cleanup()
            assert result is False

@pytest.mark.asyncio
async def test_cache_cleanup_removes(fake_engine, mocker, fake_context):  #pylint:disable=redefined-outer-name
    config = music_config({
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
    })
    cog = Music(fake_context['bot'], config, fake_context['dispatcher'], fake_engine)
    attach_in_process_broker(cog)
    mocker.patch('discord_bot.cogs.music.sleep', return_value=True)
    mocker.patch.object(MusicPlayer, 'start_tasks')
    await cog.get_player(fake_context['guild'].id, ctx=fake_context['context'])
    with TemporaryDirectory() as tmp_dir:
        with fake_media_download(tmp_dir, fake_context=fake_context) as sd:
            with fake_media_download(tmp_dir, fake_context=fake_context) as sd2:
                delete_mock = mocker.patch('discord_bot.interfaces.broker_protocols.delete_file', return_value=True)
                # Register via iterate_file only (no S3 upload — simulates pre-existing cache rows)
                await cog.broker_client.local_broker.video_cache.iterate_file(sd)
                await cog.broker_client.local_broker.video_cache.iterate_file(sd2)
                # Neither is in the broker registry, so both are evictable
                await cog.broker_client.cache_cleanup()
                delete_mock.assert_called_once()
                assert not await cog.broker_client.check_cache(sd.media_request)
