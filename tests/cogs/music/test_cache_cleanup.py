from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch

import pytest

from discord_bot.database import VideoCache, VideoCacheBackup
from discord_bot.cogs.music import Music
from discord_bot.cogs.music_helpers.music_player import MusicPlayer
from discord_bot.cogs.music_helpers.source_download import SourceDownload

from tests.helpers import mock_session, fake_source_download
from tests.helpers import fake_engine, fake_context #pylint:disable=unused-import

BASE_MUSIC_CONFIG = {
    'general': {
        'include': {
            'music': True
        }
    },
}

@pytest.mark.asyncio()
async def test_cache_cleanup_no_op(mocker, fake_engine, fake_context):  #pylint:disable=redefined-outer-name
    config = {
        'music': {
            'download': {
                'cache': {
                    'enable_cache_files': True,
                    'max_cache_files': 20
                }
            }
        }
    } | BASE_MUSIC_CONFIG

    cog = Music(fake_context['bot'], config, fake_engine)
    mocker.patch('discord_bot.cogs.music.sleep', return_value=True)
    mocker.patch.object(MusicPlayer, 'start_tasks')

    with TemporaryDirectory() as tmp_dir:
        with fake_source_download(tmp_dir, fake_context=fake_context) as sd:
            cog.video_cache.iterate_file(sd)

            with mock_session(fake_engine) as db_session:
                assert db_session.query(VideoCache).count() == 1

            await cog.cache_cleanup()

            with mock_session(fake_engine) as db_session:
                assert db_session.query(VideoCache).count() == 1

@pytest.mark.asyncio()
async def test_cache_cleanup_uploads_object_storage(fake_engine, mocker, fake_context):  #pylint:disable=redefined-outer-name
    config = {
        'music': {
            'download': {
                'cache': {
                    'enable_cache_files': True,
                    'max_cache_files': 0
                },
                'storage': {
                    'backend': 's3',
                    'bucket_name': 'test-bucket'
                }
            }
        }
    } | BASE_MUSIC_CONFIG

    mocker.patch('discord_bot.cogs.music.sleep', return_value=True)
    mocker.patch.object(MusicPlayer, 'start_tasks')
    fake_backup_client = mocker.patch('discord_bot.cogs.music.BackupClient')
    fake_backup_client.return_value.upload_file.return_value = True

    cog = Music(fake_context['bot'], config, fake_engine)

    with TemporaryDirectory() as tmp_dir:
        with fake_source_download(tmp_dir, fake_context=fake_context) as sd:
            cog.video_cache.iterate_file(sd)

            with mock_session(fake_engine) as db_session:
                assert db_session.query(VideoCache).count() == 1

            await cog.cache_cleanup()

            with mock_session(fake_engine) as db_session:
                assert db_session.query(VideoCache).count() == 0
                assert db_session.query(VideoCacheBackup).count() == 1

@pytest.mark.asyncio()
async def test_cache_cleanup_removes(fake_engine, mocker, fake_context):  #pylint:disable=redefined-outer-name
    config = {
        'music': {
            'download': {
                'cache': {
                    'enable_cache_files': True,
                    'max_cache_files': 0
                }
            }
        }
    } | BASE_MUSIC_CONFIG

    cog = Music(fake_context['bot'], config, fake_engine)
    mocker.patch('discord_bot.cogs.music.sleep', return_value=True)
    mocker.patch.object(MusicPlayer, 'start_tasks')

    with TemporaryDirectory() as tmp_dir:
        with fake_source_download(tmp_dir, fake_context=fake_context) as sd:
            cog.video_cache.iterate_file(sd)

            with mock_session(fake_engine) as db_session:
                assert db_session.query(VideoCache).count() == 1

            await cog.cache_cleanup()

            with mock_session(fake_engine) as db_session:
                assert db_session.query(VideoCache).count() == 0

@pytest.mark.asyncio()
async def test_cache_cleanup_skips_source_in_transit(fake_engine, mocker, fake_context):  #pylint:disable=redefined-outer-name
    config = {
        'music': {
            'download': {
                'cache': {
                    'enable_cache_files': True,
                    'max_cache_files': 0
                }
            }
        }
    } | BASE_MUSIC_CONFIG

    cog = Music(fake_context['bot'], config, fake_engine)
    mocker.patch('discord_bot.cogs.music.sleep', return_value=True)
    mocker.patch.object(MusicPlayer, 'start_tasks')

    with TemporaryDirectory() as tmp_dir:
        with fake_source_download(tmp_dir, fake_context=fake_context) as sd:
            cog.video_cache.iterate_file(sd)
            # Add item to in-transit dict to mimic source being processed
            cog.video_cache.processing_video_items[sd.webpage_url] = True

            with mock_session(fake_engine) as db_session:
                assert db_session.query(VideoCache).count() == 1

            await cog.cache_cleanup()

            with mock_session(fake_engine) as db_session:
                # Should still exist because it's in transit
                assert db_session.query(VideoCache).count() == 1