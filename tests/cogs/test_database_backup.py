import asyncio as _asyncio
import pytest
from freezegun import freeze_time

from discord_bot.cogs.database_backup import DatabaseBackup
from discord_bot.exceptions import CogMissingRequiredArg
from discord_bot.utils.integrations.s3 import ObjectStorageException

from tests.helpers import fake_context, fake_engine  #pylint:disable=unused-import


BASE_CONFIG = {
    'general': {
        'storage': {
            'backend': 's3'
        },
        'include': {
            'database_backup': True
        }
    },
    'database_backup': {
        'bucket_name': 'test-backup-bucket',
        'cron_schedule': '0 2 * * *'
    }
}


def test_database_backup_disabled(fake_context, fake_engine):  #pylint:disable=redefined-outer-name
    '''Test that cog raises error when disabled'''
    config = {
        'general': {
            'include': {
                'database_backup': False
            }
        }
    }
    with pytest.raises(CogMissingRequiredArg) as exc:
        DatabaseBackup(fake_context['bot'], config, fake_engine)
    assert 'Database backup not enabled' in str(exc.value)


def test_database_backup_requires_s3_backend(fake_context, fake_engine):  #pylint:disable=redefined-outer-name
    '''Test that cog requires s3 backend'''
    config = {
        'general': {
            'storage': {
                'backend': 'local'  # Not s3
            },
            'include': {
                'database_backup': True
            }
        },
        'database_backup': {
            'bucket_name': 'test-bucket',
            'cron_schedule': '0 2 * * *'
        }
    }
    with pytest.raises(CogMissingRequiredArg) as exc:
        DatabaseBackup(fake_context['bot'], config, fake_engine)
    assert 'Storage backend must be s3' in str(exc.value)


def test_database_backup_missing_storage_config(fake_context, fake_engine):  #pylint:disable=redefined-outer-name
    '''Test that cog requires storage backend to be configured'''
    config = {
        'general': {
            'include': {
                'database_backup': True
            }
        },
        'database_backup': {
            'bucket_name': 'test-bucket',
            'cron_schedule': '0 2 * * *'
        }
    }
    with pytest.raises(CogMissingRequiredArg) as exc:
        DatabaseBackup(fake_context['bot'], config, fake_engine)
    assert 'Storage backend must be s3' in str(exc.value)


def test_database_backup_requires_bucket_name(fake_context, fake_engine):  #pylint:disable=redefined-outer-name
    '''Test that bucket_name is required'''
    config = {
        'general': {
            'storage': {
                'backend': 's3'
            },
            'include': {
                'database_backup': True
            }
        },
        'database_backup': {
            'cron_schedule': '0 2 * * *'
        }
    }
    with pytest.raises(CogMissingRequiredArg) as exc:
        DatabaseBackup(fake_context['bot'], config, fake_engine)
    assert 'Invalid config given' in str(exc.value)


def test_database_backup_requires_cron_schedule(fake_context, fake_engine):  #pylint:disable=redefined-outer-name
    '''Test that cron_schedule is required'''
    config = {
        'general': {
            'storage': {
                'backend': 's3'
            },
            'include': {
                'database_backup': True
            }
        },
        'database_backup': {
            'bucket_name': 'test-bucket'
        }
    }
    with pytest.raises(CogMissingRequiredArg) as exc:
        DatabaseBackup(fake_context['bot'], config, fake_engine)
    assert 'Invalid config given' in str(exc.value)


def test_database_backup_init_success(fake_context, fake_engine):  #pylint:disable=redefined-outer-name
    '''Test successful initialization'''
    cog = DatabaseBackup(fake_context['bot'], BASE_CONFIG, fake_engine)

    assert cog.bucket_name == 'test-backup-bucket'
    assert cog.cron_schedule == '0 2 * * *'
    assert cog.object_prefix == 'backups/db/'  # Default value
    assert cog.backup_client is not None
    assert cog.db_engine == fake_engine


def test_database_backup_init_with_custom_prefix(fake_context, fake_engine):  #pylint:disable=redefined-outer-name
    '''Test initialization with custom object prefix'''
    config = BASE_CONFIG.copy()
    config['database_backup'] = {
        'bucket_name': 'test-bucket',
        'cron_schedule': '0 2 * * *',
        'object_prefix': 'custom/path/'
    }
    cog = DatabaseBackup(fake_context['bot'], config, fake_engine)

    assert cog.object_prefix == 'custom/path/'


def test_database_backup_heartbeat_callback_not_running(fake_context, fake_engine):  #pylint:disable=redefined-outer-name
    '''Test heartbeat callback when task is not running'''
    cog = DatabaseBackup(fake_context['bot'], BASE_CONFIG, fake_engine)

    # Task is None initially
    observations = cog._DatabaseBackup__loop_active_callback(None)  #pylint:disable=protected-access

    assert len(observations) == 1
    assert observations[0].value == 0  # Not running


def test_database_backup_heartbeat_callback_running(fake_context, fake_engine, mocker):  #pylint:disable=redefined-outer-name
    '''Test heartbeat callback when task is running'''
    cog = DatabaseBackup(fake_context['bot'], BASE_CONFIG, fake_engine)

    # Mock a running task
    mock_task = mocker.Mock()
    mock_task.done.return_value = False
    cog._task = mock_task  #pylint:disable=protected-access

    observations = cog._DatabaseBackup__loop_active_callback(None)  #pylint:disable=protected-access

    assert len(observations) == 1
    assert observations[0].value == 1  # Running


def test_database_backup_heartbeat_callback_done(fake_context, fake_engine, mocker):  #pylint:disable=redefined-outer-name
    '''Test heartbeat callback when task is done'''
    cog = DatabaseBackup(fake_context['bot'], BASE_CONFIG, fake_engine)

    # Mock a completed task
    mock_task = mocker.Mock()
    mock_task.done.return_value = True
    cog._task = mock_task  #pylint:disable=protected-access

    observations = cog._DatabaseBackup__loop_active_callback(None)  #pylint:disable=protected-access

    assert len(observations) == 1
    assert observations[0].value == 0  # Done


@pytest.mark.asyncio
@freeze_time('2025-12-04 00:00:00', tz_offset=0)
async def test_database_backup_loop_schedules_correctly(fake_context, fake_engine, mocker):  #pylint:disable=redefined-outer-name
    '''Test that backup loop calculates next run time correctly'''
    # Mock sleep to prevent actual waiting
    mock_sleep = mocker.patch('discord_bot.cogs.database_backup.sleep')

    # Mock backup components
    mock_backup_file = mocker.Mock()
    mock_backup_file.name = 'db_backup_2025-12-04_02-00-00.json'
    mock_backup_file.unlink = mocker.Mock()

    mocker.patch.object(
        DatabaseBackup,
        '_DatabaseBackup__loop_active_callback',
        return_value=[mocker.Mock(value=1)]
    )

    cog = DatabaseBackup(fake_context['bot'], BASE_CONFIG, fake_engine)
    cog.backup_client.create_backup = mocker.Mock(return_value=mock_backup_file)

    mocker.patch('discord_bot.cogs.database_backup.upload_file', return_value=True)

    await cog.database_backup_loop()

    # Should have calculated time until 2 AM (2 hours from frozen time)
    mock_sleep.assert_called_once()
    sleep_seconds = mock_sleep.call_args[0][0]
    assert sleep_seconds == 7200  # 2 hours in seconds


@pytest.mark.asyncio
@freeze_time('2025-12-04 00:00:00', tz_offset=0)
async def test_database_backup_loop_creates_backup(fake_context, fake_engine, mocker):  #pylint:disable=redefined-outer-name
    '''Test that backup loop creates backup file'''
    mocker.patch('discord_bot.cogs.database_backup.sleep')

    mock_backup_file = mocker.Mock()
    mock_backup_file.name = 'db_backup_2025-12-04_02-00-00.json'
    mock_backup_file.unlink = mocker.Mock()

    cog = DatabaseBackup(fake_context['bot'], BASE_CONFIG, fake_engine)
    mock_create_backup = mocker.patch.object(
        cog.backup_client,
        'create_backup',
        return_value=mock_backup_file
    )

    mocker.patch('discord_bot.cogs.database_backup.upload_file', return_value=True)

    await cog.database_backup_loop()

    # Should have called create_backup
    mock_create_backup.assert_called_once()


@pytest.mark.asyncio
@freeze_time('2025-12-04 00:00:00', tz_offset=0)
async def test_database_backup_loop_uploads_to_s3(fake_context, fake_engine, mocker):  #pylint:disable=redefined-outer-name
    '''Test that backup loop uploads file to S3'''
    mocker.patch('discord_bot.cogs.database_backup.sleep')

    mock_backup_file = mocker.Mock()
    mock_backup_file.name = 'db_backup_2025-12-04_02-00-00.json'
    mock_backup_file.unlink = mocker.Mock()

    cog = DatabaseBackup(fake_context['bot'], BASE_CONFIG, fake_engine)
    cog.backup_client.create_backup = mocker.Mock(return_value=mock_backup_file)

    mock_upload = mocker.patch('discord_bot.cogs.database_backup.upload_file', return_value=True)

    await cog.database_backup_loop()

    # Should have uploaded to S3
    mock_upload.assert_called_once_with(
        'test-backup-bucket',
        mock_backup_file,
        'backups/db/db_backup_2025-12-04_02-00-00.json'
    )


@pytest.mark.asyncio
@freeze_time('2025-12-04 00:00:00', tz_offset=0)
async def test_database_backup_loop_cleans_up_local_file(fake_context, fake_engine, mocker):  #pylint:disable=redefined-outer-name
    '''Test that backup loop deletes local file after upload'''
    mocker.patch('discord_bot.cogs.database_backup.sleep')

    mock_backup_file = mocker.Mock()
    mock_backup_file.name = 'db_backup_2025-12-04_02-00-00.json'
    mock_backup_file.unlink = mocker.Mock()

    cog = DatabaseBackup(fake_context['bot'], BASE_CONFIG, fake_engine)
    cog.backup_client.create_backup = mocker.Mock(return_value=mock_backup_file)

    mocker.patch('discord_bot.cogs.database_backup.upload_file', return_value=True)

    await cog.database_backup_loop()

    # Should have deleted local file
    mock_backup_file.unlink.assert_called_once()


@pytest.mark.asyncio
@freeze_time('2025-12-04 00:00:00', tz_offset=0)
async def test_database_backup_loop_success_flow(fake_context, fake_engine, mocker, caplog):  #pylint:disable=redefined-outer-name
    '''Test successful backup flow completes without errors'''
    mocker.patch('discord_bot.cogs.database_backup.sleep')

    mock_backup_file = mocker.Mock()
    mock_backup_file.name = 'db_backup_2025-12-04_02-00-00.json'
    mock_backup_file.unlink = mocker.Mock()

    cog = DatabaseBackup(fake_context['bot'], BASE_CONFIG, fake_engine)
    cog.backup_client.create_backup = mocker.Mock(return_value=mock_backup_file)

    mocker.patch('discord_bot.cogs.database_backup.upload_file', return_value=True)

    # Should complete without raising exception
    await cog.database_backup_loop()

    # Verify success message in logs
    assert 'Successfully uploaded backup' in caplog.text
    assert 's3://test-backup-bucket/' in caplog.text


def test_database_backup_restore_on_startup_config(fake_context, fake_engine):  #pylint:disable=redefined-outer-name
    '''Test that restore_on_startup config is parsed correctly'''
    config = {
        'general': {
            'storage': {'backend': 's3'},
            'include': {'database_backup': True}
        },
        'database_backup': {
            'bucket_name': 'test-bucket',
            'cron_schedule': '0 2 * * *',
            'restore_on_startup': True
        }
    }
    cog = DatabaseBackup(fake_context['bot'], config, fake_engine)
    assert cog.config.restore_on_startup is True


def test_database_backup_restore_on_startup_defaults_false(fake_context, fake_engine):  #pylint:disable=redefined-outer-name
    '''Test that restore_on_startup defaults to False'''
    cog = DatabaseBackup(fake_context['bot'], BASE_CONFIG, fake_engine)
    assert cog.config.restore_on_startup is False


def test_restore_on_startup_calls_restore_when_backup_found(fake_context, fake_engine, mocker):  #pylint:disable=redefined-outer-name
    '''_restore_on_startup calls restore_from_s3 when a backup key is found'''
    cog = DatabaseBackup(fake_context['bot'], BASE_CONFIG, fake_engine)
    mocker.patch.object(cog.backup_client, 'find_latest_backup', return_value='backups/db/latest.json')
    mock_restore = mocker.patch.object(cog.backup_client, 'restore_from_s3', return_value={
        'tables_restored': 3,
        'total_rows_inserted': 42,
        'tables': {},
        'metadata': {}
    })

    cog._restore_on_startup()  #pylint:disable=protected-access

    mock_restore.assert_called_once_with(cog.bucket_name, 'backups/db/latest.json',
                                         table_groups=None, on_table_restored=None)


def test_restore_on_startup_skips_restore_when_no_backup(fake_context, fake_engine, mocker, caplog):  #pylint:disable=redefined-outer-name
    '''_restore_on_startup logs info and does not call restore_from_s3 when no backup exists'''
    cog = DatabaseBackup(fake_context['bot'], BASE_CONFIG, fake_engine)
    mocker.patch.object(cog.backup_client, 'find_latest_backup', return_value=None)
    mock_restore = mocker.patch.object(cog.backup_client, 'restore_from_s3')

    cog._restore_on_startup()  #pylint:disable=protected-access

    mock_restore.assert_not_called()
    assert 'No backup found in S3' in caplog.text


def test_restore_on_startup_handles_s3_exception(fake_context, fake_engine, mocker, caplog):  #pylint:disable=redefined-outer-name
    '''_restore_on_startup logs warning and does not raise on ObjectStorageException'''
    cog = DatabaseBackup(fake_context['bot'], BASE_CONFIG, fake_engine)
    mocker.patch.object(cog.backup_client, 'find_latest_backup',
                        side_effect=ObjectStorageException('S3 unavailable'))

    # Should not raise
    cog._restore_on_startup()  #pylint:disable=protected-access

    assert 'Startup restore failed' in caplog.text


@pytest.mark.asyncio
@freeze_time('2025-12-04 00:00:00', tz_offset=0)
async def test_cog_load_runs_startup_restore_when_enabled(fake_context, fake_engine, mocker):  #pylint:disable=redefined-outer-name
    '''cog_load creates a background _restore_task when restore_on_startup=True'''
    config = {
        'general': {
            'storage': {'backend': 's3'},
            'include': {'database_backup': True}
        },
        'database_backup': {
            'bucket_name': 'test-bucket',
            'cron_schedule': '0 2 * * *',
            'restore_on_startup': True
        }
    }
    cog = DatabaseBackup(fake_context['bot'], config, fake_engine)
    fake_context['bot'].loop = mocker.Mock()
    fake_context['bot'].loop.create_task = mocker.Mock()

    await cog.cog_load()

    # create_task is called at least twice: once for _restore_task, once for _task
    assert fake_context['bot'].loop.create_task.call_count >= 2
    assert cog._restore_task is not None  #pylint:disable=protected-access


@pytest.mark.asyncio
@freeze_time('2025-12-04 00:00:00', tz_offset=0)
async def test_cog_load_skips_startup_restore_when_disabled(fake_context, fake_engine, mocker):  #pylint:disable=redefined-outer-name
    '''cog_load does not call _restore_on_startup when restore_on_startup=False'''
    cog = DatabaseBackup(fake_context['bot'], BASE_CONFIG, fake_engine)
    mock_restore = mocker.patch.object(cog, '_restore_on_startup')
    fake_context['bot'].loop = mocker.Mock()
    fake_context['bot'].loop.create_task = mocker.Mock()

    await cog.cog_load()

    mock_restore.assert_not_called()


@pytest.mark.asyncio
@freeze_time('2025-12-04 00:00:00', tz_offset=0)
async def test_database_backup_loop_upload_failure(fake_context, fake_engine, mocker, caplog):  #pylint:disable=redefined-outer-name
    '''Test that upload failure is logged'''
    mocker.patch('discord_bot.cogs.database_backup.sleep')

    mock_backup_file = mocker.Mock()
    mock_backup_file.name = 'db_backup_2025-12-04_02-00-00.json'
    mock_backup_file.unlink = mocker.Mock()

    cog = DatabaseBackup(fake_context['bot'], BASE_CONFIG, fake_engine)
    cog.backup_client.create_backup = mocker.Mock(return_value=mock_backup_file)

    # Upload fails
    mocker.patch('discord_bot.cogs.database_backup.upload_file', return_value=False)

    # Should complete without raising exception
    await cog.database_backup_loop()

    # Verify error message in logs
    assert 'Failed to upload backup to S3' in caplog.text


# ---------------------------------------------------------------------------
# _restore_on_startup_async
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_restore_on_startup_async_builds_table_groups(fake_context, fake_engine, mocker):  #pylint:disable=redefined-outer-name
    '''_restore_on_startup_async builds table_groups from bot.cogs in RESTORE_ORDER'''
    cog = DatabaseBackup(fake_context['bot'], BASE_CONFIG, fake_engine)

    music_cog = mocker.Mock()
    music_cog.REQUIRED_TABLES = ['playlist', 'playlist_item']
    markov_cog = mocker.Mock()
    markov_cog.REQUIRED_TABLES = ['markov_channel', 'markov_relation']
    fake_context['bot'].cogs = {'Music': music_cog, 'Markov': markov_cog}

    captured = {}

    def fake_restore(table_groups, _on_table_restored):
        captured['table_groups'] = table_groups

    mocker.patch.object(cog, '_restore_on_startup', side_effect=fake_restore)
    mocker.patch.object(cog, '_release_all_table_events')

    await cog._restore_on_startup_async()  #pylint:disable=protected-access

    assert captured['table_groups'] == [['playlist', 'playlist_item'], ['markov_channel', 'markov_relation']]


@pytest.mark.asyncio
async def test_restore_on_startup_async_skips_cog_without_required_tables(fake_context, fake_engine, mocker):  #pylint:disable=redefined-outer-name
    '''_restore_on_startup_async excludes cogs that lack REQUIRED_TABLES from table_groups'''
    cog = DatabaseBackup(fake_context['bot'], BASE_CONFIG, fake_engine)

    music_cog = mocker.Mock(spec=[])  # no REQUIRED_TABLES attribute
    fake_context['bot'].cogs = {'Music': music_cog}

    captured = {}

    def fake_restore(table_groups, _on_table_restored):
        captured['table_groups'] = table_groups

    mocker.patch.object(cog, '_restore_on_startup', side_effect=fake_restore)
    mocker.patch.object(cog, '_release_all_table_events')

    await cog._restore_on_startup_async()  #pylint:disable=protected-access

    assert captured['table_groups'] == []


@pytest.mark.asyncio
async def test_restore_on_startup_async_releases_events_after_restore(fake_context, fake_engine, mocker):  #pylint:disable=redefined-outer-name
    '''_restore_on_startup_async always calls _release_all_table_events after restore'''
    cog = DatabaseBackup(fake_context['bot'], BASE_CONFIG, fake_engine)
    fake_context['bot'].cogs = {}

    mocker.patch.object(cog, '_restore_on_startup')
    release_spy = mocker.patch.object(cog, '_release_all_table_events')

    await cog._restore_on_startup_async()  #pylint:disable=protected-access

    release_spy.assert_called_once()


@pytest.mark.asyncio
async def test_restore_on_startup_async_callback_sets_table_event(fake_context, fake_engine, mocker):  #pylint:disable=redefined-outer-name
    '''on_table_restored callback triggers the matching asyncio.Event'''
    cog = DatabaseBackup(fake_context['bot'], BASE_CONFIG, fake_engine)
    fake_context['bot'].cogs = {}

    # Capture the callback so we can call it directly
    captured_callback = {}

    def fake_restore(_table_groups, on_table_restored):
        captured_callback['fn'] = on_table_restored

    mocker.patch.object(cog, '_restore_on_startup', side_effect=fake_restore)
    mocker.patch.object(cog, '_release_all_table_events')

    await cog._restore_on_startup_async()  #pylint:disable=protected-access

    # Manually invoke the callback for a known table
    table_name = next(iter(cog._table_events))  #pylint:disable=protected-access
    assert not cog._table_events[table_name].is_set()  #pylint:disable=protected-access
    captured_callback['fn'](table_name)
    # call_soon_threadsafe schedules the set; run the loop briefly to process it
    await _asyncio.sleep(0)
    assert cog._table_events[table_name].is_set()  #pylint:disable=protected-access


# ---------------------------------------------------------------------------
# wait_for_tables
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_wait_for_tables_returns_when_events_already_set(fake_context, fake_engine):  #pylint:disable=redefined-outer-name
    '''wait_for_tables completes immediately when all named events are already set'''
    cog = DatabaseBackup(fake_context['bot'], BASE_CONFIG, fake_engine)

    tables = list(cog._table_events.keys())[:2]  #pylint:disable=protected-access
    for t in tables:
        cog._table_events[t].set()  #pylint:disable=protected-access

    # Should return without blocking
    await cog.wait_for_tables(tables)


@pytest.mark.asyncio
async def test_wait_for_tables_ignores_unknown_table_names(fake_context, fake_engine):  #pylint:disable=redefined-outer-name
    '''wait_for_tables silently ignores names that are not in _table_events'''
    cog = DatabaseBackup(fake_context['bot'], BASE_CONFIG, fake_engine)

    # Should not raise even with completely unknown names
    await cog.wait_for_tables(['nonexistent_table_xyz'])


# ---------------------------------------------------------------------------
# cog_unload
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_cog_unload_cancels_both_tasks(fake_context, fake_engine, mocker):  #pylint:disable=redefined-outer-name
    '''cog_unload cancels _task and _restore_task when both are set'''
    cog = DatabaseBackup(fake_context['bot'], BASE_CONFIG, fake_engine)

    mock_task = mocker.Mock()
    mock_restore_task = mocker.Mock()
    cog._task = mock_task  #pylint:disable=protected-access
    cog._restore_task = mock_restore_task  #pylint:disable=protected-access

    await cog.cog_unload()

    mock_task.cancel.assert_called_once()
    mock_restore_task.cancel.assert_called_once()


@pytest.mark.asyncio
async def test_cog_unload_handles_none_tasks(fake_context, fake_engine):  #pylint:disable=redefined-outer-name
    '''cog_unload does not raise when _task and _restore_task are None'''
    cog = DatabaseBackup(fake_context['bot'], BASE_CONFIG, fake_engine)
    # Both are None by default after __init__
    await cog.cog_unload()  # Should not raise
