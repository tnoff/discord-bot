import pytest
from freezegun import freeze_time

from discord_bot.cogs.database_backup import DatabaseBackup
from discord_bot.exceptions import CogMissingRequiredArg

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


@pytest.mark.asyncio
@freeze_time('2025-12-04 00:00:00', tz_offset=0)
async def test_database_backup_loop_handles_exception(fake_context, fake_engine, mocker, caplog):  #pylint:disable=redefined-outer-name
    '''Test that exceptions during backup are caught and logged'''
    mocker.patch('discord_bot.cogs.database_backup.sleep')

    cog = DatabaseBackup(fake_context['bot'], BASE_CONFIG, fake_engine)

    # Make backup raise an exception
    mocker.patch.object(
        cog.backup_client,
        'create_backup',
        side_effect=Exception('Test error')
    )

    # Should not raise exception (caught internally)
    await cog.database_backup_loop()

    # Verify exception was logged
    assert 'Database backup failed' in caplog.text
    assert 'Test error' in caplog.text
