from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from click.testing import CliRunner

from discord_bot.cli.db_backup import backup_main, restore_main, _get_backup_settings
from discord_bot.exceptions import DiscordBotException


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _write_minimal_config(path: str) -> None:
    '''Write a minimal valid YAML config that passes Click's exists=True check.'''
    from yaml import dump  # pylint: disable=import-outside-toplevel
    with open(path, 'w', encoding='utf-8') as f:
        dump({'general': {'discord_token': 'tok'}}, f)


def _make_general_config(sql='sqlite+aiosqlite:///:memory:'):
    cfg = MagicMock()
    cfg.sql_connection_statement = sql
    return cfg


_VALID_SETTINGS = {
    'general': {'storage': {'backend': 's3'}},
    'database_backup': {'bucket_name': 'my-bucket'},
}


# ---------------------------------------------------------------------------
# _get_backup_settings (unit tests — no config file needed)
# ---------------------------------------------------------------------------

def test_get_backup_settings_no_storage_backend():
    '''Missing storage config raises DiscordBotException.'''
    with pytest.raises(DiscordBotException, match='Storage backend must be s3'):
        _get_backup_settings({'general': {}})


def test_get_backup_settings_wrong_backend():
    '''Non-s3 storage backend raises DiscordBotException.'''
    settings = {'general': {'storage': {'backend': 'local'}}}
    with pytest.raises(DiscordBotException, match='Storage backend must be s3'):
        _get_backup_settings(settings)


def test_get_backup_settings_missing_bucket_name():
    '''Missing bucket_name raises DiscordBotException.'''
    settings = {'general': {'storage': {'backend': 's3'}}, 'database_backup': {}}
    with pytest.raises(DiscordBotException, match='bucket_name is required'):
        _get_backup_settings(settings)


def test_get_backup_settings_default_prefix():
    '''Valid settings return default object_prefix.'''
    bucket, prefix = _get_backup_settings(_VALID_SETTINGS)
    assert bucket == 'my-bucket'
    assert prefix == 'backups/db/'


def test_get_backup_settings_custom_prefix():
    '''Custom object_prefix is returned as-is.'''
    settings = {
        'general': {'storage': {'backend': 's3'}},
        'database_backup': {'bucket_name': 'b', 'object_prefix': 'custom/'},
    }
    bucket, prefix = _get_backup_settings(settings)
    assert bucket == 'b'
    assert prefix == 'custom/'


# ---------------------------------------------------------------------------
# backup_main
# ---------------------------------------------------------------------------

def test_backup_main_no_args():
    '''backup_main with no arguments exits with usage error.'''
    result = CliRunner().invoke(backup_main, [])
    assert result.exit_code == 2
    assert 'CONFIG_FILE' in result.output


def test_backup_main_config_not_s3(tmp_path):
    '''backup_main exits 1 when storage backend is not s3.'''
    cfg = tmp_path / 'config.yaml'
    _write_minimal_config(str(cfg))

    bad_settings = {'general': {'storage': {'backend': 'local'}}, 'database_backup': {}}
    general_config = _make_general_config()

    with patch('discord_bot.cli.db_backup.parse_and_validate_config',
               return_value=(bad_settings, general_config)):
        result = CliRunner().invoke(backup_main, [str(cfg)])

    assert result.exit_code == 1
    assert 'Storage backend must be s3' in result.output


def test_backup_main_missing_bucket_name(tmp_path):
    '''backup_main exits 1 when bucket_name is absent.'''
    cfg = tmp_path / 'config.yaml'
    _write_minimal_config(str(cfg))

    settings = {'general': {'storage': {'backend': 's3'}}, 'database_backup': {}}
    general_config = _make_general_config()

    with patch('discord_bot.cli.db_backup.parse_and_validate_config',
               return_value=(settings, general_config)):
        result = CliRunner().invoke(backup_main, [str(cfg)])

    assert result.exit_code == 1
    assert 'bucket_name is required' in result.output


def test_backup_main_missing_sql(tmp_path):
    '''backup_main exits 1 when sql_connection_statement is not set.'''
    cfg = tmp_path / 'config.yaml'
    _write_minimal_config(str(cfg))

    general_config = _make_general_config(sql=None)

    with patch('discord_bot.cli.db_backup.parse_and_validate_config',
               return_value=(_VALID_SETTINGS, general_config)):
        result = CliRunner().invoke(backup_main, [str(cfg)])

    assert result.exit_code == 1
    assert 'sql_connection_statement is required' in result.output


def test_backup_main_success(tmp_path, mocker):
    '''backup_main creates a backup, uploads it, deletes the local file, exits 0.'''
    cfg = tmp_path / 'config.yaml'
    _write_minimal_config(str(cfg))

    backup_file = tmp_path / 'db_backup_2024-01-01_00-00-00.json'
    backup_file.write_text('{}', encoding='utf-8')

    general_config = _make_general_config()

    mock_engine = _async_engine_mock(mocker)
    mock_client = mocker.MagicMock()
    mock_client.create_backup = AsyncMock(return_value=backup_file)
    mock_upload = mocker.patch('discord_bot.cli.db_backup.upload_file')

    with patch('discord_bot.cli.db_backup.parse_and_validate_config',
               return_value=(_VALID_SETTINGS, general_config)), \
         patch('discord_bot.cli.db_backup.create_async_engine', return_value=mock_engine), \
         patch('discord_bot.cli.db_backup.DatabaseBackupClient', return_value=mock_client):
        result = CliRunner().invoke(backup_main, [str(cfg)])

    assert result.exit_code == 0, result.output
    assert 's3://my-bucket/' in result.output
    mock_upload.assert_called_once()
    assert not backup_file.exists()


def test_backup_main_upload_exception(tmp_path, mocker):
    '''backup_main exits 1 and prints error when upload raises.'''
    cfg = tmp_path / 'config.yaml'
    _write_minimal_config(str(cfg))

    backup_file = tmp_path / 'db_backup.json'
    backup_file.write_text('{}', encoding='utf-8')

    general_config = _make_general_config()
    mock_engine = _async_engine_mock(mocker)
    mock_client = mocker.MagicMock()
    mock_client.create_backup = AsyncMock(return_value=backup_file)
    mocker.patch('discord_bot.cli.db_backup.upload_file', side_effect=RuntimeError('S3 down'))

    with patch('discord_bot.cli.db_backup.parse_and_validate_config',
               return_value=(_VALID_SETTINGS, general_config)), \
         patch('discord_bot.cli.db_backup.create_async_engine', return_value=mock_engine), \
         patch('discord_bot.cli.db_backup.DatabaseBackupClient', return_value=mock_client):
        result = CliRunner().invoke(backup_main, [str(cfg)])

    assert result.exit_code == 1
    assert 'Backup failed' in result.output


def test_backup_main_local_file_cleaned_up_on_upload_error(tmp_path, mocker):
    '''backup_main deletes the local backup file even when upload raises.'''
    cfg = tmp_path / 'config.yaml'
    _write_minimal_config(str(cfg))

    backup_file = tmp_path / 'db_backup.json'
    backup_file.write_text('{}', encoding='utf-8')

    general_config = _make_general_config()
    mock_engine = _async_engine_mock(mocker)
    mock_client = mocker.MagicMock()
    mock_client.create_backup = AsyncMock(return_value=backup_file)
    mocker.patch('discord_bot.cli.db_backup.upload_file', side_effect=RuntimeError('boom'))

    with patch('discord_bot.cli.db_backup.parse_and_validate_config',
               return_value=(_VALID_SETTINGS, general_config)), \
         patch('discord_bot.cli.db_backup.create_async_engine', return_value=mock_engine), \
         patch('discord_bot.cli.db_backup.DatabaseBackupClient', return_value=mock_client):
        CliRunner().invoke(backup_main, [str(cfg)])

    assert not backup_file.exists()


# ---------------------------------------------------------------------------
# restore_main
# ---------------------------------------------------------------------------

def test_restore_main_no_args():
    '''restore_main with no arguments exits with usage error.'''
    result = CliRunner().invoke(restore_main, [])
    assert result.exit_code == 2
    assert 'CONFIG_FILE' in result.output


def test_restore_main_config_not_s3(tmp_path):
    '''restore_main exits 1 when storage backend is not s3.'''
    cfg = tmp_path / 'config.yaml'
    _write_minimal_config(str(cfg))

    bad_settings = {'general': {'storage': {'backend': 'local'}}, 'database_backup': {}}
    general_config = _make_general_config()

    with patch('discord_bot.cli.db_backup.parse_and_validate_config',
               return_value=(bad_settings, general_config)):
        result = CliRunner().invoke(restore_main, [str(cfg)])

    assert result.exit_code == 1
    assert 'Storage backend must be s3' in result.output


def test_restore_main_missing_sql(tmp_path):
    '''restore_main exits 1 when sql_connection_statement is not set.'''
    cfg = tmp_path / 'config.yaml'
    _write_minimal_config(str(cfg))

    general_config = _make_general_config(sql=None)

    with patch('discord_bot.cli.db_backup.parse_and_validate_config',
               return_value=(_VALID_SETTINGS, general_config)):
        result = CliRunner().invoke(restore_main, [str(cfg), '--key', 'backups/x.json'])

    assert result.exit_code == 1
    assert 'sql_connection_statement is required' in result.output


def test_restore_main_explicit_key(tmp_path, mocker):
    '''restore_main with --key skips find_latest_backup and restores from that key.'''
    cfg = tmp_path / 'config.yaml'
    _write_minimal_config(str(cfg))

    general_config = _make_general_config()
    mock_engine = _async_engine_mock(mocker)
    mock_client, mock_restore = _restore_client_mock(mocker)

    with patch('discord_bot.cli.db_backup.parse_and_validate_config',
               return_value=(_VALID_SETTINGS, general_config)), \
         patch('discord_bot.cli.db_backup.create_async_engine', return_value=mock_engine), \
         patch('discord_bot.cli.db_backup.DatabaseBackupClient', return_value=mock_client):
        result = CliRunner().invoke(restore_main, [str(cfg), '--key', 'backups/db/backup.json'])

    assert result.exit_code == 0, result.output
    mock_restore.assert_called_once_with('my-bucket', 'backups/db/backup.json')
    assert 'Restored 2 tables' in result.output
    mock_client.find_latest_backup.assert_not_called()


def test_restore_main_latest_backup(tmp_path, mocker):
    '''restore_main without --key calls find_latest_backup and restores from the result.'''
    cfg = tmp_path / 'config.yaml'
    _write_minimal_config(str(cfg))

    general_config = _make_general_config()
    mock_engine = _async_engine_mock(mocker)
    mock_client, mock_restore = _restore_client_mock(mocker)
    mock_client.find_latest_backup.return_value = 'backups/db/latest.json'

    with patch('discord_bot.cli.db_backup.parse_and_validate_config',
               return_value=(_VALID_SETTINGS, general_config)), \
         patch('discord_bot.cli.db_backup.create_async_engine', return_value=mock_engine), \
         patch('discord_bot.cli.db_backup.DatabaseBackupClient', return_value=mock_client):
        result = CliRunner().invoke(restore_main, [str(cfg)])

    assert result.exit_code == 0, result.output
    mock_client.find_latest_backup.assert_called_once_with('my-bucket', 'backups/db/')
    mock_restore.assert_called_once_with('my-bucket', 'backups/db/latest.json')
    assert 'Using latest backup' in result.output


def test_restore_main_no_backups_found(tmp_path, mocker):
    '''restore_main exits 1 when no backups exist and --key is not given.'''
    cfg = tmp_path / 'config.yaml'
    _write_minimal_config(str(cfg))

    general_config = _make_general_config()
    mock_engine = _async_engine_mock(mocker)
    mock_client = mocker.MagicMock()
    mock_client.find_latest_backup.return_value = None

    with patch('discord_bot.cli.db_backup.parse_and_validate_config',
               return_value=(_VALID_SETTINGS, general_config)), \
         patch('discord_bot.cli.db_backup.create_async_engine', return_value=mock_engine), \
         patch('discord_bot.cli.db_backup.DatabaseBackupClient', return_value=mock_client):
        result = CliRunner().invoke(restore_main, [str(cfg)])

    assert result.exit_code == 1
    assert 'No backups found' in result.output


def test_restore_main_restore_exception(tmp_path, mocker):
    '''restore_main exits 1 and prints error when restore_from_s3 raises.'''
    cfg = tmp_path / 'config.yaml'
    _write_minimal_config(str(cfg))

    general_config = _make_general_config()
    mock_engine = _async_engine_mock(mocker)
    mock_client = mocker.MagicMock()
    mock_client.restore_from_s3 = AsyncMock(side_effect=RuntimeError('DB error'))

    with patch('discord_bot.cli.db_backup.parse_and_validate_config',
               return_value=(_VALID_SETTINGS, general_config)), \
         patch('discord_bot.cli.db_backup.create_async_engine', return_value=mock_engine), \
         patch('discord_bot.cli.db_backup.DatabaseBackupClient', return_value=mock_client):
        result = CliRunner().invoke(restore_main, [str(cfg), '--key', 'backups/x.json'])

    assert result.exit_code == 1
    assert 'Restore failed' in result.output


def test_restore_main_prints_per_table_stats(tmp_path, mocker):
    '''restore_main prints per-table row counts from stats.'''
    cfg = tmp_path / 'config.yaml'
    _write_minimal_config(str(cfg))

    general_config = _make_general_config()
    mock_engine = _async_engine_mock(mocker)
    mock_client = mocker.MagicMock()
    mock_client.restore_from_s3 = AsyncMock(return_value={
        'tables_restored': 2,
        'total_rows_inserted': 15,
        'tables': {'playlist': 10, 'playlist_item': 5},
    })

    with patch('discord_bot.cli.db_backup.parse_and_validate_config',
               return_value=(_VALID_SETTINGS, general_config)), \
         patch('discord_bot.cli.db_backup.create_async_engine', return_value=mock_engine), \
         patch('discord_bot.cli.db_backup.DatabaseBackupClient', return_value=mock_client):
        result = CliRunner().invoke(restore_main, [str(cfg), '--key', 'backups/x.json'])

    assert result.exit_code == 0, result.output
    assert 'playlist: 10 rows' in result.output
    assert 'playlist_item: 5 rows' in result.output


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _async_engine_mock(mocker):  # pylint: disable=unused-argument
    '''Return an async engine mock with a working begin() context manager.'''
    mock_conn = AsyncMock()
    mock_conn.run_sync = AsyncMock()

    class _FakeBegin:
        async def __aenter__(self):
            return mock_conn
        async def __aexit__(self, *args):
            return False

    mock_engine = MagicMock()
    mock_engine.begin = MagicMock(return_value=_FakeBegin())
    mock_engine.dispose = AsyncMock()
    return mock_engine


def _restore_client_mock(mocker):
    '''Return (mock_client, mock_restore_from_s3) with default stats.'''
    mock_restore = AsyncMock(return_value={
        'tables_restored': 2,
        'total_rows_inserted': 7,
        'tables': {'playlist': 7},
    })
    mock_client = mocker.MagicMock()
    mock_client.restore_from_s3 = mock_restore
    mock_client.find_latest_backup = mocker.MagicMock(return_value=None)
    return mock_client, mock_restore
