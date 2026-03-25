import json
import logging
import shutil
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch
import pytest
from sqlalchemy import text

from discord_bot.utils.database_backup_client import DatabaseBackupClient
from discord_bot.database import MarkovChannel, MarkovRelation, Playlist

from tests.helpers import fake_engine, mock_session  #pylint:disable=unused-import


def test_database_backup_client_init(fake_engine):  #pylint:disable=redefined-outer-name
    '''Test DatabaseBackupClient initialization'''
    client = DatabaseBackupClient(fake_engine)
    assert client.db_engine == fake_engine
    assert client.CHUNK_SIZE == 1000
    assert client.BATCH_SIZE == 1000


def test_create_backup_empty_database(fake_engine):  #pylint:disable=redefined-outer-name
    '''Test backup creation with empty database'''
    client = DatabaseBackupClient(fake_engine)

    backup_file = client.create_backup()

    # Verify file was created
    assert backup_file.exists()
    assert backup_file.name.startswith('db_backup_')
    assert backup_file.name.endswith('.json')

    # Verify JSON structure
    with open(backup_file, 'r', encoding='utf-8') as f:
        data = json.load(f)

    # Should have metadata
    assert isinstance(data, dict)
    assert '_metadata' in data
    assert 'backup_timestamp' in data['_metadata']
    assert 'alembic_version' in data['_metadata']
    assert 'table_count' in data['_metadata']

    # Should have all tables from BASE.metadata
    assert 'markov_channel' in data
    assert 'markov_relation' in data
    assert 'playlist' in data

    # All tables should be empty arrays
    for key, rows in data.items():
        if key != '_metadata':
            assert isinstance(rows, list)
            assert len(rows) == 0

    # Cleanup
    backup_file.unlink()


def test_create_backup_with_data(fake_engine):  #pylint:disable=redefined-outer-name
    '''Test backup creation with actual data'''
    # Add test data
    with mock_session(fake_engine) as session:
        # Add markov channels
        channel1 = MarkovChannel(channel_id='123', server_id='456', last_message_id='789')
        channel2 = MarkovChannel(channel_id='111', server_id='222', last_message_id='333')
        session.add(channel1)
        session.add(channel2)

        # Add markov relations
        relation1 = MarkovRelation(channel_id=1, leader_word='hello', follower_word='world')
        relation2 = MarkovRelation(channel_id=1, leader_word='world', follower_word='test')
        session.add(relation1)
        session.add(relation2)

        session.commit()

    client = DatabaseBackupClient(fake_engine)
    backup_file = client.create_backup()

    # Verify file was created
    assert backup_file.exists()

    # Verify JSON structure and data
    with open(backup_file, 'r', encoding='utf-8') as f:
        data = json.load(f)

    # Check markov_channel data
    assert len(data['markov_channel']) == 2
    assert data['markov_channel'][0]['channel_id'] == 123
    assert data['markov_channel'][0]['server_id'] == 456
    assert data['markov_channel'][1]['channel_id'] == 111

    # Check markov_relation data
    assert len(data['markov_relation']) == 2
    assert data['markov_relation'][0]['leader_word'] == 'hello'
    assert data['markov_relation'][0]['follower_word'] == 'world'

    # Cleanup
    backup_file.unlink()


def test_create_backup_streaming_large_table(fake_engine):  #pylint:disable=redefined-outer-name
    '''Test that backup streams data in chunks for large tables'''
    # Add more rows than CHUNK_SIZE to test streaming
    with mock_session(fake_engine) as session:
        for i in range(2500):  # More than 2x CHUNK_SIZE
            relation = MarkovRelation(
                channel_id=1,
                leader_word=f'word_{i}',
                follower_word=f'next_{i}'
            )
            session.add(relation)
        session.commit()

    client = DatabaseBackupClient(fake_engine)
    backup_file = client.create_backup()

    # Verify all data was backed up
    with open(backup_file, 'r', encoding='utf-8') as f:
        data = json.load(f)

    assert len(data['markov_relation']) == 2500

    # Verify data integrity
    assert data['markov_relation'][0]['leader_word'] == 'word_0'
    assert data['markov_relation'][1000]['leader_word'] == 'word_1000'
    assert data['markov_relation'][2499]['leader_word'] == 'word_2499'

    # Cleanup
    backup_file.unlink()


def test_create_backup_only_includes_base_tables(fake_engine):  #pylint:disable=redefined-outer-name
    '''Test that backup only includes tables from BASE.metadata'''
    # Create a table not in BASE.metadata
    with fake_engine.connect() as connection:
        connection.execute(text('CREATE TABLE custom_table (id INTEGER PRIMARY KEY, data TEXT)'))
        connection.execute(text("INSERT INTO custom_table VALUES (1, 'test')"))
        connection.commit()

    client = DatabaseBackupClient(fake_engine)
    backup_file = client.create_backup()

    with open(backup_file, 'r', encoding='utf-8') as f:
        data = json.load(f)

    # custom_table should NOT be in the backup
    assert 'custom_table' not in data

    # But BASE tables should be there
    assert 'markov_channel' in data
    assert 'playlist' in data

    # Cleanup
    backup_file.unlink()


def test_create_backup_handles_special_types(fake_engine):  #pylint:disable=redefined-outer-name
    '''Test that backup handles dates and special types correctly'''
    # Add data with dates/times
    with mock_session(fake_engine) as session:
        playlist = Playlist(
            server_id='123',
            name='test_playlist',
            is_history=False,
            created_at=datetime(2025, 1, 1, 12, 0, 0)
        )
        session.add(playlist)
        session.commit()

    client = DatabaseBackupClient(fake_engine)
    backup_file = client.create_backup()

    with open(backup_file, 'r', encoding='utf-8') as f:
        data = json.load(f)

    # Check that datetime was serialized to string
    assert len(data['playlist']) == 1
    assert data['playlist'][0]['name'] == 'test_playlist'
    # Datetime should be converted to string by default=str
    assert isinstance(data['playlist'][0]['created_at'], str)
    assert '2025-01-01' in data['playlist'][0]['created_at']

    # Cleanup
    backup_file.unlink()


def test_create_backup_logging(fake_engine, mocker):  #pylint:disable=redefined-outer-name
    '''Test that backup logs appropriate messages'''
    logger = mocker.Mock()

    # Add some test data
    with mock_session(fake_engine) as session:
        channel = MarkovChannel(channel_id='123', server_id='456')
        session.add(channel)
        session.commit()

    client = DatabaseBackupClient(fake_engine)
    with patch('discord_bot.utils.database_backup_client.logger', logger):
        backup_file = client.create_backup()

    # Check that debug logging was called for table backup
    debug_calls = [call.args[0] for call in logger.debug.call_args_list]
    assert any('Backing up table:' in call for call in debug_calls)

    # Check that info logging was called for completion
    info_calls = [call.args[0] for call in logger.info.call_args_list]
    assert any('Created backup file:' in call for call in info_calls)

    # Cleanup
    backup_file.unlink()


def test_create_backup_file_size(fake_engine, mocker):  #pylint:disable=redefined-outer-name
    '''Test that backup file size is logged correctly'''
    logger = mocker.Mock()

    client = DatabaseBackupClient(fake_engine)
    with patch('discord_bot.utils.database_backup_client.logger', logger):
        backup_file = client.create_backup()

    # Verify file size is logged
    logger.info.assert_called()
    log_message = logger.info.call_args[0][0]
    assert 'bytes' in log_message

    # Verify actual file size matches
    actual_size = backup_file.stat().st_size
    assert actual_size > 0
    assert str(actual_size) in log_message

    # Cleanup
    backup_file.unlink()


def test_create_backup_single_connection(fake_engine, mocker):  #pylint:disable=redefined-outer-name
    '''Test that create_backup uses a single connection for all tables (consistency fix)'''
    client = DatabaseBackupClient(fake_engine)
    connect_spy = mocker.spy(fake_engine, 'connect')

    backup_file = client.create_backup()

    # Only one connection should be opened for the table reads
    # (_get_alembic_version opens its own connection separately)
    assert connect_spy.call_count <= 2  # at most alembic check + one table connection

    backup_file.unlink()


def test_find_latest_backup_returns_key(fake_engine, mocker):  #pylint:disable=redefined-outer-name
    '''Returns the key of the most recent object'''
    t1 = datetime(2025, 6, 1, tzinfo=timezone.utc)
    t2 = datetime(2025, 5, 1, tzinfo=timezone.utc)
    mocker.patch(
        'discord_bot.utils.database_backup_client.list_objects',
        return_value=[
            {'key': 'backups/new.json', 'last_modified': t1},
            {'key': 'backups/old.json', 'last_modified': t2},
        ]
    )
    client = DatabaseBackupClient(fake_engine)

    result = client.find_latest_backup('my-bucket', 'backups/')

    assert result == 'backups/new.json'


def test_find_latest_backup_returns_none_when_empty(fake_engine, mocker):  #pylint:disable=redefined-outer-name
    '''Returns None when no objects exist under prefix'''
    mocker.patch(
        'discord_bot.utils.database_backup_client.list_objects',
        return_value=[]
    )
    client = DatabaseBackupClient(fake_engine)

    result = client.find_latest_backup('my-bucket', 'backups/')

    assert result is None


def test_restore_from_s3_calls_restore_backup(fake_engine, mocker):  #pylint:disable=redefined-outer-name
    '''restore_from_s3 downloads the file and calls restore_backup'''
    # Create a real backup file to restore from
    client = DatabaseBackupClient(fake_engine)
    backup_file = client.create_backup()

    def fake_get_file(_bucket, _key, path):
        shutil.copy(backup_file, path)
        return True

    mocker.patch('discord_bot.utils.database_backup_client.get_file', side_effect=fake_get_file)
    mock_restore = mocker.patch.object(client, 'restore_backup', wraps=client.restore_backup)

    stats = client.restore_from_s3('my-bucket', 'backups/latest.json')

    mock_restore.assert_called_once()
    call_kwargs = mock_restore.call_args
    assert call_kwargs.kwargs.get('clear_existing') is True or call_kwargs[1].get('clear_existing') is True
    assert isinstance(stats, dict)
    assert 'tables_restored' in stats

    backup_file.unlink()


def test_restore_from_s3_cleans_up_temp_file(fake_engine, mocker):  #pylint:disable=redefined-outer-name
    '''Temp file is deleted even if restore_backup raises'''
    tmp_paths = []

    original_named_temp = __import__('tempfile').NamedTemporaryFile

    def tracking_named_temp(*args, **kwargs):
        f = original_named_temp(*args, **kwargs)
        tmp_paths.append(Path(f.name))
        return f

    mocker.patch('discord_bot.utils.database_backup_client.tempfile.NamedTemporaryFile', side_effect=tracking_named_temp)
    mocker.patch('discord_bot.utils.database_backup_client.get_file', return_value=True)
    mocker.patch.object(
        DatabaseBackupClient,
        'restore_backup',
        side_effect=RuntimeError('restore failed')
    )

    client = DatabaseBackupClient(fake_engine)

    with __import__('pytest').raises(RuntimeError):
        client.restore_from_s3('my-bucket', 'backups/latest.json')

    # Temp file should be cleaned up
    for p in tmp_paths:
        assert not p.exists()


def test_backup_metadata_includes_alembic_version(fake_engine):  #pylint:disable=redefined-outer-name
    '''Test that backup includes alembic version metadata'''
    # Mock alembic_version table
    with fake_engine.connect() as connection:
        try:
            connection.execute(text('CREATE TABLE alembic_version (version_num VARCHAR(32))'))
            connection.execute(text("INSERT INTO alembic_version VALUES ('abc123def456')"))
            connection.commit()
        except Exception:  # pylint: disable=broad-except
            pass  # Table might already exist

    client = DatabaseBackupClient(fake_engine)
    backup_file = client.create_backup()

    with open(backup_file, 'r', encoding='utf-8') as f:
        data = json.load(f)

    # Check metadata
    assert '_metadata' in data
    assert data['_metadata']['alembic_version'] == 'abc123def456'
    assert 'backup_timestamp' in data['_metadata']
    assert isinstance(data['_metadata']['table_count'], int)

    # Cleanup
    backup_file.unlink()

# ---------------------------------------------------------------------------
# restore_backup — core paths
# ---------------------------------------------------------------------------

def test_restore_backup_file_not_found(fake_engine):  #pylint:disable=redefined-outer-name
    '''restore_backup raises FileNotFoundError when the path does not exist'''
    client = DatabaseBackupClient(fake_engine)
    with pytest.raises(FileNotFoundError):
        client.restore_backup(Path('/nonexistent/file.json'))


def test_restore_backup_restores_data(fake_engine):  #pylint:disable=redefined-outer-name
    '''restore_backup inserts rows from a backup file into empty tables'''
    with mock_session(fake_engine) as session:
        session.add(MarkovChannel(channel_id='10', server_id='20'))
        session.add(MarkovRelation(channel_id=1, leader_word='hello', follower_word='world'))
        session.commit()

    client = DatabaseBackupClient(fake_engine)
    backup_file = client.create_backup()

    with fake_engine.connect() as conn:
        conn.execute(text('DELETE FROM markov_relation'))
        conn.execute(text('DELETE FROM markov_channel'))
        conn.commit()

    stats = client.restore_backup(backup_file)

    assert stats['tables_restored'] >= 2
    assert stats['total_rows_inserted'] >= 2
    assert stats['tables'].get('markov_channel', 0) == 1
    assert stats['tables'].get('markov_relation', 0) == 1

    backup_file.unlink()


def test_restore_backup_clear_existing(fake_engine):  #pylint:disable=redefined-outer-name
    '''clear_existing=True truncates tables before restoring'''
    with mock_session(fake_engine) as session:
        session.add(MarkovChannel(channel_id='10', server_id='20'))
        session.commit()

    client = DatabaseBackupClient(fake_engine)
    backup_file = client.create_backup()

    # Add an extra row that should be wiped by clear_existing
    with mock_session(fake_engine) as session:
        session.add(MarkovChannel(channel_id='99', server_id='99'))
        session.commit()

    stats = client.restore_backup(backup_file, clear_existing=True)

    with fake_engine.connect() as conn:
        count = conn.execute(text('SELECT COUNT(*) FROM markov_channel')).scalar()
    assert count == 1
    assert stats['tables']['markov_channel'] == 1

    backup_file.unlink()


def test_restore_backup_skips_unknown_table(fake_engine, caplog):  #pylint:disable=redefined-outer-name
    '''Tables present in the backup but absent from the schema are skipped with a log message'''

    client = DatabaseBackupClient(fake_engine)
    backup_file = client.create_backup()

    with open(backup_file, 'r', encoding='utf-8') as f:
        data = json.load(f)
    data['unknown_table'] = [{'id': 1}]
    with open(backup_file, 'w', encoding='utf-8') as f:
        json.dump(data, f)

    with caplog.at_level(logging.INFO, logger='discord_bot.utils.database_backup_client'):
        stats = client.restore_backup(backup_file)

    assert 'unknown_table' not in stats['tables']
    assert 'unknown_table' in caplog.text

    backup_file.unlink()


def test_restore_backup_returns_metadata(fake_engine):  #pylint:disable=redefined-outer-name
    '''restore_backup populates stats["metadata"] from the backup file'''
    client = DatabaseBackupClient(fake_engine)
    backup_file = client.create_backup()

    stats = client.restore_backup(backup_file)

    assert 'backup_timestamp' in stats['metadata']
    assert 'table_count' in stats['metadata']

    backup_file.unlink()


# ---------------------------------------------------------------------------
# restore_backup — multi-pass (table_groups)
# ---------------------------------------------------------------------------

def test_restore_backup_table_groups_restores_data(fake_engine):  #pylint:disable=redefined-outer-name
    '''table_groups path restores each group and reports correct stats'''
    with mock_session(fake_engine) as session:
        session.add(MarkovChannel(channel_id='1', server_id='1'))
        session.add(MarkovRelation(channel_id=1, leader_word='foo', follower_word='bar'))
        session.commit()

    client = DatabaseBackupClient(fake_engine)
    backup_file = client.create_backup()

    with fake_engine.connect() as conn:
        conn.execute(text('DELETE FROM markov_relation'))
        conn.execute(text('DELETE FROM markov_channel'))
        conn.commit()

    stats = client.restore_backup(
        backup_file,
        table_groups=[['markov_channel'], ['markov_relation']],
    )

    assert stats['tables']['markov_channel'] == 1
    assert stats['tables']['markov_relation'] == 1
    # tables_restored includes empty ungrouped tables too; just check ours are present
    assert stats['tables_restored'] >= 2

    backup_file.unlink()


def test_restore_backup_table_groups_clear_existing(fake_engine):  #pylint:disable=redefined-outer-name
    '''table_groups + clear_existing=True truncates before restoring groups'''
    with mock_session(fake_engine) as session:
        session.add(MarkovChannel(channel_id='1', server_id='1'))
        session.commit()

    client = DatabaseBackupClient(fake_engine)
    backup_file = client.create_backup()

    with mock_session(fake_engine) as session:
        session.add(MarkovChannel(channel_id='99', server_id='99'))
        session.commit()

    client.restore_backup(backup_file, clear_existing=True,
                          table_groups=[['markov_channel']])

    with fake_engine.connect() as conn:
        count = conn.execute(text('SELECT COUNT(*) FROM markov_channel')).scalar()
    assert count == 1

    backup_file.unlink()


def test_restore_backup_table_groups_on_table_restored_callback(fake_engine):  #pylint:disable=redefined-outer-name
    '''on_table_restored is called once per restored table'''
    with mock_session(fake_engine) as session:
        session.add(MarkovChannel(channel_id='1', server_id='1'))
        session.add(MarkovRelation(channel_id=1, leader_word='a', follower_word='b'))
        session.commit()

    client = DatabaseBackupClient(fake_engine)
    backup_file = client.create_backup()

    with fake_engine.connect() as conn:
        conn.execute(text('DELETE FROM markov_relation'))
        conn.execute(text('DELETE FROM markov_channel'))
        conn.commit()

    restored = []
    client.restore_backup(
        backup_file,
        table_groups=[['markov_channel'], ['markov_relation']],
        on_table_restored=restored.append,
    )

    assert restored.count('markov_channel') == 1
    assert restored.count('markov_relation') == 1

    backup_file.unlink()


def test_restore_backup_table_groups_ungrouped_tables_restored_first(fake_engine):  #pylint:disable=redefined-outer-name
    '''Tables not in any group are restored in pass 0 before the named groups'''
    with mock_session(fake_engine) as session:
        pl = Playlist(server_id='1', name='p', is_history=False)
        session.add(pl)
        session.add(MarkovChannel(channel_id='1', server_id='1'))
        session.commit()

    client = DatabaseBackupClient(fake_engine)
    backup_file = client.create_backup()

    with fake_engine.connect() as conn:
        conn.execute(text('DELETE FROM playlist'))
        conn.execute(text('DELETE FROM markov_channel'))
        conn.commit()

    restore_order = []
    client.restore_backup(
        backup_file,
        table_groups=[['markov_channel']],   # playlist is ungrouped -> pass 0
        on_table_restored=restore_order.append,
    )

    playlist_idx = next((i for i, t in enumerate(restore_order) if t == 'playlist'), None)
    channel_idx = next((i for i, t in enumerate(restore_order) if t == 'markov_channel'), None)
    assert playlist_idx is not None
    assert channel_idx is not None
    assert playlist_idx < channel_idx

    backup_file.unlink()


def test_restore_backup_table_groups_skips_unknown_table(fake_engine, caplog):  #pylint:disable=redefined-outer-name
    '''_restore_pass logs and skips tables absent from the schema'''

    client = DatabaseBackupClient(fake_engine)
    backup_file = client.create_backup()

    with open(backup_file, 'r', encoding='utf-8') as f:
        data = json.load(f)
    data['ghost_table'] = [{'id': 1}]
    with open(backup_file, 'w', encoding='utf-8') as f:
        json.dump(data, f)

    with caplog.at_level(logging.INFO, logger='discord_bot.utils.database_backup_client'):
        stats = client.restore_backup(backup_file, table_groups=[['ghost_table']])

    assert 'ghost_table' not in stats['tables']
    assert 'ghost_table' in caplog.text

    backup_file.unlink()


# ---------------------------------------------------------------------------
# _restore_table
# ---------------------------------------------------------------------------

def test_restore_table_empty_rows_returns_zero(fake_engine):  #pylint:disable=redefined-outer-name
    '''_restore_table returns 0 and does nothing when rows is empty'''
    client = DatabaseBackupClient(fake_engine)
    with fake_engine.begin() as conn:
        result = client._restore_table(conn, 'markov_channel', [])  #pylint:disable=protected-access
    assert result == 0


def test_restore_table_insert_failure_continues(fake_engine, mocker, caplog):  #pylint:disable=redefined-outer-name
    '''_restore_table logs the error and skips failed batches rather than raising'''
    client = DatabaseBackupClient(fake_engine)

    with fake_engine.begin() as conn:
        mocker.patch.object(conn, 'execute', side_effect=Exception('db error'))
        result = client._restore_table(  #pylint:disable=protected-access
            conn, 'markov_channel', [{'channel_id': 1, 'server_id': 1}]
        )

    assert result == 0
    assert 'Failed to insert batch' in caplog.text


def test_restore_table_large_dataset_batching(fake_engine):  #pylint:disable=redefined-outer-name
    '''_restore_table inserts more rows than BATCH_SIZE correctly across multiple batches'''
    client = DatabaseBackupClient(fake_engine)
    rows = [{'channel_id': i, 'server_id': i} for i in range(1, 2502)]

    with fake_engine.begin() as conn:
        result = client._restore_table(conn, 'markov_channel', rows)  #pylint:disable=protected-access

    assert result == 2501
    with fake_engine.connect() as conn:
        count = conn.execute(text('SELECT COUNT(*) FROM markov_channel')).scalar()
    assert count == 2501

def test_restore_backup_mid_table_batch_flush(fake_engine):  #pylint:disable=redefined-outer-name
    '''restore_backup flushes row buffer mid-table when BATCH_SIZE is exceeded (single-pass path)'''
    with mock_session(fake_engine) as session:
        for i in range(1, 1502):   # More than BATCH_SIZE=1000
            session.add(MarkovRelation(channel_id=1, leader_word=f'w{i}', follower_word=f'n{i}'))
        session.commit()

    client = DatabaseBackupClient(fake_engine)
    backup_file = client.create_backup()

    with fake_engine.connect() as conn:
        conn.execute(text('DELETE FROM markov_relation'))
        conn.commit()

    stats = client.restore_backup(backup_file)

    assert stats['tables']['markov_relation'] == 1501
    with fake_engine.connect() as conn:
        count = conn.execute(text('SELECT COUNT(*) FROM markov_relation')).scalar()
    assert count == 1501

    backup_file.unlink()


def test_restore_backup_table_groups_mid_table_batch_flush(fake_engine):  #pylint:disable=redefined-outer-name
    '''_restore_pass flushes row buffer mid-table when BATCH_SIZE is exceeded (multi-pass path)'''
    with mock_session(fake_engine) as session:
        for i in range(1, 1502):
            session.add(MarkovRelation(channel_id=1, leader_word=f'w{i}', follower_word=f'n{i}'))
        session.commit()

    client = DatabaseBackupClient(fake_engine)
    backup_file = client.create_backup()

    with fake_engine.connect() as conn:
        conn.execute(text('DELETE FROM markov_relation'))
        conn.commit()

    stats = client.restore_backup(backup_file, table_groups=[['markov_relation']])

    assert stats['tables']['markov_relation'] == 1501

    backup_file.unlink()


def test_truncate_tables_handles_pragma_exception(fake_engine, mocker):  #pylint:disable=redefined-outer-name
    '''_truncate_tables silently ignores failures from PRAGMA statements'''
    client = DatabaseBackupClient(fake_engine)

    call_count = 0
    original_execute = None

    def execute_side_effect(stmt, *a, **kw):
        nonlocal call_count
        call_count += 1
        stmt_str = str(stmt)
        if 'PRAGMA' in stmt_str:
            raise RuntimeError('PRAGMA not supported')
        return original_execute(stmt, *a, **kw)

    with fake_engine.begin() as conn:
        original_execute = conn.execute
        mocker.patch.object(conn, 'execute', side_effect=execute_side_effect)
        # Should not raise even when PRAGMA fails
        client._truncate_tables(conn, [])  #pylint:disable=protected-access


def test_truncate_tables_handles_delete_exception(fake_engine, mocker, caplog):  #pylint:disable=redefined-outer-name
    '''_truncate_tables logs debug and continues when a DELETE fails'''

    client = DatabaseBackupClient(fake_engine)

    def execute_side_effect(stmt, *_a, **_kw):
        if 'DELETE' in str(stmt):
            raise RuntimeError('delete failed')

    with fake_engine.begin() as conn:
        mocker.patch.object(conn, 'execute', side_effect=execute_side_effect)
        with caplog.at_level(logging.DEBUG, logger='discord_bot.utils.database_backup_client'):
            client._truncate_tables(conn, list(__import__('discord_bot.database', fromlist=['BASE']).BASE.metadata.tables.keys()))  #pylint:disable=protected-access

    assert 'Failed to truncate' in caplog.text
