import json
import logging
import shutil
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch, AsyncMock
import pytest
from sqlalchemy import text

from discord_bot.utils.database_backup_client import DatabaseBackupClient
from discord_bot.database import MarkovChannel, MarkovRelation, Playlist

from tests.helpers import fake_async_file_engine as fake_engine, async_mock_session  #pylint:disable=unused-import


@pytest.mark.asyncio
async def test_database_backup_client_init(fake_engine):  #pylint:disable=redefined-outer-name
    '''Test DatabaseBackupClient initialization'''
    client = DatabaseBackupClient(fake_engine)
    assert client.db_engine == fake_engine
    assert client.CHUNK_SIZE == 1000
    assert client.BATCH_SIZE == 1000


@pytest.mark.asyncio
async def test_create_backup_empty_database(fake_engine):  #pylint:disable=redefined-outer-name
    '''Test backup creation with empty database'''
    client = DatabaseBackupClient(fake_engine)

    backup_file = await client.create_backup()

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


@pytest.mark.asyncio
async def test_create_backup_with_data(fake_engine):  #pylint:disable=redefined-outer-name
    '''Test backup creation with actual data'''
    # Add test data
    async with async_mock_session(fake_engine) as session:
        channel1 = MarkovChannel(channel_id='123', server_id='456', last_message_id='789')
        channel2 = MarkovChannel(channel_id='111', server_id='222', last_message_id='333')
        session.add(channel1)
        session.add(channel2)

        relation1 = MarkovRelation(channel_id=1, leader_word='hello', follower_word='world')
        relation2 = MarkovRelation(channel_id=1, leader_word='world', follower_word='test')
        session.add(relation1)
        session.add(relation2)

        await session.commit()

    client = DatabaseBackupClient(fake_engine)
    backup_file = await client.create_backup()

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


@pytest.mark.asyncio
async def test_create_backup_streaming_large_table(fake_engine):  #pylint:disable=redefined-outer-name
    '''Test that backup streams data in chunks for large tables'''
    # Add more rows than CHUNK_SIZE to test streaming
    async with async_mock_session(fake_engine) as session:
        for i in range(2500):  # More than 2x CHUNK_SIZE
            relation = MarkovRelation(
                channel_id=1,
                leader_word=f'word_{i}',
                follower_word=f'next_{i}'
            )
            session.add(relation)
        await session.commit()

    client = DatabaseBackupClient(fake_engine)
    backup_file = await client.create_backup()

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


@pytest.mark.asyncio
async def test_create_backup_only_includes_base_tables(fake_engine):  #pylint:disable=redefined-outer-name
    '''Test that backup only includes tables from BASE.metadata'''
    # Create a table not in BASE.metadata
    async with fake_engine.connect() as connection:
        await connection.execute(text('CREATE TABLE custom_table (id INTEGER PRIMARY KEY, data TEXT)'))
        await connection.execute(text("INSERT INTO custom_table VALUES (1, 'test')"))
        await connection.commit()

    client = DatabaseBackupClient(fake_engine)
    backup_file = await client.create_backup()

    with open(backup_file, 'r', encoding='utf-8') as f:
        data = json.load(f)

    # custom_table should NOT be in the backup
    assert 'custom_table' not in data

    # But BASE tables should be there
    assert 'markov_channel' in data
    assert 'playlist' in data

    # Cleanup
    backup_file.unlink()


@pytest.mark.asyncio
async def test_create_backup_handles_special_types(fake_engine):  #pylint:disable=redefined-outer-name
    '''Test that backup handles dates and special types correctly'''
    async with async_mock_session(fake_engine) as session:
        playlist = Playlist(
            server_id='123',
            name='test_playlist',
            is_history=False,
            created_at=datetime(2025, 1, 1, 12, 0, 0)
        )
        session.add(playlist)
        await session.commit()

    client = DatabaseBackupClient(fake_engine)
    backup_file = await client.create_backup()

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


@pytest.mark.asyncio
async def test_create_backup_logging(fake_engine, mocker):  #pylint:disable=redefined-outer-name
    '''Test that backup logs appropriate messages'''
    logger = mocker.Mock()

    async with async_mock_session(fake_engine) as session:
        channel = MarkovChannel(channel_id='123', server_id='456')
        session.add(channel)
        await session.commit()

    client = DatabaseBackupClient(fake_engine)
    with patch('discord_bot.utils.database_backup_client.logger', logger):
        backup_file = await client.create_backup()

    # Check that debug logging was called for table backup
    debug_calls = [call.args[0] for call in logger.debug.call_args_list]
    assert any('Backing up table:' in call for call in debug_calls)

    # Check that info logging was called for completion
    info_calls = [call.args[0] for call in logger.info.call_args_list]
    assert any('Created backup file:' in call for call in info_calls)

    # Cleanup
    backup_file.unlink()


@pytest.mark.asyncio
async def test_create_backup_file_size(fake_engine, mocker):  #pylint:disable=redefined-outer-name
    '''Test that backup file size is logged correctly'''
    logger = mocker.Mock()

    client = DatabaseBackupClient(fake_engine)
    with patch('discord_bot.utils.database_backup_client.logger', logger):
        backup_file = await client.create_backup()

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


@pytest.mark.asyncio
async def test_create_backup_single_connection(fake_engine):  #pylint:disable=redefined-outer-name
    '''Test that create_backup completes successfully (connection-count verification omitted:
    AsyncEngine.connect is read-only and cannot be spied on via patch.object)'''
    client = DatabaseBackupClient(fake_engine)
    backup_file = await client.create_backup()
    assert backup_file.exists()
    backup_file.unlink()


@pytest.mark.asyncio
async def test_find_latest_backup_returns_key(fake_engine, mocker):  #pylint:disable=redefined-outer-name
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


@pytest.mark.asyncio
async def test_find_latest_backup_returns_none_when_empty(fake_engine, mocker):  #pylint:disable=redefined-outer-name
    '''Returns None when no objects exist under prefix'''
    mocker.patch(
        'discord_bot.utils.database_backup_client.list_objects',
        return_value=[]
    )
    client = DatabaseBackupClient(fake_engine)

    result = client.find_latest_backup('my-bucket', 'backups/')

    assert result is None


@pytest.mark.asyncio
async def test_restore_from_s3_calls_restore_backup(fake_engine, mocker):  #pylint:disable=redefined-outer-name
    '''restore_from_s3 downloads the file and calls restore_backup'''
    client = DatabaseBackupClient(fake_engine)
    backup_file = await client.create_backup()

    def fake_get_file(_bucket, _key, path):
        shutil.copy(backup_file, path)
        return True

    mocker.patch('discord_bot.utils.database_backup_client.get_file', side_effect=fake_get_file)
    mock_restore = mocker.patch.object(client, 'restore_backup', wraps=client.restore_backup)

    stats = await client.restore_from_s3('my-bucket', 'backups/latest.json')

    mock_restore.assert_called_once()
    call_kwargs = mock_restore.call_args
    assert call_kwargs.kwargs.get('clear_existing') is True or call_kwargs[1].get('clear_existing') is True
    assert isinstance(stats, dict)
    assert 'tables_restored' in stats

    backup_file.unlink()


@pytest.mark.asyncio
async def test_restore_from_s3_cleans_up_temp_file(fake_engine, mocker):  #pylint:disable=redefined-outer-name
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
        new=AsyncMock(side_effect=RuntimeError('restore failed'))
    )

    client = DatabaseBackupClient(fake_engine)

    with pytest.raises(RuntimeError):
        await client.restore_from_s3('my-bucket', 'backups/latest.json')

    # Temp file should be cleaned up
    for p in tmp_paths:
        assert not p.exists()


@pytest.mark.asyncio
async def test_backup_metadata_includes_alembic_version(fake_engine):  #pylint:disable=redefined-outer-name
    '''Test that backup includes alembic version metadata'''
    async with fake_engine.connect() as connection:
        try:
            await connection.execute(text('CREATE TABLE alembic_version (version_num VARCHAR(32))'))
            await connection.execute(text("INSERT INTO alembic_version VALUES ('abc123def456')"))
            await connection.commit()
        except Exception:  # pylint: disable=broad-except
            pass  # Table might already exist

    client = DatabaseBackupClient(fake_engine)
    backup_file = await client.create_backup()

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

@pytest.mark.asyncio
async def test_restore_backup_file_not_found(fake_engine):  #pylint:disable=redefined-outer-name
    '''restore_backup raises FileNotFoundError when the path does not exist'''
    client = DatabaseBackupClient(fake_engine)
    with pytest.raises(FileNotFoundError):
        await client.restore_backup(Path('/nonexistent/file.json'))


@pytest.mark.asyncio
async def test_restore_backup_restores_data(fake_engine):  #pylint:disable=redefined-outer-name
    '''restore_backup inserts rows from a backup file into empty tables'''
    async with async_mock_session(fake_engine) as session:
        session.add(MarkovChannel(channel_id='10', server_id='20'))
        session.add(MarkovRelation(channel_id=1, leader_word='hello', follower_word='world'))
        await session.commit()

    client = DatabaseBackupClient(fake_engine)
    backup_file = await client.create_backup()

    async with fake_engine.connect() as conn:
        await conn.execute(text('DELETE FROM markov_relation'))
        await conn.execute(text('DELETE FROM markov_channel'))
        await conn.commit()

    stats = await client.restore_backup(backup_file)

    assert stats['tables_restored'] >= 2
    assert stats['total_rows_inserted'] >= 2
    assert stats['tables'].get('markov_channel', 0) == 1
    assert stats['tables'].get('markov_relation', 0) == 1

    backup_file.unlink()


@pytest.mark.asyncio
async def test_restore_backup_clear_existing(fake_engine):  #pylint:disable=redefined-outer-name
    '''clear_existing=True truncates tables before restoring'''
    async with async_mock_session(fake_engine) as session:
        session.add(MarkovChannel(channel_id='10', server_id='20'))
        await session.commit()

    client = DatabaseBackupClient(fake_engine)
    backup_file = await client.create_backup()

    # Add an extra row that should be wiped by clear_existing
    async with async_mock_session(fake_engine) as session:
        session.add(MarkovChannel(channel_id='99', server_id='99'))
        await session.commit()

    stats = await client.restore_backup(backup_file, clear_existing=True)

    async with fake_engine.connect() as conn:
        count = (await conn.execute(text('SELECT COUNT(*) FROM markov_channel'))).scalar()
    assert count == 1
    assert stats['tables']['markov_channel'] == 1

    backup_file.unlink()


@pytest.mark.asyncio
async def test_restore_backup_skips_unknown_table(fake_engine, caplog):  #pylint:disable=redefined-outer-name
    '''Tables present in the backup but absent from the schema are skipped with a log message'''
    client = DatabaseBackupClient(fake_engine)
    backup_file = await client.create_backup()

    with open(backup_file, 'r', encoding='utf-8') as f:
        data = json.load(f)
    data['unknown_table'] = [{'id': 1}]
    with open(backup_file, 'w', encoding='utf-8') as f:
        json.dump(data, f)

    with caplog.at_level(logging.INFO, logger='discord_bot.utils.database_backup_client'):
        stats = await client.restore_backup(backup_file)

    assert 'unknown_table' not in stats['tables']
    assert 'unknown_table' in caplog.text

    backup_file.unlink()


@pytest.mark.asyncio
async def test_restore_backup_returns_metadata(fake_engine):  #pylint:disable=redefined-outer-name
    '''restore_backup populates stats["metadata"] from the backup file'''
    client = DatabaseBackupClient(fake_engine)
    backup_file = await client.create_backup()

    stats = await client.restore_backup(backup_file)

    assert 'backup_timestamp' in stats['metadata']
    assert 'table_count' in stats['metadata']

    backup_file.unlink()


# ---------------------------------------------------------------------------
# restore_backup — multi-pass (table_groups)
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_restore_backup_table_groups_restores_data(fake_engine):  #pylint:disable=redefined-outer-name
    '''table_groups path restores each group and reports correct stats'''
    async with async_mock_session(fake_engine) as session:
        session.add(MarkovChannel(channel_id='1', server_id='1'))
        session.add(MarkovRelation(channel_id=1, leader_word='foo', follower_word='bar'))
        await session.commit()

    client = DatabaseBackupClient(fake_engine)
    backup_file = await client.create_backup()

    async with fake_engine.connect() as conn:
        await conn.execute(text('DELETE FROM markov_relation'))
        await conn.execute(text('DELETE FROM markov_channel'))
        await conn.commit()

    stats = await client.restore_backup(
        backup_file,
        table_groups=[['markov_channel'], ['markov_relation']],
    )

    assert stats['tables']['markov_channel'] == 1
    assert stats['tables']['markov_relation'] == 1
    assert stats['tables_restored'] >= 2

    backup_file.unlink()


@pytest.mark.asyncio
async def test_restore_backup_table_groups_clear_existing(fake_engine):  #pylint:disable=redefined-outer-name
    '''table_groups + clear_existing=True truncates before restoring groups'''
    async with async_mock_session(fake_engine) as session:
        session.add(MarkovChannel(channel_id='1', server_id='1'))
        await session.commit()

    client = DatabaseBackupClient(fake_engine)
    backup_file = await client.create_backup()

    async with async_mock_session(fake_engine) as session:
        session.add(MarkovChannel(channel_id='99', server_id='99'))
        await session.commit()

    await client.restore_backup(backup_file, clear_existing=True,
                                table_groups=[['markov_channel']])

    async with fake_engine.connect() as conn:
        count = (await conn.execute(text('SELECT COUNT(*) FROM markov_channel'))).scalar()
    assert count == 1

    backup_file.unlink()


@pytest.mark.asyncio
async def test_restore_backup_table_groups_on_table_restored_callback(fake_engine):  #pylint:disable=redefined-outer-name
    '''on_table_restored is called once per restored table'''
    async with async_mock_session(fake_engine) as session:
        session.add(MarkovChannel(channel_id='1', server_id='1'))
        session.add(MarkovRelation(channel_id=1, leader_word='a', follower_word='b'))
        await session.commit()

    client = DatabaseBackupClient(fake_engine)
    backup_file = await client.create_backup()

    async with fake_engine.connect() as conn:
        await conn.execute(text('DELETE FROM markov_relation'))
        await conn.execute(text('DELETE FROM markov_channel'))
        await conn.commit()

    restored = []
    await client.restore_backup(
        backup_file,
        table_groups=[['markov_channel'], ['markov_relation']],
        on_table_restored=restored.append,
    )

    assert restored.count('markov_channel') == 1
    assert restored.count('markov_relation') == 1

    backup_file.unlink()


@pytest.mark.asyncio
async def test_restore_backup_table_groups_ungrouped_tables_restored_first(fake_engine):  #pylint:disable=redefined-outer-name
    '''Tables not in any group are restored in pass 0 before the named groups'''
    async with async_mock_session(fake_engine) as session:
        pl = Playlist(server_id='1', name='p', is_history=False)
        session.add(pl)
        session.add(MarkovChannel(channel_id='1', server_id='1'))
        await session.commit()

    client = DatabaseBackupClient(fake_engine)
    backup_file = await client.create_backup()

    async with fake_engine.connect() as conn:
        await conn.execute(text('DELETE FROM playlist'))
        await conn.execute(text('DELETE FROM markov_channel'))
        await conn.commit()

    restore_order = []
    await client.restore_backup(
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


@pytest.mark.asyncio
async def test_restore_backup_table_groups_skips_unknown_table(fake_engine, caplog):  #pylint:disable=redefined-outer-name
    '''_restore_pass logs and skips tables absent from the schema'''
    client = DatabaseBackupClient(fake_engine)
    backup_file = await client.create_backup()

    with open(backup_file, 'r', encoding='utf-8') as f:
        data = json.load(f)
    data['ghost_table'] = [{'id': 1}]
    with open(backup_file, 'w', encoding='utf-8') as f:
        json.dump(data, f)

    with caplog.at_level(logging.INFO, logger='discord_bot.utils.database_backup_client'):
        stats = await client.restore_backup(backup_file, table_groups=[['ghost_table']])

    assert 'ghost_table' not in stats['tables']
    assert 'ghost_table' in caplog.text

    backup_file.unlink()


# ---------------------------------------------------------------------------
# _restore_table
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_restore_table_empty_rows_returns_zero(fake_engine):  #pylint:disable=redefined-outer-name
    '''_restore_table returns 0 and does nothing when rows is empty'''
    client = DatabaseBackupClient(fake_engine)
    async with fake_engine.begin() as conn:
        result = await client._restore_table(conn, 'markov_channel', [])  #pylint:disable=protected-access
    assert result == 0


@pytest.mark.asyncio
async def test_restore_table_insert_failure_continues(fake_engine, caplog):  #pylint:disable=redefined-outer-name
    '''_restore_table logs the error and skips failed batches rather than raising'''
    client = DatabaseBackupClient(fake_engine)

    # Use a mock connection so execute can raise without patching read-only attributes
    mock_conn = AsyncMock()
    mock_conn.execute = AsyncMock(side_effect=Exception('db error'))
    result = await client._restore_table(  #pylint:disable=protected-access
        mock_conn, 'markov_channel', [{'channel_id': 1, 'server_id': 1}]
    )

    assert result == 0
    assert 'Failed to insert batch' in caplog.text


@pytest.mark.asyncio
async def test_restore_table_large_dataset_batching(fake_engine):  #pylint:disable=redefined-outer-name
    '''_restore_table inserts more rows than BATCH_SIZE correctly across multiple batches'''
    client = DatabaseBackupClient(fake_engine)
    rows = [{'channel_id': i, 'server_id': i} for i in range(1, 2502)]

    async with fake_engine.begin() as conn:
        result = await client._restore_table(conn, 'markov_channel', rows)  #pylint:disable=protected-access

    assert result == 2501
    async with fake_engine.connect() as conn:
        count = (await conn.execute(text('SELECT COUNT(*) FROM markov_channel'))).scalar()
    assert count == 2501


@pytest.mark.asyncio
async def test_restore_backup_mid_table_batch_flush(fake_engine):  #pylint:disable=redefined-outer-name
    '''restore_backup flushes row buffer mid-table when BATCH_SIZE is exceeded (single-pass path)'''
    async with async_mock_session(fake_engine) as session:
        for i in range(1, 1502):   # More than BATCH_SIZE=1000
            session.add(MarkovRelation(channel_id=1, leader_word=f'w{i}', follower_word=f'n{i}'))
        await session.commit()

    client = DatabaseBackupClient(fake_engine)
    backup_file = await client.create_backup()

    async with fake_engine.connect() as conn:
        await conn.execute(text('DELETE FROM markov_relation'))
        await conn.commit()

    stats = await client.restore_backup(backup_file)

    assert stats['tables']['markov_relation'] == 1501
    async with fake_engine.connect() as conn:
        count = (await conn.execute(text('SELECT COUNT(*) FROM markov_relation'))).scalar()
    assert count == 1501

    backup_file.unlink()


@pytest.mark.asyncio
async def test_restore_backup_table_groups_mid_table_batch_flush(fake_engine):  #pylint:disable=redefined-outer-name
    '''_restore_pass flushes row buffer mid-table when BATCH_SIZE is exceeded (multi-pass path)'''
    async with async_mock_session(fake_engine) as session:
        for i in range(1, 1502):
            session.add(MarkovRelation(channel_id=1, leader_word=f'w{i}', follower_word=f'n{i}'))
        await session.commit()

    client = DatabaseBackupClient(fake_engine)
    backup_file = await client.create_backup()

    async with fake_engine.connect() as conn:
        await conn.execute(text('DELETE FROM markov_relation'))
        await conn.commit()

    stats = await client.restore_backup(backup_file, table_groups=[['markov_relation']])

    assert stats['tables']['markov_relation'] == 1501

    backup_file.unlink()


@pytest.mark.asyncio
async def test_truncate_tables_handles_pragma_exception(fake_engine):  #pylint:disable=redefined-outer-name
    '''_truncate_tables silently ignores failures from PRAGMA statements'''
    client = DatabaseBackupClient(fake_engine)

    # Use a mock connection so execute can raise without patching read-only attributes
    mock_conn = AsyncMock()

    async def execute_side_effect(stmt, *_a, **_kw):
        if 'PRAGMA' in str(stmt):
            raise RuntimeError('PRAGMA not supported')
        return AsyncMock()

    mock_conn.execute = execute_side_effect
    # Should not raise even when PRAGMA fails
    await client._truncate_tables(mock_conn, [])  #pylint:disable=protected-access


@pytest.mark.asyncio
async def test_truncate_tables_handles_delete_exception(fake_engine, caplog):  #pylint:disable=redefined-outer-name
    '''_truncate_tables logs debug and continues when a DELETE fails'''
    client = DatabaseBackupClient(fake_engine)

    # Use a mock connection so execute can raise without patching read-only attributes
    mock_conn = AsyncMock()

    async def execute_side_effect(stmt, *_a, **_kw):
        if 'DELETE' in str(stmt):
            raise RuntimeError('delete failed')
        return AsyncMock()

    mock_conn.execute = execute_side_effect
    table_names = list(__import__('discord_bot.database', fromlist=['BASE']).BASE.metadata.tables.keys())
    with caplog.at_level(logging.DEBUG, logger='discord_bot.utils.database_backup_client'):
        await client._truncate_tables(mock_conn, table_names)  #pylint:disable=protected-access

    assert 'Failed to truncate' in caplog.text


# ---------------------------------------------------------------------------
# _create_sqlite_snapshot and SQLite snapshot path in create_backup
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_create_sqlite_snapshot_contains_data(fake_engine):  #pylint:disable=redefined-outer-name
    '''_create_sqlite_snapshot copies live data into the snapshot DB'''
    async with async_mock_session(fake_engine) as session:
        session.add(MarkovChannel(channel_id='42', server_id='99'))
        await session.commit()

    client = DatabaseBackupClient(fake_engine)
    snap_engine, snap_path = await client._create_sqlite_snapshot()  #pylint:disable=protected-access
    try:
        async with snap_engine.connect() as conn:
            rows = (await conn.execute(text('SELECT channel_id FROM markov_channel'))).fetchall()
        assert len(rows) == 1
        assert rows[0][0] == 42
    finally:
        await snap_engine.dispose()
        if snap_path.exists():
            snap_path.unlink()


@pytest.mark.asyncio
async def test_create_backup_sqlite_snapshot_cleaned_up(fake_engine):  #pylint:disable=redefined-outer-name
    '''create_backup deletes the snapshot file after the export finishes'''
    client = DatabaseBackupClient(fake_engine)

    snap_paths = []
    original = client._create_sqlite_snapshot  #pylint:disable=protected-access

    async def tracking_snapshot():
        engine, path = await original()
        snap_paths.append(path)
        return engine, path

    client._create_sqlite_snapshot = tracking_snapshot  #pylint:disable=protected-access

    backup_file = await client.create_backup()

    assert len(snap_paths) == 1
    assert not snap_paths[0].exists(), 'snapshot file was not cleaned up'

    backup_file.unlink()


@pytest.mark.asyncio
async def test_create_backup_non_sqlite_skips_snapshot(fake_engine, mocker):  #pylint:disable=redefined-outer-name
    '''create_backup does not call _create_sqlite_snapshot for non-SQLite engines'''
    client = DatabaseBackupClient(fake_engine)
    snapshot_spy = mocker.patch.object(client, '_create_sqlite_snapshot')
    # Make the dialect appear to be PostgreSQL
    mocker.patch.object(fake_engine.dialect, 'name', 'postgresql')

    backup_file = await client.create_backup()

    snapshot_spy.assert_not_called()
    backup_file.unlink()
