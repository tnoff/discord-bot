'''Test Database Restore Client'''
import json
import tempfile
from pathlib import Path
import pytest
from sqlalchemy import create_engine, text
from discord_bot.database import BASE
from discord_bot.utils.database_backup_client import DatabaseBackupClient


@pytest.fixture
def db_engine():
    '''Create an in-memory SQLite database for testing'''
    engine = create_engine('sqlite:///:memory:', pool_pre_ping=True)
    BASE.metadata.create_all(engine)
    BASE.metadata.bind = engine
    return engine


@pytest.fixture
def backup_client(db_engine, mocker):  #pylint:disable=redefined-outer-name
    '''Create a DatabaseBackupClient instance'''
    logger = mocker.Mock()
    return DatabaseBackupClient(db_engine=db_engine, logger=logger)


def test_restore_empty_backup(backup_client):  #pylint:disable=redefined-outer-name
    '''Test restoring an empty backup'''
    # Create an empty backup file
    with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
        json.dump({}, f)
        backup_file = Path(f.name)

    try:
        stats = backup_client.restore_backup(backup_file)
        assert stats['tables_restored'] == 0
        assert stats['total_rows_inserted'] == 0
    finally:
        backup_file.unlink()


def test_restore_backup_with_data(backup_client, db_engine):  #pylint:disable=redefined-outer-name
    '''Test restoring a backup with actual data'''
    # Create sample data with metadata
    backup_data = {
        '_metadata': {
            'backup_timestamp': '2024-01-01_12-00-00',
            'alembic_version': 'test123',
            'table_count': 2
        },
        'guild': [
            {'id': 1, 'server_id': '123456789'},
            {'id': 2, 'server_id': '987654321'}
        ],
        'playlist': [
            {
                'id': 1,
                'name': 'Test Playlist',
                'server_id': '123456789',
                'last_queued': '2024-01-01 00:00:00',
                'created_at': '2024-01-01 00:00:00',
                'is_history': False
            }
        ]
    }

    # Create backup file
    with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
        json.dump(backup_data, f)
        backup_file = Path(f.name)

    try:
        # Restore backup
        stats = backup_client.restore_backup(backup_file)

        # Verify statistics
        assert stats['tables_restored'] == 2
        assert stats['total_rows_inserted'] == 3
        assert stats['tables']['guild'] == 2
        assert stats['tables']['playlist'] == 1

        # Verify metadata was captured
        assert stats['metadata']['backup_timestamp'] == '2024-01-01_12-00-00'
        assert stats['metadata']['alembic_version'] == 'test123'
        assert stats['metadata']['table_count'] == 2

        # Verify data was actually inserted
        with db_engine.connect() as conn:
            result = conn.execute(text('SELECT COUNT(*) FROM guild'))
            assert result.scalar() == 2

            result = conn.execute(text('SELECT COUNT(*) FROM playlist'))
            assert result.scalar() == 1

    finally:
        backup_file.unlink()


def test_restore_with_clear_existing(backup_client, db_engine):  #pylint:disable=redefined-outer-name
    '''Test restoring a backup with clear_existing=True'''
    # Insert some initial data
    with db_engine.begin() as conn:
        conn.execute(
            text("INSERT INTO guild (id, server_id) VALUES (:id, :server_id)"),
            [{'id': 99, 'server_id': '999999999'}]
        )

    # Verify initial data exists
    with db_engine.connect() as conn:
        result = conn.execute(text('SELECT COUNT(*) FROM guild'))
        assert result.scalar() == 1

    # Create backup with different data
    backup_data = {
        'guild': [
            {'id': 1, 'server_id': '123456789'}
        ]
    }

    with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
        json.dump(backup_data, f)
        backup_file = Path(f.name)

    try:
        # Restore with clear_existing=True
        stats = backup_client.restore_backup(backup_file, clear_existing=True)
        assert stats['tables_restored'] == 1

        # Verify old data was cleared and new data inserted
        with db_engine.connect() as conn:
            result = conn.execute(text('SELECT COUNT(*) FROM guild'))
            assert result.scalar() == 1

            result = conn.execute(text('SELECT server_id FROM guild WHERE id = 1'))
            row = result.fetchone()
            assert row[0] == '123456789'

    finally:
        backup_file.unlink()


def test_restore_without_clear_existing(backup_client, db_engine):  #pylint:disable=redefined-outer-name
    '''Test restoring a backup without clearing (merge mode)'''
    # Insert some initial data
    with db_engine.begin() as conn:
        conn.execute(
            text("INSERT INTO guild (id, server_id) VALUES (:id, :server_id)"),
            [{'id': 99, 'server_id': '999999999'}]
        )

    # Create backup with additional data
    backup_data = {
        'guild': [
            {'id': 1, 'server_id': '123456789'}
        ]
    }

    with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
        json.dump(backup_data, f)
        backup_file = Path(f.name)

    try:
        # Restore without clearing
        stats = backup_client.restore_backup(backup_file, clear_existing=False)
        assert stats['tables_restored'] == 1

        # Verify both old and new data exist
        with db_engine.connect() as conn:
            result = conn.execute(text('SELECT COUNT(*) FROM guild'))
            assert result.scalar() == 2

    finally:
        backup_file.unlink()


def test_restore_nonexistent_file(backup_client):  #pylint:disable=redefined-outer-name
    '''Test restoring from a non-existent file'''
    backup_file = Path('/nonexistent/path/backup.json')

    with pytest.raises(FileNotFoundError):
        backup_client.restore_backup(backup_file)


def test_backup_and_restore_roundtrip(backup_client, db_engine):  #pylint:disable=redefined-outer-name
    '''Test creating a backup and then restoring it'''
    # Insert some test data
    with db_engine.begin() as conn:
        conn.execute(
            text("INSERT INTO guild (id, server_id) VALUES (:id, :server_id)"),
            [
                {'id': 1, 'server_id': '111111111'},
                {'id': 2, 'server_id': '222222222'}
            ]
        )

    # Create backup
    backup_file = backup_client.create_backup()

    try:
        # Clear the database
        with db_engine.begin() as conn:
            conn.execute(text('DELETE FROM guild'))

        # Verify database is empty
        with db_engine.connect() as conn:
            result = conn.execute(text('SELECT COUNT(*) FROM guild'))
            assert result.scalar() == 0

        # Restore from backup
        stats = backup_client.restore_backup(backup_file)

        # Verify data was restored
        assert stats['total_rows_inserted'] >= 2

        with db_engine.connect() as conn:
            result = conn.execute(text('SELECT COUNT(*) FROM guild'))
            assert result.scalar() == 2

            result = conn.execute(text('SELECT server_id FROM guild ORDER BY id'))
            rows = result.fetchall()
            assert rows[0][0] == '111111111'
            assert rows[1][0] == '222222222'

    finally:
        if backup_file.exists():
            backup_file.unlink()
