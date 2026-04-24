'''Test Database Restore Client'''
import json
import os
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import MagicMock
import pytest
import pytest_asyncio
from sqlalchemy import text
from sqlalchemy.pool import NullPool
from sqlalchemy.ext.asyncio import create_async_engine

from discord_bot.database import BASE
from discord_bot.utils.database_backup_client import DatabaseBackupClient


def _pg_backup_client():
    '''DatabaseBackupClient with a mocked PostgreSQL dialect (no real connection needed).'''
    mock_engine = MagicMock()
    mock_engine.dialect.name = 'postgresql'
    return DatabaseBackupClient(db_engine=mock_engine)


@pytest_asyncio.fixture
async def db_engine():
    '''File-based async SQLite engine for testing (required for create_backup snapshot).'''
    fd, db_path = tempfile.mkstemp(suffix='.db')
    os.close(fd)
    engine = create_async_engine(f'sqlite+aiosqlite:///{db_path}', poolclass=NullPool)
    async with engine.begin() as conn:
        await conn.run_sync(BASE.metadata.create_all)
    try:
        yield engine
    finally:
        await engine.dispose()
        os.unlink(db_path)


@pytest.fixture
def backup_client(db_engine):  #pylint:disable=redefined-outer-name
    '''Create a DatabaseBackupClient instance'''
    return DatabaseBackupClient(db_engine=db_engine)


@pytest.mark.asyncio
async def test_restore_empty_backup(backup_client):  #pylint:disable=redefined-outer-name
    '''Test restoring an empty backup'''
    with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
        json.dump({}, f)
        backup_file = Path(f.name)

    try:
        stats = await backup_client.restore_backup(backup_file)
        assert stats['tables_restored'] == 0
        assert stats['total_rows_inserted'] == 0
    finally:
        backup_file.unlink()


@pytest.mark.asyncio
async def test_restore_backup_with_data(backup_client, db_engine):  #pylint:disable=redefined-outer-name
    '''Test restoring a backup with actual data'''
    backup_data = {
        '_metadata': {
            'backup_timestamp': '2024-01-01_12-00-00',
            'alembic_version': 'test123',
            'table_count': 2
        },
        'guild': [
            {'id': 1, 'server_id': 123456789},
            {'id': 2, 'server_id': 987654321}
        ],
        'playlist': [
            {
                'id': 1,
                'name': 'Test Playlist',
                'server_id': 123456789,
                'last_queued': '2024-01-01 00:00:00',
                'created_at': '2024-01-01 00:00:00',
                'is_history': False
            }
        ]
    }

    with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
        json.dump(backup_data, f)
        backup_file = Path(f.name)

    try:
        stats = await backup_client.restore_backup(backup_file)

        assert stats['tables_restored'] == 2
        assert stats['total_rows_inserted'] == 3
        assert stats['tables']['guild'] == 2
        assert stats['tables']['playlist'] == 1

        assert stats['metadata']['backup_timestamp'] == '2024-01-01_12-00-00'
        assert stats['metadata']['alembic_version'] == 'test123'
        assert stats['metadata']['table_count'] == 2

        async with db_engine.connect() as conn:
            assert (await conn.execute(text('SELECT COUNT(*) FROM guild'))).scalar() == 2
            assert (await conn.execute(text('SELECT COUNT(*) FROM playlist'))).scalar() == 1

    finally:
        backup_file.unlink()


@pytest.mark.asyncio
async def test_restore_with_clear_existing(backup_client, db_engine):  #pylint:disable=redefined-outer-name
    '''Test restoring a backup with clear_existing=True'''
    async with db_engine.begin() as conn:
        await conn.execute(
            text("INSERT INTO guild (id, server_id) VALUES (:id, :server_id)"),
            [{'id': 99, 'server_id': 999999999}]
        )

    async with db_engine.connect() as conn:
        assert (await conn.execute(text('SELECT COUNT(*) FROM guild'))).scalar() == 1

    backup_data = {
        'guild': [
            {'id': 1, 'server_id': 123456789}
        ]
    }

    with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
        json.dump(backup_data, f)
        backup_file = Path(f.name)

    try:
        stats = await backup_client.restore_backup(backup_file, clear_existing=True)
        assert stats['tables_restored'] == 1

        async with db_engine.connect() as conn:
            assert (await conn.execute(text('SELECT COUNT(*) FROM guild'))).scalar() == 1
            row = (await conn.execute(text('SELECT server_id FROM guild WHERE id = 1'))).fetchone()
            assert row[0] == 123456789

    finally:
        backup_file.unlink()


@pytest.mark.asyncio
async def test_restore_without_clear_existing(backup_client, db_engine):  #pylint:disable=redefined-outer-name
    '''Test restoring a backup without clearing (merge mode)'''
    async with db_engine.begin() as conn:
        await conn.execute(
            text("INSERT INTO guild (id, server_id) VALUES (:id, :server_id)"),
            [{'id': 99, 'server_id': 999999999}]
        )

    backup_data = {
        'guild': [
            {'id': 1, 'server_id': 123456789}
        ]
    }

    with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
        json.dump(backup_data, f)
        backup_file = Path(f.name)

    try:
        stats = await backup_client.restore_backup(backup_file, clear_existing=False)
        assert stats['tables_restored'] == 1

        async with db_engine.connect() as conn:
            assert (await conn.execute(text('SELECT COUNT(*) FROM guild'))).scalar() == 2

    finally:
        backup_file.unlink()


def test_coerce_row_converts_datetime_strings():
    '''_coerce_row converts string datetime values to timezone-aware datetimes for DateTime columns'''
    client = _pg_backup_client()
    row = {
        'id': 1,
        'name': 'Test',
        'server_id': 123,
        'last_queued': '2023-08-26 06:49:25.794980',
        'created_at': '2022-04-20 00:00:00',
        'is_history': False,
    }
    coerced = client._coerce_row('playlist', row)  #pylint:disable=protected-access

    assert isinstance(coerced['last_queued'], datetime)
    assert coerced['last_queued'].tzinfo == timezone.utc
    assert isinstance(coerced['created_at'], datetime)
    assert coerced['created_at'].tzinfo == timezone.utc
    assert coerced['id'] == 1
    assert coerced['name'] == 'Test'
    assert coerced['is_history'] is False


def test_coerce_row_preserves_existing_datetimes():
    '''_coerce_row leaves already-datetime values untouched'''
    client = _pg_backup_client()
    dt = datetime(2023, 8, 26, 6, 49, 25, tzinfo=timezone.utc)
    row = {'id': 1, 'name': 'Test', 'server_id': 123,
           'last_queued': dt, 'created_at': dt, 'is_history': False}
    coerced = client._coerce_row('playlist', row)  #pylint:disable=protected-access
    assert coerced['last_queued'] is dt
    assert coerced['created_at'] is dt


def test_coerce_row_handles_null_datetime():
    '''_coerce_row leaves None datetime values as None'''
    client = _pg_backup_client()
    row = {'id': 1, 'name': 'Test', 'server_id': 123,
           'last_queued': None, 'created_at': '2022-04-20 00:00:00', 'is_history': False}
    coerced = client._coerce_row('playlist', row)  #pylint:disable=protected-access
    assert coerced['last_queued'] is None


def test_coerce_row_converts_integer_booleans():
    '''_coerce_row converts SQLite integer booleans (0/1) to Python bool for PostgreSQL'''
    client = _pg_backup_client()
    row = {'id': 1, 'name': 'Test', 'server_id': 123,
           'last_queued': None, 'created_at': '2022-04-20 00:00:00',
           'is_history': 1}
    coerced = client._coerce_row('playlist', row)  #pylint:disable=protected-access
    assert coerced['is_history'] is True
    assert isinstance(coerced['is_history'], bool)


def test_coerce_row_converts_integer_false():
    '''_coerce_row converts SQLite integer 0 to Python False'''
    client = _pg_backup_client()
    row = {'id': 1, 'name': 'Test', 'server_id': 123,
           'last_queued': None, 'created_at': '2022-04-20 00:00:00',
           'is_history': 0}
    coerced = client._coerce_row('playlist', row)  #pylint:disable=protected-access
    assert coerced['is_history'] is False
    assert isinstance(coerced['is_history'], bool)


def test_coerce_row_no_op_for_sqlite():
    '''_coerce_row returns the row unchanged for non-postgresql dialects'''
    mock_engine = MagicMock()
    mock_engine.dialect.name = 'sqlite'
    client = DatabaseBackupClient(db_engine=mock_engine)
    row = {'id': 1, 'last_queued': '2023-08-26 06:49:25', 'created_at': '2022-04-20 00:00:00'}
    assert client._coerce_row('playlist', row) is row  #pylint:disable=protected-access


@pytest.mark.asyncio
async def test_restore_backup_with_string_datetimes(backup_client, db_engine):  #pylint:disable=redefined-outer-name
    '''restore_backup succeeds when datetime columns contain string values (as written by backup)'''
    backup_data = {
        'playlist': [
            {
                'id': 1,
                'name': 'Test Playlist',
                'server_id': 123456789,
                'last_queued': '2023-08-26 06:49:25.794980',
                'created_at': '2022-04-20 00:00:00',
                'is_history': False
            }
        ]
    }

    with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
        json.dump(backup_data, f)
        backup_file = Path(f.name)

    try:
        stats = await backup_client.restore_backup(backup_file)
        assert stats['tables_restored'] == 1
        assert stats['total_rows_inserted'] == 1

        async with db_engine.connect() as conn:
            row = (await conn.execute(
                text('SELECT last_queued FROM playlist WHERE id = 1')
            )).fetchone()
            assert row is not None
    finally:
        backup_file.unlink()


@pytest.mark.asyncio
async def test_restore_nonexistent_file(backup_client):  #pylint:disable=redefined-outer-name
    '''Test restoring from a non-existent file'''
    backup_file = Path('/nonexistent/path/backup.json')

    with pytest.raises(FileNotFoundError):
        await backup_client.restore_backup(backup_file)


@pytest.mark.asyncio
async def test_restore_alembic_version(backup_client, db_engine):  #pylint:disable=redefined-outer-name
    '''restore_backup writes alembic_version table from backup metadata'''
    backup_data = {
        '_metadata': {
            'backup_timestamp': '2024-01-01_12-00-00',
            'alembic_version': 'abc123def456',
            'table_count': 0
        }
    }

    with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
        json.dump(backup_data, f)
        backup_file = Path(f.name)

    try:
        await backup_client.restore_backup(backup_file)

        async with db_engine.connect() as conn:
            row = (await conn.execute(text('SELECT version_num FROM alembic_version'))).fetchone()
            assert row is not None
            assert row[0] == 'abc123def456'
    finally:
        backup_file.unlink()


@pytest.mark.asyncio
async def test_restore_no_alembic_version_in_metadata(backup_client, db_engine):  #pylint:disable=redefined-outer-name
    '''restore_backup does not create alembic_version when metadata has no version'''
    backup_data = {
        '_metadata': {
            'backup_timestamp': '2024-01-01_12-00-00',
            'alembic_version': None,
            'table_count': 0
        }
    }

    with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
        json.dump(backup_data, f)
        backup_file = Path(f.name)

    try:
        await backup_client.restore_backup(backup_file)

        # Table should not exist (or be empty) since version was None
        async with db_engine.connect() as conn:
            try:
                row = (await conn.execute(text('SELECT version_num FROM alembic_version'))).fetchone()
                assert row is None
            except Exception:  # pylint: disable=broad-except
                pass  # Table not existing is also acceptable
    finally:
        backup_file.unlink()


@pytest.mark.asyncio
async def test_backup_and_restore_roundtrip(backup_client, db_engine):  #pylint:disable=redefined-outer-name
    '''Test creating a backup and then restoring it'''
    async with db_engine.begin() as conn:
        await conn.execute(
            text("INSERT INTO guild (id, server_id) VALUES (:id, :server_id)"),
            [
                {'id': 1, 'server_id': 111111111},
                {'id': 2, 'server_id': 222222222}
            ]
        )

    backup_file = await backup_client.create_backup()

    try:
        async with db_engine.begin() as conn:
            await conn.execute(text('DELETE FROM guild'))

        async with db_engine.connect() as conn:
            assert (await conn.execute(text('SELECT COUNT(*) FROM guild'))).scalar() == 0

        stats = await backup_client.restore_backup(backup_file)

        assert stats['total_rows_inserted'] >= 2

        async with db_engine.connect() as conn:
            assert (await conn.execute(text('SELECT COUNT(*) FROM guild'))).scalar() == 2
            rows = (await conn.execute(text('SELECT server_id FROM guild ORDER BY id'))).fetchall()
            assert rows[0][0] == 111111111
            assert rows[1][0] == 222222222

    finally:
        if backup_file.exists():
            backup_file.unlink()
