'''Test Database Restore Client'''
import json
import os
import tempfile
from pathlib import Path
import pytest
import pytest_asyncio
from sqlalchemy import text
from sqlalchemy.pool import NullPool
from sqlalchemy.ext.asyncio import create_async_engine

from discord_bot.database import BASE
from discord_bot.utils.database_backup_client import DatabaseBackupClient


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


@pytest.mark.asyncio
async def test_restore_nonexistent_file(backup_client):  #pylint:disable=redefined-outer-name
    '''Test restoring from a non-existent file'''
    backup_file = Path('/nonexistent/path/backup.json')

    with pytest.raises(FileNotFoundError):
        await backup_client.restore_backup(backup_file)


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
