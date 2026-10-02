'''SQLite as a supported backend for the db pod.

The store clients are exercised on both backends by the `fake_engine` fixture
(conftest.py parametrizes it), so what is left here is the plumbing around them:
which DSNs are accepted, how the engine is configured, and how a SQLite file
gets its schema.
'''
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest
from alembic.script import ScriptDirectory
from alembic.config import Config
from sqlalchemy import create_engine, inspect, text
from sqlalchemy.ext.asyncio import AsyncSession

from discord_core.exceptions import DiscordBotException
from discord_core.utils.common import GeneralConfig

from discord_db.cli._lib import migrations
from discord_db.cli._lib.db import setup_db
from discord_db.cli._lib.db_url import async_url, backend_name
from discord_db.database import BASE, Playlist

from tests.helpers import fake_engine  # pylint:disable=unused-import

ALEMBIC_INI = Path(__file__).resolve().parents[3] / 'alembic.ini'


@pytest.mark.parametrize('raw,expected', [
    ('postgresql://u:p@h:5432/db', 'postgresql+asyncpg'),
    ('postgresql+psycopg://u:p@h:5432/db', 'postgresql+asyncpg'),
    ('sqlite:///local.db', 'sqlite+aiosqlite'),
    ('sqlite:///:memory:', 'sqlite+aiosqlite'),
])
def test_async_url_picks_the_async_driver(raw, expected):
    '''The configured form is rewritten; the rest of the URL is untouched.'''
    url = async_url(raw)
    assert url.drivername == expected
    assert url.database == make_database(raw)


def make_database(raw):
    '''Database component of a DSN, for comparing before and after the rewrite.'''
    return raw.rsplit('/', 1)[-1]


@pytest.mark.parametrize('raw', ['mysql://u:p@h/db', 'oracle://u:p@h/db'])
def test_unsupported_backends_are_rejected(raw):
    '''Anything but postgresql and sqlite raises, naming the driver.'''
    with pytest.raises(ValueError, match='Unsupported database driver'):
        backend_name(raw)


@pytest.mark.asyncio
async def test_sqlite_engine_enforces_foreign_keys_and_waits_on_locks(tmp_path):
    '''SQLite ignores foreign keys and fails fast on lock contention by default.

    Both are per-connection pragmas, so the engine has to set them on every
    connection it opens. WAL is a property of the file and is asserted too.
    '''
    engine = setup_db(GeneralConfig(sql_connection_statement=f'sqlite:///{tmp_path / "t.db"}'))
    try:
        async with engine.connect() as conn:
            assert (await conn.execute(text('PRAGMA foreign_keys'))).scalar() == 1
            assert (await conn.execute(text('PRAGMA busy_timeout'))).scalar() == 30000
            assert (await conn.execute(text('PRAGMA journal_mode'))).scalar() == 'wal'
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_in_memory_sqlite_engine_builds():
    '''An in-memory database takes no pool sizing; passing it raises at creation.'''
    engine = setup_db(GeneralConfig(sql_connection_statement='sqlite:///:memory:'))
    try:
        async with engine.connect() as conn:
            assert (await conn.execute(text('SELECT 1'))).scalar() == 1
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_datetimes_read_back_aware_and_in_utc(fake_engine):  # pylint:disable=redefined-outer-name
    '''An aware, non-UTC datetime comes back as the same instant, in UTC.

    Runs on both backends. SQLite stores wall-clock text and would return a naive
    value, which neither compares with an aware one nor serializes with an
    offset; UTCDateTime is what closes that gap.
    '''
    local = timezone(timedelta(hours=5))
    written = datetime(2026, 1, 2, 12, 0, 0, tzinfo=local)
    async with AsyncSession(fake_engine, expire_on_commit=False) as session:
        row = Playlist(name='p', server_id=1, created_at=written)
        session.add(row)
        await session.commit()
        playlist_id = row.id
    async with AsyncSession(fake_engine) as session:
        read = (await session.get(Playlist, playlist_id)).created_at
    assert read == written
    assert read.utcoffset() == timedelta(0)


def _sqlite_config(path) -> GeneralConfig:
    return GeneralConfig(sql_connection_statement=f'sqlite:///{path}', run_migrations=True)


def _head() -> str:
    return ScriptDirectory.from_config(Config(str(ALEMBIC_INI))).get_current_head()


def test_fresh_sqlite_file_is_built_from_the_models_and_stamped_at_head(tmp_path, monkeypatch):
    '''The chain is not replayed on SQLite; the schema comes from the models.

    Replaying it fails partway on ALTER COLUMN, so a fresh file is create_all'd
    and stamped. Every table the models declare must exist afterwards.
    '''
    monkeypatch.setenv('ALEMBIC_CONFIG', str(ALEMBIC_INI))
    db_file = tmp_path / 'fresh.db'
    assert migrations.run_pending_migrations(_sqlite_config(db_file)) is True

    engine = create_engine(f'sqlite:///{db_file}')
    try:
        tables = set(inspect(engine).get_table_names())
        with engine.connect() as conn:
            version = conn.execute(text('SELECT version_num FROM alembic_version')).scalar()
    finally:
        engine.dispose()
    assert {table.name for table in BASE.metadata.sorted_tables} <= tables
    assert version == _head()


def test_rerunning_on_a_stamped_sqlite_file_keeps_its_data(tmp_path, monkeypatch):
    '''A file that already carries a version goes through upgrade, not a rebuild.'''
    monkeypatch.setenv('ALEMBIC_CONFIG', str(ALEMBIC_INI))
    db_file = tmp_path / 'again.db'
    migrations.run_pending_migrations(_sqlite_config(db_file))

    engine = create_engine(f'sqlite:///{db_file}')
    try:
        with engine.begin() as conn:
            conn.execute(text("INSERT INTO guild (server_id) VALUES (42)"))
    finally:
        engine.dispose()

    assert migrations.run_pending_migrations(_sqlite_config(db_file)) is True

    engine = create_engine(f'sqlite:///{db_file}')
    try:
        with engine.connect() as conn:
            assert conn.execute(text('SELECT server_id FROM guild')).scalar() == 42
    finally:
        engine.dispose()


def test_migrations_refuse_an_in_memory_sqlite_database(monkeypatch):
    '''A throwaway engine would build the schema in a different database.'''
    monkeypatch.setenv('ALEMBIC_CONFIG', str(ALEMBIC_INI))
    config = GeneralConfig(sql_connection_statement='sqlite:///:memory:', run_migrations=True)
    with pytest.raises(DiscordBotException, match='in-memory'):
        migrations.run_pending_migrations(config)
