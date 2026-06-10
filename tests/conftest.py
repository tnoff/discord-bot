'''
Session-scoped postgres test database.

Brings up a single postgres cluster via pytest-postgresql, creates one
database with the full schema, and tears the database down at session end.
Per-test isolation is provided by the function-scoped `fake_engine` fixture
in `tests/helpers.py`, which TRUNCATEs every table between tests.

Two modes:

- **proc** (default): pytest-postgresql starts its own postgres process.
  Requires `pg_ctl`, `initdb`, `postgres` on PATH. CI installs these via
  `TOX_EXTRA_APT: postgresql` in `.gitlab-ci.yml`.
- **noproc**: connect to an already-running postgres. Activated when
  `POSTGRES_TEST_HOST` is set. Suited to local dev (`docker compose -f
  docker/docker-compose.yml up -d postgres`).

Configure noproc via env vars: `POSTGRES_TEST_HOST`, `POSTGRES_TEST_PORT`
(default 5432), `POSTGRES_TEST_USER` (default discord),
`POSTGRES_TEST_PASSWORD` (optional).
'''
import asyncio
import os

import fakeredis.aioredis
import pytest
from pytest_postgresql import factories
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy.pool import NullPool

from discord_bot.database import BASE

_TEST_DB_NAME = 'discord_bot_test'

if os.environ.get('POSTGRES_TEST_HOST'):
    postgresql_proc = factories.postgresql_noproc(
        host=os.environ['POSTGRES_TEST_HOST'],
        port=int(os.environ.get('POSTGRES_TEST_PORT', '5432')),
        user=os.environ.get('POSTGRES_TEST_USER', 'discord'),
        password=os.environ.get('POSTGRES_TEST_PASSWORD', ''),
    )
else:
    postgresql_proc = factories.postgresql_proc(
        postgres_options='-c fsync=off -c synchronous_commit=off -c full_page_writes=off',
    )


def _admin_url(proc) -> str:
    auth = proc.user
    password = os.environ.get('POSTGRES_TEST_PASSWORD', '')
    if password:
        auth = f'{proc.user}:{password}'
    return f'postgresql+asyncpg://{auth}@{proc.host}:{proc.port}/postgres'


def _test_url(proc) -> str:
    auth = proc.user
    password = os.environ.get('POSTGRES_TEST_PASSWORD', '')
    if password:
        auth = f'{proc.user}:{password}'
    return f'postgresql+asyncpg://{auth}@{proc.host}:{proc.port}/{_TEST_DB_NAME}'


async def _create_database(proc) -> None:
    engine = create_async_engine(_admin_url(proc), isolation_level='AUTOCOMMIT', poolclass=NullPool)
    try:
        async with engine.connect() as conn:
            await conn.execute(text(f'DROP DATABASE IF EXISTS "{_TEST_DB_NAME}" WITH (FORCE)'))
            await conn.execute(text(f'CREATE DATABASE "{_TEST_DB_NAME}"'))
    finally:
        await engine.dispose()

    schema_engine = create_async_engine(_test_url(proc), poolclass=NullPool)
    try:
        async with schema_engine.begin() as conn:
            await conn.run_sync(BASE.metadata.create_all)
    finally:
        await schema_engine.dispose()


async def _drop_database(proc) -> None:
    engine = create_async_engine(_admin_url(proc), isolation_level='AUTOCOMMIT', poolclass=NullPool)
    try:
        async with engine.connect() as conn:
            await conn.execute(text(f'DROP DATABASE IF EXISTS "{_TEST_DB_NAME}" WITH (FORCE)'))
    finally:
        await engine.dispose()


@pytest.fixture(scope='session')
def pg_test_db_url(postgresql_proc):  # pylint: disable=redefined-outer-name
    '''SQLAlchemy URL of a fresh test database with the bot schema applied.'''
    asyncio.run(_create_database(postgresql_proc))
    try:
        yield _test_url(postgresql_proc)
    finally:
        asyncio.run(_drop_database(postgresql_proc))


# protocol=2 forces RESP2 on every FakeRedis in the test suite (here and in test
# files that construct one directly). fakeredis 2.36.0 + redis-py 8.0.0 returns
# RESP3 wire shape from stream commands but never decodes the bytes — the new
# parse_xread_resp3_to_resp2_legacy -> pairs_to_dict path runs with
# decode_keys=False and ignores decode_responses=True, so XREADGROUP/XINFO GROUPS
# leak b'...' through. Drop protocol=2 (and grep the suite) once fakeredis ships
# a fix; upstream tracking issue is cunla/fakeredis-py#488, and the
# XREADGROUP-specific symptom isn't filed yet — open a focused repro when
# removing this workaround.
@pytest.fixture
def redis_client():
    '''Return a FakeRedis instance with decode_responses=True.'''
    return fakeredis.aioredis.FakeRedis(decode_responses=True, protocol=2)
