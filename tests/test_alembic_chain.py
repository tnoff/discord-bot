'''
The alembic chain must replay from base on an empty postgres, and the schema it
produces must be the schema the ORM expects.

Both halves are load-bearing and neither implies the other.

The replay half exists because the chain was broken for nine months without
anyone noticing: revision a399ba3deb56 changed five VARCHAR id columns to
INTEGER, and its postgres branch issued bare ALTER COLUMN ... TYPE INTEGER with
no USING clause. Postgres refuses that cast. Alembic wraps the whole upgrade in
one transaction, so the failure did not leave a half-migrated database to
notice -- it rolled back to zero relations. Prod never saw it because prod was
built by BASE.metadata.create_all and hand-patched forward, and is stamped past
that revision; only a FRESH database ever replays it.

The equivalence half exists because "the chain runs to completion" is a much
weaker property than "the chain builds the right schema". A revision that
silently drops a column or an index still replays green. Since create_all is
what actually built prod, it is the reference the migrations have to reproduce.

Deliberately compares against BASE.metadata.create_all rather than a golden dump
checked into the repo: a dump would need hand-updating on every model change,
which is the kind of chore that gets skipped and then lies.
'''
import os
import subprocess
import sys
from pathlib import Path

import pytest
from sqlalchemy import create_engine, inspect, text
from sqlalchemy.pool import NullPool

from discord_bot.database import BASE

REPO_ROOT = Path(__file__).resolve().parent.parent

REPLAY_DB = 'discord_bot_alembic_replay'
REFERENCE_DB = 'discord_bot_createall_ref'

# Alembic's own bookkeeping table. It exists only in the replayed database by
# definition, so it is excluded rather than treated as a difference.
ALEMBIC_TABLE = 'alembic_version'

# The chain head. Pinned so that adding a revision without running it here is a
# failure rather than a silent no-op.
EXPECTED_HEAD = 'd1c8b4e6f7a2'


def _sync_url(proc, database: str) -> str:
    '''Sync psycopg URL. The async driver is not needed to introspect a schema.'''
    password = os.environ.get('POSTGRES_TEST_PASSWORD', '')
    auth = f'{proc.user}:{password}' if password else proc.user
    return f'postgresql+psycopg://{auth}@{proc.host}:{proc.port}/{database}'


def _plain_url(proc, database: str) -> str:
    '''Driverless URL for DATABASE_URL; alembic/env.py picks the driver itself.'''
    password = os.environ.get('POSTGRES_TEST_PASSWORD', '')
    auth = f'{proc.user}:{password}' if password else proc.user
    return f'postgresql://{auth}@{proc.host}:{proc.port}/{database}'


def _recreate_database(proc, database: str) -> None:
    engine = create_engine(
        _sync_url(proc, 'postgres'), isolation_level='AUTOCOMMIT', poolclass=NullPool
    )
    try:
        with engine.connect() as conn:
            conn.execute(text(f'DROP DATABASE IF EXISTS "{database}" WITH (FORCE)'))
            conn.execute(text(f'CREATE DATABASE "{database}"'))
    finally:
        engine.dispose()


def _drop_database(proc, database: str) -> None:
    engine = create_engine(
        _sync_url(proc, 'postgres'), isolation_level='AUTOCOMMIT', poolclass=NullPool
    )
    try:
        with engine.connect() as conn:
            conn.execute(text(f'DROP DATABASE IF EXISTS "{database}" WITH (FORCE)'))
    finally:
        engine.dispose()


def _run_alembic(proc, database: str, *args: str) -> subprocess.CompletedProcess:
    '''Invoke the real CLI, not the Python API.

    A deploy runs `alembic upgrade head` as a command, and env.py reads
    DATABASE_URL from the environment. Driving the API in-process would skip
    both, which is where a failure would actually live.
    '''
    env = dict(os.environ, DATABASE_URL=_plain_url(proc, database))
    return subprocess.run(
        [sys.executable, '-m', 'alembic', *args],
        cwd=REPO_ROOT,
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )


def _describe(url: str) -> dict:
    '''Introspect a live schema into comparable plain data.

    Everything is sorted and name-keyed so a mismatch reports as a readable
    diff rather than an ordering artifact.
    '''
    engine = create_engine(url, poolclass=NullPool)
    try:
        inspector = inspect(engine)
        tables = sorted(t for t in inspector.get_table_names() if t != ALEMBIC_TABLE)
        described = {
            'tables': tables,
            'columns': {},
            'primary_keys': {},
            'foreign_keys': {},
            'unique_constraints': {},
            'check_constraints': {},
            'indexes': {},
        }
        for table in tables:
            described['columns'][table] = {
                col['name']: (str(col['type']), bool(col['nullable']))
                for col in inspector.get_columns(table)
            }
            described['primary_keys'][table] = tuple(
                inspector.get_pk_constraint(table).get('constrained_columns') or ()
            )
            described['foreign_keys'][table] = sorted(
                (
                    tuple(fk['constrained_columns']),
                    fk['referred_table'],
                    tuple(fk['referred_columns']),
                )
                for fk in inspector.get_foreign_keys(table)
            )
            described['unique_constraints'][table] = sorted(
                (uq['name'], tuple(uq['column_names']))
                for uq in inspector.get_unique_constraints(table)
            )
            described['check_constraints'][table] = sorted(
                (ck['name'], str(ck['sqltext']))
                for ck in inspector.get_check_constraints(table)
            )
            described['indexes'][table] = sorted(
                (ix['name'], tuple(ix['column_names']), bool(ix.get('unique')))
                for ix in inspector.get_indexes(table)
            )
        return described
    finally:
        engine.dispose()


@pytest.fixture(name='replayed_schema', scope='module')
def fixture_replayed_schema(postgresql_proc):
    '''An empty database taken from base to head by the alembic CLI.'''
    _recreate_database(postgresql_proc, REPLAY_DB)
    try:
        result = _run_alembic(postgresql_proc, REPLAY_DB, 'upgrade', 'head')
        assert result.returncode == 0, (
            'alembic upgrade head failed against an empty postgres. The whole '
            'upgrade is one transaction, so the database is now empty rather '
            f'than half-migrated.\nstdout:\n{result.stdout}\nstderr:\n{result.stderr}'
        )
        yield _describe(_sync_url(postgresql_proc, REPLAY_DB))
    finally:
        _drop_database(postgresql_proc, REPLAY_DB)


@pytest.fixture(name='reference_schema', scope='module')
def fixture_reference_schema(postgresql_proc):
    '''An empty database built by BASE.metadata.create_all -- what prod is.'''
    _recreate_database(postgresql_proc, REFERENCE_DB)
    engine = create_engine(_sync_url(postgresql_proc, REFERENCE_DB), poolclass=NullPool)
    try:
        BASE.metadata.create_all(engine)
        engine.dispose()
        yield _describe(_sync_url(postgresql_proc, REFERENCE_DB))
    finally:
        engine.dispose()
        _drop_database(postgresql_proc, REFERENCE_DB)


def test_chain_replays_from_base_to_head(postgresql_proc, replayed_schema):
    '''base -> head succeeds and lands on the expected revision.'''
    assert replayed_schema['tables'], 'replay produced no tables'

    engine = create_engine(_sync_url(postgresql_proc, REPLAY_DB), poolclass=NullPool)
    try:
        with engine.connect() as conn:
            stamped = conn.execute(text(f'SELECT version_num FROM {ALEMBIC_TABLE}')).scalar_one()
    finally:
        engine.dispose()
    assert stamped == EXPECTED_HEAD


def test_rerunning_upgrade_on_a_stamped_database_is_a_noop(postgresql_proc, replayed_schema):
    '''A deploy against an already-migrated database must not re-run revisions.

    This is the property that makes a migration runner safe to put on a pod that
    restarts: prod is stamped at head, so the first automated run has to be a
    no-op there.
    '''
    assert replayed_schema['tables']
    before = _describe(_sync_url(postgresql_proc, REPLAY_DB))

    result = _run_alembic(postgresql_proc, REPLAY_DB, 'upgrade', 'head')
    assert result.returncode == 0, f'stdout:\n{result.stdout}\nstderr:\n{result.stderr}'
    assert 'Running upgrade' not in result.stderr, (
        f'a second upgrade re-ran revisions:\n{result.stderr}'
    )
    assert _describe(_sync_url(postgresql_proc, REPLAY_DB)) == before


@pytest.mark.parametrize(
    'aspect',
    [
        'tables',
        'columns',
        'primary_keys',
        'foreign_keys',
        'unique_constraints',
        'check_constraints',
        'indexes',
    ],
)
def test_migrated_schema_matches_create_all(replayed_schema, reference_schema, aspect):
    '''The migrated schema is the schema the ORM would have built.

    Each aspect is asserted non-empty first. A comparison of two empty sets
    passes while proving nothing, and that is not hypothetical -- it is exactly
    how an earlier hand-run of this comparison reported a clean constraint match
    from a query that had errored on both sides.
    '''
    if aspect == 'tables':
        assert reference_schema[aspect], 'reference schema has no tables'
    else:
        populated = sum(len(v) for v in reference_schema[aspect].values())
        if aspect in ('columns', 'primary_keys', 'indexes'):
            assert populated, f'reference schema reported no {aspect}'

    assert replayed_schema[aspect] == reference_schema[aspect]
