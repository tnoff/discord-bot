'''
The in-process migration runner against a real SQLite file.

The alembic chain itself is not replayed here. It was written for postgres and
its revisions use ALTER COLUMN ... TYPE, which SQLite cannot do, so a fresh
SQLite file is built from the models and stamped at head instead (see
migrations._bootstrap_sqlite and test_sqlite_support.py). What remains worth
holding under test is what does not depend on the dialect: that the runner
leaves the process's own logging alone, and that setup_db builds no schema.
'''
import asyncio
import logging
from pathlib import Path

import pytest
from sqlalchemy import create_engine, inspect

from discord_core.utils.common import GeneralConfig

from discord_db.cli._lib.db import setup_db
from discord_db.cli._lib.migrations import run_pending_migrations

REPO_ROOT = Path(__file__).resolve().parents[2]


def test_in_process_upgrade_leaves_application_logging_alone(tmp_path, monkeypatch):
    '''The runner must not switch the process's own logging off.

    The db entrypoint calls run_pending_migrations after the OTLP LoggingHandler
    is already attached, and env.py's fileConfig() defaults to
    disable_existing_loggers=True -- which disables every logger not named in
    alembic.ini's `[loggers] keys =` line.

    This is a regression test in the literal sense. It shipped broken: the first
    prod roll with general.run_migrations true served traffic correctly and sent
    ZERO lines to Loki, and nothing failed, alerted, or restarted to say so.

    So the assertion is about the logger the app owns, not about alembic's.
    A fresh file takes the bootstrap path, whose `command.stamp` runs env.py --
    the code that calls fileConfig() -- so it exercises the same guard.
    '''
    monkeypatch.setenv('ALEMBIC_CONFIG', str(REPO_ROOT / 'alembic.ini'))

    app_logger = logging.getLogger('discord_db.cli.database')
    alembic_logger = logging.getLogger('alembic')
    previous_level = alembic_logger.level

    records = []

    class _Collector(logging.Handler):
        def emit(self, record):
            records.append(record)

    handler = _Collector()
    root = logging.getLogger()
    root.addHandler(handler)

    try:
        general_config = GeneralConfig(
            sql_connection_statement=f'sqlite:///{tmp_path / "logging.db"}',
            run_migrations=True,
        )
        assert run_pending_migrations(general_config) is True

        # The property that broke. Not "logging still works somewhere" -- the
        # named logger the entrypoint writes through, still enabled.
        assert app_logger.disabled is False
        assert logging.getLogger('main').disabled is False

        # And alembic's own account of what it did has to reach the app's
        # handlers, or the flag's effects are only visible on stdout.
        stamped = [r for r in records if r.name.startswith('alembic')
                   and 'Running stamp_revision' in r.getMessage()]
        assert stamped, 'no alembic record reached the application handlers'
    finally:
        root.removeHandler(handler)
        alembic_logger.setLevel(previous_level)


def test_setup_db_does_not_build_a_schema(tmp_path):
    '''setup_db against an empty database leaves it empty.

    The direct assertion behind "setup_db no longer calls create_all". It is
    worth having as a database fact rather than as "the call is not in the
    source", because the two can diverge -- create_all can be reached through a
    helper, an import, or a well-meaning fixture, and only one of those shows up
    in a grep.

    It also names the split this project exists to create: the migration runner
    builds a schema, setup_db connects to it. While both did, a fresh database
    came up with create_all's output and an empty alembic_version, which is the
    state that made prod unmigratable in the first place.
    '''
    db_file = tmp_path / 'empty.db'
    engine = setup_db(GeneralConfig(sql_connection_statement=f'sqlite:///{db_file}'))
    assert engine is not None
    try:
        inspection = create_engine(f'sqlite:///{db_file}')
        try:
            assert inspect(inspection).get_table_names() == []
        finally:
            inspection.dispose()
    finally:
        asyncio.run(engine.dispose())
