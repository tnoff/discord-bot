'''Bring the database schema to head, from inside the process that serves it.

Its own module rather than a function in cli/_lib/db.py, for the same reason
that file exists: this one imports alembic, and only the db entrypoint may. The
import-boundary test asserts the shape from the other side.

**Nothing ran migrations before this.** `cli/_lib/db.py::setup_db` called
`BASE.metadata.create_all`, which creates missing tables and never ALTERs
anything, so every schema change up to 2026-09-06 was applied by hand. That call
is gone; this module replaced it. See projects/alembic-migration-ownership.md
(docs) for how the chain was repaired and why the runner landed here rather than
in an initContainer or a Job.

**The flag defaults to False and is True in prod.** It shipped disabled so that
merging the runner changed nothing until the ConfigMap said otherwise, which
bought the window in which prod's live schema was measured against the chain
rather than assumed to match it. That measurement is done: `alembic_version`
reads head, the one revision this project believed had never run turned out to
have run, and the two drifted rows were repaired. The default stays False
because it is the right default for anything that is not this pod, not because
the question is still open.
'''
import logging
import os
from pathlib import Path

from sqlalchemy import create_engine, inspect
from sqlalchemy.engine.url import make_url

from alembic import command
from alembic.config import Config

from discord_core.exceptions import DiscordBotException
from discord_core.utils.common import GeneralConfig

from discord_db.cli._lib.db_url import SQLITE, backend_name, is_in_memory_sqlite
from discord_db.database import BASE

# alembic.ini sets `script_location = %(here)s/alembic`, so the ini's own
# directory decides where the revisions are read from. Relative by design: it
# resolves against the working directory, which is the repo root in development
# and /opt/discord in the image, and both hold the pair. ALEMBIC_CONFIG overrides
# it for anything that runs from somewhere else.
DEFAULT_CONFIG_FILENAME = 'alembic.ini'


def _config_path() -> Path:
    return Path(os.environ.get('ALEMBIC_CONFIG', DEFAULT_CONFIG_FILENAME)).resolve()


def _bootstrap_sqlite(dsn: str, config: Config) -> bool:
    '''Build a never-migrated SQLite file from the models and stamp it at head.

    Returns whether it did; False means the file already carries a version, so
    the normal upgrade path applies.

    The alembic chain is not replayed on SQLite. It was written against
    postgres and its revisions use ALTER COLUMN ... TYPE, which SQLite cannot
    do, so a fresh file replayed from base fails partway. There is also nothing
    to migrate: a SQLite database starts at the current models, which is the
    same schema the chain ends in (test_alembic_chain.py holds the chain to that
    for postgres). Stamping head then means a later `alembic upgrade head`
    applies only revisions written after this one -- and those must be
    SQLite-safe (batch operations), because they run against this file.

    dsn : Configured SQLite DSN
    config : Alembic config, already carrying the DSN
    '''
    # Sync driver, deliberately: this runs before the serving loop exists, and
    # create_all against a throwaway engine needs no loop at all.
    engine = create_engine(make_url(dsn).set(drivername='sqlite'))
    try:
        if inspect(engine).has_table('alembic_version'):
            return False
        BASE.metadata.create_all(engine)
    finally:
        engine.dispose()
    command.stamp(config, 'head')
    return True


def run_pending_migrations(general_config: GeneralConfig) -> bool:
    '''Run `alembic upgrade head` when enabled. Returns whether it ran.

    Called before the process binds a port, and deliberately not inside the
    event loop: alembic/env.py calls asyncio.run() at module scope, so an
    already-running loop turns this into a RuntimeError at the worst possible
    moment. test_migrations.py pins that ordering.
    '''
    if not general_config.run_migrations:
        return False
    if not general_config.sql_connection_statement:
        raise DiscordBotException(
            'general.run_migrations is set but general.sql_connection_statement is not'
        )
    config_path = _config_path()
    if not config_path.is_file():
        # Loud, and naming the path. The alternative is alembic reporting "no
        # such revision" or building nothing at all, from an image whose
        # Dockerfile forgot to COPY the migrations -- a failure that reads as a
        # broken chain rather than a missing file.
        raise DiscordBotException(
            f'general.run_migrations is set but no alembic config at {config_path}'
        )
    config = Config(str(config_path))
    # env.py reads this in preference to DATABASE_URL, so the upgrade lands on
    # the database this process is configured to serve rather than on whatever
    # the environment happens to say.
    config.attributes['database_url'] = general_config.sql_connection_statement
    # Keep alembic out of this process's logging config. env.py would otherwise
    # call fileConfig(), which defaults to disable_existing_loggers=True and
    # switches off every logger built before it -- including the ones already
    # carrying the OTLP handler. That is not a theoretical risk: it took the db
    # tier dark in Loki on the first prod roll with this flag enabled, while the
    # pod stayed healthy and served normally, so nothing announced it.
    config.attributes['configure_logger'] = False
    # With fileConfig() skipped, alembic's own records are subject to this
    # process's third_party_log_level (WARNING), which would drop `Running
    # upgrade` -- the one line that says which revisions actually ran. Raise just
    # this logger so it reaches Loki through the handler the app already
    # installed, rather than only stdout the way it did before.
    logging.getLogger('alembic').setLevel(logging.INFO)
    dsn = general_config.sql_connection_statement
    if backend_name(dsn) == SQLITE:
        if is_in_memory_sqlite(make_url(dsn)):
            # A separate engine would build the schema in a different database
            # from the one the pod then serves.
            raise DiscordBotException(
                'general.run_migrations cannot be used with an in-memory sqlite database'
            )
        if _bootstrap_sqlite(dsn, config):
            return True
    command.upgrade(config, 'head')
    return True
