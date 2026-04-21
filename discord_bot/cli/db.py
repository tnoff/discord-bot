'''Database setup helpers for CLI entry points that include SQLAlchemy.

Kept separate from cli/common.py so that the dispatcher process,
which has no database, does not import SQLAlchemy at all.
'''
import asyncio
import contextlib
import sys

from opentelemetry import trace
from opentelemetry.instrumentation.sqlalchemy import SQLAlchemyInstrumentor
from sqlalchemy import create_engine
from sqlalchemy.engine.url import make_url
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy.pool import NullPool

from discord_bot.database import BASE
from discord_bot.utils.common import GeneralConfig


def setup_db(general_config: GeneralConfig):
    '''Create sync + async DB engines, run migrations, return the async engine (or None).'''
    if not general_config.sql_connection_statement:
        print('Unable to find sql statement in settings, assuming no db', file=sys.stderr)
        return None
    sync_engine = create_engine(general_config.sql_connection_statement, poolclass=NullPool)
    BASE.metadata.create_all(sync_engine)
    sync_engine.dispose()
    url = make_url(general_config.sql_connection_statement)
    if url.drivername.startswith('postgresql'):
        url = url.set(drivername='postgresql+asyncpg')
    elif url.drivername == 'sqlite':
        url = url.set(drivername='sqlite+aiosqlite')
    return create_async_engine(url, pool_pre_ping=True)


def dispose_db_engine(db_engine) -> None:
    '''Dispose the async DB engine, scheduling a task if a loop is already running.'''
    if not db_engine:
        return
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        loop = None
    if loop and loop.is_running():
        loop.create_task(db_engine.dispose())
    else:
        asyncio.run(db_engine.dispose())


@contextlib.contextmanager
def managed_db(general_config):
    '''Context manager: set up DB engine, yield it, then dispose on exit.'''
    db_engine = setup_db(general_config)
    try:
        yield db_engine
    finally:
        dispose_db_engine(db_engine)


def instrument_sqlalchemy() -> None:
    '''Instrument SQLAlchemy with the active OpenTelemetry tracer.'''
    SQLAlchemyInstrumentor().instrument(
        tracer_provider=trace.get_tracer_provider(),
        enable_commenter=True,
        commenter_options={},
    )
