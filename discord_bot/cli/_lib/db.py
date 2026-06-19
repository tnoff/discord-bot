'''Database setup helpers for CLI entry points that include SQLAlchemy.

Kept separate from cli/_lib/common.py so that the dispatcher process,
which has no database, does not import SQLAlchemy at all.
'''
import asyncio
import concurrent.futures
import contextlib
import sys

from opentelemetry import trace
from opentelemetry.instrumentation.sqlalchemy import SQLAlchemyInstrumentor
from sqlalchemy.engine.url import make_url
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy.pool import NullPool

from discord_bot.database import BASE
from discord_bot.utils.common import GeneralConfig


async def _create_tables(engine):
    async with engine.begin() as conn:
        await conn.run_sync(BASE.metadata.create_all)


def setup_db(general_config: GeneralConfig):
    '''Create the async DB engine, run migrations, return the engine (or None).

    PostgreSQL is the only supported backend; non-postgres drivernames raise.
    '''
    if not general_config.sql_connection_statement:
        print('Unable to find sql statement in settings, assuming no db', file=sys.stderr)
        return None
    url = make_url(general_config.sql_connection_statement)
    if not url.drivername.startswith('postgresql'):
        raise ValueError(
            f'Unsupported database driver {url.drivername!r}; only postgresql is supported'
        )
    url = url.set(drivername='postgresql+asyncpg')
    engine = create_async_engine(url, poolclass=NullPool)
    with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
        pool.submit(asyncio.run, _create_tables(engine)).result()
    return engine


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


def instrument_sqlalchemy(db_engine=None) -> None:
    '''Instrument SQLAlchemy with the active OpenTelemetry tracer.'''
    kwargs = {
        'tracer_provider': trace.get_tracer_provider(),
        'enable_commenter': True,
        'commenter_options': {},
    }
    if db_engine is not None:
        kwargs['engine'] = db_engine.sync_engine
    SQLAlchemyInstrumentor().instrument(**kwargs)
