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

from discord_bot.database import BASE
from discord_bot.utils.common import GeneralConfig


async def _create_tables(engine):
    '''Build the schema, then hand the pool back empty.

    The dispose is load-bearing, not tidiness. setup_db runs this coroutine in a
    throwaway thread with its own event loop, so every connection opened here is
    bound to a loop that is about to close. Under NullPool that was harmless --
    the connection was discarded at the end of the block and never reused. Now
    that the engine pools, a surviving connection is handed to the serving loop
    on the very first query and fails with "got Future attached to a different
    loop". Measured against a scratch postgres: pooled without this dispose, the
    first real query raises; with it, it succeeds.
    '''
    async with engine.begin() as conn:
        await conn.run_sync(BASE.metadata.create_all)
    await engine.dispose()


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
    # Pooled. This was poolclass=NullPool, which opened and closed a physical
    # connection for every session -- prod span metrics show `connect` and
    # `SELECT` at the same rate on all three pods, one dial per statement.
    # Measured means, from the spanmetrics counters:
    #
    #   discord-bot     33.9ms connect vs  4.0ms SELECT   (n=150,500)
    #   discord-broker  71.5ms connect vs  6.8ms SELECT   (n=288)
    #   discord-db      48.0ms connect vs  5.3ms SELECT   (n=154, probe only)
    #
    # So setup was ~8.5x the query it existed to serve on the best-sampled pod,
    # across ~23,000 connections a day fleetwide. The markov batching in
    # clients/markov_client.py exists to hold that cost to one connection per
    # message rather than one per word; pooling is the other half of the same
    # problem, and it matters more after the MR 4 cutover, when every store call
    # from the bot and the broker funnels through one pod.
    #
    # pool_pre_ping is not optional now that connections are held. discord-pg is
    # a single instance, so a restart or failover leaves every pooled connection
    # stale, and without the ping the next checkout raises instead of
    # reconnecting -- trading a slow path for a broken one. pool_recycle caps how
    # long a connection can sit before it is replaced regardless.
    #
    # 5 + 10 are SQLAlchemy's own defaults, stated rather than inherited because
    # they now bound something NullPool did not. A pool refuses work once it is
    # exhausted -- 15 concurrent sessions here, then a 30s wait and a TimeoutError
    # -- where NullPool simply dialled again every time and was bounded only by
    # postgres' max_connections. That trade is the right way round, and it is not
    # close: db_client_connections_usage{state="used"} peaks at 1 over 24h on both
    # the bot and the broker, so the headroom is 15x measured peak.
    #
    # The other side of the ceiling is shared: three processes against one
    # postgres instance caps the fleet at 45 backends. After the MR 4 cutover only
    # the db pod holds any of them.
    engine = create_async_engine(
        url,
        pool_pre_ping=True,
        pool_recycle=1800,
        pool_size=5,
        max_overflow=10,
    )
    with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
        pool.submit(asyncio.run, _create_tables(engine)).result()
    return engine


def dispose_db_engine(db_engine) -> None:
    '''Dispose the async DB engine, scheduling a task if a loop is already running.

    The no-running-loop branch passes close=False on purpose. managed_db's finally
    runs after run_loop's asyncio.run has returned, so the loop that owns every
    pooled connection is already closed; asyncio.run here starts a *different*
    one. Closing from it makes asyncpg schedule a cancel on the dead loop, which
    raises inside SQLAlchemy's pool and gets logged as "Exception closing
    connection" with a traceback -- on every pod roll, once per pooled
    connection, looking exactly like a crash on the way out. Under NullPool the
    pool was always empty so this never came up.

    close=False is SQLAlchemy's documented answer for a pool whose connections
    belong to another loop or process: release them without touching them and
    let process exit close the sockets, which is the next thing that happens.
    Nothing is in flight by then -- main_loop has already drained the server.
    '''
    if not db_engine:
        return
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        loop = None
    if loop and loop.is_running():
        loop.create_task(db_engine.dispose())
    else:
        asyncio.run(db_engine.dispose(close=False))


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
