'''
Full bot process — gateway connection, all cogs, SQLAlchemy DB.
'''
import asyncio
import concurrent.futures
import sys

from opentelemetry.instrumentation.sqlalchemy import SQLAlchemyInstrumentor
from opentelemetry import trace

from sqlalchemy.engine.url import make_url
from sqlalchemy.ext.asyncio import create_async_engine

from discord_bot.cogs.delete_messages import DeleteMessages
from discord_bot.cogs.database_backup import DatabaseBackup
from discord_bot.cogs.general import General
from discord_bot.cogs.markov import Markov
from discord_bot.cogs.message_dispatcher import MessageDispatcher
from discord_bot.cogs.music import Music
from discord_bot.cogs.role import RoleAssignment
from discord_bot.cogs.urban import UrbanDictionary
from discord_bot.database import BASE
from discord_bot.servers.health_server import HealthServer
from discord_bot.utils.common import GeneralConfig

from discord_bot.cli.common import (
    build_bot, load_cogs, run_bot,
    setup_logging, setup_otlp, setup_profiling,
)

POSSIBLE_COGS = [
    MessageDispatcher,   # must be first — music/markov depend on it
    DeleteMessages,
    DatabaseBackup,
    Markov,
    Music,
    RoleAssignment,
    UrbanDictionary,
    General,
]


async def _create_tables(engine):
    async with engine.begin() as conn:
        await conn.run_sync(BASE.metadata.create_all)


def _setup_db(general_config: GeneralConfig):
    if general_config.sql_connection_statement:
        url = make_url(general_config.sql_connection_statement)
        if url.drivername.startswith('postgresql'):
            url = url.set(drivername='postgresql+asyncpg')
        elif url.drivername == 'sqlite':
            url = url.set(drivername='sqlite+aiosqlite')
        engine = create_async_engine(url, pool_pre_ping=True)
        with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
            pool.submit(asyncio.run, _create_tables(engine)).result()
        return engine
    print('Unable to find sql statement in settings, assuming no db', file=sys.stderr)
    return None


def run(settings: dict, general_config: GeneralConfig):
    '''Entry point for the full bot process.'''
    db_engine = _setup_db(general_config)
    try:
        logger_provider = setup_otlp(general_config)
        if logger_provider:
            # SQLAlchemy instrumentation only makes sense with a DB
            SQLAlchemyInstrumentor().instrument(
                tracer_provider=trace.get_tracer_provider(),
                enable_commenter=True,
                commenter_options={},
            )
        logger = setup_logging(general_config, logger_provider)
        setup_profiling(general_config, logger)

        bot, cog_list = build_bot(general_config, settings)
        cog_list += load_cogs(bot, POSSIBLE_COGS, settings, db_engine)

        health_server = None
        if general_config.monitoring and general_config.monitoring.health_server \
                and general_config.monitoring.health_server.enabled:
            health_server = HealthServer(bot, port=general_config.monitoring.health_server.port)

        rejectlist_guilds = list(general_config.rejectlist_guilds)
        logger.info(f'Main :: Gathered guild reject list {rejectlist_guilds}')

        @bot.event
        async def on_ready():
            logger.info(f'Main :: Starting bot, logged in as {bot.user} (ID: {bot.user.id})')
            guilds = [guild async for guild in bot.fetch_guilds(limit=150)]
            for guild in guilds:
                if guild.id in rejectlist_guilds:
                    logger.info(f'Main :: Bot currently in guild {guild.id} thats within reject list, leaving server')
                    await guild.leave()
                    continue
                logger.info(f'Main :: Bot associated with guild {guild.id} with name "{guild.name}"')

        run_bot(general_config, bot, cog_list, health_server=health_server)
    finally:
        if db_engine:
            try:
                loop = asyncio.get_running_loop()
            except RuntimeError:
                loop = None
            if loop and loop.is_running():
                loop.create_task(db_engine.dispose())
            else:
                asyncio.run(db_engine.dispose())
