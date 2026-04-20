'''
Full bot process — gateway connection, all cogs, SQLAlchemy DB.
'''
import asyncio
import concurrent.futures
import sys

import click
from opentelemetry.instrumentation.sqlalchemy import SQLAlchemyInstrumentor
from opentelemetry import trace

from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy.pool import NullPool

from discord_bot.cogs.delete_messages import DeleteMessages
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
    build_bot, load_cogs, make_async_db_url, run_bot,
    setup_logging, setup_otlp, setup_profiling,
    parse_and_validate_config,
)

POSSIBLE_COGS = [
    MessageDispatcher,   # must be first — music/markov depend on it
    DeleteMessages,
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
        url = make_async_db_url(general_config.sql_connection_statement)
        engine = create_async_engine(url, poolclass=NullPool)
        with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
            pool.submit(asyncio.run, _create_tables(engine)).result()
        return engine
    print('Unable to find sql statement in settings, assuming no db', file=sys.stderr)
    return None


@click.command()
@click.argument('config_file', type=click.Path(dir_okay=False))
def main(config_file):
    '''Run the full Discord bot (gateway connection, all cogs).'''
    settings, general_config = parse_and_validate_config(config_file)
    run(settings, general_config)


def run(settings: dict, general_config: GeneralConfig):
    '''Entry point for the full bot process.'''
    db_engine = _setup_db(general_config)
    try:
        logger_provider = setup_otlp(general_config)
        if logger_provider and db_engine:
            SQLAlchemyInstrumentor().instrument(
                engine=db_engine.sync_engine,
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
            health_server = HealthServer(bot, port=general_config.monitoring.health_server.port,
                                         db_engine=db_engine)

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

        run_bot(general_config, bot, cog_list, health_server=health_server, dispatch_gateway=True)
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


if __name__ == '__main__':
    main()  # pylint: disable=no-value-for-parameter
