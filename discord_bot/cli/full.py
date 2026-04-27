'''
Full single-process bot — gateway connection, all cogs, SQLAlchemy DB, local dispatcher.

Use this when running everything in one process (no Redis, no HA).
For HA deployments use discord-bot (gateway + cogs) + discord-dispatcher (worker) instead.
'''
import logging

import click
from discord.ext.commands import Bot

from discord_bot.cogs.error import CommandErrorHandler
from discord_bot.workers.asyncio_queues import AsyncioBundleStore, AsyncioWorkQueue
from discord_bot.workers.message_dispatcher import MessageDispatcher
from discord_bot.utils.common import GeneralConfig

from discord_bot.cli.common import (
    build_bot, bot_lifecycle, load_cogs, run_loop,
    setup_observability, setup_health_server, register_on_ready,
    parse_and_validate_config,
)
from discord_bot.cli.cog_registry import POSSIBLE_COGS
from discord_bot.cli.db import managed_db, instrument_sqlalchemy



@click.command()
@click.argument('config_file', type=click.Path(dir_okay=False))
def main(config_file):
    '''Run the full single-process Discord bot (gateway, all cogs, local dispatcher).'''
    settings, general_config = parse_and_validate_config(config_file)
    run(settings, general_config)


async def main_loop(bot: Bot, cog_list: list, token: str,
                    dispatcher: MessageDispatcher, health_server=None):
    '''Main loop for the full single-process bot.'''
    logger = logging.getLogger('main')
    await dispatcher.start()
    async with bot_lifecycle(bot, cog_list, health_server=health_server,
                              on_shutdown=dispatcher.stop):
        logger.info('Main :: Starting bot in full single-process mode')
        await bot.start(token)


def run_bot(general_config: GeneralConfig, bot: Bot, cog_list: list,
            dispatcher: MessageDispatcher, health_server=None):
    '''Schedule main_loop on an existing event loop or start a new one.'''
    run_loop(main_loop(bot, cog_list, general_config.discord_token, dispatcher,
                       health_server=health_server))


def run(settings: dict, general_config: GeneralConfig):
    '''Entry point for the full single-process bot.'''
    with managed_db(general_config) as db_engine:
        logger = setup_observability(general_config)
        instrument_sqlalchemy()

        bot = build_bot(general_config)
        dispatcher = MessageDispatcher(bot, settings,
                                       bundle_store=AsyncioBundleStore(),
                                       work_queue=AsyncioWorkQueue())
        cog_list = [CommandErrorHandler(bot, settings, dispatcher)]
        cog_list += load_cogs(bot, POSSIBLE_COGS, settings, db_engine, dispatcher)

        register_on_ready(bot, general_config, logger)
        run_bot(general_config, bot, cog_list, dispatcher,
                health_server=setup_health_server(bot, general_config))


if __name__ == '__main__':  # pragma: no cover
    main()  # pylint: disable=no-value-for-parameter
