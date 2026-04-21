'''
HA bot process — gateway connection, all cogs, SQLAlchemy DB.

Use this for HA deployments where a separate discord-dispatcher worker handles message queuing.
Configure dispatch_http_url in general settings so cogs route via HttpDispatchClient.
For single-process deployments use discord-bot-full instead.
'''
import logging

import click
from discord.ext.commands import Bot

from discord_bot.clients.http_dispatch_client import HttpDispatchClient
from discord_bot.exceptions import DiscordBotException
from discord_bot.cogs.error import CommandErrorHandler
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
    '''Run the HA Discord bot process (gateway, all cogs, no local dispatcher).'''
    settings, general_config = parse_and_validate_config(config_file)
    run(settings, general_config)


async def main_loop(bot: Bot, cog_list: list, token: str, health_server=None):
    '''Main loop for the HA bot process.'''
    logger = logging.getLogger('main')
    async with bot_lifecycle(bot, cog_list, health_server=health_server):
        logger.info('Main :: Starting bot in HA mode')
        await bot.start(token)


def run_bot(general_config: GeneralConfig, bot: Bot, cog_list: list, health_server=None):
    '''Schedule main_loop on an existing event loop or start a new one.'''
    run_loop(main_loop(bot, cog_list, general_config.discord_token, health_server=health_server))


def run(settings: dict, general_config: GeneralConfig):
    '''Entry point for the HA bot process.'''
    with managed_db(general_config) as db_engine:
        logger = setup_observability(general_config)
        instrument_sqlalchemy()

        dispatch_http_url = settings.get('general', {}).get('dispatch_http_url')
        if not dispatch_http_url:
            raise DiscordBotException('dispatch_http_url required for HA bot mode')
        http_dispatcher = HttpDispatchClient(dispatch_http_url)
        bot = build_bot(general_config)
        cog_list = [CommandErrorHandler(bot, settings, http_dispatcher)]
        cog_list += load_cogs(bot, POSSIBLE_COGS, settings, db_engine, http_dispatcher)

        register_on_ready(bot, general_config, logger)
        run_bot(general_config, bot, cog_list,
                health_server=setup_health_server(bot, general_config))


if __name__ == '__main__':
    main()  # pylint: disable=no-value-for-parameter
