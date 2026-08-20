'''
The bot process — gateway connection, all cogs, SQLAlchemy DB.

The only bot entrypoint: a separate discord-dispatcher worker handles message
queuing, so dispatch_http_url is required and cogs route via HttpDispatchClient.
Registered as discord-bot (and, until the deployed manifests stop asking for it,
also as discord-bot-min).  The single-process entrypoint that ran the gateway,
the cogs and the dispatcher in one process was retired — see
projects/discord-bot-ha-only in the docs repo.
'''
import logging

import click
from discord.ext.commands import Bot

from discord_bot.clients.http_dispatch_client import HttpDispatchClient
from discord_bot.cogs.error import CommandErrorHandler
from discord_bot.exceptions import DiscordBotException
from discord_bot.utils.common import GeneralConfig

from discord_bot.cli._lib.common import (
    build_bot, bot_lifecycle, load_cogs, run_loop,
    setup_observability, register_on_ready,
    parse_and_validate_config, require_discord_token,
)
from discord_bot.cli._lib.cog_registry import POSSIBLE_COGS
from discord_bot.cli._lib.db import managed_db, instrument_sqlalchemy
from discord_bot.cli.health import setup_health_server


@click.command()
@click.argument('config_file', type=click.Path(dir_okay=False))
def main(config_file):
    '''Run the Discord bot process (gateway, all cogs; dispatch goes to the dispatcher pod).'''
    settings, general_config = parse_and_validate_config(config_file)
    run(settings, general_config)


async def main_loop(bot: Bot, cog_list: list, token: str, health_server=None):
    '''Main loop for the bot process.'''
    logger = logging.getLogger('main')
    async with bot_lifecycle(bot, cog_list, health_server=health_server):
        logger.info('Main :: Starting bot in HA mode')
        await bot.start(token)


def run_bot(general_config: GeneralConfig, bot: Bot, cog_list: list, health_server=None):
    '''Schedule main_loop on an existing event loop or start a new one.'''
    run_loop(main_loop(bot, cog_list, require_discord_token(general_config), health_server=health_server))


def run(settings: dict, general_config: GeneralConfig):
    '''Entry point for the bot process.'''
    with managed_db(general_config) as db_engine:
        logger = setup_observability(general_config)
        instrument_sqlalchemy(db_engine)

        dispatch_http_url = settings.get('general', {}).get('dispatch_http_url')
        if not dispatch_http_url:
            raise DiscordBotException('dispatch_http_url required for HA bot mode')
        http_dispatcher = HttpDispatchClient(dispatch_http_url)
        bot = build_bot(general_config)
        cog_list = [CommandErrorHandler(bot, settings, http_dispatcher)]
        cog_list += load_cogs(bot, POSSIBLE_COGS, settings, db_engine, http_dispatcher)

        register_on_ready(bot, general_config, logger)
        run_bot(general_config, bot, cog_list,
                health_server=setup_health_server(
                    bot, general_config,
                    db_engine=db_engine,
                    dispatch_http_url=dispatch_http_url,
                ))


if __name__ == '__main__':  # pragma: no cover
    main()  # pylint: disable=no-value-for-parameter
