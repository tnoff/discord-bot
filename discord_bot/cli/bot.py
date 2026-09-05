'''
The bot process — gateway connection, all cogs, remote persistence.

The only bot entrypoint: a separate discord-dispatcher worker handles message
queuing, so dispatch_http_url is required and cogs route via HttpDispatchClient.
Registered as discord-bot.  The single-process entrypoint that ran the gateway,
the cogs and the dispatcher in one process was retired, and the transitional
discord-bot-min alias was dropped once the deployed manifests stopped naming it
— see projects/discord-bot-ha-only in the docs repo.
'''
import logging

import click
from discord.ext.commands import Bot

from discord_bot.clients.database_stores import build_http_stores
from discord_bot.clients.http_dispatch_client import HttpDispatchClient
from discord_bot.cogs.error import CommandErrorHandler
from discord_bot.exceptions import DiscordBotException
from discord_bot.utils.common import GeneralConfig

from discord_bot.cli._lib.common import (
    bot_lifecycle, load_cogs, run_loop,
    setup_observability, register_on_ready,
    parse_and_validate_config, require_discord_token,
)
from discord_bot.cli._lib.gateway import build_bot
from discord_bot.cli._lib.cog_registry import POSSIBLE_COGS
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
    logger = setup_observability(general_config)

    general_settings = settings.get('general', {})
    dispatch_http_url = general_settings.get('dispatch_http_url')
    if not dispatch_http_url:
        raise DiscordBotException('dispatch_http_url required for HA bot mode')
    # Required, not optional. Before MR 4b a missing DSN left managed_db returning
    # None and the cogs degrading to no-persistence — playlists, markov and
    # analytics silently absent on a bot that otherwise came up fine. That was
    # survivable when the fallback was "no database"; it is not a mode worth
    # keeping now that the database is a pod this one is deployed alongside, and a
    # bot that silently loses half its commands is the failure this project has
    # spent its whole length removing.
    database_http_url = general_settings.get('database_http_url')
    if not database_http_url:
        raise DiscordBotException('database_http_url required for HA bot mode')

    http_dispatcher = HttpDispatchClient(dispatch_http_url)
    stores = build_http_stores(database_http_url)
    bot = build_bot(general_config)
    cog_list = [CommandErrorHandler(bot, settings, http_dispatcher)]
    cog_list += load_cogs(bot, POSSIBLE_COGS, settings, stores, http_dispatcher)

    register_on_ready(bot, general_config, logger)
    run_bot(general_config, bot, cog_list,
            health_server=setup_health_server(
                bot, general_config,
                dispatch_http_url=dispatch_http_url,
                database_http_url=database_http_url,
            ))


if __name__ == '__main__':  # pragma: no cover
    main()  # pylint: disable=no-value-for-parameter
