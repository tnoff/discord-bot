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

from discord_bot.services.bot.clients.database_stores import build_http_stores
from discord_bot.core.clients.http_client_base import start_seam_checks
from discord_bot.seams.dispatch.clients.http_dispatch_client import HttpDispatchClient
from discord_bot.services.bot.cogs.error import CommandErrorHandler
from discord_bot.core.exceptions import DiscordBotException
from discord_bot.core.utils.common import GeneralConfig

from discord_bot.core.cli._lib.common import (
    bot_lifecycle, load_cogs, run_loop,
    setup_observability, register_on_ready,
    parse_and_validate_config, require_discord_token,
)
from discord_bot.core.cli._lib.gateway import build_bot
from discord_bot.services.bot.cli._lib.cog_registry import POSSIBLE_COGS
from discord_bot.services.bot.cli.health import setup_health_server


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


def register_seam_checks(bot: Bot) -> None:
    """Start each client's peer route check once the gateway is up.

    Registered with ``add_listener`` rather than ``@bot.event``: the decorator
    REPLACES the handler bound to an event name, so a second ``on_ready``
    decorated anywhere would silently unregister ``register_on_ready``'s
    guild-rejectlist pass. Listeners are additive; the decorator is not.

    ``on_ready`` rather than a one-shot task because it is the first async point
    this process reaches, and the clients are built in a synchronous ``run()``.
    A gateway reconnect re-fires it, which is harmless: ``SeamContractCheck.start``
    is a no-op while its task is still live.

    **Lives here rather than in cli/_lib/common.py, and that is load-bearing.**
    common.py is imported by all six images; importing the seam-check machinery
    there pulled ``clients/http_client_base`` into the dispatcher image, which
    serves its seam and calls none — measured as +2 modules by
    docs/image-dependencies.md. This function is bot-only, so it belongs in the
    bot-only module. See http-seam-contract.md, acceptance criterion one.

    Takes no client list. Every client that was given a seam contract config
    enrolled itself when it was built, so this covers the ones this process owns
    directly (its dispatch client and database stores) AND anything a cog built
    in ``cog_load``, which runs first. The music cog also calls
    ``start_seam_checks`` itself; both calls are no-ops for a check already
    running, and between them nothing built in either place can be missed.
    """
    async def _on_ready_seam_checks():
        start_seam_checks()
    bot.add_listener(_on_ready_seam_checks, 'on_ready')


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

    http_dispatcher = HttpDispatchClient(dispatch_http_url,
                                         seam_contract=general_config.seam_contract)
    stores = build_http_stores(database_http_url,
                               seam_contract=general_config.seam_contract)
    bot = build_bot(general_config)
    cog_list = [CommandErrorHandler(bot, settings, http_dispatcher)]
    cog_list += load_cogs(bot, POSSIBLE_COGS, settings, stores, http_dispatcher)

    register_on_ready(bot, general_config, logger)
    # No client list: the dispatch client, the three stores and the music cog's
    # four all enrolled themselves at construction. See SeamClientRegistry.
    register_seam_checks(bot)
    run_bot(general_config, bot, cog_list,
            health_server=setup_health_server(
                bot, general_config,
                dispatch_http_url=dispatch_http_url,
                database_http_url=database_http_url,
            ))


if __name__ == '__main__':  # pragma: no cover
    main()  # pylint: disable=no-value-for-parameter
