'''
Dispatcher process — HTTP-only, Redis health check, MessageDispatcher cog only.
No SQLAlchemy, no gateway connection.
'''
from discord_bot.clients.redis_client import RedisManager
from discord_bot.cogs.message_dispatcher import MessageDispatcher
from discord_bot.servers.dispatch_health_server import DispatchHealthServer
from discord_bot.utils.common import GeneralConfig

from discord_bot.cli.common import (
    build_bot, load_cogs, run_bot,
    setup_logging, setup_otlp, setup_profiling,
)

POSSIBLE_COGS = [
    MessageDispatcher,
]


def run(settings: dict, general_config: GeneralConfig):
    '''Entry point for the dispatcher process.'''
    logger_provider = setup_otlp(general_config)
    logger = setup_logging(general_config, logger_provider)
    setup_profiling(general_config, logger)

    redis_manager = RedisManager(general_config.redis_url) if general_config.redis_url else None

    bot, cog_list = build_bot(general_config, settings)
    cog_list += load_cogs(bot, POSSIBLE_COGS, settings, db_engine=None, redis_manager=redis_manager)

    health_server = None
    if general_config.monitoring and general_config.monitoring.health_server \
            and general_config.monitoring.health_server.enabled \
            and redis_manager is not None:
        health_server = DispatchHealthServer(
            redis_manager,
            port=general_config.monitoring.health_server.port,
            bind_address=general_config.monitoring.health_server.bind_address,
        )

    run_bot(general_config, bot, cog_list, health_server=health_server, redis_manager=redis_manager)
