'''
Dispatcher process — HTTP-only, Redis health check, MessageDispatcher service only.
No SQLAlchemy, no gateway connection.
'''
import asyncio
import logging
import uuid


import click
from discord.ext.commands import Bot
from opentelemetry import trace
from opentelemetry.instrumentation.redis import RedisInstrumentor


from discord_bot.clients.redis_client import RedisManager
from discord_bot.workers.redis_queues import RedisBundleStore, RedisWorkQueue
from discord_bot.workers.message_dispatcher import MessageDispatcher
from discord_bot.exceptions import DiscordBotException
from discord_bot.servers.dispatch_server import DispatchHealthServer, DispatchHttpServer
from discord_bot.utils.common import GeneralConfig

from discord_bot.cli.common import (
    build_bot, bot_lifecycle, run_loop,
    setup_logging, setup_otlp, setup_profiling,
    parse_and_validate_config,
)


@click.command()
@click.argument('config_file', type=click.Path(dir_okay=False))
def main(config_file):
    '''Run the dispatcher process (HTTP-only, no gateway connection).'''
    settings, general_config = parse_and_validate_config(config_file)
    run(settings, general_config)

async def main_loop(bot: Bot, token: str, redis_manager: RedisManager, dispatcher: MessageDispatcher,
                    health_server=None, dispatch_http_server=None):
    '''Main loop for the dispatcher process.'''
    logger = logging.getLogger('main')
    await redis_manager.start()
    await dispatcher.start()

    async def _on_shutdown():
        await dispatcher.stop()
        await redis_manager.close()

    async with bot_lifecycle(bot, [], health_server=health_server, on_shutdown=_on_shutdown):
        if dispatch_http_server:
            asyncio.create_task(dispatch_http_server.serve())
        logger.info('Main :: Starting dispatcher for HA mode')
        await bot.login(token)
        while not bot.is_closed():
            await asyncio.sleep(1)


def run_bot(general_config: GeneralConfig, bot: Bot, redis_manager: RedisManager,
            dispatcher: MessageDispatcher, health_server=None, dispatch_http_server=None):
    '''Schedule main_loop on an existing event loop or start a new one.'''
    run_loop(main_loop(bot, general_config.discord_token, redis_manager, dispatcher,
                       health_server=health_server,
                       dispatch_http_server=dispatch_http_server))


def run(settings: dict, general_config: GeneralConfig):
    '''Entry point for the dispatcher process.'''
    setup_otlp(general_config)
    RedisInstrumentor().instrument(tracer_provider=trace.get_tracer_provider())
    logger = setup_logging(general_config)
    setup_profiling(general_config, logger)

    if not general_config.redis_url:
        raise DiscordBotException('Redis required for dispatcher HA mode')

    redis_manager = RedisManager(general_config.redis_url)

    bundle_store = RedisBundleStore(redis_manager)
    shard_id = int(settings.get('general', {}).get('dispatch_shard_id', 0))
    process_id = settings.get('general', {}).get('dispatch_process_id') or str(uuid.uuid4())
    work_queue = RedisWorkQueue(redis_manager, shard_id, process_id)

    bot = build_bot(general_config)
    dispatcher = MessageDispatcher(bot, settings, bundle_store=bundle_store, work_queue=work_queue)

    cfg = settings.get('general', {}).get('dispatch_server', {})
    dispatch_http_server = DispatchHttpServer(
        dispatcher, work_queue,
        host=cfg.get('host', '0.0.0.0'),
        port=int(cfg.get('port', 8082)),
    )

    health_server = None
    if general_config.monitoring and general_config.monitoring.health_server \
            and general_config.monitoring.health_server.enabled:
        health_server = DispatchHealthServer(
            redis_manager,
            port=general_config.monitoring.health_server.port,
        )

    run_bot(general_config, bot, redis_manager, dispatcher,
            health_server=health_server, dispatch_http_server=dispatch_http_server)
