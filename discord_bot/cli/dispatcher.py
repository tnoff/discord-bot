'''
Dispatcher process — HTTP-only, Redis health check, MessageDispatcher cog only.
No SQLAlchemy, no gateway connection.
'''
import click
from opentelemetry import trace
from opentelemetry.instrumentation.redis import RedisInstrumentor

from discord_bot.cogs.message_dispatcher import MessageDispatcher
from discord_bot.servers.health_server import DispatchHealthServer
from discord_bot.utils.common import GeneralConfig

from discord_bot.cli.common import (
    build_bot, load_cogs, run_bot,
    setup_logging, setup_otlp, setup_profiling,
    parse_and_validate_config,
)

POSSIBLE_COGS = [
    MessageDispatcher,
]


@click.command()
@click.argument('config_file', type=click.Path(dir_okay=False))
def main(config_file):
    '''Run the dispatcher process (HTTP-only, no gateway connection).'''
    settings, general_config = parse_and_validate_config(config_file)
    run(settings, general_config)


def run(settings: dict, general_config: GeneralConfig):
    '''Entry point for the dispatcher process.'''
    setup_otlp(general_config)
    RedisInstrumentor().instrument(tracer_provider=trace.get_tracer_provider())
    logger = setup_logging(general_config)
    setup_profiling(general_config, logger)

    bot, cog_list = build_bot(general_config, settings)
    cog_list += load_cogs(bot, POSSIBLE_COGS, settings, db_engine=None)

    health_server = None
    if general_config.monitoring and general_config.monitoring.health_server \
            and general_config.monitoring.health_server.enabled \
            and general_config.redis_url:
        health_server = DispatchHealthServer(
            general_config.redis_url,
            port=general_config.monitoring.health_server.port,
        )

    run_bot(general_config, bot, cog_list, health_server=health_server, dispatch_gateway=False)
