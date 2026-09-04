'''
HealthServer factory for the bot/full entry-points.

Lives in its own module so importing ``discord_bot.cli._lib.common`` does not
transitively pull in sqlalchemy via ``servers.health_server``. The
dispatcher image installs only the base dependency set and would crash on
that import; ``cli.dispatcher`` constructs ``DispatchHealthServer`` (which
does not need sqlalchemy) directly instead of going through this module.
'''
from discord.ext.commands import Bot

from discord_bot.servers.health_server import HealthServer
from discord_bot.utils.common import GeneralConfig, resolve_tracing_config


def setup_health_server(bot: Bot, general_config: GeneralConfig,
                        db_engine=None, dispatch_http_url: str | None = None):
    '''Return a HealthServer if monitoring.health_server.enabled, else None.'''
    if (general_config.monitoring and general_config.monitoring.health_server
            and general_config.monitoring.health_server.enabled):
        return HealthServer(
            bot,
            port=general_config.monitoring.health_server.port,
            bind_address=general_config.monitoring.health_server.bind_address,
            db_engine=db_engine,
            dispatch_http_url=dispatch_http_url,
            suppress_db_probe_auto_instrumentation=resolve_tracing_config(
                general_config).suppress_db_probe_auto_instrumentation,
        )
    return None
