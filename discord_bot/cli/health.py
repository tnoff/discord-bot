'''
HealthServer factory for the bot/full entry-points.

Lives in its own module because importing ``discord_bot.cli._lib.common`` used
to pull sqlalchemy in transitively via ``servers.health_server``. As of MR 4b
that chain is gone -- HealthServer probes the db POD over TCP instead of pinging
an engine -- so the split no longer earns its keep on those grounds. It stays
because ``cli.dispatcher`` still constructs ``DispatchHealthServer`` directly and
has no reason to import a factory for a server it does not use.
'''
from discord.ext.commands import Bot

from discord_bot.servers.health_server import HealthServer
from discord_bot.utils.common import GeneralConfig


def setup_health_server(bot: Bot, general_config: GeneralConfig,
                        dispatch_http_url: str | None = None,
                        database_http_url: str | None = None):
    '''Return a HealthServer if monitoring.health_server.enabled, else None.'''
    if (general_config.monitoring and general_config.monitoring.health_server
            and general_config.monitoring.health_server.enabled):
        return HealthServer(
            bot,
            port=general_config.monitoring.health_server.port,
            bind_address=general_config.monitoring.health_server.bind_address,
            dispatch_http_url=dispatch_http_url,
            database_http_url=database_http_url,
        )
    return None
