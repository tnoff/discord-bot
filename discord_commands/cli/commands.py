'''
Commands process: HTTP-only, runs the logic behind the gateway's commands (#1030, Phase 2).
No gateway connection and no Discord token; it replies through the dispatcher pod.

Configure with:
    general.dispatch_http_url — Dispatcher pod URL; every reply goes through it (required —
                                raises DiscordBotException if missing)
    general.commands_server  — {host, port} for the HTTP server (default 0.0.0.0:8086)
    general.monitoring       — optional OTLP / health-server config
'''
import asyncio
import logging

import click

from discord_core.cli._lib.common import (
    parse_and_validate_config, run_loop, setup_observability, shutdown_event_signals,
)
from discord_core.clients.http_client_base import start_seam_checks
from discord_core.clients.http_dispatch_client import HttpDispatchClient
from discord_core.exceptions import DiscordBotException
from discord_core.utils.common import GeneralConfig

from discord_commands.command_logic.reply import dispatcher_reply
from discord_commands.handlers import build_handlers
from discord_commands.servers.commands_health_server import CommandsHealthServer
from discord_commands.servers.commands_server import CommandsHttpServer
from discord_commands.workers.command_executor import CommandExecutor


@click.command()
@click.argument('config_file', type=click.Path(dir_okay=False))
def main(config_file):
    '''Run the commands process.'''
    settings, general_config = parse_and_validate_config(config_file)
    run(settings, general_config)


async def main_loop(http_server: CommandsHttpServer, executor: CommandExecutor, health_server=None,
                    dispatcher: HttpDispatchClient | None = None):
    '''Serve until SIGTERM/SIGINT, then drain the HTTP server, stop the executor and close the dispatcher.'''
    logger = logging.getLogger('main')
    with shutdown_event_signals() as stop_event:
        if health_server:
            asyncio.create_task(health_server.serve())
        asyncio.create_task(http_server.serve())
        # Clients enrol themselves when built, in the synchronous run(); the probes start here, in
        # the first async context, like the gateway does on_ready.
        start_seam_checks()
        logger.info('Main :: commands running')
        await stop_event.wait()
        # Drain the HTTP server first so nothing is accepted after the executor stops, which would
        # be a 202 for a command that is never run.
        logger.info('Main :: Draining commands server...')
        await http_server.drain_and_stop()
        await executor.close()
        # After the executor: a command still running needs the dispatcher to send its reply.
        if dispatcher:
            await dispatcher.close()
        logger.info('Main :: Shutdown complete')


def run(settings: dict, general_config: GeneralConfig):
    '''Entry point for the commands process.'''
    logger = setup_observability(general_config)

    dispatch_http_url = settings.get('general', {}).get('dispatch_http_url')
    if not dispatch_http_url:
        raise DiscordBotException('dispatch_http_url required for the commands pod')
    dispatcher = HttpDispatchClient(dispatch_http_url, seam_contract=general_config.seam_contract)
    executor = CommandExecutor(build_handlers(dispatcher_reply(dispatcher), logger))
    cfg = settings.get('general', {}).get('commands_server', {})
    # bandit B104: '0.0.0.0' default is intentional — the gateway reaches this pod across the docker/k8s network; override via commands_server.host config
    http_server = CommandsHttpServer(
        executor,
        host=cfg.get('host', '0.0.0.0'),  # nosec B104
        port=int(cfg.get('port', 8086)),
    )

    health_server = None
    if general_config.monitoring and general_config.monitoring.health_server \
            and general_config.monitoring.health_server.enabled:
        health_server = CommandsHealthServer(
            port=general_config.monitoring.health_server.port,
            bind_address=general_config.monitoring.health_server.bind_address,
        )

    run_loop(main_loop(http_server, executor, health_server=health_server, dispatcher=dispatcher))


if __name__ == '__main__':  # pragma: no cover
    main()  # pylint: disable=no-value-for-parameter
