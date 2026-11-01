'''
Commands process: HTTP-only, runs the logic behind the gateway's commands (#1030).
No gateway connection and no Discord token; it replies through the dispatcher pod.

Configure with:
    general.dispatch_http_url — Dispatcher pod URL; every reply goes through it (required —
                                raises DiscordBotException if missing)
    general.database_http_url — discord-db pod URL; the markov and playlist commands read and write
                                through it (required — raises DiscordBotException if missing)
    markov.server_reject_list — optional guild ids markov refuses to run for
    general.include           — which optional loops run here (markov, delete_messages), as for the cogs
    markov.*                  — loop_sleep_interval, message_check_limit, history_retention_days (see docs/markov.md)
    delete_messages.*         — discord_channels and loop_sleep_interval (see docs/delete_messages.md)
    role.*                    — per-guild role rules for the role commands, see docs/role.md (optional)
    music.playlist.server_playlist_max_size — most items a playlist holds (default 64; merge and save honour it)
    music.general.message_delete_after — seconds before music and playlist replies are removed (default 300)
    music.broker_client.url — broker pod URL: the play queue and history, gateway intents (required)
    music.download_client.url — downloader pod URL (required)
    music.youtube_music_search_client.url — search pod URL, for the search queue (required)
    music.media_search_client.url — search pod URL, for expanding Spotify and YouTube links (required)
    music.player.queue_max_size — longest play queue (default 128)
    music.download.server_queue_priority — [{server_id, priority}] download priority per guild
    general.commands_server   — {host, port} for the HTTP server (default 0.0.0.0:8086)
    general.monitoring        — optional OTLP / health-server config
'''
import asyncio
import logging
from typing import Awaitable, Callable

import click

from discord_core.cli._lib.common import (
    parse_and_validate_config, run_loop, setup_observability, shutdown_event_signals,
)
from discord_core.clients.http_client_base import start_seam_checks
from discord_core.clients.http_broker_client import HttpBrokerClient
from discord_core.exceptions import DiscordBotException
from discord_core.utils.common import GeneralConfig, tracing_config_from_settings

from discord_commands.clients.http_guild_analytics_store import HttpGuildAnalyticsStore
from discord_commands.clients.http_guild_roles_client import HttpGuildRolesClient
from discord_commands.clients.http_download_client import HttpDownloadClient
from discord_commands.clients.http_markov_store import HttpMarkovStore
from discord_commands.clients.http_media_search_client import HttpMediaSearchClient
from discord_commands.clients.http_playlist_store import HttpPlaylistStore
from discord_commands.clients.http_youtube_music_search_client import HttpYoutubeMusicSearchClient
from discord_commands.clients.search_client import SearchClient
from discord_commands.command_logic.broker_player_gateway import BrokerPlayerGateway
from discord_commands.command_logic.music import MusicCommands
from discord_commands.command_logic.playlist import PlaylistCommands
from discord_commands.command_logic.reply import dispatcher_reply
from discord_commands.command_logic.role import RoleCommands
from discord_commands.handlers import build_handlers
from discord_commands.loop_logic.music_results import MusicResultsLoop
from discord_commands.servers.commands_health_server import CommandsHealthServer
from discord_commands.servers.commands_server import CommandsHttpServer
from discord_commands.types.role_config import RoleConfig
from discord_commands.utils.media_enqueuer import MediaEnqueuer, TrackQueuer
from discord_commands.workers.command_executor import CommandExecutor
from discord_commands.workers.dispatch_loops import DispatchLoopPair, build_dispatch_loops
from discord_commands.workers.guild_teardown import GuildTeardown
from discord_commands.workers.result_loops import ResultLoops


@click.command()
@click.argument('config_file', type=click.Path(dir_okay=False))
def main(config_file):
    '''Run the commands process.'''
    settings, general_config = parse_and_validate_config(config_file)
    run(settings, general_config)


async def main_loop(http_server: CommandsHttpServer, executor: CommandExecutor, teardown: GuildTeardown,
                    health_server=None,
                    dispatcher: HttpGuildRolesClient | None = None,
                    closers: list[Callable[[], Awaitable]] | None = None,
                    download_client: HttpDownloadClient | None = None,
                    result_loops: ResultLoops | None = None,
                    dispatch_loops: list[DispatchLoopPair] | None = None):
    '''
    Serve until SIGTERM/SIGINT, then drain the HTTP server, stop the executor and close the clients.

    teardown : stopped after the HTTP server has drained, so no teardown is accepted after it stops,
        and before the clients it uses close.
    closers : each client's close (or stop), awaited in order after the executor.
    download_client : started here, in the first async context, for the status poller that its
        queue_size reads from.
    result_loops : the search-result and download-result loops, started here and stopped first on
        shutdown so no popped result is dropped while the clients they use are closing.
    dispatch_loops : the markov and delete-messages loops (general.include), started and stopped with
        the result loops.
    '''
    logger = logging.getLogger('main')
    with shutdown_event_signals() as stop_event:
        if health_server:
            asyncio.create_task(health_server.serve())
        asyncio.create_task(http_server.serve())
        # Clients enrol themselves when built, in the synchronous run(); the probes start here, in
        # the first async context, like the gateway does on_ready.
        start_seam_checks()
        if download_client:
            await download_client.start()
        if result_loops:
            result_loops.start(stop_event)
        for loop in dispatch_loops or []:
            loop.start(stop_event)
        logger.info('Main :: commands running')
        await stop_event.wait()
        if result_loops:
            logger.info('Main :: Stopping result loops...')
            await result_loops.stop()
        for loop in dispatch_loops or []:
            await loop.stop()
        # Drain the HTTP server first so nothing is accepted after the executor stops, which would
        # be a 202 for a command that is never run.
        logger.info('Main :: Draining commands server...')
        await http_server.drain_and_stop()
        await executor.close()
        await teardown.close()
        # After the executor: a command still running needs the dispatcher to send its reply.
        if dispatcher:
            await dispatcher.close()
        for close in closers or []:
            await close()
        logger.info('Main :: Shutdown complete')


def _music_url(music: dict, client: str) -> str:
    '''The url of one of the music pod's peers, which the music commands cannot run without.'''
    url = music.get(client, {}).get('url')
    if not url:
        raise DiscordBotException(f'music.{client}.url required for the commands pod')
    return url


def run(settings: dict, general_config: GeneralConfig):
    '''Entry point for the commands process.'''
    logger = setup_observability(general_config)

    dispatch_http_url = settings.get('general', {}).get('dispatch_http_url')
    if not dispatch_http_url:
        raise DiscordBotException('dispatch_http_url required for the commands pod')
    dispatcher = HttpGuildRolesClient(dispatch_http_url, seam_contract=general_config.seam_contract)
    database_http_url = settings.get('general', {}).get('database_http_url')
    if not database_http_url:
        raise DiscordBotException('database_http_url required for the commands pod')
    markov_store = HttpMarkovStore(database_http_url, seam_contract=general_config.seam_contract)
    playlist_store = HttpPlaylistStore(database_http_url, seam_contract=general_config.seam_contract)
    analytics_store = HttpGuildAnalyticsStore(database_http_url, seam_contract=general_config.seam_contract)
    markov_reject_list = settings.get('markov', {}).get('server_reject_list', [])
    music = settings.get('music', {})
    reply = dispatcher_reply(dispatcher)

    broker_client = HttpBrokerClient(_music_url(music, 'broker_client'), seam_contract=general_config.seam_contract)
    # The status poller is what queue_size reads, so it is started in main_loop.
    download_client = HttpDownloadClient(
        _music_url(music, 'download_client'),
        trace_status_poll=tracing_config_from_settings(settings).trace_queue_worker_status_poll,
        seam_contract=general_config.seam_contract)
    youtube_music_search_client = HttpYoutubeMusicSearchClient(
        _music_url(music, 'youtube_music_search_client'), seam_contract=general_config.seam_contract)
    media_search_client = HttpMediaSearchClient(
        _music_url(music, 'media_search_client'), seam_contract=general_config.seam_contract)

    message_delete_after = music.get('general', {}).get('message_delete_after', 300)
    server_playlist_max_size = music.get('playlist', {}).get('server_playlist_max_size', 64)
    queue_max_size = music.get('player', {}).get('queue_max_size', 128)
    server_queue_priority = {int(item['server_id']): item['priority']
                             for item in music.get('download', {}).get('server_queue_priority') or []}

    player_gateway = BrokerPlayerGateway(broker_client, download_client,
                                         TrackQueuer(broker_client, logger, queue_max_size))
    enqueuer = MediaEnqueuer(
        broker_client, SearchClient(media_search_client), youtube_music_search_client, download_client,
        playlist_store, player_gateway, dispatcher, logger, queue_max_size, server_playlist_max_size,
        message_delete_after, server_queue_priority)
    playlist = PlaylistCommands(reply, logger, playlist_store, message_delete_after, server_playlist_max_size,
                                broker_client, player_gateway, enqueuer)
    result_loops = ResultLoops(
        broker_client,
        MusicResultsLoop(logger, broker_client, download_client, enqueuer, player_gateway, playlist_store,
                         server_queue_priority),
        logger)
    music_commands = MusicCommands(reply, broker_client, analytics_store, player_gateway, logger,
                                   message_delete_after, enqueuer, playlist)
    role_commands = RoleCommands(reply, dispatcher, RoleConfig.model_validate(settings.get('role', {})).model_dump())
    executor = CommandExecutor(build_handlers(reply, logger, markov_store, markov_reject_list,
                                              playlist, music_commands, role_commands))
    teardown = GuildTeardown(download_client, youtube_music_search_client, broker_client, enqueuer, logger)
    cfg = settings.get('general', {}).get('commands_server', {})
    # bandit B104: '0.0.0.0' default is intentional — the gateway reaches this pod across the docker/k8s network; override via commands_server.host config
    http_server = CommandsHttpServer(
        executor, teardown,
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

    closers = [markov_store.close, playlist_store.close, analytics_store.close, broker_client.close,
               download_client.stop, youtube_music_search_client.close, media_search_client.close]
    run_loop(main_loop(http_server, executor, teardown, health_server=health_server, dispatcher=dispatcher,
                       closers=closers, download_client=download_client, result_loops=result_loops,
                       dispatch_loops=build_dispatch_loops(settings, dispatcher, markov_store, logger)))


if __name__ == '__main__':  # pragma: no cover
    main()  # pylint: disable=no-value-for-parameter
