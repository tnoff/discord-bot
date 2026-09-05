'''
Standalone persistence process — the discord-db pod.

One process owns postgres. The bot and the broker reach markov, playlists, guild
analytics and the video-cache catalog through HttpDatabaseClient instead of
holding an engine of their own, which is what lets `database` leave `[bot]` and
`[broker]` at the cutover. MR 3 of projects/discord-db-tier-extraction.

**This pod runs no migrations, deliberately.** `alembic upgrade head` replays
from base on postgres as of #915, but who runs it is a decision this MR does not
make — see projects/alembic-migration-ownership. The entrypoint ships inert the
same way this tier's 33 routes shipped inert, and the runner lands on this pod
afterwards. `cli/_lib/db.setup_db` therefore still calls
`BASE.metadata.create_all`: until something runs migrations it is the only thing
that builds a schema, so removing it here would break fresh deploys rather than
fix them.

Unlike every other pod in the fleet this one has **no Redis and no consumer
loop**. A store call is request/response by definition — the caller cannot
proceed until the row comes back — so there is nothing to queue and nothing to
drive. That is worth protecting: it earns the pod a smaller netpol and no redis
dependency, the way the search tier's credential-free property was worth
protecting until media_search ended it.

Configure with:
    general.sql_connection_statement — PostgreSQL DSN (required; no engine means
                                       no pod, so this raises rather than warns)
    general.database_server          — {host, port} for the HTTP server
                                       (default 0.0.0.0:8085)
    general.monitoring               — optional OTLP / health-server config
    music.download.cache             — VideoCache catalog config. Disabled or
                                       absent means the video_cache routes are
                                       not registered at all and answer 404,
                                       which is the server's designed behaviour
                                       for a group with no store.
'''
import asyncio
import logging
from contextlib import asynccontextmanager
from functools import partial

import click
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from discord_bot.clients.guild_analytics_client import GuildAnalyticsClient
from discord_bot.clients.markov_client import MarkovClient
from discord_bot.clients.playlist_client import PlaylistClient
from discord_bot.cogs.music_helpers.video_cache_client import VideoCacheClient
from discord_bot.types.video_cache import MusicCacheConfig
from discord_bot.exceptions import DiscordBotException
from discord_bot.servers.database_health_server import DatabasePingHealthServer
from discord_bot.servers.database_server import DEFAULT_PORT, DatabaseHttpServer
from discord_bot.utils.common import GeneralConfig, resolve_tracing_config

from discord_bot.cli._lib.common import (parse_and_validate_config, run_loop,
                                         setup_observability, shutdown_event_signals)
from discord_bot.cli._lib.db import instrument_sqlalchemy, managed_db

logger = logging.getLogger(__name__)


@click.command()
@click.argument('config_file', type=click.Path(dir_okay=False))
def main(config_file):
    '''Run the standalone persistence process (HTTP server over PostgreSQL).'''
    settings, general_config = parse_and_validate_config(config_file)
    run(settings, general_config)


def build_session_generator(db_engine):
    '''
    Return a callable yielding an AsyncSession context manager over db_engine.

    Every store takes this rather than the engine, which is the seam that made
    the Protocols implementable in the first place: a store that owns a session
    generator can be swapped for one that owns an HTTP client without its callers
    seeing a difference.
    '''
    @asynccontextmanager
    async def with_db_session():
        factory = async_sessionmaker(bind=db_engine, class_=AsyncSession, expire_on_commit=False)
        async with factory() as session:
            yield session

    return partial(with_db_session)


def build_video_cache_store(cache_cfg: dict, session_generator):
    '''
    Construct a VideoCacheClient if the catalog is enabled, else None.

    Validated through MusicCacheConfig for the same reason the broker does it: a
    bare .get('max_cache_files') yields None and crashes ready_remove's
    subtraction.

    **max_cache_files and storage_type are the pod's, not the caller's.** They
    describe the catalog this tier owns, so they are read here rather than sent
    over the wire on every request -- sending them would let two callers disagree
    about one catalog's policy. That asymmetry with the playlist group's
    max_size, which genuinely belongs to the guild making the request, is
    deliberate and is spelled out in servers/database_server.py.
    '''
    cache = MusicCacheConfig(**cache_cfg)
    if not cache.enable_cache_files:
        return None
    max_mb = cache.max_cache_size_mb
    return VideoCacheClient(
        cache.max_cache_files,
        session_generator,
        max_cache_size_bytes=(max_mb * 1024 * 1024 if max_mb else None),
        storage_type='s3',
    )


async def main_loop(database_server: DatabaseHttpServer, health_server):
    '''
    Run the persistence pod until SIGTERM/SIGINT, then drain the HTTP server.

    No LoopHealth entry and no heartbeat gauge: this pod has no background loop
    to report on, and registering one would publish a series that can only ever
    say "fine". The liveness signal that means something here is the database
    ping in DatabasePingHealthServer.
    '''
    with shutdown_event_signals() as stop_event:
        try:
            if health_server:
                asyncio.create_task(health_server.serve())
            asyncio.create_task(database_server.serve())
            logger.info('Main :: Database running')
            await stop_event.wait()
        finally:
            logger.info('Main :: Draining database server...')
            await database_server.drain_and_stop()
            logger.info('Main :: Shutdown complete')


def run_database(database_server: DatabaseHttpServer, health_server):
    '''Schedule main_loop on an event loop.'''
    run_loop(main_loop(database_server, health_server))


def run(settings: dict, general_config: GeneralConfig):
    '''Entry point for the standalone persistence process.'''
    setup_observability(general_config)

    with managed_db(general_config) as db_engine:
        # managed_db returns None when no DSN is configured. Every other pod can
        # degrade without a database; this one IS the database, so a missing DSN
        # is a startup failure rather than a warning and a pod that serves 404s.
        if db_engine is None:
            raise DiscordBotException(
                'general.sql_connection_statement required for the database pod'
            )
        instrument_sqlalchemy(db_engine)

        session_generator = build_session_generator(db_engine)
        download_cfg = settings.get('music', {}).get('download', {})

        server_cfg = settings.get('general', {}).get('database_server', {})
        # bandit B104: '0.0.0.0' default is intentional — bot and broker pods reach this across the k8s network; override via general.database_server.host
        host = server_cfg.get('host', '0.0.0.0')  # nosec B104
        port = int(server_cfg.get('port', DEFAULT_PORT))

        database_server = DatabaseHttpServer(
            guild_analytics_store=GuildAnalyticsClient(session_generator),
            markov_store=MarkovClient(session_generator),
            playlist_store=PlaylistClient(session_generator),
            video_cache_store=build_video_cache_store(
                download_cfg.get('cache', {}), session_generator),
            host=host,
            port=port,
        )

        health_server = None
        if (general_config.monitoring and general_config.monitoring.health_server
                and general_config.monitoring.health_server.enabled):
            health_server = DatabasePingHealthServer(
                db_engine,
                port=general_config.monitoring.health_server.port,
                bind_address=general_config.monitoring.health_server.bind_address,
                suppress_db_probe_auto_instrumentation=resolve_tracing_config(
                    general_config).suppress_db_probe_auto_instrumentation,
            )

        run_database(database_server, health_server)


if __name__ == '__main__':  # pragma: no cover
    main()  # pylint: disable=no-value-for-parameter
