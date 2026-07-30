'''
Standalone media broker process — HTTP front-end, Redis state, PostgreSQL DB.

Serves the full BrokerHttpServer surface to bot pods and download workers and
keeps all broker registry/bundle state in Redis so multiple instances can run
simultaneously.  PostgreSQL is required (SQLite cannot be shared across pods);
managed_db enforces that.

Configure with:
    general.redis_url             — Redis connection URL (required)
    general.sql_connection_statement — PostgreSQL DSN (required)
    general.broker_server         — {host, port} for the HTTP server (default 0.0.0.0:8081)
    general.dispatch_http_url     — Dispatcher URL; the broker pushes bundle-UI
                                    edits / failure summaries through it.  Without
                                    it the broker tracks bundle state but cannot
                                    update Discord messages.
    general.monitoring            — optional OTLP / health-server config
    music.storage.bucket_name     — S3 bucket (required for HA checkout)
    music.download.cache          — optional VideoCache config
'''
import asyncio
import logging
import signal
from contextlib import asynccontextmanager
from functools import partial

import click
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from discord_bot.clients.http_dispatch_client import HttpDispatchClient
from discord_bot.clients.redis_client import RedisManager
from discord_bot.cogs.music_helpers.video_cache_client import VideoCacheClient, MusicCacheConfig
from discord_bot.servers.broker_health_server import BrokerHealthServer
from discord_bot.servers.broker_server import BrokerHttpServer
from discord_bot.utils.common import GeneralConfig
from discord_bot.workers.broker_metrics import BrokerMetrics
from discord_bot.workers.broker_registry import RedisBrokerRegistry
from discord_bot.workers.redis_broker import RedisBroker
from discord_bot.workers.redis_queues import RedisDownloadResultQueue

from discord_bot.cli._lib.common import parse_and_validate_config, run_loop, setup_observability
from discord_bot.cli._lib.db import instrument_sqlalchemy, managed_db

logger = logging.getLogger(__name__)


@click.command()
@click.argument('config_file', type=click.Path(dir_okay=False))
def main(config_file):
    '''Run the standalone media broker process (HTTP, Redis state, PostgreSQL).'''
    settings, general_config = parse_and_validate_config(config_file)
    run(settings, general_config)


def _build_video_cache(cache_cfg: dict, db_engine, bucket_name: str | None):
    '''Construct a VideoCacheClient if caching is enabled and a DB + bucket exist.'''
    # Validate through the shared model so raw-dict config gets the same
    # defaults/validation as the cog path (a bare .get('max_cache_files')
    # yields None and crashes ready_remove's subtraction).
    cache = MusicCacheConfig(**cache_cfg)
    if not (cache.enable_cache_files and db_engine and bucket_name):
        return None

    @asynccontextmanager
    async def with_db_session():
        factory = async_sessionmaker(bind=db_engine, class_=AsyncSession, expire_on_commit=False)
        async with factory() as session:
            yield session

    max_mb = cache.max_cache_size_mb
    return VideoCacheClient(
        cache.max_cache_files,
        partial(with_db_session),
        max_cache_size_bytes=(max_mb * 1024 * 1024 if max_mb else None),
        storage_type='s3',
    )


async def main_loop(broker_server: BrokerHttpServer, health_server, redis_manager: RedisManager,
                    broker_metrics: BrokerMetrics):
    '''Run the broker until SIGTERM/SIGINT, then drain the HTTP server and Redis.'''
    await redis_manager.start()
    stop_event = asyncio.Event()
    loop = asyncio.get_running_loop()

    def _handle_signal(signum, _frame):
        logger.info('Main :: Received %s, triggering graceful shutdown...', signal.Signals(signum).name)
        loop.call_soon_threadsafe(stop_event.set)

    signal.signal(signal.SIGTERM, _handle_signal)
    signal.signal(signal.SIGINT, _handle_signal)
    try:
        if health_server:
            asyncio.create_task(health_server.serve())
        asyncio.create_task(broker_server.serve())
        # Metrics poller exits on its own when stop_event is set.
        asyncio.create_task(broker_metrics.run(stop_event))
        logger.info('Main :: Broker running')
        await stop_event.wait()
    finally:
        logger.info('Main :: Draining broker server...')
        await broker_server.drain_and_stop()
        await redis_manager.close()
        logger.info('Main :: Shutdown complete')


def run_broker(broker_server: BrokerHttpServer, health_server, redis_manager: RedisManager,
               broker_metrics: BrokerMetrics):
    '''Schedule main_loop on an event loop.'''
    run_loop(main_loop(broker_server, health_server, redis_manager, broker_metrics))


def run(settings: dict, general_config: GeneralConfig):
    '''Entry point for the standalone broker process.'''
    setup_observability(general_config)
    redis_manager = RedisManager.from_general_config(general_config)
    registry = RedisBrokerRegistry(redis_manager)

    with managed_db(general_config) as db_engine:
        instrument_sqlalchemy(db_engine)

        general_settings = settings.get('general', {})
        music_settings = settings.get('music', {})
        bucket_name = music_settings.get('storage', {}).get('bucket_name')
        download_cfg = music_settings.get('download', {})

        video_cache = _build_video_cache(download_cfg.get('cache', {}), db_engine, bucket_name)

        dispatch_http_url = general_settings.get('dispatch_http_url')
        if dispatch_http_url:
            dispatcher = HttpDispatchClient(dispatch_http_url)
        else:
            # No dispatcher wired: the broker still tracks bundle state but every
            # request_bundle render / failure summary is silently dropped, so the
            # user sees no "queued / downloading / ready" messages. That failure is
            # invisible without this warning (a mis-keyed general.dispatch_http_url
            # lands here just like an intentional omission).
            logger.warning(
                'No general.dispatch_http_url configured — broker will track bundle '
                'state but cannot push any bundle UI / failure messages to Discord.'
            )
            dispatcher = None

        broker = RedisBroker(
            registry,
            video_cache=video_cache,
            bucket_name=bucket_name,
            dispatcher=dispatcher,
            download_max_retries=int(download_cfg.get('max_download_retries', 3)),
            search_max_retries=int(download_cfg.get('max_youtube_music_search_retries', 3)),
        )

        # Redis-backed bot-ready queue so multiple broker pods share it and a pod
        # restart doesn't lose in-flight DownloadResults.
        result_queue = RedisDownloadResultQueue(redis_manager)
        broker_metrics = BrokerMetrics(result_queue, registry)

        broker_cfg = general_settings.get('broker_server', {})
        broker_server = BrokerHttpServer(
            broker,
            host=broker_cfg.get('host', '0.0.0.0'),  # nosec B104
            port=int(broker_cfg.get('port', 8081)),
            result_queue=result_queue,
        )

        health_server = None
        if (general_config.monitoring and general_config.monitoring.health_server
                and general_config.monitoring.health_server.enabled):
            health_server = BrokerHealthServer(
                redis_manager,
                port=general_config.monitoring.health_server.port,
                bind_address=general_config.monitoring.health_server.bind_address,
            )

        run_broker(broker_server, health_server, redis_manager, broker_metrics)
