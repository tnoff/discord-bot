'''
Standalone media broker process — HTTP, Redis state, PostgreSQL DB.

Accepts HTTP requests from bot pods and download workers. Stores all broker
registry state in Redis so multiple instances can run simultaneously. Requires
PostgreSQL (SQLite cannot be shared across pods).

Configure with:
    general.redis_url         — Redis connection URL (required)
    general.broker_server     — {host, port} for the HTTP server (default 0.0.0.0:8081)
    general.monitoring        — optional OTLP/health server config
    music.storage.bucket_name — S3 bucket (required for checkout in HA mode)
    music.download.cache      — optional VideoCache config
'''
import asyncio
import logging
import signal

import click
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from discord_bot.clients.redis_client import RedisManager
from discord_bot.workers.redis_broker import RedisBroker
from discord_bot.cogs.music_helpers.video_cache_client import VideoCacheClient
from discord_bot.servers.broker_server import BrokerHealthServer, BrokerHttpServer
from discord_bot.utils.common import GeneralConfig
from discord_bot.workers.broker_registry import RedisBrokerRegistry

from discord_bot.cli.common import (
    run_loop, setup_redis_observability,
    parse_and_validate_config,
)
from discord_bot.cli.db import instrument_sqlalchemy, managed_db, require_postgres


@click.command()
@click.argument('config_file', type=click.Path(dir_okay=False))
def main(config_file):
    '''Run the standalone media broker process (HTTP, Redis state, PostgreSQL).'''
    settings, general_config = parse_and_validate_config(config_file)
    run(settings, general_config)


async def main_loop(broker_server: BrokerHttpServer, health_server,
                    redis_manager: RedisManager):
    '''Main loop for the broker process. Runs until SIGTERM or SIGINT.'''
    logger = logging.getLogger('main')
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
        logger.info('Main :: Broker running')
        await stop_event.wait()
    finally:
        logger.info('Main :: Draining broker server...')
        await broker_server.drain_and_stop()
        await redis_manager.close()
        logger.info('Main :: Shutdown complete')


def run_broker(broker_server: BrokerHttpServer, health_server, redis_manager: RedisManager):
    '''Schedule main_loop on an existing event loop or start a new one.'''
    run_loop(main_loop(broker_server, health_server, redis_manager))


def run(settings: dict, general_config: GeneralConfig):
    '''Entry point for the standalone broker process.'''
    require_postgres(general_config)
    setup_redis_observability(general_config, 'broker HA mode')

    redis_manager = RedisManager(general_config.redis_url)
    registry = RedisBrokerRegistry(redis_manager)

    with managed_db(general_config) as db_engine:
        instrument_sqlalchemy()

        video_cache = None
        if db_engine:
            session_factory = async_sessionmaker(
                bind=db_engine, class_=AsyncSession, expire_on_commit=False
            )
            music_cfg = settings.get('music', {})
            cache_cfg = music_cfg.get('download', {}).get('cache', {})
            if cache_cfg.get('enable_cache_files'):
                max_files = cache_cfg.get('max_cache_files', 100)
                max_size_mb = cache_cfg.get('max_cache_size_mb')
                video_cache = VideoCacheClient(
                    max_files,
                    session_factory,
                    max_cache_size_bytes=max_size_mb * 1024 * 1024 if max_size_mb else None,
                    storage_type='s3',
                )

        bucket_name = settings.get('music', {}).get('storage', {}).get('bucket_name')
        broker = RedisBroker(registry, video_cache=video_cache, bucket_name=bucket_name)

        broker_cfg = settings.get('general', {}).get('broker_server', {})
        broker_server = BrokerHttpServer(
            broker,
            host=broker_cfg.get('host', '0.0.0.0'),
            port=int(broker_cfg.get('port', 8081)),
            ha_mode=True,
        )

        health_server = None
        if (general_config.monitoring and general_config.monitoring.health_server
                and general_config.monitoring.health_server.enabled):
            health_server = BrokerHealthServer(
                redis_manager,
                port=general_config.monitoring.health_server.port,
                db_engine=db_engine,
            )

        run_broker(broker_server, health_server, redis_manager)


if __name__ == '__main__':
    main()  # pylint: disable=no-value-for-parameter
