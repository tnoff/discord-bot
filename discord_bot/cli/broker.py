'''
Standalone media broker process — HTTP front-end, Redis state, remote DB.

Serves the full BrokerHttpServer surface to bot pods and download workers and
keeps all broker registry/bundle state in Redis so multiple instances can run
simultaneously.

This process holds **no database engine**. The video-cache catalog it reads and
writes lives on the discord-db pod and is reached over HTTP, which is why
[database] left the broker extra: SQLAlchemy, asyncpg and alembic are no longer
installed here and no import may reach for them. What did NOT move is the object
side -- MediaBroker still checks media out of S3 -- so this pod keeps [storage].

Configure with:
    general.redis_url             — Redis connection URL (required)
    general.database_http_url     — discord-db pod URL, e.g. http://discord-db:8085.
                                    Required when music.download.cache is enabled;
                                    without it the catalog is unreachable and the
                                    cache is disabled with a warning.
    general.broker_server         — {host, port} for the HTTP server (default 0.0.0.0:8081)
    general.dispatch_http_url     — Dispatcher URL; the broker pushes bundle-UI
                                    edits / failure summaries through it.  Without
                                    it the broker tracks bundle state but cannot
                                    update Discord messages.
    general.monitoring            — optional OTLP / health-server config
    music.storage.bucket_name     — S3 bucket (required for HA checkout)
    music.download.cache          — optional video-cache config. Only
                                    enable_cache_files is read here; the eviction
                                    policy belongs to the db pod that owns the rows.
    music.general.message_delete_after — seconds before Discord auto-expires the
                                    bundle summary / failure summary messages this
                                    process sends (default 300)
'''
import asyncio
import logging
import signal

import click

from discord_bot.clients.http_dispatch_client import HttpDispatchClient
from discord_bot.clients.http_video_cache_store import HttpVideoCacheStore
from discord_bot.clients.redis_client import RedisManager
from discord_bot.servers.broker_health_server import BrokerHealthServer
from discord_bot.servers.broker_server import BrokerHttpServer
from discord_bot.types.video_cache import MusicCacheConfig
from discord_bot.utils.common import GeneralConfig
from discord_bot.workers.broker_metrics import BrokerMetrics
from discord_bot.workers.broker_registry import RedisBrokerRegistry
from discord_bot.workers.redis_broker import RedisBroker
from discord_bot.workers.redis_queues import RedisDownloadResultQueue, RedisSearchResultQueue

from discord_bot.cli._lib.common import parse_and_validate_config, run_loop, setup_observability

logger = logging.getLogger(__name__)

# Mirror of MusicGeneralConfig.message_delete_after (discord_bot/cogs/music.py).
# Duplicated rather than imported: cogs.music drags the whole cog import chain
# (discord.py, spotipy, yt-dlp) into this slim process.
DEFAULT_MESSAGE_DELETE_AFTER = 300


@click.command()
@click.argument('config_file', type=click.Path(dir_okay=False))
def main(config_file):
    '''Run the standalone media broker process (HTTP, Redis state, PostgreSQL).'''
    settings, general_config = parse_and_validate_config(config_file)
    run(settings, general_config)


def _build_video_cache(cache_cfg: dict, database_http_url: str | None,
                       bucket_name: str | None):
    '''Construct an HttpVideoCacheStore if caching is enabled and a db pod + bucket exist.

    **max_cache_files, max_cache_size_mb and storage_type are deliberately not
    passed.** They describe the catalog, and the catalog now belongs to the db
    pod, which reads them from its own config (cli/database.build_video_cache_store).
    Sending them per request would let two callers disagree about one catalog's
    eviction policy. Only enable_cache_files is read here, because that decides
    whether THIS pod uses the catalog at all -- a local question, not a policy one.
    '''
    # Still validated through the shared model rather than read raw: it is the one
    # place enable_cache_files gets a defined default, and a typo'd key silently
    # reading as False is exactly the "looks configured, does nothing" failure the
    # warning below exists to catch.
    cache = MusicCacheConfig(**cache_cfg)
    if not cache.enable_cache_files:
        return None
    if not (database_http_url and bucket_name):
        # Loud, because the config asked for a cache and will not get one. Under
        # the in-process client a missing engine failed the same way and just as
        # quietly; with the catalog behind a URL there is one more way to
        # fat-finger it, so the silence is no longer affordable.
        logger.warning(
            'music.download.cache is enabled but %s is missing — video cache disabled.',
            'general.database_http_url' if not database_http_url else 'music.storage.bucket_name',
        )
        return None
    return HttpVideoCacheStore(database_http_url)


async def main_loop(broker_server: BrokerHttpServer, health_server, redis_manager: RedisManager,
                    broker_metrics: BrokerMetrics, video_cache=None):
    '''Run the broker until SIGTERM/SIGINT, then drain the HTTP server, Redis and the db client.'''
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
        # The catalog is an HTTP client now, so it owns an aiohttp session that
        # aiohttp complains about ("Unclosed client session") if the process exits
        # holding it. Ordered after drain_and_stop because an in-flight request may
        # still be reading the cache. Guarded on the attribute rather than on None:
        # tests pass a stub, and a stub without close() should not take the
        # shutdown path down with it.
        if video_cache is not None and hasattr(video_cache, 'close'):
            await video_cache.close()
        logger.info('Main :: Shutdown complete')


def run_broker(broker_server: BrokerHttpServer, health_server, redis_manager: RedisManager,
               broker_metrics: BrokerMetrics, video_cache=None):
    '''Schedule main_loop on an event loop.'''
    run_loop(main_loop(broker_server, health_server, redis_manager, broker_metrics,
                       video_cache=video_cache))


def run(settings: dict, general_config: GeneralConfig):
    '''Entry point for the standalone broker process.'''
    setup_observability(general_config)
    redis_manager = RedisManager.from_general_config(general_config)
    registry = RedisBrokerRegistry(redis_manager)

    general_settings = settings.get('general', {})
    music_settings = settings.get('music', {})
    bucket_name = music_settings.get('storage', {}).get('bucket_name')
    download_cfg = music_settings.get('download', {})
    music_general_cfg = music_settings.get('general', {})

    database_http_url = general_settings.get('database_http_url')
    video_cache = _build_video_cache(download_cfg.get('cache', {}), database_http_url,
                                     bucket_name)

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
        # Without this the base class keeps message_delete_after=None, and every
        # message this process sends outlives the session: the "Error Details for
        # Failed Downloads" summary goes out through send_message with no
        # delete_after (nothing tracks it afterwards, so it is unreachable and
        # sits in the channel forever), and the finished "Completed N/N" bundle
        # summary neither expires nor gets dropped from the dispatcher's store.
        # The in-process path (cogs/music.py) has always passed this; the
        # standalone broker never did, so the fix that added it was a no-op in HA.
        message_delete_after=int(
            music_general_cfg.get('message_delete_after', DEFAULT_MESSAGE_DELETE_AFTER)
        ),
    )

    # Redis-backed bot-ready queues so multiple broker pods share them and a
    # pod restart doesn't lose in-flight DownloadResults / SearchResolutions.
    result_queue = RedisDownloadResultQueue(redis_manager)
    search_result_queue = RedisSearchResultQueue(redis_manager)
    broker_metrics = BrokerMetrics(result_queue, registry,
                                   search_result_queue=search_result_queue)

    broker_cfg = general_settings.get('broker_server', {})
    broker_server = BrokerHttpServer(
        broker,
        host=broker_cfg.get('host', '0.0.0.0'),  # nosec B104
        port=int(broker_cfg.get('port', 8081)),
        result_queue=result_queue,
        search_result_queue=search_result_queue,
    )

    health_server = None
    if (general_config.monitoring and general_config.monitoring.health_server
            and general_config.monitoring.health_server.enabled):
        health_server = BrokerHealthServer(
            redis_manager,
            port=general_config.monitoring.health_server.port,
            bind_address=general_config.monitoring.health_server.bind_address,
        )

    run_broker(broker_server, health_server, redis_manager, broker_metrics,
               video_cache=video_cache)
