'''
Standalone search process — the shared search pod.

Pulls MediaRequests off the Redis search queue (per-guild ZSETs with round-robin
fairness), resolves each to a YouTube videoId through ytmusicapi, and posts the
resolution back to the bot pod through the broker's search-result queue via
HttpBrokerClient.  The pod also runs a YoutubeMusicSearchHttpServer so bot pods
(via HttpYoutubeMusicSearchClient) can submit / clear / block searches and poll
queue+backoff status over HTTP.

**This is the search TIER's entrypoint, not a ytmusic-only one.**  The
media_search providers (Spotify + the YouTube Data API expansion) are thin HTTP
clients of the same shape, so they land in this image and this pod rather than
one Deployment, image pin, OCIR repo and netpol tier each — every extra pod is
another pin a revert-then-auto-bump can strand out of step with the bot, which is
what broke this seam twice.  Which is why the routes are already namespaced per
provider (/search/ytmusic, MR 4) and why this module is `search`, not
`youtube_music_search`: when the next provider lands it adds its own worker, its
own consumer loop and its own LoopHealth here, and the composite app that fronts
several route prefixes replaces the single server built below.

Like the downloader, the pod drives a SINGLE consumer loop: ytmusicapi's 429
window is per source IP and shared through Redis anyway, so parallel resolves in
one pod would just race each other into the same bucket.

Configure with:
    general.redis_url / general.redis_sentinel — Redis connection (required)
    general.monitoring                 — optional OTLP / health-server config
    general.search_server              — {host, port} for the HTTP server
                                         (default 0.0.0.0:8084)
    music.broker_client.url            — Broker pod URL (required; the pod pushes
                                         RETRY_SEARCH / FAILED lifecycle updates
                                         and registers search results)
    music.download.youtube_wait_period_minimum / _max_variance — 429 backoff shape
    music.download.max_youtube_music_search_retries — per-request retry budget
    music.download.failure_tracking_max_size / _max_age_seconds — failure queue
    music.download.server_queue_priority — [{server_id, priority}] used when a
                                         retried request is re-enqueued
'''
import asyncio
import logging

import click

from discord_bot.clients.http_broker_client import HttpBrokerClient
from discord_bot.clients.redis_client import RedisManager
from discord_bot.servers.youtube_music_search_server import YoutubeMusicSearchHttpServer
from discord_bot.utils.common import GeneralConfig
from discord_bot.utils.failure_queue import FailureQueue
from discord_bot.utils.integrations.youtube_music import YoutubeMusicClient
from discord_bot.utils.loop_health import LoopHealth
from discord_bot.workers.redis_youtube_music_search_worker import RedisYoutubeMusicSearchWorker
from discord_bot.workers.search_metrics import SearchMetrics
from discord_bot.workers.youtube_music_search_driver import YoutubeMusicSearchDriver

from discord_bot.cli._lib.common import parse_and_validate_config, run_loop, setup_observability
from discord_bot.cli._lib.worker_pod import (
    build_redis_health_server, drive_loop, require_broker_url, require_redis_manager,
    worker_pod_main_loop,
)

logger = logging.getLogger(__name__)

# LoopHealth registry key for the pod's search consumer loop.
#
# Deliberately the same name the cog's search loop uses
# (cogs/music.py LOOP_YOUTUBE_MUSIC_SEARCH): it is the same logical loop, moved
# to another process at the MR 6 cutover, and the series are already separated by
# the pod's own service/job labels.  Renaming it would make the heartbeat look
# like it vanished exactly when the cutover happens.
#
# It is NOT the server's heartbeat name — QueueWorkerHttpServer publishes
# `youtube_music_search_server` for its listener, so the pod's two liveness
# signals (the consumer loop and the HTTP listener) stay distinct series.
LOOP_SEARCH_WORKER = 'youtube_music_search'

DEFAULT_SEARCH_SERVER_PORT = 8084


@click.command()
@click.argument('config_file', type=click.Path(dir_okay=False))
def main(config_file):
    '''Run the standalone search process (Redis search worker + HTTP server).'''
    settings, general_config = parse_and_validate_config(config_file)
    run(settings, general_config)


async def _drive_search(driver: YoutubeMusicSearchDriver, stop_event: asyncio.Event,
                        driver_logger: logging.Logger, health: LoopHealth) -> None:
    '''
    Drive the search loop until shutdown.

    ``driver.run_once()`` resolves at most one queued search per call and
    returns — including on the idle and backoff-slice paths, which is what keeps
    a long 429 window from reading as a wedge (!192).  The guarded while loop and
    the health reporting come from cli/_lib/worker_pod.drive_loop, shared with the
    downloader pod.
    '''
    await drive_loop(driver.run_once, stop_event, driver_logger, health,
                     'Search :: worker loop error, backing off')


async def main_loop(driver: YoutubeMusicSearchDriver,
                    search_http_server: YoutubeMusicSearchHttpServer,
                    health_server, redis_manager: RedisManager,
                    search_metrics: SearchMetrics, broker_client=None):
    '''
    Run the search pod until SIGTERM/SIGINT, then drain the HTTP server and Redis.

    ONE consumer loop per pod: the 429 window is shared through Redis and keyed to
    the egress IP, so a second loop here would only race the first into the same
    rate limit.  Scale with pods if that ever changes — which is also where the
    next provider's loop will slot in, as a second entry from this factory with
    its own LoopHealth.
    '''
    def _tasks(stop_event, search_health):
        return [
            _drive_search(driver, stop_event, logger, search_health),
            # Metrics poller exits on its own when stop_event is set.
            search_metrics.run(stop_event),
        ]

    await worker_pod_main_loop(search_http_server, health_server, redis_manager,
                               LOOP_SEARCH_WORKER, 'Search', _tasks,
                               broker_client=broker_client)


def run_search(driver: YoutubeMusicSearchDriver,
               search_http_server: YoutubeMusicSearchHttpServer,
               health_server, redis_manager: RedisManager, search_metrics: SearchMetrics,
               broker_client=None):
    '''Schedule main_loop on an event loop.'''
    run_loop(main_loop(driver, search_http_server, health_server, redis_manager,
                       search_metrics, broker_client=broker_client))


def run(settings: dict, general_config: GeneralConfig):
    '''Entry point for the standalone search process.'''
    setup_observability(general_config)

    redis_manager = require_redis_manager(general_config, 'search')
    # No bucket_name: the search pod never checks out media, it only pushes
    # lifecycle updates and resolutions.
    broker_client = HttpBrokerClient(require_broker_url(settings, 'search'))

    download_cfg = settings.get('music', {}).get('download', {})
    # Reuse the already-validated LoggingConfig off general_config — the same
    # object the cog passes as self.logging_config; get_logger tolerates None.
    worker = RedisYoutubeMusicSearchWorker(
        general_config.logging,
        YoutubeMusicClient(),
        FailureQueue(
            max_size=int(download_cfg.get('failure_tracking_max_size', 100)),
            max_age_seconds=int(download_cfg.get('failure_tracking_max_age_seconds', 600)),
        ),
        int(download_cfg.get('youtube_wait_period_minimum', 30)),
        int(download_cfg.get('youtube_wait_period_max_variance', 10)),
        redis_manager=redis_manager,
    )

    # Guild priorities are the same config the cog reads, so a request re-enqueued
    # by a retry here keeps the priority it was submitted with instead of dropping
    # to the default bucket.
    queue_priority = {}
    for item in download_cfg.get('server_queue_priority') or []:
        queue_priority[int(item['server_id'])] = item['priority']

    # The driver pops and resolves straight off the worker: the pod IS the loop's
    # home under HA, so there is no client indirection here (and
    # HttpYoutubeMusicSearchClient deliberately has no pop/resolve surface).
    driver = YoutubeMusicSearchDriver(
        worker,
        broker_client,
        logger,
        max_retries=int(download_cfg.get('max_youtube_music_search_retries', 3)),
        queue_priority=queue_priority,
    )

    server_cfg = settings.get('general', {}).get('search_server', {})
    # bandit B104: '0.0.0.0' default is intentional — bot pods reach the search pod across the docker/k8s network; override via general.search_server.host
    search_http_server = YoutubeMusicSearchHttpServer(
        worker,
        host=server_cfg.get('host', '0.0.0.0'),  # nosec B104
        port=int(server_cfg.get('port', DEFAULT_SEARCH_SERVER_PORT)),
    )

    health_server = build_redis_health_server(general_config, redis_manager)

    search_metrics = SearchMetrics(worker)

    run_search(driver, search_http_server, health_server, redis_manager, search_metrics,
               broker_client=broker_client)


if __name__ == '__main__':  # pragma: no cover
    main()  # pylint: disable=no-value-for-parameter
