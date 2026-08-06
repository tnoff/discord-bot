'''
Standalone downloader process — yt-dlp + ffmpeg + S3 upload, Redis-backed queue.

Pulls MediaRequests off a redis queue (per-guild ZSETs with round-robin
fairness), runs yt-dlp/ffmpeg/S3, and posts results back to the broker pod via
HttpBrokerClient.  Multiple downloader pods can run concurrently — they share the
redis queue, the YouTube wait stamp, and the failure ZSET — but each pod drives a
SINGLE consumer loop: most deployments tunnel through one egress IP, so parallel
yt-dlp calls just race each other into the same rate-limit bucket.  Scale out with
more pods (distinct egress), not more loops per pod.

The pod also runs a DownloadHttpServer so bot pods (via HttpDownloadClient) can
submit / clear / block downloads and poll queue/backoff status over HTTP.

Configure with:
    general.redis_url / general.redis_sentinel — Redis connection (required)
    general.monitoring                 — optional OTLP / health-server config
    music.broker_client.url            — Broker pod URL (required; downloader posts
                                         IN_PROGRESS / RETRY / register_download_result)
    music.download.storage.bucket_name — S3 bucket (required for cache-mode checkout)
    music.download.download_dir_path   — scratch dir (default: a TemporaryDirectory)
    music.download.*                   — yt-dlp + backoff config (extra_ytdlp_options,
                                         max_video_length, banned_videos_list,
                                         youtube_wait_period_minimum, etc.)
    general.downloader_server          — {host, port} for the HTTP server
                                         (default 0.0.0.0:8083)
'''
import asyncio
import logging
from pathlib import Path
from tempfile import TemporaryDirectory

import click

from discord_bot.clients.broker_client import HttpBrokerClient
from discord_bot.clients.redis_client import RedisManager
from discord_bot.exceptions import DiscordBotException, ExitEarlyException
from discord_bot.servers.download_server import DownloadHttpServer
from discord_bot.servers.redis_health_server import RedisPingHealthServer
from discord_bot.utils.common import GeneralConfig
from discord_bot.utils.loop_health import LOOP_HEALTH, LoopHealth
from discord_bot.utils.integrations.egress_probe import build_exit_probe, ExitProbe
from discord_bot.utils.integrations.egress_pool import EGRESS_MODE_HTTP_PROXY
from discord_bot.workers.download_metrics import DownloadMetrics
from discord_bot.workers.redis_download_worker import RedisDownloadWorker

from discord_bot.cli._lib.common import (
    parse_and_validate_config, run_loop, setup_observability, shutdown_event_signals,
)

logger = logging.getLogger(__name__)

# LoopHealth registry key for the pod's download consumer driver pool.  One key
# for the whole pool, not one per driver: the drivers are interchangeable
# consumers of the same Redis queue, so "any driver making progress" is the
# health bit that matters (same rationale as the message dispatcher's worker
# pool).  Per-driver keys would fail this pod's liveness probe whenever a single
# driver sat in a slow download — and in pool mode a driver blocked on a flagged
# exit is routine, so that would turn ordinary exit flakiness into a restart loop.
LOOP_DOWNLOADER_WORKER = 'downloader_worker'


@click.command()
@click.argument('config_file', type=click.Path(dir_okay=False))
def main(config_file):
    '''Run the standalone downloader process (Redis download worker + HTTP server).'''
    settings, general_config = parse_and_validate_config(config_file)
    run(settings, general_config)


async def _drive_worker(worker: RedisDownloadWorker, stop_event: asyncio.Event,
                        driver_logger: logging.Logger, health: LoopHealth) -> None:
    '''
    Drive the worker's consumer loop until shutdown.

    ``worker.run(stop_event)`` consumes at most one queued item per call and
    returns; the while loop re-drives it.  In fixed-proxy mode one driver runs,
    because one egress IP means concurrent yt-dlp calls just collide in the same
    rate-limit bucket; pool (socks5) mode runs several, each on a distinct exit
    (see module docstring).

    ``health`` is the pool's shared LoopHealth, registered once by the caller and
    passed in — every driver reports into it, so the pod probes healthy while any
    driver is still making progress.  Marking it stopped is the caller's job too:
    a driver returning is not the pool shutting down when its siblings are still
    running.

    The broad ``except Exception`` is load-bearing.  An unguarded loop that dies
    on an unexpected error (e.g. a redis/broker blip) wedges the downloader
    forever.  ``broad-except`` is disabled globally in .pylintrc, so no inline
    disable is needed.

    The other half of that story — "while the health server keeps reporting
    green" — is what LoopHealth fixes: retrying forever is right, but it used to
    mean a permanently wedged worker still probed healthy.  Now the drivers
    report each iteration, so a pool that stops making progress altogether fails
    this pod's own health check.
    '''
    while not stop_event.is_set():
        try:
            await worker.run(stop_event)
            health.record_success()
        except ExitEarlyException:
            break
        except Exception:
            health.record_error()
            driver_logger.exception('Downloader :: worker loop error, backing off')
            await asyncio.sleep(1)


async def main_loop(worker: RedisDownloadWorker, download_http_server: DownloadHttpServer,
                    health_server, redis_manager: RedisManager,
                    download_metrics: DownloadMetrics, exit_probe: ExitProbe | None,
                    driver_count: int = 1):
    '''Run the downloader until SIGTERM/SIGINT, then drain the HTTP server and Redis.'''
    await redis_manager.start()
    with shutdown_event_signals() as stop_event:
        try:
            if health_server:
                asyncio.create_task(health_server.serve())
            asyncio.create_task(download_http_server.serve())
            # In-pod consumer loops draining the shared Redis queue.  Pool (socks5)
            # mode fans out across driver_count concurrent drivers, each popping and
            # reserving a distinct exit so downloads run in parallel; fixed-proxy
            # mode runs a single driver (one egress IP means concurrent yt-dlp calls
            # would just collide in the same rate-limit bucket).
            # One shared LoopHealth for the pool, registered here rather than inside
            # the driver: registering per-driver would re-arm the same entry N times
            # at startup, and letting each driver mark it stopped on exit would have
            # the first one to finish report the whole pool stopped while the rest
            # are still working.
            worker_health = LOOP_HEALTH.register(LOOP_DOWNLOADER_WORKER)
            for _ in range(driver_count):
                asyncio.create_task(_drive_worker(worker, stop_event, logger, worker_health))
            # Metrics poller exits on its own when stop_event is set.
            asyncio.create_task(download_metrics.run(stop_event))
            # Egress exit probe (optional): refreshes the cached exit through the
            # proxy; failures never propagate into the download path.
            if exit_probe is not None:
                asyncio.create_task(exit_probe.run(stop_event))
            logger.info('Main :: Downloader running')
            await stop_event.wait()
        finally:
            # A deliberate drain is not a wedge: mark the pool stopped once, here,
            # so a draining pod doesn't fail its own liveness probe while the
            # drivers wind down (the drivers never mark it themselves).
            LOOP_HEALTH.mark_stopped(LOOP_DOWNLOADER_WORKER)
            logger.info('Main :: Draining download server...')
            await download_http_server.drain_and_stop()
            await redis_manager.close()
            logger.info('Main :: Shutdown complete')


def run_downloader(worker: RedisDownloadWorker, download_http_server: DownloadHttpServer,
                   health_server, redis_manager: RedisManager,
                   download_metrics: DownloadMetrics, exit_probe: ExitProbe | None,
                   driver_count: int = 1):
    '''Schedule main_loop on an event loop.'''
    run_loop(main_loop(worker, download_http_server, health_server, redis_manager,
                       download_metrics, exit_probe, driver_count=driver_count))


def run(settings: dict, general_config: GeneralConfig):
    '''Entry point for the standalone downloader process.'''
    setup_observability(general_config)

    if not (general_config.redis_url or general_config.redis_sentinel):
        raise DiscordBotException('Redis required for downloader HA mode')
    redis_manager = RedisManager.from_general_config(general_config)

    music_settings = settings.get('music', {})
    broker_client_cfg = music_settings.get('broker_client') or {}
    broker_url = broker_client_cfg.get('url')
    if not broker_url:
        raise DiscordBotException('music.broker_client.url required for downloader HA mode')

    download_cfg = music_settings.get('download', {})
    storage_cfg = download_cfg.get('storage') or {}
    bucket_name = storage_cfg.get('bucket_name')
    broker_client = HttpBrokerClient(broker_url, bucket_name=bucket_name)

    download_dir_path = download_cfg.get('download_dir_path')
    if download_dir_path:
        download_dir = Path(download_dir_path)
    else:
        download_dir = Path(TemporaryDirectory().name)  # pylint:disable=consider-using-with
    download_dir.mkdir(exist_ok=True, parents=True)

    egress_mode = download_cfg.get('egress_mode', EGRESS_MODE_HTTP_PROXY)

    # Reuse the already-validated LoggingConfig off general_config — the same
    # object the cog passes as self.logging_config; get_logger tolerates None.
    worker = RedisDownloadWorker(
        general_config.logging,
        download_dir,
        redis_manager=redis_manager,
        youtube_egress_key=download_cfg.get('youtube_egress_key', 'default'),
        extra_ytdlp_options=download_cfg.get('extra_ytdlp_options'),
        max_video_length=download_cfg.get('max_video_length'),
        banned_video_list=download_cfg.get('banned_videos_list'),
        wait_period_minimum=int(download_cfg.get('youtube_wait_period_minimum', 30)),
        wait_period_max_variance=int(download_cfg.get('youtube_wait_period_max_variance', 10)),
        bucket_name=bucket_name,
        normalize_audio=bool(download_cfg.get('normalize_audio', False)),
        broker=broker_client,
        max_retries=int(download_cfg.get('max_download_retries', 3)),
        egress_mode=egress_mode,
        egress_exits=download_cfg.get('egress_exits'),
    )

    server_cfg = settings.get('general', {}).get('downloader_server', {})
    # bandit B104: '0.0.0.0' default is intentional — bot pods reach the downloader across the docker/k8s network; override via general.downloader_server.host
    download_http_server = DownloadHttpServer(
        worker,
        host=server_cfg.get('host', '0.0.0.0'),  # nosec B104
        port=int(server_cfg.get('port', 8083)),
    )

    health_server = None
    if (general_config.monitoring and general_config.monitoring.health_server
            and general_config.monitoring.health_server.enabled):
        health_server = RedisPingHealthServer(
            redis_manager,
            port=general_config.monitoring.health_server.port,
            bind_address=general_config.monitoring.health_server.bind_address,
        )

    download_metrics = DownloadMetrics(worker)

    # Exit attribution:
    #  * pool mode (mullvad-socks5, ...): each download leases a distinct exit, so
    #    the lease names the exit directly — a single-exit probe would be wrong.
    #    No probe is wired; the worker reads the leased exit_name.
    #  * fixed http-proxy mode: every download shares one exit, so probe it through
    #    the SAME proxy yt-dlp uses and cache the hostname for spans + failure logs.
    #    Selected by music.download.egress_probe (e.g. 'mullvad'); absent -> no probe
    #    and attribution reads 'unknown'. Proxy may be absent too, in which case the
    #    probe queries the exit directly.
    exit_probe = None
    if egress_mode == EGRESS_MODE_HTTP_PROXY:
        extra_ytdlp_options = download_cfg.get('extra_ytdlp_options') or {}
        exit_probe = build_exit_probe(download_cfg.get('egress_probe'),
                                      extra_ytdlp_options.get('proxy'))
        worker.set_exit_probe(exit_probe)

    # Concurrency: pool mode fans out to worker_count drivers (each leases a distinct
    # exit), capped at the number of exits so no driver is permanently starved. The
    # fixed http-proxy mode stays single-driver — one egress IP shares one rate limit.
    driver_count = 1
    if egress_mode != EGRESS_MODE_HTTP_PROXY:
        worker_count = max(1, int(download_cfg.get('worker_count', 1)))
        driver_count = min(worker_count, len(download_cfg.get('egress_exits') or [worker_count]))

    run_downloader(worker, download_http_server, health_server, redis_manager,
                   download_metrics, exit_probe, driver_count=driver_count)


if __name__ == '__main__':  # pragma: no cover
    main()  # pylint: disable=no-value-for-parameter
