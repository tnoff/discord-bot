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
from discord_bot.utils.loop_health import LOOP_HEALTH
from discord_bot.utils.integrations.egress_probe import build_exit_probe, ExitProbe
from discord_bot.workers.download_metrics import DownloadMetrics
from discord_bot.workers.redis_download_worker import RedisDownloadWorker

from discord_bot.cli._lib.common import (
    parse_and_validate_config, run_loop, setup_observability, shutdown_event_signals,
)

logger = logging.getLogger(__name__)

# LoopHealth registry key for the pod's single download consumer driver
LOOP_DOWNLOADER_WORKER = 'downloader_worker'


@click.command()
@click.argument('config_file', type=click.Path(dir_okay=False))
def main(config_file):
    '''Run the standalone downloader process (Redis download worker + HTTP server).'''
    settings, general_config = parse_and_validate_config(config_file)
    run(settings, general_config)


async def _drive_worker(worker: RedisDownloadWorker, stop_event: asyncio.Event,
                        driver_logger: logging.Logger) -> None:
    '''
    Drive the worker's consumer loop until shutdown.

    ``worker.run(stop_event)`` consumes at most one queued item per call and
    returns; the while loop re-drives it.  A single driver per pod is deliberate:
    one egress IP means concurrent yt-dlp calls just collide in the same
    rate-limit bucket (see module docstring).

    The broad ``except Exception`` is load-bearing.  An unguarded loop that dies
    on an unexpected error (e.g. a redis/broker blip) wedges the downloader
    forever.  ``broad-except`` is disabled globally in .pylintrc, so no inline
    disable is needed.

    The other half of that story — "while the health server keeps reporting
    green" — is what LoopHealth fixes: retrying forever is right, but it used to
    mean a permanently wedged worker still probed healthy.  Now the driver
    reports each iteration, so a worker that stops making progress fails this
    pod's own health check.
    '''
    health = LOOP_HEALTH.register(LOOP_DOWNLOADER_WORKER)
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
    # Loop left on purpose (shutdown or ExitEarly), not wedged.
    health.mark_stopped()


async def main_loop(worker: RedisDownloadWorker, download_http_server: DownloadHttpServer,
                    health_server, redis_manager: RedisManager,
                    download_metrics: DownloadMetrics, exit_probe: ExitProbe | None):
    '''Run the downloader until SIGTERM/SIGINT, then drain the HTTP server and Redis.'''
    await redis_manager.start()
    with shutdown_event_signals() as stop_event:
        try:
            if health_server:
                asyncio.create_task(health_server.serve())
            asyncio.create_task(download_http_server.serve())
            # Single in-pod consumer loop drains the shared Redis queue.
            asyncio.create_task(_drive_worker(worker, stop_event, logger))
            # Metrics poller exits on its own when stop_event is set.
            asyncio.create_task(download_metrics.run(stop_event))
            # Egress exit probe (optional): refreshes the cached exit through the
            # proxy; failures never propagate into the download path.
            if exit_probe is not None:
                asyncio.create_task(exit_probe.run(stop_event))
            logger.info('Main :: Downloader running')
            await stop_event.wait()
        finally:
            logger.info('Main :: Draining download server...')
            await download_http_server.drain_and_stop()
            await redis_manager.close()
            logger.info('Main :: Shutdown complete')


def run_downloader(worker: RedisDownloadWorker, download_http_server: DownloadHttpServer,
                   health_server, redis_manager: RedisManager,
                   download_metrics: DownloadMetrics, exit_probe: ExitProbe | None):
    '''Schedule main_loop on an event loop.'''
    run_loop(main_loop(worker, download_http_server, health_server, redis_manager,
                       download_metrics, exit_probe))


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

    # Probe the live egress exit through the SAME proxy yt-dlp downloads use, and
    # give the worker a handle so create_source spans + failure logs can read the
    # cached exit hostname. Selected by music.download.egress_probe (e.g. 'mullvad');
    # absent -> no probe and attribution reads 'unknown'. Proxy may be absent too,
    # in which case the probe just queries the exit directly.
    extra_ytdlp_options = download_cfg.get('extra_ytdlp_options') or {}
    exit_probe = build_exit_probe(download_cfg.get('egress_probe'),
                                  extra_ytdlp_options.get('proxy'))
    worker.set_exit_probe(exit_probe)

    run_downloader(worker, download_http_server, health_server, redis_manager,
                   download_metrics, exit_probe)


if __name__ == '__main__':  # pragma: no cover
    main()  # pylint: disable=no-value-for-parameter
