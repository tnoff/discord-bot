'''
Standalone download worker process — reads MediaRequests from a Redis Stream,
downloads them via yt-dlp, and writes DownloadResults back to a Redis Stream.
No Discord gateway, no database.
'''
import asyncio
from functools import partial
import signal
from pathlib import Path
from tempfile import TemporaryDirectory

from discord_bot.cogs.music_helpers.config import MusicDownloadConfig
from discord_bot.cogs.music_helpers.download_client import DownloadClient
from discord_bot.exceptions import ExitEarlyException
from discord_bot.utils.common import GeneralConfig, get_logger
from discord_bot.utils.failure_queue import FailureQueue
from discord_bot.utils.redis_client import get_redis_client


async def _worker_task_loop(function, shutdown_event, logger):
    '''
    Minimal loop runner for the standalone worker.
    Loops while shutdown_event is not set.
    Exits cleanly on ExitEarlyException (normal shutdown signal).
    Logs and exits on any other unhandled exception.
    '''
    while not shutdown_event.is_set():
        try:
            await function()
        except ExitEarlyException:
            return
        except Exception as exc:  #pylint:disable=broad-exception-caught
            logger.exception('Worker task loop error: %s', type(exc).__name__, exc_info=True)
            return


async def _run_worker(settings: dict, general_config: GeneralConfig):
    '''Main async body of the worker process.'''
    logger = get_logger('download_worker', general_config.logging)
    settings_general = settings.get('general', {})
    download_config = MusicDownloadConfig(**settings.get('download', {}))

    redis_url = settings_general['redis_url']
    process_id = settings_general['download_worker_process_id']

    redis_client = get_redis_client(redis_url)
    failure_queue = FailureQueue(
        max_size=download_config.failure_tracking_max_size,
        max_age_seconds=download_config.failure_tracking_max_age_seconds,
    )

    if download_config.download_dir_path:
        download_dir = Path(download_config.download_dir_path)
        download_dir.mkdir(exist_ok=True, parents=True)
    else:
        download_dir = Path(TemporaryDirectory().name)  #pylint:disable=consider-using-with
        download_dir.mkdir(exist_ok=True, parents=True)

    storage_bucket_name = download_config.storage.bucket_name if download_config.storage else None

    download_client = DownloadClient(
        general_config.logging,
        download_dir,
        extra_ytdlp_options=download_config.extra_ytdlp_options,
        max_video_length=download_config.max_video_length,
        banned_video_list=download_config.banned_videos_list,
        failure_queue=failure_queue,
        wait_period_minimum=download_config.youtube_wait_period_minimum,
        wait_period_max_variance=download_config.youtube_wait_period_max_variance,
        bucket_name=storage_bucket_name,
        normalize_audio=download_config.normalize_audio,
        max_retries=download_config.max_download_retries,
        redis_client=redis_client,
        redis_process_id=process_id,
    )

    shutdown_event = asyncio.Event()
    loop = asyncio.get_running_loop()

    def _handle_signal(signum, _frame):
        sig_name = signal.Signals(signum).name
        logger.info('Download worker received %s, shutting down', sig_name)
        loop.call_soon_threadsafe(shutdown_event.set)

    signal.signal(signal.SIGTERM, _handle_signal)
    signal.signal(signal.SIGINT, _handle_signal)

    logger.info('Download worker starting, process_id=%s', process_id)

    feeder_task = asyncio.create_task(
        _worker_task_loop(partial(download_client.run_redis_feeder, shutdown_event), shutdown_event, logger)
    )
    runner_task = asyncio.create_task(
        _worker_task_loop(partial(download_client.run, shutdown_event), shutdown_event, logger)
    )

    await asyncio.gather(feeder_task, runner_task)
    logger.info('Download worker shutdown complete')


def run(settings: dict, general_config: GeneralConfig):
    '''Entry point for the standalone download worker process.'''
    asyncio.run(_run_worker(settings, general_config))
