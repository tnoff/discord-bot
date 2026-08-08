'''
Background metrics collector for the standalone downloader process.

The polling body lives on QueueMetricsBase (shared with the YouTube-Music search
pod); this module supplies the downloader's metric names + job label.

Gauges (job="discord-downloader", background_job="downloader"):
    download_queue_depth              — total pending downloads across all guilds
    download_youtube_backoff_seconds  — seconds left on the shared YouTube window
    download_failure_count            — failures in the shared YouTube ZSET
'''
from typing import ClassVar

from discord_bot.utils.otel import MetricNaming
from discord_bot.workers.queue_metrics import DEFAULT_POLL_INTERVAL_SECONDS, QueueMetricsBase

__all__ = ['DownloadMetrics', 'DEFAULT_POLL_INTERVAL_SECONDS']


class DownloadMetrics(QueueMetricsBase):
    '''Polls Redis-backed downloader state into gauge-friendly cached values.'''

    QUEUE_DEPTH_METRIC: ClassVar[str] = MetricNaming.DOWNLOAD_QUEUE_DEPTH.value
    QUEUE_DEPTH_DESCRIPTION: ClassVar[str] = 'Pending downloads across all guild queues'
    BACKOFF_METRIC: ClassVar[str] = MetricNaming.DOWNLOAD_YOUTUBE_BACKOFF.value
    BACKOFF_DESCRIPTION: ClassVar[str] = 'Seconds remaining on the shared YouTube backoff window'
    FAILURE_COUNT_METRIC: ClassVar[str] = MetricNaming.DOWNLOAD_FAILURE_COUNT.value
    FAILURE_COUNT_DESCRIPTION: ClassVar[str] = 'Failures in the shared YouTube failure queue'
    JOB_LABEL: ClassVar[str] = 'downloader'
