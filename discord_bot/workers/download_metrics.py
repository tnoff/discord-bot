'''
Background metrics collector for the standalone downloader process.

The polling body lives on QueueMetricsBase (shared with the YouTube-Music search
pod); this module supplies the downloader's metric names + job label.

Gauges (job="discord-downloader", background_job="downloader"):
    download_queue_depth              — total pending downloads across all guilds
    download_youtube_backoff_seconds  — seconds left on the shared YouTube window
    download_failure_count            — failures in the shared YouTube ZSET
'''
from enum import Enum
from typing import ClassVar

from discord_bot.workers.queue_metrics import DEFAULT_POLL_INTERVAL_SECONDS, QueueMetricsBase

__all__ = ['DownloadMetrics', 'DEFAULT_POLL_INTERVAL_SECONDS']

class DownloadMetricNaming(Enum):
    '''
    Metric names the downloader emits, and nothing else.

    Split out of utils/otel.py (2026-09-17): these were three members of a
    naming enum all six images import, so adding a downloader metric rebuilt
    six images to ship a string one of them uses.
    '''
    DOWNLOAD_QUEUE_DEPTH = 'download_queue_depth'
    DOWNLOAD_YOUTUBE_BACKOFF = 'download_youtube_backoff_seconds'
    DOWNLOAD_FAILURE_COUNT = 'download_failure_count'


class DownloadMetrics(QueueMetricsBase):
    '''Polls Redis-backed downloader state into gauge-friendly cached values.'''

    QUEUE_DEPTH_METRIC: ClassVar[str] = DownloadMetricNaming.DOWNLOAD_QUEUE_DEPTH.value
    QUEUE_DEPTH_DESCRIPTION: ClassVar[str] = 'Pending downloads across all guild queues'
    BACKOFF_METRIC: ClassVar[str] = DownloadMetricNaming.DOWNLOAD_YOUTUBE_BACKOFF.value
    BACKOFF_DESCRIPTION: ClassVar[str] = 'Seconds remaining on the shared YouTube backoff window'
    FAILURE_COUNT_METRIC: ClassVar[str] = DownloadMetricNaming.DOWNLOAD_FAILURE_COUNT.value
    FAILURE_COUNT_DESCRIPTION: ClassVar[str] = 'Failures in the shared YouTube failure queue'
    JOB_LABEL: ClassVar[str] = 'downloader'
