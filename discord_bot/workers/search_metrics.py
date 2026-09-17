'''
Background metrics collector for the standalone YouTube-Music search process.

The polling body lives on QueueMetricsBase (shared with the downloader); this
module supplies the search pod's metric names + job label.

Gauges (job="discord-youtube-music-search", background_job="youtube_music_search"):
    search_queue_depth              — total pending searches across all guilds
    search_youtube_backoff_seconds  — seconds left on the shared 429 window
    search_failure_count            — failures in the shared search failure ZSET

The backoff/failure gauges are the search tier's own shared state, not the
downloader's: RedisYoutubeMusicSearchWorker keeps its window and failure ZSET
under discord_bot:ytmusic_search:*, separate from the download egress buckets.
'''
from enum import Enum
from typing import ClassVar

from discord_bot.workers.queue_metrics import DEFAULT_POLL_INTERVAL_SECONDS, QueueMetricsBase

__all__ = ['SearchMetrics', 'DEFAULT_POLL_INTERVAL_SECONDS']

class SearchMetricNaming(Enum):
    '''
    Metric names the search pod emits, and nothing else.

    Split out of utils/otel.py (2026-09-17), for the same reason as
    DownloadMetricNaming next door.
    '''
    SEARCH_QUEUE_DEPTH = 'search_queue_depth'
    SEARCH_YOUTUBE_BACKOFF = 'search_youtube_backoff_seconds'
    SEARCH_FAILURE_COUNT = 'search_failure_count'


class SearchMetrics(QueueMetricsBase):
    '''Polls Redis-backed search state into gauge-friendly cached values.'''

    QUEUE_DEPTH_METRIC: ClassVar[str] = SearchMetricNaming.SEARCH_QUEUE_DEPTH.value
    QUEUE_DEPTH_DESCRIPTION: ClassVar[str] = 'Pending searches across all guild queues'
    BACKOFF_METRIC: ClassVar[str] = SearchMetricNaming.SEARCH_YOUTUBE_BACKOFF.value
    BACKOFF_DESCRIPTION: ClassVar[str] = 'Seconds remaining on the shared search backoff window'
    FAILURE_COUNT_METRIC: ClassVar[str] = SearchMetricNaming.SEARCH_FAILURE_COUNT.value
    FAILURE_COUNT_DESCRIPTION: ClassVar[str] = 'Failures in the shared search failure queue'
    JOB_LABEL: ClassVar[str] = 'youtube_music_search'
