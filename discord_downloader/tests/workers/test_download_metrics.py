'''Tests for DownloadMetrics — the downloader pod's gauge poller.'''
from discord_downloader.workers.download_metrics import DownloadMetrics

from tests.gauge_poller_suite import GaugePollerSuite


class TestDownloadMetrics(GaugePollerSuite):
    '''The shared gauge-poller cases, against the download pod.'''
    metrics_cls = DownloadMetrics
    job = 'downloader'
