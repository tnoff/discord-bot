'''Tests for SearchMetrics — the search pod's gauge poller.'''
# NOTE: test names here deliberately avoid being exactly 40 characters long.
# trufflehog's Lob detector matches `test_` + 35 chars — the exact shape of a
# Lob test API key — so a 40-char test function name is reported as a VERIFIED
# secret and fails pr-check:secrets. Renaming is cheaper than an allowlist.
from discord_search.workers.search_metrics import SearchMetrics

from testing.gauge_poller_suite import GaugePollerSuite


class TestSearchMetrics(GaugePollerSuite):
    '''The shared gauge-poller cases, against the search pod.'''
    metrics_cls = SearchMetrics
    job = 'youtube_music_search'
