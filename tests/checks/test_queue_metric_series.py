'''
The search pod and the downloader pod report the same queue-worker gauges, so what keeps their series apart is a
property of BOTH, and it is pinned here, where both pods may be imported, and not in either pod's own tests.
'''
from discord_downloader.workers.download_metrics import DownloadMetrics
from discord_search.workers.search_metrics import SearchMetrics


def test_search_series_stay_distinct_from_the_downloader_series():
    '''
    The two pods share a metric NAME but must never share a SERIES.

    This assertion used to read the other way round -- that the search pod
    published `search_queue_depth` while the downloader published
    `download_queue_depth` -- and distinct names were how the separation was
    guaranteed. The property being protected was never the names, though: the
    two pods have separate backoff windows and separate failure ZSETs, so their
    numbers must not merge.

    That guarantee now comes from labels, which is where it belonged. Both pods
    emit `queue_worker_depth`, and `background_job` (plus `job`, from the pod
    itself) keeps the series apart. Asserting the labels differ is a stricter
    check than asserting the names did: two identical labels under one name
    would silently sum two pods' queues into one number, which is the failure
    the original test was written to prevent and could not actually see.
    '''
    assert SearchMetrics.QUEUE_DEPTH_METRIC == DownloadMetrics.QUEUE_DEPTH_METRIC
    assert SearchMetrics.BACKOFF_METRIC == DownloadMetrics.BACKOFF_METRIC
    assert SearchMetrics.FAILURE_COUNT_METRIC == DownloadMetrics.FAILURE_COUNT_METRIC
    assert SearchMetrics.JOB_LABEL != DownloadMetrics.JOB_LABEL, (
        'both queue workers would report under one series and their queue depths '
        'would sum into a single meaningless number'
    )
