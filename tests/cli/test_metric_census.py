'''
Tests for the metric census (`tests/cli/_metric_census.py`): that the
generated blocks in docs/monitoring/metrics_reference.md match a live
measurement, and a few pins against known, hand-verified call sites.
'''
import os

from tests.cli._otel_resolve import Corpus
from tests.cli._metric_census import (
    METRICS_REFERENCE_DOC, measure, render_metrics_reference_doc,
)


def test_metrics_reference_doc_is_current():
    '''docs/monitoring/metrics_reference.md's generated blocks match a live
    measurement.
    '''
    resolved, exempt = measure()
    original = METRICS_REFERENCE_DOC.read_text(encoding='utf-8')
    rendered = render_metrics_reference_doc(original, resolved, exempt)
    if os.environ.get('UPDATE_METRIC_CENSUS'):
        METRICS_REFERENCE_DOC.write_text(rendered, encoding='utf-8')
        original = rendered
    assert original == rendered, (
        'docs/monitoring/metrics_reference.md is out of date. Regenerate with:\n'
        '    UPDATE_METRIC_CENSUS=1 pytest tests/cli/test_metric_census.py'
    )


def test_message_dispatcher_queue_depth_is_confirmed_not_emitted():
    '''Re-verifies, by measurement rather than a one-off grep, the status an
    earlier pass corrected by hand: no real `create_observable_gauge` call
    site anywhere in the tree names this metric.
    '''
    resolved, _exempt = measure()
    names = {e.name for e in resolved}
    assert 'message_dispatcher_queue_depth' not in names


def test_heartbeat_is_emitted_by_every_pod_that_should():
    '''Cross-checks the Heartbeat Metrics section's claim that every pod
    emits `heartbeat` -- measured, not re-typed from the hand-written table.
    '''
    resolved, _exempt = measure()
    heartbeat_pods = {e.pod for e in resolved if e.name == 'heartbeat'}
    assert heartbeat_pods == {'bot', 'broker', 'dispatcher', 'downloader', 'search'}


def test_subclass_fanout_resolves_queue_worker_metrics_to_both_pods():
    '''`QueueMetricsBase` declares its metric names/descriptions with no
    default -- only `DownloadMetrics` and `SearchMetrics` supply real values.
    Pins the fan-out behaviour this doc's queue-worker rows depend on, and
    that pod attribution follows the OVERRIDE, not the shared base file
    (the bug this generator's own source comments explain finding).
    '''
    resolved, _exempt = measure()
    by_name_pod = {(e.name, e.pod) for e in resolved}
    assert ('queue_worker_depth', 'downloader') in by_name_pod
    assert ('queue_worker_depth', 'search') in by_name_pod
    assert ('queue_worker_backoff_seconds', 'downloader') in by_name_pod
    assert ('queue_worker_backoff_seconds', 'search') in by_name_pod


def test_known_dynamic_descriptions_stay_exempt():
    '''A pod-label-interpolated description and a for-loop-bound description
    parameter are genuinely dynamic -- pins that the resolver correctly
    refuses to guess at them.
    '''
    _resolved, exempt = measure()
    sources = {e.source.split(':')[0] for e in exempt}
    assert 'discord_core/cli/_lib/worker_pod.py' in sources
    assert 'discord_gateway/cogs/music.py' in sources


def test_corpus_scan_finds_the_whole_tree():
    corpus = Corpus()
    assert len(corpus.files) > 150
