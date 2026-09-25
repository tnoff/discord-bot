'''
The standardised metric names, pinned where renaming them would go unnoticed.

These are not tests of the emitting code -- the health server and broker tests
next door cover that. They are tests of the SCHEME: that dimensions stay labels
rather than drifting back into metric names, and that the collapsed pairs really
do share one name. A rename that undoes this would otherwise fail nothing, since
every consumer of these strings lives in another repository.
'''
import pytest

from discord_core.utils.otel import AttributeNaming, MetricNaming

from discord_bot.services.bot.utils.bot_metrics import BotMetricNaming
from discord_bot.services.broker.workers.broker_metrics import BrokerMetricNaming

# The scheme applies to every metric this project emits, and they no longer live
# in one enum: names only one image emits moved to that image's module, so a
# check that read MetricNaming alone would stop seeing most of them. Gathering
# all three is what keeps these assertions about the SCHEME rather than about
# one file.
ALL_NAMING = (MetricNaming, BotMetricNaming, BrokerMetricNaming)
ALL_NAMES = {m.value for enum in ALL_NAMING for m in enum}


def test_no_metric_name_uses_dots():
    '''
    Dots are a second spelling of the same series.

    OTLP normalises `broker.entries` to `broker_entries` on the way into Mimir,
    so a dotted source name only ever existed in the emitting code -- and made
    `broker.entries` and `download_queue_depth` look like different conventions
    when they were the same one written two ways.
    '''
    dotted = sorted(n for n in ALL_NAMES if '.' in n)
    assert not dotted, f'metric names containing dots: {dotted}'


def test_no_gauge_is_named_count():
    '''
    `_count` is reserved by Prometheus for the count half of a histogram.

    `download_failure_count` was a GAUGE of the current failure queue, so the
    suffix promised a monotonic counter and delivered a level.
    '''
    counted = sorted(n for n in ALL_NAMES if n.endswith('_count'))
    assert not counted, f'gauges named like counters: {counted}'


@pytest.mark.parametrize('name', ['queue_worker_backoff_seconds',
                                  'cache_filesystem_max_bytes',
                                  'cache_filesystem_used_bytes'])
def test_units_are_in_the_name_where_they_are_not_obvious(name):
    '''A bare `cache_filesystem_max` does not say what it counts.'''
    assert name in ALL_NAMES


def test_the_queue_workers_share_one_set_of_names():
    '''
    The downloader and the search pod emit the same three measurements.

    They are separated by `job` and by the `background_job` attribute the base
    class has always set, so two name prefixes for it bought nothing. Asserting
    the absence of the old prefixes is what stops them growing back one PR at a
    time, which is how there came to be six names for three things.
    '''
    names = ALL_NAMES
    assert {'queue_worker_depth', 'queue_worker_backoff_seconds',
            'queue_worker_failures'} <= names
    for gone in ('download_queue_depth', 'search_queue_depth',
                 'download_youtube_backoff_seconds', 'search_youtube_backoff_seconds',
                 'download_failure_count', 'search_failure_count'):
        assert gone not in names, f'{gone} came back; it is a label, not a name'


def test_readiness_separates_self_reports_from_peer_probes():
    '''
    Two metrics, because they answer different questions.

    `dispatcher_ready_check` was the trap: it read like a pod reporting its own
    health and was emitted by the BOT probing one. Folding both kinds into a
    single name would make every query depend on remembering a filter, and a
    forgotten filter reads as a plausible number rather than an error.
    '''
    names = ALL_NAMES
    assert {'pod_ready_check', 'peer_ready_check'} <= names
    for gone in ('broker.ready_check', 'database.ready_check',
                 'dispatcher_ready_check', 'database_peer_ready_check'):
        assert gone not in names
    assert {AttributeNaming.POD.value, AttributeNaming.SOURCE.value,
            AttributeNaming.TARGET.value} == {'pod', 'source', 'target'}


def test_the_broker_pairs_kept_one_name_and_gained_a_label():
    '''
    Collapsing needs a label here, where the queue workers needed none.

    Both halves of each broker pair come from the broker process, so `job`
    cannot separate them the way it separates the downloader from the search
    pod. That is the whole rule: collapse to a label only when the existing
    labels do not already distinguish the series.
    '''
    names = ALL_NAMES
    assert {'result_queue_depth', 'result_fetch'} <= names
    for gone in ('music.download_result_queue_depth', 'music.search_result_queue_depth',
                 'broker.result_fetch', 'broker.search_result_fetch',
                 'broker_result_queue_depth', 'broker_result_fetch',
                 'dispatch_result_queue_depth'):
        assert gone not in names
    assert AttributeNaming.RESULT_TYPE.value == 'result_type'


def test_no_metric_name_carries_a_pod_name_that_job_already_supplies():
    '''
    A pod name in a metric name is a dimension in the wrong place.

    `job` already says which pod emitted a series, so `broker_result_fetch` spelt
    the same fact twice -- and worse, it stopped the broker's result queues being
    comparable with the bot's, which measured the same thing under
    `dispatch_result_queue_depth`. One name, separated by `job` and
    `result_type`, and they can finally be summed.

    `broker_entries` and `broker_bundles` are the deliberate exception and the
    reason this list is explicit rather than a regex over pod names: there the
    prefix is the CONCEPT, not the pod. Bare `entries` and `bundles` would mean
    nothing, so the prefix is carrying meaning rather than repeating `job`.
    '''
    concept_is_the_prefix = {'broker_entries', 'broker_bundles'}
    offenders = sorted(n for n in ALL_NAMES
                       if n.startswith(('broker_', 'bot_', 'database_', 'dispatcher_',
                                        'downloader_', 'search_'))
                       and n not in concept_is_the_prefix)
    assert not offenders, (
        f'metric names repeating what `job` already says: {offenders}. '
        f'Drop the pod prefix, or add it to the documented exceptions if the '
        f'prefix is the concept rather than the pod.'
    )


def test_metric_names_are_unique():
    '''Two members sharing a value would silently merge two series into one.'''
    values = [m.value for enum in ALL_NAMING for m in enum]
    assert len(values) == len(set(values)), (
        'duplicate metric name values across the naming enums -- two members '
        'sharing a value would silently merge two series into one'
    )
