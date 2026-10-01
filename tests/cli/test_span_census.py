'''
Tests for the span census (`tests/cli/_span_census.py`): that the generated
blocks in docs/monitoring/trace_linking.md match a live measurement, and a
few resolver spot-checks against known, hand-verified call sites so a future
change to the resolution engine can't silently start producing wrong answers
that still happen to round-trip.
'''
import os

from tests.cli._otel_resolve import Corpus
from tests.cli._span_census import (
    TRACE_LINKING_DOC, measure, render_trace_linking_doc,
)


def test_trace_linking_doc_is_current():
    '''docs/monitoring/trace_linking.md's generated blocks match a live measurement.

    This is the test that would have caught both staleness incidents this
    session found by hand: the leftover `_redis`-suffixed span names, and the
    four real spans the hand-written table never listed.
    '''
    resolved, exempt = measure()
    original = TRACE_LINKING_DOC.read_text(encoding='utf-8')
    rendered = render_trace_linking_doc(original, resolved, exempt)
    if os.environ.get('UPDATE_SPAN_CENSUS'):
        TRACE_LINKING_DOC.write_text(rendered, encoding='utf-8')
        original = rendered
    assert original == rendered, (
        'docs/monitoring/trace_linking.md is out of date. Regenerate with:\n'
        '    UPDATE_SPAN_CENSUS=1 pytest tests/cli/test_span_census.py'
    )


def test_every_dispatcher_span_from_the_hand_written_table_is_still_measured():
    '''All sixteen spans the hand-written table used to list are still found
    by measurement, under their REAL (non-`_redis`-suffixed) names -- so a
    regression in the resolver that silently dropped dispatcher spans would
    fail here even if it happened to also update the doc to match.
    '''
    resolved, _exempt = measure()
    names = {e.name for e in resolved}
    expected = {
        'dispatch.send', 'dispatch.delete', 'dispatch.update_mutable',
        'dispatch.remove_mutable', 'dispatch.update_mutable_channel',
        'dispatch.fetch_history', 'dispatch.fetch_emojis',
        'dispatch_client.fetch_history', 'dispatch_client.fetch_emojis',
        'message_dispatcher.process_mutable', 'message_dispatcher.remove_mutable',
        'message_dispatcher.send', 'message_dispatcher.delete',
        'message_dispatcher.update_mutable_channel',
        'message_dispatcher.fetch_history', 'message_dispatcher.fetch_emojis',
    }
    missing = expected - names
    assert not missing, f'Dispatcher spans no longer measured: {missing}'


def test_subclass_fanout_resolves_the_queue_worker_span_prefix():
    '''`HttpQueueWorkerClient.submit/block/clear` declare `SPAN_PREFIX` with no
    default -- only `HttpDownloadClient` ('downloader') and
    `HttpYoutubeMusicSearchClient` ('youtube_music_search') supply real
    values. Pins the fan-out behaviour the resolver depends on for this
    (and the `HttpStoreBase`/per-store) call sites.
    '''
    resolved, _exempt = measure()
    names = {e.name for e in resolved}
    assert 'downloader.submit' in names
    assert 'youtube_music_search.submit' in names
    assert 'downloader.block' in names
    assert 'youtube_music_search.block' in names


def test_known_dynamic_call_sites_stay_exempt():
    '''A route-parameterised span name (`HttpStoreBase`) and a
    `ctx`-resolved command name (`@command_wrapper`) are genuinely dynamic --
    pins that the resolver correctly refuses to guess at them, rather than
    silently resolving to something wrong.
    '''
    _resolved, exempt = measure()
    sources = {e.source.split(':')[0] for e in exempt}
    assert 'discord_core/clients/http_store_base.py' in sources
    assert 'discord_gateway/utils/otel_command.py' in sources


def test_exempt_spans_are_never_silently_empty():
    '''A resolver change that accidentally resolves everything (hiding a real
    gap) is exactly as wrong as one that exempts everything -- this pins that
    SOME call sites are expected to stay exempt, matched against the measured
    corpus rather than a hardcoded number that would itself go stale.
    '''
    _resolved, exempt = measure()
    assert len(exempt) > 0


def test_corpus_scan_finds_the_whole_tree():
    '''A sanity floor on the scan itself: if `source_files()` ever returned an
    empty or tiny list (a path bug, a bad exclude), every other test here
    would pass vacuously. Pinned low enough to not need bumping on every
    file added, high enough to catch the scan silently returning nothing.
    '''
    corpus = Corpus()
    assert len(corpus.files) > 150
