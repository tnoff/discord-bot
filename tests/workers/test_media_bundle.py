'''Tests for BundleState serialisation and BundleRenderer behaviour.'''
import json

import pytest

from discord_bot.cogs.music_helpers.common import MediaRequestLifecycleStage, SearchType
from discord_bot.types.playlist_add_request import PlaylistAddRequest
from discord_bot.types.search import SearchResult
from discord_bot.workers.media_bundle import (
    BundleRenderer,
    BundleState,
    BundledRequestState,
)

from tests.helpers import fake_context, fake_source_dict  # pylint: disable=unused-import


# ---------------------------------------------------------------------------
# BundleState serialisation
# ---------------------------------------------------------------------------

def test_bundle_state_round_trips_through_json(fake_context):  # pylint: disable=redefined-outer-name
    '''Pydantic dump/load preserves all bundle fields including nested requests.'''
    state = BundleState(
        guild_id=fake_context['guild'].id,
        channel_id=fake_context['channel'].id,
        input_string='Best of Steely Dan',
        has_search_banner=True,
    )
    state.bundled_requests.append(BundledRequestState(
        media_request=fake_source_dict(fake_context),
        table_index=0,
        stored_status=MediaRequestLifecycleStage.QUEUED,
    ))
    state.total = 1

    payload = state.model_dump(mode='json')
    restored = BundleState.model_validate(json.loads(json.dumps(payload)))

    assert restored.guild_id == state.guild_id
    assert restored.channel_id == state.channel_id
    assert restored.input_string == state.input_string
    assert restored.has_search_banner is True
    assert restored.total == 1
    assert len(restored.bundled_requests) == 1
    assert str(restored.bundled_requests[0].media_request.uuid) == str(
        state.bundled_requests[0].media_request.uuid
    )
    assert restored.bundled_requests[0].stored_status == MediaRequestLifecycleStage.QUEUED


def test_bundle_state_defaults_are_consistent():
    '''Counters start at 0; flags start unset; uuid generated.'''
    state = BundleState(guild_id=1, channel_id=2)
    assert state.total == 0
    assert state.completed == 0
    assert state.failed == 0
    assert state.discarded == 0
    assert state.all_requests_enqueued is False
    assert state.is_shutdown is False
    assert state.has_search_banner is False
    assert state.uuid.startswith('request.bundle.')


# ---------------------------------------------------------------------------
# BundleRenderer construction
# ---------------------------------------------------------------------------

def test_renderer_new_creates_empty_state(fake_context):  # pylint: disable=redefined-outer-name
    '''BundleRenderer.new wraps a fresh empty state.'''
    renderer = BundleRenderer.new(
        fake_context['guild'].id, fake_context['channel'].id,
    )
    assert renderer.state.total == 0
    assert renderer.print() == []


def test_renderer_str_returns_uuid(fake_context):  # pylint: disable=redefined-outer-name
    '''__str__ matches the legacy MultiMediaRequestBundle contract.'''
    renderer = BundleRenderer.new(
        fake_context['guild'].id, fake_context['channel'].id,
    )
    assert str(renderer) == renderer.state.uuid


# ---------------------------------------------------------------------------
# add_media_request + initial render
# ---------------------------------------------------------------------------

def test_add_media_request_assigns_table_index_for_queued(fake_context):  # pylint: disable=redefined-outer-name
    '''QUEUED requests get a visible row + a stable table_index.'''
    renderer = BundleRenderer.new(
        fake_context['guild'].id, fake_context['channel'].id,
    )
    mr = fake_source_dict(fake_context)
    mr.state_machine.mark_queued()
    renderer.add_media_request(mr)
    assert renderer.state.total == 1
    assert renderer.state.bundled_requests[0].table_index is not None
    assert mr.bundle_uuid == renderer.state.uuid


def test_add_media_request_skips_row_for_discarded(fake_context):  # pylint: disable=redefined-outer-name
    '''DISCARDED-on-arrival counts toward totals but gets no row.'''
    renderer = BundleRenderer.new(
        fake_context['guild'].id, fake_context['channel'].id,
    )
    mr = fake_source_dict(fake_context)
    mr.state_machine.mark_discarded()
    renderer.add_media_request(mr)
    assert renderer.state.total == 1
    assert renderer.state.discarded == 1
    assert renderer.state.bundled_requests[0].table_index is None


# ---------------------------------------------------------------------------
# State transitions reflected in rendered output
# ---------------------------------------------------------------------------

def test_print_shows_queued_then_in_progress(fake_context):  # pylint: disable=redefined-outer-name
    '''Lifecycle transition from QUEUED → IN_PROGRESS rewrites the row.

    Mirrors the cog flow: set_initial_search seeds a placeholder row 0 that
    all_requests_added blanks out before requests render.
    '''
    renderer = BundleRenderer.new(
        fake_context['guild'].id, fake_context['channel'].id,
    )
    renderer.set_initial_search('test search')
    mr = fake_source_dict(fake_context)
    mr.state_machine.mark_queued()
    renderer.add_media_request(mr)
    renderer.all_requests_added()

    pages = renderer.print()
    assert any('Media request queued for download' in p for p in pages)

    mr.state_machine.mark_in_progress()
    pages = renderer.print()
    assert any('Downloading and processing media request' in p for p in pages)


def test_search_banner_counter_advances_through_completion(fake_context):  # pylint: disable=redefined-outer-name
    '''has_search_banner header reflects completed/total counters.'''
    renderer = BundleRenderer.new(
        fake_context['guild'].id, fake_context['channel'].id,
    )
    renderer.set_multi_input_request('Best of Steely Dan')
    requests = []
    for _ in range(3):
        mr = fake_source_dict(fake_context)
        mr.state_machine.mark_queued()
        renderer.add_media_request(mr)
        requests.append(mr)
    renderer.all_requests_added()

    pages = renderer.print()
    joined = '\n'.join(pages)
    assert '0/3 media requests processed successfully, 0 failed' in joined

    requests[0].state_machine.mark_in_progress()
    requests[0].state_machine.mark_completed()
    pages = renderer.print()
    joined = '\n'.join(pages)
    assert '1/3 media requests processed successfully, 0 failed' in joined


def test_finished_when_all_terminal(fake_context):  # pylint: disable=redefined-outer-name
    '''finished flips True only after all requests reach a terminal stage.'''
    renderer = BundleRenderer.new(
        fake_context['guild'].id, fake_context['channel'].id,
    )
    mr = fake_source_dict(fake_context)
    mr.state_machine.mark_queued()
    renderer.add_media_request(mr)
    renderer.all_requests_added()
    assert renderer.finished is False

    mr.state_machine.mark_in_progress()
    mr.state_machine.mark_completed()
    renderer.update_request_status()
    assert renderer.finished is True
    assert renderer.finished_successfully is True
    assert renderer.state.finished_at is not None


def test_shutdown_makes_print_empty(fake_context):  # pylint: disable=redefined-outer-name
    '''shutdown() suppresses all rendered output.'''
    renderer = BundleRenderer.new(
        fake_context['guild'].id, fake_context['channel'].id,
    )
    renderer.set_initial_search('something')
    renderer.shutdown()
    assert renderer.print() == []


# ---------------------------------------------------------------------------
# Failure / retry summaries
# ---------------------------------------------------------------------------

def test_get_failure_summary_returns_unsent_failures_then_marks_them(fake_context):  # pylint: disable=redefined-outer-name
    '''First call returns the failure messages; second returns None.'''
    renderer = BundleRenderer.new(
        fake_context['guild'].id, fake_context['channel'].id,
    )
    mr = fake_source_dict(fake_context)
    mr.state_machine.mark_queued()
    renderer.add_media_request(mr)
    renderer.all_requests_added()

    mr.state_machine.mark_failed('cannot find video')

    summary = renderer.get_failure_summary()
    assert summary is not None
    assert any('cannot find video' in s for s in summary)
    # Second call: the unsent flag has been cleared.
    assert renderer.get_failure_summary() is None


def test_get_retry_summary_includes_attempt_count(fake_context):  # pylint: disable=redefined-outer-name
    '''Retry messages include attempt n/N + the truncated reason.'''
    renderer = BundleRenderer.new(
        fake_context['guild'].id, fake_context['channel'].id,
    )
    mr = fake_source_dict(fake_context)
    mr.state_machine.mark_queued()
    renderer.add_media_request(mr)
    renderer.all_requests_added()

    mr.download_retry_information.retry_count = 2
    mr.state_machine.mark_retry_download('http 500', backoff_seconds=15)
    messages = renderer.get_retry_summary(download_max_retries=3, search_max_retries=3)
    assert messages is not None
    # Each entry is (request_uuid, message_content).
    assert any(uuid == str(mr.uuid) and 'attempt 2/3' in content
               for uuid, content in messages)
    assert any('http 500' in content for _uuid, content in messages)
    # No timing promise is made — the request just goes back on the queue.
    assert all('retrying in' not in content for _uuid, content in messages)
    # Second call returns None because retry_reason_sent is set.
    assert renderer.get_retry_summary(3, 3) is None


def test_get_retry_summary_prefers_worker_reported_max(fake_context):  # pylint: disable=redefined-outer-name
    '''The M in "attempt N/M" comes from the worker that owns the N.

    Regression for prod 2026-08-13: the downloader's config raised
    max_download_retries to 5 while the broker's own config file still defaulted
    to 3, so a live count rendered against a stale budget as "attempt 4/3".
    '''
    renderer = BundleRenderer.new(
        fake_context['guild'].id, fake_context['channel'].id,
    )
    mr = fake_source_dict(fake_context)
    mr.state_machine.mark_queued()
    renderer.add_media_request(mr)
    renderer.all_requests_added()

    mr.state_machine.mark_retry_download('http 500', retry_count=4, max_retries=5)
    # The renderer's own config still says 3 — the worker's 5 must win.
    messages = renderer.get_retry_summary(download_max_retries=3, search_max_retries=3)
    assert messages is not None
    assert any('attempt 4/5' in content for _uuid, content in messages)
    assert all('attempt 4/3' not in content for _uuid, content in messages)


def test_get_retry_summary_search_prefers_worker_reported_max(fake_context):  # pylint: disable=redefined-outer-name
    '''The search half reads its budget from the search retry block, not the
    download one — the two phases carry independent counters and budgets.'''
    renderer = BundleRenderer.new(
        fake_context['guild'].id, fake_context['channel'].id,
    )
    mr = fake_source_dict(fake_context)
    mr.state_machine.mark_queued()
    renderer.add_media_request(mr)
    renderer.all_requests_added()

    mr.download_retry_information.retry_max = 5
    mr.state_machine.mark_retry_search('429 rate limit', retry_count=3, max_retries=4)
    messages = renderer.get_retry_summary(download_max_retries=3, search_max_retries=3)
    assert messages is not None
    assert any('attempt 3/4' in content for _uuid, content in messages)


def test_get_retry_cleanups_fires_once_on_terminal(fake_context):  # pylint: disable=redefined-outer-name
    '''A request with a live retry note is cleaned up once it reaches a terminal
    stage; still-retrying requests are left alone and cleanup is not repeated.'''
    renderer = BundleRenderer.new(
        fake_context['guild'].id, fake_context['channel'].id,
    )
    mr = fake_source_dict(fake_context)
    mr.state_machine.mark_queued()
    renderer.add_media_request(mr)
    renderer.all_requests_added()

    mr.state_machine.mark_retry_download('http 500', retry_count=1)
    messages = renderer.get_retry_summary(download_max_retries=3, search_max_retries=3)
    assert messages is not None
    # Still retrying → nothing to clean up yet.
    assert not renderer.get_retry_cleanups()

    mr.state_machine.mark_completed()
    assert renderer.get_retry_cleanups() == [str(mr.uuid)]
    # Idempotent: the outstanding flag was cleared, so it won't fire again.
    assert not renderer.get_retry_cleanups()


# ---------------------------------------------------------------------------
# Reanimation: BundleRenderer rebuilt from a saved BundleState
# ---------------------------------------------------------------------------

def test_renderer_rebuilds_from_saved_state(fake_context):  # pylint: disable=redefined-outer-name
    '''Persist state, recreate the renderer, ensure rendered output matches.'''
    renderer = BundleRenderer.new(
        fake_context['guild'].id, fake_context['channel'].id,
    )
    renderer.set_multi_input_request('Best of Steely Dan')
    for _ in range(2):
        mr = fake_source_dict(fake_context)
        mr.state_machine.mark_queued()
        renderer.add_media_request(mr)
    renderer.all_requests_added()
    renderer.state.bundled_requests[0].media_request.state_machine.mark_in_progress()
    renderer.update_request_status()

    serialised = renderer.state.model_dump(mode='json')
    restored_state = BundleState.model_validate(json.loads(json.dumps(serialised)))
    restored_renderer = BundleRenderer(restored_state)

    pages = restored_renderer.print()
    joined = '\n'.join(pages)
    assert 'Best of Steely Dan' in joined
    assert 'Downloading and processing media request' in joined


@pytest.mark.parametrize('stage,expected_fragment', [
    (MediaRequestLifecycleStage.IN_PROGRESS, 'Downloading and processing'),
    (MediaRequestLifecycleStage.BACKOFF, 'Waiting to process'),
    (MediaRequestLifecycleStage.RETRY_DOWNLOAD, 'Failed, will retry'),
])
def test_update_request_status_renders_each_stage(fake_context, stage, expected_fragment):  # pylint: disable=redefined-outer-name
    '''Each lifecycle stage maps to a distinct row text.'''
    renderer = BundleRenderer.new(
        fake_context['guild'].id, fake_context['channel'].id,
    )
    mr = fake_source_dict(fake_context)
    mr.state_machine.mark_queued()
    renderer.add_media_request(mr)
    renderer.all_requests_added()
    mr.lifecycle_stage = stage
    renderer.update_request_status()
    pages = renderer.print()
    assert any(expected_fragment in p for p in pages)


# ---------------------------------------------------------------------------
# Defensive arms: rows skipped when table_index is None
# ---------------------------------------------------------------------------

def test_add_media_request_completed_on_arrival_increments_completed(fake_context):  # pylint: disable=redefined-outer-name
    '''A request that arrives already-COMPLETED (e.g. cache hit) bumps the
    completed counter and does not get a visible row.'''
    renderer = BundleRenderer.new(
        fake_context['guild'].id, fake_context['channel'].id,
    )
    mr = fake_source_dict(fake_context)
    mr.state_machine.mark_completed()
    renderer.add_media_request(mr)
    assert renderer.state.total == 1
    assert renderer.state.completed == 1
    assert renderer.state.bundled_requests[0].table_index is None


def test_all_requests_added_skips_position_lookup_for_rowless_entries(fake_context):  # pylint: disable=redefined-outer-name
    '''Bundled entries with no table_index (DISCARDED on arrival) keep the
    default row_collection_index/row_index_in_collection (None) after
    all_requests_added — the position-mapping arm is skipped.'''
    renderer = BundleRenderer.new(
        fake_context['guild'].id, fake_context['channel'].id,
    )
    discarded = fake_source_dict(fake_context)
    discarded.state_machine.mark_discarded()
    renderer.add_media_request(discarded)

    queued = fake_source_dict(fake_context)
    queued.state_machine.mark_queued()
    renderer.add_media_request(queued)

    renderer.all_requests_added()

    # Discarded entry: kept default None positions.
    assert renderer.state.bundled_requests[0].row_collection_index is None
    assert renderer.state.bundled_requests[0].row_index_in_collection is None
    # Queued entry: mapping populated.
    assert renderer.state.bundled_requests[1].row_collection_index is not None


def test_edit_row_data_returns_false_for_rowless_entry(fake_context):  # pylint: disable=redefined-outer-name
    '''_edit_row_data short-circuits to False for a request with no table_index
    so callers know the edit was a no-op (the BundleRenderer rebuild path
    relies on this guard).'''
    renderer = BundleRenderer.new(
        fake_context['guild'].id, fake_context['channel'].id,
    )
    mr = fake_source_dict(fake_context)
    mr.state_machine.mark_discarded()
    renderer.add_media_request(mr)
    req_state = renderer.state.bundled_requests[0]
    # pylint: disable=protected-access
    assert renderer._edit_row_data(req_state, 'ignored') is False


def test_bundle_pagination_shrink_does_not_crash_edit(fake_context):  # pylint: disable=redefined-outer-name
    '''After items reach terminal stages their rows blank out and the table
    re-paginates to fewer pages.  Stored row_collection_index values from the
    earlier (larger) layout would otherwise index past the end of
    _row_collections — _edit_row_data crashes with IndexError on the next
    status push.  _rebuild_table must refresh those positions.'''
    # Use a tiny pagination_length so we can force multi-page layout with
    # only a handful of QUEUED-text rows.
    state = BundleState(
        guild_id=fake_context['guild'].id,
        channel_id=fake_context['channel'].id,
        input_string='multi-track input',
        has_search_banner=True,
        pagination_length=200,
    )
    renderer = BundleRenderer(state)
    requests = []
    for _ in range(6):
        mr = fake_source_dict(fake_context)
        mr.state_machine.mark_queued()
        renderer.add_media_request(mr)
        requests.append(mr)
    renderer.all_requests_added()
    assert len(renderer._row_collections) > 1  # pylint: disable=protected-access

    # Mark every request COMPLETED so the next render blanks every row.
    for mr in requests:
        mr.state_machine.mark_completed()

    # Round-trip through Pydantic so the renderer rebuilds from saved state.
    payload = renderer.state.model_dump(mode='json')
    restored_state = BundleState.model_validate(json.loads(json.dumps(payload)))
    rebuilt = BundleRenderer(restored_state)

    # Pagination collapsed; stored positions on the bundled requests now
    # need to match the new layout, otherwise update_request_status raises
    # IndexError on the first row_collections lookup.
    rebuilt.update_request_status()
    # No exception means the fix is in place; verify pagination did shrink.
    assert len(rebuilt._row_collections) < len(renderer._row_collections)  # pylint: disable=protected-access


def test_bundle_state_preserves_playlist_add_request_subclass(fake_context):  # pylint: disable=redefined-outer-name
    '''A bundle holding a PlaylistAddRequest must round-trip through Pydantic
    validation without losing playlist_id — otherwise the broker rejects the
    bundle on reload (cause of the 422 from `!playlist item-add`).'''
    par = PlaylistAddRequest(
        guild_id=fake_context['guild'].id,
        channel_id=fake_context['channel'].id,
        requester_id=fake_context['author'].id,
        requester_name=fake_context['author'].display_name,
        search_result=SearchResult(search_type=SearchType.SEARCH, raw_search_string='input'),
        playlist_id=99,
    )
    state = BundleState(
        guild_id=fake_context['guild'].id, channel_id=fake_context['channel'].id,
    )
    state.bundled_requests.append(BundledRequestState(
        media_request=par, table_index=0,
        stored_status=MediaRequestLifecycleStage.QUEUED,
    ))

    payload = state.model_dump(mode='json')
    restored = BundleState.model_validate(json.loads(json.dumps(payload)))

    assert isinstance(restored.bundled_requests[0].media_request, PlaylistAddRequest)
    assert restored.bundled_requests[0].media_request.playlist_id == 99


def test_renderer_rebuild_skips_rowless_entries(fake_context):  # pylint: disable=redefined-outer-name
    '''When BundleRenderer is reconstructed from a saved state, bundled
    requests with table_index=None (DISCARDED on arrival) must be skipped in
    the table rebuild — otherwise the rebuilt table grows extra rows.'''
    renderer = BundleRenderer.new(
        fake_context['guild'].id, fake_context['channel'].id,
    )
    renderer.set_multi_input_request('mix')
    discarded = fake_source_dict(fake_context)
    discarded.state_machine.mark_discarded()
    renderer.add_media_request(discarded)
    queued = fake_source_dict(fake_context)
    queued.state_machine.mark_queued()
    renderer.add_media_request(queued)
    renderer.all_requests_added()

    serialised = renderer.state.model_dump(mode='json')
    restored = BundleState.model_validate(json.loads(json.dumps(serialised)))
    rebuilt = BundleRenderer(restored)

    # Banner row + one queued row only — the discarded entry contributed no row.
    # pylint: disable=protected-access
    assert rebuilt._table.size == 2


# ---------------------------------------------------------------------------
# FAILED / QUEUED transitions, retry-search, shutdown, rebuild branches
# ---------------------------------------------------------------------------

def test_failed_request_increments_counter_and_renders_row(fake_context):  # pylint: disable=redefined-outer-name
    '''A QUEUED request transitioning to FAILED bumps the failed counter and
    rewrites its row to the failure line.'''
    renderer = BundleRenderer.new(fake_context['guild'].id, fake_context['channel'].id)
    mr = fake_source_dict(fake_context)
    mr.state_machine.mark_queued()
    renderer.add_media_request(mr)
    renderer.all_requests_added()

    mr.state_machine.mark_failed('cannot download')
    renderer.update_request_status()

    assert renderer.state.failed == 1
    assert any('Media request failed download' in p for p in renderer.print())


def test_queued_transition_keeps_existing_row(fake_context):  # pylint: disable=redefined-outer-name
    '''A SEARCHING request transitioning to QUEUED keeps its row (no edit).'''
    renderer = BundleRenderer.new(fake_context['guild'].id, fake_context['channel'].id)
    mr = fake_source_dict(fake_context)
    mr.state_machine.mark_searching()
    renderer.add_media_request(mr)
    renderer.all_requests_added()

    mr.state_machine.mark_queued()
    renderer.update_request_status()

    assert renderer.state.bundled_requests[0].stored_status == MediaRequestLifecycleStage.QUEUED
    assert any('Media request queued for download' in p for p in renderer.print())


def test_finished_true_when_shutdown(fake_context):  # pylint: disable=redefined-outer-name
    '''The finished property short-circuits True for a shut-down bundle.'''
    renderer = BundleRenderer.new(fake_context['guild'].id, fake_context['channel'].id)
    renderer.shutdown()
    assert renderer.finished is True


def test_finished_true_when_enqueued_with_no_requests(fake_context):  # pylint: disable=redefined-outer-name
    '''A finalised bundle holding no requests is finished.'''
    renderer = BundleRenderer.new(fake_context['guild'].id, fake_context['channel'].id)
    renderer.all_requests_added()
    assert renderer.finished is True


def test_retry_summary_search_retry_uses_search_max(fake_context):  # pylint: disable=redefined-outer-name
    '''RETRY_SEARCH pulls youtube_music_retry_information + search_max_retries,
    and makes no timing promise regardless of the backoff.'''
    renderer = BundleRenderer.new(fake_context['guild'].id, fake_context['channel'].id)
    mr = fake_source_dict(fake_context)
    mr.state_machine.mark_queued()
    renderer.add_media_request(mr)
    renderer.all_requests_added()

    mr.youtube_music_retry_information.retry_count = 1
    mr.state_machine.mark_retry_search('throttled', backoff_seconds=120)
    messages = renderer.get_retry_summary(download_max_retries=3, search_max_retries=5)

    assert messages is not None
    assert any('attempt 1/5' in content for _uuid, content in messages)
    assert all('retrying in' not in content for _uuid, content in messages)
    assert any('throttled' in content for _uuid, content in messages)


def test_rebuild_single_track_not_enqueued_keeps_search_placeholder(fake_context):  # pylint: disable=redefined-outer-name
    '''Rebuilding a non-finalised single-track bundle re-adds the
    "Processing search" placeholder row (input_string set, no banner).'''
    renderer = BundleRenderer.new(fake_context['guild'].id, fake_context['channel'].id)
    renderer.set_initial_search('a single track')
    mr = fake_source_dict(fake_context)
    mr.state_machine.mark_queued()
    renderer.add_media_request(mr)
    # NOT all_requests_added -> all_requests_enqueued stays False

    restored = BundleState.model_validate(json.loads(json.dumps(renderer.state.model_dump(mode='json'))))
    rebuilt = BundleRenderer(restored)
    assert any('Processing search' in p for p in rebuilt.print())


def test_rebuild_single_track_enqueued_blanks_placeholder_row(fake_context):  # pylint: disable=redefined-outer-name
    '''Rebuilding a finalised single-track bundle re-adds a blank row 0 (the
    placeholder was blanked at all_requests_added) and renders the request.'''
    renderer = BundleRenderer.new(fake_context['guild'].id, fake_context['channel'].id)
    renderer.set_initial_search('a single track')
    mr = fake_source_dict(fake_context)
    mr.state_machine.mark_queued()
    renderer.add_media_request(mr)
    renderer.all_requests_added()  # enqueued=True; placeholder blanked

    restored = BundleState.model_validate(json.loads(json.dumps(renderer.state.model_dump(mode='json'))))
    rebuilt = BundleRenderer(restored)
    assert any('Media request queued for download' in p for p in rebuilt.print())


def test_refresh_positions_nulls_unmapped_table_index(fake_context):  # pylint: disable=redefined-outer-name
    '''A bundled request whose stored table_index exceeds the rebuilt table's
    row count gets its cached position cleared — the defensive guard for a
    pagination layout that shrank between persistence and reload.'''
    state = BundleState(
        guild_id=fake_context['guild'].id, channel_id=fake_context['channel'].id,
        all_requests_enqueued=True,
    )
    mr = fake_source_dict(fake_context)
    mr.state_machine.mark_queued()
    state.bundled_requests.append(BundledRequestState(
        media_request=mr, table_index=999,  # far past any real row
        stored_status=MediaRequestLifecycleStage.QUEUED,
        row_collection_index=5, row_index_in_collection=5,
    ))

    renderer = BundleRenderer(state)  # _rebuild_table -> _refresh_request_positions
    rs = renderer.state.bundled_requests[0]
    assert rs.row_collection_index is None
    assert rs.row_index_in_collection is None


def test_edit_search_banner_noop_without_banner(fake_context):  # pylint: disable=redefined-outer-name
    '''_edit_search_banner returns early for a bundle with no search banner.'''
    renderer = BundleRenderer.new(fake_context['guild'].id, fake_context['channel'].id)
    renderer._edit_search_banner('ignored')  # pylint: disable=protected-access
    assert renderer.state.has_search_banner is False
