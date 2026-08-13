'''
Media-broker bundle state + renderer.

The cog used to own MultiMediaRequestBundle directly (state + table interleaved).
For HA broker mode the broker pod needs to own the bundle and persist its state
in Redis, so we split the class into two halves:

* BundleState (Pydantic) — all data the broker needs to recompute the
  Discord-message content of a bundle.  Serialises to JSON cleanly and can be
  stored alongside the rest of the broker's registry in Redis.

* BundleRenderer — wraps a BundleState with a transient DapperTable.  Methods
  match the old MultiMediaRequestBundle so the cog migration is mechanical.
  The table is rebuilt from BundleState on construction so a renderer can be
  reanimated from a stored state without losing row positions.

The on-the-wire shape is BundleState; the renderer is purely process-local.
'''
from datetime import datetime, timezone
from typing import List, Optional, Tuple
from uuid import uuid4

from dappertable import DapperTable, PaginationLength, shorten_string
from pydantic import BaseModel, Field

from discord_bot.common import DISCORD_MAX_MESSAGE_LENGTH
from discord_bot.cogs.music_helpers.common import MediaRequestLifecycleStage
from discord_bot.types.media_request import MediaRequest
from discord_bot.types.playlist_add_request import AnyMediaRequest
from discord_bot.utils.discord_utils import discord_format_string_embed


_TERMINAL_STAGES = frozenset({
    MediaRequestLifecycleStage.COMPLETED,
    MediaRequestLifecycleStage.FAILED,
    MediaRequestLifecycleStage.DISCARDED,
})


class BundledRequestState(BaseModel):
    '''Per-request state inside a bundle.

    Tracks where a request lives in the rendered table and what UI follow-ups
    have already been emitted for it (so we don't double-send retry / failure
    explanations).
    '''
    media_request: AnyMediaRequest
    table_index: Optional[int] = None
    row_collection_index: Optional[int] = None
    row_index_in_collection: Optional[int] = None
    failure_reason_sent: bool = False
    # True while a per-request retry summary message is live in Discord. The
    # message is mutable (keyed request_retry-{bundle}-{uuid}), so it is edited
    # in place across retries and torn down once the request reaches a terminal
    # stage — deleting the "Retrying …" note the moment a later attempt succeeds.
    retry_message_outstanding: bool = False
    stored_status: MediaRequestLifecycleStage = MediaRequestLifecycleStage.SEARCHING


class BundleState(BaseModel):
    '''Bundle data the broker persists.

    Everything in here is serialisable.  BundleRenderer derives the rendered
    message content from this state plus a transient DapperTable.

    Caveat: ``bundled_requests[i].media_request`` is a *snapshot* of the
    request at the time it was attached to the bundle.  In single-process
    mode it shares a Python reference with the broker registry's entry, so
    lifecycle transitions are seen automatically.  In HA / Redis mode the
    bundle and the registry entry are independent JSON blobs — every broker
    op that mutates the registry's MediaRequest must also call
    ``RedisBroker._sync_request_into_bundle`` or the bundle's copy goes stale
    and counters never advance.
    '''
    guild_id: int
    channel_id: int
    uuid: str = Field(default_factory=lambda: f'request.bundle.{uuid4()}')
    pagination_length: int = DISCORD_MAX_MESSAGE_LENGTH
    input_string: Optional[str] = None
    has_search_banner: bool = False
    bundled_requests: List[BundledRequestState] = Field(default_factory=list)
    total: int = 0
    completed: int = 0
    failed: int = 0
    discarded: int = 0
    all_requests_enqueued: bool = False
    is_shutdown: bool = False
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    finished_at: Optional[datetime] = None
    # Set once the terminal "Completed N/N" summary has been dispatched with a
    # delete_after.  That dispatch makes the message dispatcher drop its mutable
    # tracking for the bundle key, so any later finished-render would create a
    # duplicate summary instead of editing the existing one — this guards it to
    # a single dispatch.
    summary_dispatched: bool = False

    def sync_request(self, media_request: AnyMediaRequest) -> bool:
        '''Replace the stored copy of a bundled request (matched by uuid) with
        ``media_request``, returning True when a matching row was found.

        Brokers keep the authoritative MediaRequest in their registry; the
        bundle holds its own copy.  In single-process those copies normally
        share a Python reference, but the alias is lost when a registry entry is
        rebuilt from a deserialised request, and in Redis the two are always
        independent JSON blobs.  Both brokers call this to write the latest
        lifecycle state back into the bundle before rendering.
        '''
        target = str(media_request.uuid)
        for req_state in self.bundled_requests:
            if str(req_state.media_request.uuid) == target:
                req_state.media_request = media_request
                return True
        return False


def _request_row_text(req_state: BundledRequestState) -> str:
    '''Compute the table-row text for a request given its current lifecycle stage.

    Returns an empty string for stages whose row should be blanked out.  The
    initial table-row text on add_media_request lives separately because that
    path also has to assign a table_index.
    '''
    media_request = req_state.media_request
    stage = media_request.lifecycle_stage
    name = media_request.display_name
    if stage == MediaRequestLifecycleStage.IN_PROGRESS:
        return f'Downloading and processing media request: "{name}"'
    if stage == MediaRequestLifecycleStage.BACKOFF:
        return f'Waiting to process: "{name}"'
    if stage in (MediaRequestLifecycleStage.RETRY_DOWNLOAD, MediaRequestLifecycleStage.RETRY_SEARCH):
        return f'Failed, will retry: "{name}"'
    if stage in (MediaRequestLifecycleStage.QUEUED, MediaRequestLifecycleStage.SEARCHING):
        return f'Media request queued for download: "{name}"'
    if stage == MediaRequestLifecycleStage.FAILED:
        return f'Media request failed download: "{name}"'
    return ''


def _search_banner_text(state: BundleState, finished: bool) -> str:
    '''Compute the search-banner header text (row 0 when has_search_banner).'''
    multi_input = (
        discord_format_string_embed(state.input_string)
        if state.input_string else state.input_string
    )
    top_line = f'Processing "{multi_input}"'
    if state.all_requests_enqueued:
        if finished:
            top_line = f'Completed processing of "{multi_input}"'
        top_line = (
            f'{top_line}\n{state.completed}/{state.total - state.discarded} '
            f'media requests processed successfully, {state.failed} failed'
        )
    return top_line


class BundleRenderer:
    '''Operations on a BundleState — adds requests, applies state-machine
    transitions, returns paginated Discord-message strings.

    Mirrors the public surface of the old MultiMediaRequestBundle so the cog
    migration is a swap rather than a rewrite.
    '''

    def __init__(self, state: BundleState):
        self.state = state
        self._table = DapperTable(
            pagination_options=PaginationLength(state.pagination_length)
        )
        self._row_collections: list = []
        self._rebuild_table()

    @classmethod
    def new(cls, guild_id: int, channel_id: int,
            pagination_length: int = DISCORD_MAX_MESSAGE_LENGTH) -> 'BundleRenderer':
        '''Create a fresh renderer + state pair.'''
        return cls(BundleState(
            guild_id=guild_id,
            channel_id=channel_id,
            pagination_length=pagination_length,
        ))

    def _rebuild_table(self) -> None:
        '''Reconstruct the internal DapperTable from BundleState.

        Used at construction time so a freshly-loaded BundleState (e.g. from
        Redis) lands with the same table layout the producer had before
        persistence.  Row 0 carries either the search banner or the initial
        search line (if input_string is set); after finalize the placeholder
        is blanked out but the row stays so saved table_index values keep
        pointing at the right slots.  Subsequent rows are one per bundled
        request with a table_index.
        '''
        s = self.state
        finished = self._is_finished()
        if s.has_search_banner:
            self._table.add_row(_search_banner_text(s, finished))
        elif s.input_string is not None:
            if s.all_requests_enqueued:
                self._table.add_row('')
            else:
                self._table.add_row(
                    f'Processing search: "{discord_format_string_embed(s.input_string)}"'
                )
        for req_state in s.bundled_requests:
            if req_state.table_index is None:
                continue
            text = _request_row_text(req_state)
            self._table.add_row(text)
        if s.all_requests_enqueued:
            self._row_collections = self._table.get_pages()
            # Pagination depends on row text length, so the page layout
            # changes as rows are blanked when items reach terminal stages.
            # Without recomputing here, stored row_collection_index values
            # carried over from a previous render can point past the now-
            # smaller _row_collections — _edit_row_data crashes with
            # IndexError on the next status push.
            self._refresh_request_positions()

    def _is_finished(self) -> bool:
        '''True when nothing more will progress this bundle.'''
        s = self.state
        if s.is_shutdown:
            return True
        if not s.all_requests_enqueued:
            return False
        if s.bundled_requests:
            return all(
                req.media_request.lifecycle_stage in _TERMINAL_STAGES
                for req in s.bundled_requests
            )
        return True

    @property
    def finished(self) -> bool:
        '''Public read-only view of bundle completion (matches old API).'''
        return self._is_finished()

    @property
    def finished_successfully(self) -> bool:
        '''All non-discarded requests reached COMPLETED.'''
        return (self.state.completed + self.state.discarded) == self.state.total

    def __str__(self) -> str:
        return self.state.uuid

    def shutdown(self) -> None:
        '''Mark bundle inert — print() will return [] until removed.'''
        self.state.is_shutdown = True

    def set_initial_search(self, input_string: str) -> None:
        '''Add the "Processing search" placeholder row (single-track flow).'''
        self.state.input_string = shorten_string(input_string.replace(' shuffle', ''), 256)
        self._table.add_row(
            f'Processing search: "{discord_format_string_embed(self.state.input_string)}"'
        )

    def set_multi_input_request(self, input_string: str) -> None:
        '''Mark this bundle as a multi-track search; sets has_search_banner.'''
        self.state.has_search_banner = True
        self.state.input_string = shorten_string(input_string.replace(' shuffle', ''), 256)
        multi_input = (
            discord_format_string_embed(self.state.input_string)
            if self.state.input_string else self.state.input_string
        )
        self._table.add_row(f'Processing "{multi_input}"')

    def _increment_counter_for_stage(self, stage: MediaRequestLifecycleStage) -> None:
        if stage == MediaRequestLifecycleStage.COMPLETED:
            self.state.completed += 1
        elif stage == MediaRequestLifecycleStage.DISCARDED:
            self.state.discarded += 1
        elif stage == MediaRequestLifecycleStage.FAILED:
            self.state.failed += 1

    def add_media_request(self, media_request: MediaRequest) -> None:
        '''Attach a request to the bundle and seed its table row.

        DISCARDED / COMPLETED-on-arrival paths only bump counters — only QUEUED
        and SEARCHING requests get a visible row.  Sets media_request.bundle_uuid
        so the on_change pipeline can find this bundle.
        '''
        s = self.state
        table_index = None
        if media_request.lifecycle_stage == MediaRequestLifecycleStage.DISCARDED:
            self._increment_counter_for_stage(media_request.lifecycle_stage)
        elif media_request.lifecycle_stage in (
            MediaRequestLifecycleStage.QUEUED, MediaRequestLifecycleStage.SEARCHING
        ):
            table_index = self._table.add_row(
                f'Media request queued for download: "{media_request.display_name}"'
            )
        elif media_request.lifecycle_stage == MediaRequestLifecycleStage.COMPLETED:
            self._increment_counter_for_stage(media_request.lifecycle_stage)
        s.bundled_requests.append(BundledRequestState(
            media_request=media_request,
            table_index=table_index,
            stored_status=media_request.lifecycle_stage,
        ))
        s.total += 1
        media_request.bundle_uuid = s.uuid

    def all_requests_added(self) -> None:
        '''Lock pagination and finalise the table layout.

        After this point row positions are fixed and edits target a specific
        (collection_idx, row_idx) — necessary so subsequent _edit_row_data
        edits see consistent positions even if the underlying table mutates.
        '''
        s = self.state
        s.all_requests_enqueued = True
        self._check_finished()

        # Drop the initial "Processing search" placeholder for non-banner bundles
        # — only present when the producer called set_initial_search, i.e. when
        # input_string is set.  Empty bundles created with no input_string never
        # seed row 0 in the first place.
        if not s.has_search_banner and s.input_string is not None:
            self._table.edit_row(0, '')

        self._row_collections = self._table.get_pages()
        self._refresh_request_positions()

        if s.has_search_banner:
            self._edit_search_banner(_search_banner_text(s, self._is_finished()))

    def _refresh_request_positions(self) -> None:
        '''Recompute (row_collection_index, row_index_in_collection) for each
        bundled request based on the current _row_collections.

        Pagination is not stable across renders — when items reach terminal
        stages their rows blank out and the same content fits in fewer pages.
        Stale stored positions then index past the end of _row_collections
        and _edit_row_data crashes.  Re-derive from the current layout.'''
        s = self.state
        table_index_to_position: dict[int, tuple[int, int]] = {}
        current_table_index = 0
        for collection_idx, row_collection in enumerate(self._row_collections):
            for row_idx in range(len(row_collection)):
                table_index_to_position[current_table_index] = (collection_idx, row_idx)
                current_table_index += 1
        for req_state in s.bundled_requests:
            if req_state.table_index is None:
                continue
            position = table_index_to_position.get(req_state.table_index)
            if position is not None:
                req_state.row_collection_index = position[0]
                req_state.row_index_in_collection = position[1]
            else:
                req_state.row_collection_index = None
                req_state.row_index_in_collection = None

    def _edit_row_data(self, req_state: BundledRequestState, message: str) -> bool:
        '''Edit a request's row in both the live table and any cached pages.

        Returns False when the request has no table_index (DISCARDED on arrival,
        etc.) so callers know the edit was a no-op.

        Out-of-bounds position checks guard against the case where the bundle
        was rebuilt from a saved state whose pagination collapsed (rows blanked
        as items finished → fewer pages now).  Editing the live table is
        always safe (it's keyed by table_index); only the cached page edit
        is skipped when stale positions don't fit the new layout.
        '''
        if req_state.table_index is None:
            return False
        self._table.edit_row(req_state.table_index, message)
        if (self._row_collections
                and req_state.row_collection_index is not None
                and req_state.row_index_in_collection is not None
                and req_state.row_collection_index < len(self._row_collections)
                and req_state.row_index_in_collection
                < len(self._row_collections[req_state.row_collection_index])):
            self._row_collections[req_state.row_collection_index][
                req_state.row_index_in_collection
            ].edit(message)
        return True

    def _edit_search_banner(self, message: str) -> None:
        '''Update the row-0 banner — only meaningful when has_search_banner.'''
        if not self.state.has_search_banner:
            return
        self._table.edit_row(0, message)
        if self._row_collections and self._row_collections[0]:
            self._row_collections[0][0].edit(message)

    def _check_finished(self) -> bool:
        '''Refresh the search-banner counters and (if ended) record finished_at.'''
        s = self.state
        if not s.all_requests_enqueued:
            return True
        if s.has_search_banner:
            self._edit_search_banner(_search_banner_text(s, self._is_finished()))
        return True

    def update_request_status(self) -> bool:
        '''Reconcile every request's lifecycle_stage with its rendered row.

        Idempotent — only edits rows when stored_status has drifted from the
        live state.lifecycle_stage, so calling this on every render is cheap.
        '''
        for req in self.state.bundled_requests:
            current_stage = req.media_request.lifecycle_stage
            if req.stored_status == current_stage:
                continue
            if current_stage == MediaRequestLifecycleStage.QUEUED:
                # Keep the existing "queued for download" row; nothing to edit.
                pass
            elif current_stage in (
                MediaRequestLifecycleStage.IN_PROGRESS,
                MediaRequestLifecycleStage.BACKOFF,
                MediaRequestLifecycleStage.RETRY_DOWNLOAD,
                MediaRequestLifecycleStage.RETRY_SEARCH,
            ):
                self._edit_row_data(req, _request_row_text(req))
            elif current_stage in (
                MediaRequestLifecycleStage.COMPLETED,
                MediaRequestLifecycleStage.DISCARDED,
            ):
                self._edit_row_data(req, '')
                self._increment_counter_for_stage(current_stage)
            elif current_stage == MediaRequestLifecycleStage.FAILED:
                self._increment_counter_for_stage(current_stage)
                # Keep the row text short — the failure reason goes in
                # get_failure_summary's separate message.
                self._edit_row_data(req, _request_row_text(req))
            req.stored_status = current_stage
        if self.finished and self.state.finished_at is None:
            self.state.finished_at = datetime.now(timezone.utc)
        self._check_finished()
        return True

    def print(self) -> List[str]:
        '''Render to a list of Discord-ready message strings.'''
        if self.state.is_shutdown:
            return []
        self.update_request_status()
        if not self._row_collections:
            return self._table.render()
        result_strings = [self._table.format_page(rc) for rc in self._row_collections]
        return [s for s in result_strings if s != '']

    def get_failure_summary(self) -> Optional[List[str]]:
        '''Return a single-message error summary for unsent failures, or None.'''
        failed = [
            req for req in self.state.bundled_requests
            if req.media_request.lifecycle_stage == MediaRequestLifecycleStage.FAILED
            and req.media_request.failure_reason
            and not req.failure_reason_sent
        ]
        if not failed:
            return None
        table = DapperTable(
            pagination_options=PaginationLength(self.state.pagination_length),
            prefix='Error Details for Failed Downloads\n',
        )
        for req in failed:
            table.add_row(
                f'Media Request "{req.media_request.display_name}", '
                f'Failure: {req.media_request.failure_reason}'
            )
            req.failure_reason_sent = True
        return table.render()

    def get_retry_summary(
        self, download_max_retries: int, search_max_retries: int
    ) -> Optional[List[Tuple[str, str]]]:
        '''Return ``(request_uuid, message)`` pairs for unsent retries, or None.

        Each pair is dispatched as a per-request mutable message so that a later
        successful attempt (or terminal failure) can delete it — see
        ``get_retry_cleanups``. The message reports the real attempt number
        (``attempt N/M``); it deliberately makes no promise about *when* the
        retry runs, since the request simply goes back on the queue.

        The M prefers the budget the retrying worker reported alongside the
        count (``retry_max``); download_max_retries / search_max_retries are the
        fallback for a request last touched by a worker that sent no budget.
        They are this process's own config, which is a *different file* from the
        worker's — trusting them first renders a live N against a stale M.
        '''
        retry_stages = {
            MediaRequestLifecycleStage.RETRY_DOWNLOAD,
            MediaRequestLifecycleStage.RETRY_SEARCH,
        }
        retries = [
            req for req in self.state.bundled_requests
            if req.media_request.lifecycle_stage in retry_stages
            and req.media_request.active_retry_information.retry_reason
            and not req.media_request.active_retry_information.retry_reason_sent
        ]
        if not retries:
            return None

        messages: List[Tuple[str, str]] = []
        for req in retries:
            info = req.media_request.active_retry_information
            max_r = info.retry_max or (
                download_max_retries
                if req.media_request.lifecycle_stage == MediaRequestLifecycleStage.RETRY_DOWNLOAD
                else search_max_retries
            )
            prefix = (
                f'Retrying "{req.media_request.display_name}" '
                f'(attempt {info.retry_count}/{max_r}):\n```\n'
            )
            suffix = '\n```'
            available = DISCORD_MAX_MESSAGE_LENGTH - len(prefix) - len(suffix)
            truncated = shorten_string(info.retry_reason, available)
            messages.append((str(req.media_request.uuid), f'{prefix}{truncated}{suffix}'))
            info.retry_reason_sent = True
            req.retry_message_outstanding = True
        return messages

    def get_retry_cleanups(self) -> List[str]:
        '''Return uuids of requests whose retry message should now be removed.

        A retry message is torn down once its request leaves the retry stages
        for good — either it finally downloaded (COMPLETED) or gave up
        (FAILED/DISCARDED). The outstanding flag is cleared so we only emit the
        removal once.
        '''
        terminal_stages = {
            MediaRequestLifecycleStage.COMPLETED,
            MediaRequestLifecycleStage.FAILED,
            MediaRequestLifecycleStage.DISCARDED,
        }
        cleanups: List[str] = []
        for req in self.state.bundled_requests:
            if (req.retry_message_outstanding
                    and req.media_request.lifecycle_stage in terminal_stages):
                cleanups.append(str(req.media_request.uuid))
                req.retry_message_outstanding = False
        return cleanups
