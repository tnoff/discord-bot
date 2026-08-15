'''Tests for the MediaBrokerBase engine ABC and the cog-facing Protocols.

MediaBrokerBase ships unwired in this MR — no concrete impl is constructed in
production yet (AsyncioBroker / RedisBroker land in later broker MRs).  These
tests drive the base's concrete template methods (cache helpers + bundle
lifecycle) through a minimal in-test subclass so the base's behaviour is
locked before the real impls build on it.
'''
from unittest.mock import MagicMock

import pytest

from discord_bot.interfaces.broker_protocols import (
    BrokerClient,
    BrokerEntry,
    BundleDispatchSink,
    CheckoutResult,
    MediaBrokerBase,
    Zone,
)
from discord_bot.workers.media_bundle import BundleRenderer

from tests.helpers import fake_context, fake_source_dict  # pylint: disable=unused-import


class _StorageBroker(MediaBrokerBase):
    '''Minimal concrete MediaBrokerBase: dict-backed bundle storage plus the
    register_request attach behaviour the real impls provide, so the base's
    template methods can be exercised.  The non-bundle registry ops are unused
    here and raise.'''

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self._bundles = {}

    # --- bundle storage hooks --------------------------------------------
    async def _load_bundle(self, bundle_uuid):
        return self._bundles.get(bundle_uuid)

    async def _save_bundle(self, state):
        self._bundles[state.uuid] = state

    async def _drop_bundle(self, bundle_uuid):
        self._bundles.pop(bundle_uuid, None)

    async def list_bundles_for_guild(self, guild_id):
        return [u for u, s in self._bundles.items() if s.guild_id == guild_id]

    # --- register_request attach (mirrors AsyncioBroker) -----------------
    async def register_request(self, media_request):
        bundle_uuid = media_request.bundle_uuid
        if bundle_uuid and bundle_uuid in self._bundles:
            renderer = BundleRenderer(self._bundles[bundle_uuid])
            renderer.add_media_request(media_request)
            self._bundles[bundle_uuid] = renderer.state
            await self._maybe_render_bundle(media_request)

    # --- unused registry surface (must exist to instantiate the ABC) -----
    async def can_evict_base(self, webpage_url):  # pylint: disable=unused-argument
        raise NotImplementedError

    async def update_request_status(self, request_uuid, update):  # pylint: disable=unused-argument
        raise NotImplementedError

    async def register_download_result(self, result):  # pylint: disable=unused-argument
        raise NotImplementedError

    async def register_download(self, media_download):  # pylint: disable=unused-argument
        raise NotImplementedError

    async def checkout(self, media_request_uuid, guild_id, guild_path=None):  # pylint: disable=unused-argument
        raise NotImplementedError

    async def remove(self, media_request_uuid):  # pylint: disable=unused-argument
        raise NotImplementedError

    async def release(self, media_request_uuid):  # pylint: disable=unused-argument
        raise NotImplementedError

    async def discard(self, media_request_uuid):  # pylint: disable=unused-argument
        raise NotImplementedError

    async def prefetch(self, queue_items, guild_id, guild_path, limit):  # pylint: disable=unused-argument
        raise NotImplementedError

    async def can_evict_request(self, media_request_uuid):  # pylint: disable=unused-argument
        raise NotImplementedError

    async def get_entry(self, media_request_uuid):  # pylint: disable=unused-argument
        raise NotImplementedError

    async def save_player_session(self, session):  # pylint: disable=unused-argument
        raise NotImplementedError

    async def list_player_sessions(self):
        raise NotImplementedError

    async def delete_player_session(self, guild_id):  # pylint: disable=unused-argument
        raise NotImplementedError

    async def get_cache_count(self):
        raise NotImplementedError

    async def get_checked_out_by(self, guild_id):  # pylint: disable=unused-argument
        raise NotImplementedError


def _make_dispatcher():
    '''A MagicMock satisfying BundleDispatchSink (sync fire-and-forget API).'''
    dispatcher = MagicMock()
    dispatcher.update_mutable = MagicMock()
    dispatcher.remove_mutable = MagicMock()
    dispatcher.send_message = MagicMock()
    return dispatcher


async def _add_queued_request(broker, fake_ctx, bundle_uuid):
    '''Register a fresh QUEUED request into a bundle, returning the request.'''
    media_request = fake_source_dict(fake_ctx)
    media_request.state_machine.mark_queued()
    media_request.bundle_uuid = bundle_uuid
    await broker.register_request(media_request)
    return media_request


# ---------------------------------------------------------------------------
# Abstract enforcement + value types
# ---------------------------------------------------------------------------

def test_media_broker_base_is_abstract():
    '''MediaBrokerBase cannot be instantiated directly.'''
    with pytest.raises(TypeError):
        MediaBrokerBase()  # pylint: disable=abstract-class-instantiated


def test_value_types_have_expected_defaults():
    '''CheckoutResult / BrokerEntry / Zone carry the documented defaults.'''
    checkout = CheckoutResult()
    assert checkout.local_path is None
    assert checkout.s3_key is None
    assert checkout.bucket_name is None

    request = MagicMock()
    entry = BrokerEntry(request=request)
    assert entry.zone is Zone.IN_FLIGHT
    assert entry.download is None
    assert entry.checked_out_by is None
    assert {z.value for z in Zone} == {'in_flight', 'available', 'checked_out'}


def test_client_protocols_declare_their_surface():
    '''The cog-facing Protocols declare the methods later impls must expose.'''
    broker_methods = {n for n in vars(BrokerClient) if not n.startswith('_')}
    assert {'register_request', 'checkout', 'create_bundle', 'next_result'} <= broker_methods
    sink_methods = {n for n in vars(BundleDispatchSink) if not n.startswith('_')}
    assert sink_methods == {'update_mutable', 'remove_mutable', 'send_message'}


# ---------------------------------------------------------------------------
# Bundle lifecycle: create / finalize / delete
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_create_bundle_persists_state_without_dispatcher(fake_context):  # pylint: disable=redefined-outer-name
    '''create_bundle stores a retrievable BundleState even with no dispatcher.'''
    broker = _StorageBroker()
    bundle_uuid = await broker.create_bundle(
        fake_context['guild'].id, fake_context['channel'].id,
        input_string='playlist', has_search_banner=True,
    )
    state = await broker._load_bundle(bundle_uuid)  # pylint: disable=protected-access
    assert state.uuid == bundle_uuid
    assert state.guild_id == fake_context['guild'].id
    assert state.has_search_banner is True
    assert state.all_requests_enqueued is False


@pytest.mark.asyncio
async def test_create_bundle_dispatches_initial_banner(fake_context):  # pylint: disable=redefined-outer-name
    '''create_bundle posts the "Processing X" banner immediately.'''
    dispatcher = _make_dispatcher()
    broker = _StorageBroker(dispatcher=dispatcher)
    bundle_uuid = await broker.create_bundle(
        fake_context['guild'].id, fake_context['channel'].id,
        input_string='Channel History', has_search_banner=True,
    )
    dispatcher.update_mutable.assert_called_once()
    args = dispatcher.update_mutable.call_args.args
    assert args[0] == f'request_bundle-{bundle_uuid}'
    assert any('Processing "Channel History"' in line for line in args[2])


@pytest.mark.asyncio
async def test_finalize_bundle_unknown_uuid_is_noop(fake_context):  # pylint: disable=redefined-outer-name,unused-argument
    '''finalize_bundle on an unknown uuid logs and returns without raising.'''
    await _StorageBroker().finalize_bundle('does-not-exist')


@pytest.mark.asyncio
async def test_finalize_bundle_marks_enqueued_and_dispatches(fake_context):  # pylint: disable=redefined-outer-name
    '''finalize_bundle flips all_requests_enqueued and pushes the counter line.'''
    dispatcher = _make_dispatcher()
    broker = _StorageBroker(dispatcher=dispatcher)
    bundle_uuid = await broker.create_bundle(
        fake_context['guild'].id, fake_context['channel'].id,
        input_string='multi', has_search_banner=True,
    )
    await _add_queued_request(broker, fake_context, bundle_uuid)
    dispatcher.reset_mock()
    await broker.finalize_bundle(bundle_uuid)
    state = await broker._load_bundle(bundle_uuid)  # pylint: disable=protected-access
    assert state.all_requests_enqueued is True
    content = dispatcher.update_mutable.call_args.args[2]
    assert any('media requests processed successfully' in line for line in content)


@pytest.mark.asyncio
async def test_delete_bundle_with_dispatcher(fake_context):  # pylint: disable=redefined-outer-name
    '''delete_bundle drops the Discord message and wipes stored state.'''
    dispatcher = _make_dispatcher()
    broker = _StorageBroker(dispatcher=dispatcher)
    bundle_uuid = await broker.create_bundle(
        fake_context['guild'].id, fake_context['channel'].id,
    )
    await broker.delete_bundle(bundle_uuid)
    dispatcher.remove_mutable.assert_called_once_with(f'request_bundle-{bundle_uuid}')
    assert await broker._load_bundle(bundle_uuid) is None  # pylint: disable=protected-access


@pytest.mark.asyncio
async def test_delete_bundle_without_dispatcher(fake_context):  # pylint: disable=redefined-outer-name
    '''delete_bundle is a clean no-dispatch teardown when no dispatcher wired.'''
    broker = _StorageBroker()
    bundle_uuid = await broker.create_bundle(
        fake_context['guild'].id, fake_context['channel'].id,
    )
    await broker.delete_bundle(bundle_uuid)
    assert await broker._load_bundle(bundle_uuid) is None  # pylint: disable=protected-access


# ---------------------------------------------------------------------------
# Render/dispatch flow: defer guard, summaries, teardown, no-ops
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_render_missing_bundle_is_noop():
    '''Rendering an unknown bundle uuid is a silent no-op.'''
    broker = _StorageBroker(dispatcher=_make_dispatcher())
    await broker._render_and_dispatch_bundle('missing')  # pylint: disable=protected-access
    broker.dispatcher.update_mutable.assert_not_called()


@pytest.mark.asyncio
async def test_render_shutdown_bundle_is_noop(fake_context):  # pylint: disable=redefined-outer-name
    '''A shut-down bundle renders to nothing and never dispatches.'''
    dispatcher = _make_dispatcher()
    broker = _StorageBroker(dispatcher=dispatcher)
    bundle_uuid = await broker.create_bundle(
        fake_context['guild'].id, fake_context['channel'].id,
    )
    state = await broker._load_bundle(bundle_uuid)  # pylint: disable=protected-access
    state.is_shutdown = True
    await broker._save_bundle(state)  # pylint: disable=protected-access
    dispatcher.reset_mock()
    await broker._render_and_dispatch_bundle(bundle_uuid)  # pylint: disable=protected-access
    dispatcher.update_mutable.assert_not_called()


@pytest.mark.asyncio
async def test_register_request_during_enqueue_defers_dispatch(fake_context):  # pylint: disable=redefined-outer-name
    '''Mid-loop register_request on a multi-track bundle updates state but
    defers dispatch until finalize_bundle.'''
    dispatcher = _make_dispatcher()
    broker = _StorageBroker(dispatcher=dispatcher)
    bundle_uuid = await broker.create_bundle(
        fake_context['guild'].id, fake_context['channel'].id,
        input_string='Channel History', has_search_banner=True,
    )
    dispatcher.reset_mock()
    for _ in range(3):
        await _add_queued_request(broker, fake_context, bundle_uuid)
    dispatcher.update_mutable.assert_not_called()
    state = await broker._load_bundle(bundle_uuid)  # pylint: disable=protected-access
    assert state.total == 3


@pytest.mark.asyncio
async def test_no_dispatcher_still_advances_state(fake_context):  # pylint: disable=redefined-outer-name
    '''With no dispatcher the broker still mutates and persists bundle state.'''
    broker = _StorageBroker()
    bundle_uuid = await broker.create_bundle(
        fake_context['guild'].id, fake_context['channel'].id,
        input_string='multi', has_search_banner=True,
    )
    await _add_queued_request(broker, fake_context, bundle_uuid)
    await broker.finalize_bundle(bundle_uuid)
    state = await broker._load_bundle(bundle_uuid)  # pylint: disable=protected-access
    assert state.all_requests_enqueued is True
    assert state.total == 1


@pytest.mark.asyncio
async def test_maybe_render_skips_when_no_bundle(fake_context):  # pylint: disable=redefined-outer-name
    '''_maybe_render_bundle is a no-op for None or an unbundled request.'''
    broker = _StorageBroker(dispatcher=_make_dispatcher())
    await broker._maybe_render_bundle(None)  # pylint: disable=protected-access
    unbundled = fake_source_dict(fake_context)
    await broker._maybe_render_bundle(unbundled)  # pylint: disable=protected-access
    broker.dispatcher.update_mutable.assert_not_called()


@pytest.mark.asyncio
async def test_failed_request_emits_failure_summary(fake_context):  # pylint: disable=redefined-outer-name
    '''A FAILED request with a reason triggers a separate failure-summary message.'''
    dispatcher = _make_dispatcher()
    broker = _StorageBroker(dispatcher=dispatcher)
    bundle_uuid = await broker.create_bundle(
        fake_context['guild'].id, fake_context['channel'].id,
        input_string='multi', has_search_banner=True,
    )
    media_request = await _add_queued_request(broker, fake_context, bundle_uuid)
    await broker.finalize_bundle(bundle_uuid)
    dispatcher.reset_mock()
    media_request.state_machine.mark_failed('network exploded')
    await broker._render_and_dispatch_bundle(bundle_uuid)  # pylint: disable=protected-access
    assert dispatcher.send_message.called
    sent = ' '.join(str(c.args[2]) for c in dispatcher.send_message.call_args_list)
    assert 'network exploded' in sent


@pytest.mark.asyncio
async def test_retrying_request_emits_retry_summary(fake_context):  # pylint: disable=redefined-outer-name
    '''A RETRY_DOWNLOAD request emits a per-request mutable retry note keyed by
    uuid, carrying the real attempt count and no timing promise.'''
    dispatcher = _make_dispatcher()
    broker = _StorageBroker(dispatcher=dispatcher)
    bundle_uuid = await broker.create_bundle(
        fake_context['guild'].id, fake_context['channel'].id,
        input_string='multi', has_search_banner=True,
    )
    media_request = await _add_queued_request(broker, fake_context, bundle_uuid)
    await broker.finalize_bundle(bundle_uuid)
    dispatcher.reset_mock()
    media_request.download_retry_information.retry_count = 1
    media_request.state_machine.mark_retry_download(
        'rate limited', backoff_seconds=120, retry_count=1)
    await broker._render_and_dispatch_bundle(bundle_uuid)  # pylint: disable=protected-access
    retry_key = f'request_retry-{bundle_uuid}-{media_request.uuid}'
    retry_calls = [c for c in dispatcher.update_mutable.call_args_list
                   if c.args[0] == retry_key]
    assert len(retry_calls) == 1
    content = retry_calls[0].args[2]
    # update_mutable expects a list of page-strings, one Discord message per
    # element. A bare str would be iterated character-by-character, rendering
    # "Retrying" as one letter per message (prod bug 2026-07-24).
    assert isinstance(content, list)
    assert len(content) == 1
    assert 'Retrying' in content[0]
    assert 'attempt 1/' in content[0]
    assert 'retrying in' not in content[0]
    # No delete_after — the note is torn down explicitly, not auto-expired.
    assert retry_calls[0].kwargs.get('delete_after') is None
    # It is not removed while still retrying.
    assert all(c.args[0] != retry_key
               for c in dispatcher.remove_mutable.call_args_list)


@pytest.mark.asyncio
async def test_retry_note_removed_when_request_completes(fake_context):  # pylint: disable=redefined-outer-name
    '''Once a retried request downloads, its retry note is deleted so the
    "Retrying …" message doesn't linger after a later attempt succeeds.'''
    dispatcher = _make_dispatcher()
    broker = _StorageBroker(dispatcher=dispatcher)
    bundle_uuid = await broker.create_bundle(
        fake_context['guild'].id, fake_context['channel'].id,
        input_string='multi', has_search_banner=True,
    )
    media_request = await _add_queued_request(broker, fake_context, bundle_uuid)
    await broker.finalize_bundle(bundle_uuid)
    media_request.state_machine.mark_retry_download(
        'rate limited', retry_count=1)
    await broker._render_and_dispatch_bundle(bundle_uuid)  # pylint: disable=protected-access
    dispatcher.reset_mock()

    retry_key = f'request_retry-{bundle_uuid}-{media_request.uuid}'
    media_request.state_machine.mark_completed()
    await broker._render_and_dispatch_bundle(bundle_uuid)  # pylint: disable=protected-access
    dispatcher.remove_mutable.assert_any_call(retry_key)


@pytest.mark.asyncio
async def test_delete_bundle_removes_outstanding_retry_notes(fake_context):  # pylint: disable=redefined-outer-name
    '''Tearing down a bundle mid-retry drops the still-live retry note too.'''
    dispatcher = _make_dispatcher()
    broker = _StorageBroker(dispatcher=dispatcher)
    bundle_uuid = await broker.create_bundle(
        fake_context['guild'].id, fake_context['channel'].id,
        input_string='multi', has_search_banner=True,
    )
    media_request = await _add_queued_request(broker, fake_context, bundle_uuid)
    await broker.finalize_bundle(bundle_uuid)
    media_request.state_machine.mark_retry_download('rate limited', retry_count=1)
    await broker._render_and_dispatch_bundle(bundle_uuid)  # pylint: disable=protected-access
    dispatcher.reset_mock()

    await broker.delete_bundle(bundle_uuid)
    retry_key = f'request_retry-{bundle_uuid}-{media_request.uuid}'
    dispatcher.remove_mutable.assert_any_call(retry_key)


@pytest.mark.asyncio
async def test_finished_single_track_bundle_is_torn_down(fake_context):  # pylint: disable=redefined-outer-name
    '''A completed single-track bundle renders to nothing → message removed and
    state dropped, so no stale "Downloading..." line lingers in Discord.'''
    dispatcher = _make_dispatcher()
    broker = _StorageBroker(dispatcher=dispatcher)
    bundle_uuid = await broker.create_bundle(
        fake_context['guild'].id, fake_context['channel'].id,
        input_string='single-track input',
    )
    media_request = await _add_queued_request(broker, fake_context, bundle_uuid)
    await broker.finalize_bundle(bundle_uuid)
    media_request.state_machine.mark_in_progress()
    await broker._render_and_dispatch_bundle(bundle_uuid)  # pylint: disable=protected-access
    dispatcher.reset_mock()
    media_request.state_machine.mark_completed()
    await broker._render_and_dispatch_bundle(bundle_uuid)  # pylint: disable=protected-access
    dispatcher.remove_mutable.assert_called_once_with(f'request_bundle-{bundle_uuid}')
    assert await broker._load_bundle(bundle_uuid) is None  # pylint: disable=protected-access


@pytest.mark.asyncio
async def test_finished_multi_track_bundle_expires_summary(fake_context):  # pylint: disable=redefined-outer-name
    '''A completed multi-track bundle dispatches its final "Completed N/N" summary
    with delete_after (Discord expires it on its own — the pre-broker behaviour).
    No outright remove_mutable and no drop: the content is non-empty, so the
    bundle stays tracked/queryable and the summary lingers until it auto-expires.'''
    dispatcher = _make_dispatcher()
    broker = _StorageBroker(dispatcher=dispatcher, message_delete_after=300)
    bundle_uuid = await broker.create_bundle(
        fake_context['guild'].id, fake_context['channel'].id,
        input_string='multi playlist', has_search_banner=True,
    )
    media_request = await _add_queued_request(broker, fake_context, bundle_uuid)
    await broker.finalize_bundle(bundle_uuid)
    # While in flight, the render carries no delete_after (the banner persists).
    assert broker.dispatcher.update_mutable.call_args.kwargs.get('delete_after') is None
    dispatcher.reset_mock()
    media_request.state_machine.mark_completed()
    await broker._render_and_dispatch_bundle(bundle_uuid)  # pylint: disable=protected-access
    dispatcher.remove_mutable.assert_not_called()
    call = dispatcher.update_mutable.call_args
    assert any('Completed processing' in line for line in call.args[2])
    assert call.kwargs.get('delete_after') == 300
    # Non-empty summary → bundle stays tracked (queryable) until Discord expires it.
    assert await broker._load_bundle(bundle_uuid) is not None  # pylint: disable=protected-access


@pytest.mark.asyncio
async def test_finished_summary_dispatched_only_once(fake_context):  # pylint: disable=redefined-outer-name
    '''Re-rendering an already-finished bundle must NOT re-dispatch the terminal
    summary.  The first finished render sends "Completed N/N" with delete_after,
    which makes the message dispatcher drop its mutable tracking; a second
    dispatch would orphan-create a duplicate summary (the prod double-message
    bug).  Guarded by BundleState.summary_dispatched.'''
    dispatcher = _make_dispatcher()
    broker = _StorageBroker(dispatcher=dispatcher, message_delete_after=300)
    bundle_uuid = await broker.create_bundle(
        fake_context['guild'].id, fake_context['channel'].id,
        input_string='multi playlist', has_search_banner=True,
    )
    media_request = await _add_queued_request(broker, fake_context, bundle_uuid)
    await broker.finalize_bundle(bundle_uuid)
    media_request.state_machine.mark_completed()

    # First finished render dispatches the terminal summary once.
    await broker._render_and_dispatch_bundle(bundle_uuid)  # pylint: disable=protected-access
    first = [c for c in dispatcher.update_mutable.call_args_list
             if any('Completed processing' in line for line in c.args[2])]
    assert len(first) == 1
    assert first[0].kwargs.get('delete_after') == 300
    assert (await broker._load_bundle(bundle_uuid)).summary_dispatched is True  # pylint: disable=protected-access

    # A later lifecycle push re-renders the same finished bundle — no second summary.
    dispatcher.reset_mock()
    await broker._render_and_dispatch_bundle(bundle_uuid)  # pylint: disable=protected-access
    dispatcher.update_mutable.assert_not_called()
    dispatcher.remove_mutable.assert_not_called()
