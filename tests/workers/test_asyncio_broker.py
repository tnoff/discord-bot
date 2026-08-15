'''Tests for AsyncioBroker branches not exercised by the ported engine tests.

The bulk of AsyncioBroker's registry / checkout / eviction behaviour is covered
by tests/cogs/music_helpers/test_media_broker.py and tests/utils/test_media_broker.py
(converted from the legacy MediaBroker suite).  This file covers the remaining
branches: the full update_request_status lifecycle ladder, the register_request
bundle-attach path, get_cache_count with a cache, and the bundle-storage hooks.
'''
from pathlib import Path
from tempfile import TemporaryDirectory
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest

from discord_bot.cogs.music_helpers.common import MediaRequestLifecycleStage
from discord_bot.interfaces.broker_protocols import BrokerEntry, CheckoutResult, Zone
from discord_bot.types.download import LifecycleEvent, LifecycleStatusUpdate
from discord_bot.types.player_session import PlayerSession
from discord_bot.types.playlist_add_request import parse_media_request
from discord_bot.workers.asyncio_broker import AsyncioBroker

from tests.helpers import fake_context, fake_media_download, fake_source_dict  # pylint: disable=unused-import


async def _register(broker, fake_ctx):
    '''Register a fresh request and return it.'''
    media_request = fake_source_dict(fake_ctx)
    await broker.register_request(media_request)
    return media_request


@pytest.mark.asyncio
@pytest.mark.parametrize('event,expected_stage,kwargs', [
    (LifecycleEvent.QUEUED, MediaRequestLifecycleStage.QUEUED, {}),
    (LifecycleEvent.BACKOFF, MediaRequestLifecycleStage.BACKOFF, {}),
    (LifecycleEvent.IN_PROGRESS, MediaRequestLifecycleStage.IN_PROGRESS, {}),
    (LifecycleEvent.RETRY, MediaRequestLifecycleStage.RETRY_DOWNLOAD,
     {'error_detail': 'boom', 'backoff_seconds': 5}),
    (LifecycleEvent.RETRY_SEARCH, MediaRequestLifecycleStage.RETRY_SEARCH,
     {'error_detail': 'no hit', 'backoff_seconds': 5}),
    (LifecycleEvent.DISCARDED, MediaRequestLifecycleStage.DISCARDED, {}),
    (LifecycleEvent.COMPLETED, MediaRequestLifecycleStage.COMPLETED, {}),
    (LifecycleEvent.FAILED, MediaRequestLifecycleStage.FAILED, {'failure_reason': 'dead'}),
])
async def test_update_request_status_drives_each_event(fake_context, event, expected_stage, kwargs):  # pylint: disable=redefined-outer-name
    '''Every LifecycleEvent maps to the matching state-machine transition.'''
    broker = AsyncioBroker()
    media_request = await _register(broker, fake_context)
    await broker.update_request_status(str(media_request.uuid), LifecycleStatusUpdate(event=event, **kwargs))
    assert media_request.lifecycle_stage == expected_stage


@pytest.mark.asyncio
@pytest.mark.parametrize('event,attribute', [
    (LifecycleEvent.RETRY, 'download_retry_information'),
    (LifecycleEvent.RETRY_SEARCH, 'youtube_music_retry_information'),
])
async def test_update_request_status_stores_reported_retry_budget(fake_context, event, attribute):  # pylint: disable=redefined-outer-name
    '''RETRY / RETRY_SEARCH persist the worker's budget next to its count, so the
    renderer never has to fall back to its own (possibly stale) configured max.'''
    broker = AsyncioBroker()
    media_request = await _register(broker, fake_context)
    await broker.update_request_status(str(media_request.uuid), LifecycleStatusUpdate(
        event=event, error_detail='boom', backoff_seconds=5, retry_count=4, max_retries=5,
    ))
    info = getattr(media_request, attribute)
    assert info.retry_count == 4
    assert info.retry_max == 5


@pytest.mark.asyncio
@pytest.mark.parametrize('event,kwargs', [
    (LifecycleEvent.DISCARDED, {}),
    (LifecycleEvent.FAILED, {'failure_reason': 'dead'}),
])
async def test_update_request_status_terminal_drops_entry(fake_context, event, kwargs):  # pylint: disable=redefined-outer-name
    '''DISCARDED/FAILED remove the registry entry so terminal requests don't leak.'''
    broker = AsyncioBroker()
    media_request = await _register(broker, fake_context)
    assert await broker.get_entry(str(media_request.uuid)) is not None
    await broker.update_request_status(str(media_request.uuid), LifecycleStatusUpdate(event=event, **kwargs))
    assert await broker.get_entry(str(media_request.uuid)) is None


@pytest.mark.asyncio
async def test_update_request_status_unknown_uuid_warns(fake_context):  # pylint: disable=redefined-outer-name,unused-argument
    '''An update for an unregistered uuid logs and returns without raising.'''
    await AsyncioBroker().update_request_status(
        'nope', LifecycleStatusUpdate(event=LifecycleEvent.QUEUED),
    )


@pytest.mark.asyncio
async def test_update_request_status_clears_bundle_when_registry_alias_broke(fake_context):  # pylint: disable=redefined-outer-name
    '''A terminal lifecycle push clears the bundle even when the registry entry
    is no longer the same Python object the bundle attached.

    Regression: a single-track "Downloading and processing…" message stayed on
    screen forever after the track finished and played.  The bundle's stored
    request normally shares a reference with the registry entry, but that alias
    is lost when the registry entry is rebuilt from a deserialised request (e.g.
    register_download's entry-absent branch fed a DownloadResult.media_request).
    Once broken, the COMPLETED mark landed on the registry object while the
    bundle kept rendering its stale IN_PROGRESS snapshot, so it never reached
    finished and the message was never removed.  update_request_status must
    re-sync the authoritative request into the bundle before rendering.
    '''
    dispatcher = MagicMock()
    broker = AsyncioBroker(dispatcher=dispatcher, message_delete_after=60)
    bundle_uuid = await broker.create_bundle(
        fake_context['guild'].id, fake_context['channel'].id,
        input_string='https://on.soundcloud.com/abc',
    )
    media_request = fake_source_dict(fake_context, is_direct_search=True)
    media_request.bundle_uuid = bundle_uuid
    await broker.register_request(media_request)
    await broker.update_request_status(
        str(media_request.uuid), LifecycleStatusUpdate(event=LifecycleEvent.QUEUED))
    await broker.finalize_bundle(bundle_uuid)
    await broker.update_request_status(
        str(media_request.uuid), LifecycleStatusUpdate(event=LifecycleEvent.IN_PROGRESS))
    bundle_key = f'request_bundle-{bundle_uuid}'
    assert any('Downloading and processing' in str(c.args[2])
               for c in dispatcher.update_mutable.call_args_list)

    # Break the alias the way a rebuilt registry entry does: the registry now
    # holds a deserialised copy, while the bundle still references the original.
    rebuilt = parse_media_request(media_request.model_dump(mode='json'))
    assert rebuilt is not media_request
    broker._registry[str(media_request.uuid)] = BrokerEntry(  # pylint: disable=protected-access
        request=rebuilt, zone=Zone.AVAILABLE)

    await broker.update_request_status(
        str(rebuilt.uuid), LifecycleStatusUpdate(event=LifecycleEvent.COMPLETED))

    # The finished single-track bundle is torn down rather than left showing the
    # stale download line.
    dispatcher.remove_mutable.assert_called_once_with(bundle_key)
    assert broker.get_bundle_state(bundle_uuid) is None


@pytest.mark.asyncio
async def test_update_request_status_with_dangling_bundle_uuid_is_noop(fake_context):  # pylint: disable=redefined-outer-name
    '''A lifecycle push for a request whose bundle_uuid points at no stored
    bundle syncs/renders nothing and does not raise (the bundle-absent guard).'''
    dispatcher = MagicMock()
    broker = AsyncioBroker(dispatcher=dispatcher)
    media_request = fake_source_dict(fake_context)
    media_request.bundle_uuid = 'request.bundle.gone'
    await broker.register_request(media_request)
    await broker.update_request_status(
        str(media_request.uuid), LifecycleStatusUpdate(event=LifecycleEvent.COMPLETED))
    assert media_request.lifecycle_stage == MediaRequestLifecycleStage.COMPLETED
    dispatcher.update_mutable.assert_not_called()
    dispatcher.remove_mutable.assert_not_called()


@pytest.mark.asyncio
async def test_register_request_attaches_to_known_bundle(fake_context):  # pylint: disable=redefined-outer-name
    '''register_request attaches the request to its bundle when bundle_uuid is set.'''
    broker = AsyncioBroker()
    bundle_uuid = await broker.create_bundle(
        fake_context['guild'].id, fake_context['channel'].id,
        input_string='multi', has_search_banner=True,
    )
    media_request = fake_source_dict(fake_context)
    media_request.state_machine.mark_queued()
    media_request.bundle_uuid = bundle_uuid
    await broker.register_request(media_request)
    state = broker.get_bundle_state(bundle_uuid)
    assert state.total == 1
    assert str(state.bundled_requests[0].media_request.uuid) == str(media_request.uuid)


@pytest.mark.asyncio
async def test_register_request_ignores_unknown_bundle(fake_context):  # pylint: disable=redefined-outer-name
    '''A bundle_uuid not in storage leaves the request unattached (no raise).'''
    broker = AsyncioBroker()
    media_request = fake_source_dict(fake_context)
    media_request.bundle_uuid = 'request.bundle.does-not-exist'
    await broker.register_request(media_request)
    assert len(broker) == 1


@pytest.mark.asyncio
async def test_get_cache_count_with_cache(fake_context):  # pylint: disable=redefined-outer-name,unused-argument
    '''get_cache_count delegates to the video cache when one is configured.'''
    cache = AsyncMock()
    cache.get_cache_count.return_value = 7
    broker = AsyncioBroker(video_cache=cache)
    assert await broker.get_cache_count() == 7


@pytest.mark.asyncio
async def test_prefetch_skips_items_missing_from_registry(fake_context):  # pylint: disable=redefined-outer-name,unused-argument
    '''prefetch ignores queue items that have no registry entry (S3 mode).'''
    broker = AsyncioBroker(bucket_name='bucket')
    item = SimpleNamespace(media_request=SimpleNamespace(uuid='not-registered'))
    await broker.prefetch([item], guild_id=123, guild_path=Path('/tmp/prefetch-test'), limit=5)
    assert len(broker) == 0


@pytest.mark.asyncio
async def test_bundle_storage_hooks_roundtrip(fake_context):  # pylint: disable=redefined-outer-name
    '''create/list/delete drive AsyncioBroker's _save/_load/_drop hooks + get_bundle_state.'''
    broker = AsyncioBroker()
    a = await broker.create_bundle(fake_context['guild'].id, fake_context['channel'].id)
    b = await broker.create_bundle(fake_context['guild'].id, fake_context['channel'].id)
    other = await broker.create_bundle(9999, fake_context['channel'].id)
    assert broker.get_bundle_state(a) is not None
    assert set(await broker.list_bundles_for_guild(fake_context['guild'].id)) == {a, b}
    assert await broker.list_bundles_for_guild(9999) == [other]
    await broker.delete_bundle(a)
    assert broker.get_bundle_state(a) is None


@pytest.mark.asyncio
async def test_checkout_returns_checkoutresult_and_caches_local_path(fake_context): #pylint:disable=redefined-outer-name
    '''checkout stages the file and returns CheckoutResult(local_path); a second
    checkout hits the already-CHECKED_OUT early-return with the same local_path.'''
    broker = AsyncioBroker()
    with TemporaryDirectory() as guild_dir:
        with fake_media_download(guild_dir, fake_context=fake_context) as md:
            await broker.register_download(md)
            first = await broker.checkout(str(md.media_request.uuid), 1, Path(guild_dir))
            second = await broker.checkout(str(md.media_request.uuid), 1, Path(guild_dir))
            assert isinstance(first, CheckoutResult)
            assert first.local_path is not None
            assert second.local_path == first.local_path


@pytest.mark.asyncio
async def test_single_track_search_cache_hit_tears_down_bundle(fake_context):  # pylint: disable=redefined-outer-name
    '''A single-track SEARCH→cache-hit bundle tears down once its request
    completes — the message must not strand on "Media request queued…".

    Drives the cog's real ordering for `!play <search>` that resolves from the
    video cache: create_bundle (set_initial_search placeholder) → register_request
    (SEARCHING) → finalize_bundle → QUEUED push → register_download then a
    COMPLETED push (the cog pushes COMPLETED more than once; the transitions are
    idempotent).  The terminal push must blank the single row, reach `finished`,
    and dispatch remove_mutable + drop the bundle.
    '''
    dispatcher = MagicMock()
    broker = AsyncioBroker(dispatcher=dispatcher, message_delete_after=60)
    bundle_uuid = await broker.create_bundle(
        fake_context['guild'].id, fake_context['channel'].id,
        input_string='in the Yuma by Chris lake',
    )
    media_request = fake_source_dict(fake_context)
    media_request.bundle_uuid = bundle_uuid
    await broker.register_request(media_request)
    await broker.finalize_bundle(bundle_uuid)

    await broker.update_request_status(
        str(media_request.uuid), LifecycleStatusUpdate(event=LifecycleEvent.QUEUED))
    with TemporaryDirectory() as tmpd:
        with fake_media_download(Path(tmpd), media_request=media_request) as media_download:
            await broker.register_download(media_download)
            await broker.update_request_status(
                str(media_request.uuid), LifecycleStatusUpdate(event=LifecycleEvent.COMPLETED))
            await broker.update_request_status(
                str(media_request.uuid), LifecycleStatusUpdate(event=LifecycleEvent.COMPLETED))

    dispatcher.remove_mutable.assert_any_call(f'request_bundle-{bundle_uuid}')
    assert broker.get_bundle_state(bundle_uuid) is None


# ---------------------------------------------------------------------------
# Player sessions
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_player_session_save_list_delete(fake_context):  # pylint: disable=redefined-outer-name
    '''Single-process sessions round-trip in memory.

    They do not survive the restart they exist for — without a separate broker
    process there is nothing to survive into — but the surface has to behave so
    the cog can call it in both deployment modes.
    '''
    broker = AsyncioBroker()
    session = PlayerSession(
        guild_id=fake_context['guild'].id,
        voice_channel_id=10,
        text_channel_id=fake_context['channel'].id,
        queue=[fake_source_dict(fake_context)],
        was_playing=True,
    )

    await broker.save_player_session(session)
    listed = await broker.list_player_sessions()
    assert [s.guild_id for s in listed] == [fake_context['guild'].id]
    assert listed[0].was_playing is True
    assert len(listed[0].queue) == 1

    await broker.delete_player_session(fake_context['guild'].id)
    assert await broker.list_player_sessions() == []


@pytest.mark.asyncio
async def test_player_session_delete_absent_is_noop():
    '''Deleting a session that was never saved does not raise.'''
    broker = AsyncioBroker()
    await broker.delete_player_session(4242)
    assert await broker.list_player_sessions() == []
