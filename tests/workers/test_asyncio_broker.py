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
from unittest.mock import AsyncMock

import pytest

from discord_bot.cogs.music_helpers.common import MediaRequestLifecycleStage
from discord_bot.interfaces.broker_protocols import CheckoutResult
from discord_bot.types.download import LifecycleEvent, LifecycleStatusUpdate
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
async def test_update_request_status_unknown_uuid_warns(fake_context):  # pylint: disable=redefined-outer-name,unused-argument
    '''An update for an unregistered uuid logs and returns without raising.'''
    await AsyncioBroker().update_request_status(
        'nope', LifecycleStatusUpdate(event=LifecycleEvent.QUEUED),
    )


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
