from logging import getLogger
from unittest.mock import AsyncMock, MagicMock

import pytest

from discord_core.types.download import DownloadErrorType, DownloadResult, DownloadStatus, LifecycleEvent

from discord_gateway.loop_logic.music_results import MusicResultsLoop
from tests.helpers import fake_context, fake_media_download, fake_source_dict  #pylint:disable=unused-import


def make_loop(enqueue_result='ok', live_player=True):
    events = []
    broker = MagicMock()
    broker.register_download = AsyncMock()
    broker.enqueue_track = AsyncMock(return_value=enqueue_result)
    broker.discard = AsyncMock()
    broker.cache_cleanup = AsyncMock()
    enqueue = MagicMock()
    enqueue.push_state = AsyncMock(side_effect=lambda _request, event, **_details: events.append(event))
    gateway = MagicMock()
    gateway.has_live_player = AsyncMock(return_value=live_player)
    gateway.add_cached = AsyncMock(return_value=True)
    store = MagicMock()
    store.delete_item = AsyncMock()
    loop = MusicResultsLoop(getLogger('test'), broker, MagicMock(), enqueue, gateway, store, {})
    return loop, broker, enqueue, gateway, store, events


@pytest.mark.asyncio
async def test_requeue_search_result_logs_when_the_broker_is_down():
    loop, broker, _, _, _, _ = make_loop()
    broker.register_search_result = AsyncMock(side_effect=RuntimeError('down'))
    await loop.requeue_search_result(MagicMock())


def download_result(fake_context, success=True, history_item=None):  #pylint:disable=redefined-outer-name
    request = fake_source_dict(fake_context)
    request.history_playlist_item_id = history_item
    status = DownloadStatus(success=success, error_type=None if success else DownloadErrorType.PRIVATE_VIDEO,
                            user_message=None if success else 'nope')
    return DownloadResult(status=status, media_request=request, ytdlp_data={'title': 't'}, file_name=None)


@pytest.mark.asyncio
async def test_terminal_failure_drops_the_history_item(fake_context):  #pylint:disable=redefined-outer-name
    loop, _, _, gateway, store, events = make_loop()
    await loop.process_download_result(download_result(fake_context, success=False, history_item=9))
    assert events == [LifecycleEvent.FAILED]
    store.delete_item.assert_awaited_once_with(9)
    gateway.add_cached.assert_not_called()


@pytest.mark.asyncio
async def test_result_for_a_gone_player_is_discarded(fake_context):  #pylint:disable=redefined-outer-name
    loop, broker, _, gateway, _, events = make_loop(live_player=False)
    await loop.process_download_result(download_result(fake_context))
    assert events == [LifecycleEvent.DISCARDED]
    gateway.add_cached.assert_not_called()
    broker.cache_cleanup.assert_not_called()


@pytest.mark.asyncio
async def test_result_for_a_live_player_is_queued(fake_context):  #pylint:disable=redefined-outer-name
    loop, broker, _, gateway, _, _ = make_loop()
    await loop.process_download_result(download_result(fake_context))
    gateway.add_cached.assert_awaited_once()
    broker.cache_cleanup.assert_awaited_once()
