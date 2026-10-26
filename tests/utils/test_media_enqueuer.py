from logging import getLogger
from tempfile import TemporaryDirectory
from unittest.mock import AsyncMock, MagicMock

import pytest

from discord_core.types.download import LifecycleEvent
from discord_core.utils.media_enqueuer import MediaEnqueuer

from tests.helpers import fake_context, fake_media_download  #pylint:disable=unused-import


def make_enqueuer(enqueue_result='ok'):
    events = []
    broker = MagicMock()
    broker.register_download = AsyncMock()
    broker.enqueue_track = AsyncMock(return_value=enqueue_result)
    broker.discard = AsyncMock()
    enqueuer = MediaEnqueuer(broker, MagicMock(), MagicMock(), MagicMock(), MagicMock(), MagicMock(),
                             MagicMock(), getLogger('test'), 5, 64, 300, {})
    enqueuer.push_state = AsyncMock(side_effect=lambda _request, event, **_details: events.append(event))
    return enqueuer, broker, events


@pytest.mark.asyncio
async def test_enqueue_download_wakes_the_player_before_completing(fake_context):  #pylint:disable=redefined-outer-name
    enqueuer, broker, events = make_enqueuer()
    on_queued = MagicMock(side_effect=lambda: events.append('woken'))
    with TemporaryDirectory() as tmp_dir:
        with fake_media_download(tmp_dir, fake_context=fake_context) as download:
            assert await enqueuer.enqueue_download(download, on_queued) is True
    assert events == ['woken', LifecycleEvent.COMPLETED]
    broker.register_download.assert_awaited_once()
    broker.discard.assert_not_called()


@pytest.mark.asyncio
@pytest.mark.parametrize('answer,event', [('full', LifecycleEvent.FAILED), ('closed', LifecycleEvent.DISCARDED)])
async def test_enqueue_download_refused(fake_context, answer, event):  #pylint:disable=redefined-outer-name
    enqueuer, broker, events = make_enqueuer(enqueue_result=answer)
    on_queued = MagicMock()
    with TemporaryDirectory() as tmp_dir:
        with fake_media_download(tmp_dir, fake_context=fake_context) as download:
            assert await enqueuer.enqueue_download(download, on_queued) is False
    assert events == [event]
    broker.discard.assert_awaited_once_with(str(download.media_request.uuid))
    on_queued.assert_not_called()


@pytest.mark.asyncio
async def test_enqueue_download_of_a_duplicate_still_wakes_and_completes(fake_context):  #pylint:disable=redefined-outer-name
    '''Already queued means the track will play; nothing to undo.'''
    enqueuer, broker, events = make_enqueuer(enqueue_result='duplicate')
    with TemporaryDirectory() as tmp_dir:
        with fake_media_download(tmp_dir, fake_context=fake_context) as download:
            assert await enqueuer.enqueue_download(download, MagicMock()) is True
    assert events == [LifecycleEvent.COMPLETED]
    broker.discard.assert_not_called()
