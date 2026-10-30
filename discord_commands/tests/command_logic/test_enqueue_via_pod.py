from tempfile import TemporaryDirectory
from unittest.mock import AsyncMock, Mock

import pytest
from aiohttp import ClientConnectionError

from tests.fakes.pipeline_fakes import (fake_envelope, yield_search_client_check_source,
                                        yield_search_client_check_source_raises,
                                        yield_search_client_check_source_raises_transport)
from tests.helpers import fake_media_download, fake_source_dict
from tests.helpers import fake_engine, fake_context, fake_stores  #pylint:disable=unused-import
from tests.results_pod import ResultsPod, music_settings


@pytest.mark.asyncio()
async def test_play_with_downloads_blocked_tears_the_bundle_down(mocker, fake_context, fake_stores):  #pylint:disable=redefined-outer-name
    s = fake_source_dict(fake_context, is_direct_search=True)
    s1 = fake_source_dict(fake_context, is_direct_search=True)
    pod = ResultsPod(fake_context, fake_stores, dispatcher=Mock(),
                     source_search_client=yield_search_client_check_source([s, s1])())
    delete_bundle = mocker.spy(pod.broker_client, 'delete_bundle')
    # The guild's queue exists once something has been queued for it, so there is one to block.
    await pod.download_client.submit(fake_context['guild'].id, s)
    await pod.download_client.block_guild(fake_context['guild'].id)

    await pod.enqueuer.generate_media_requests_from_search(fake_envelope(fake_context), 'foo bar')

    # Puts are blocked, so the bundle the search made is torn down rather than left unresolved.
    delete_bundle.assert_awaited_once()


@pytest.mark.asyncio()
async def test_play_past_the_search_queue_limit_stops_queueing(fake_context, fake_stores):  #pylint:disable=redefined-outer-name
    """The search queue holds twice the player's queue limit; what does not fit is dropped, not raised."""
    sources = [fake_source_dict(fake_context) for _ in range(3)]
    pod = ResultsPod(fake_context, fake_stores, music=music_settings({'player': {'queue_max_size': 1}}),
                     dispatcher=Mock(), with_search=True,
                     source_search_client=yield_search_client_check_source(sources)())

    await pod.enqueuer.generate_media_requests_from_search(fake_envelope(fake_context), 'foo bar')

    assert await pod.youtube_music_search_client.queue_size(fake_context['guild'].id) == 2


@pytest.mark.asyncio()
async def test_a_search_that_fails_tells_the_channel_and_tears_the_bundle_down(fake_context, fake_stores):  #pylint:disable=redefined-outer-name
    pod = ResultsPod(fake_context, fake_stores, dispatcher=Mock(),
                     source_search_client=yield_search_client_check_source_raises()())

    await pod.enqueuer.generate_media_requests_from_search(fake_envelope(fake_context), 'foo bar')

    pod.dispatcher.send_message.assert_called()
    assert pod.dispatcher.send_message.call_args[0][2] == 'Error searching input "foo bar", message: woopsie'
    assert await pod.broker_client.list_bundles_for_guild(fake_context['guild'].id) == []


@pytest.mark.asyncio()
async def test_play_tears_down_the_bundle_when_the_search_pod_is_unreachable(fake_context, fake_stores):  #pylint:disable=redefined-outer-name
    '''
    A transport failure out of check_source must not strand the bundle.

    create_bundle runs before check_source, so the placeholder row is already rendered when the
    call fails. check_source is an HTTP round trip, so it can raise out of aiohttp as well as
    SearchException, and an uncaught one leaves a row that never resolves and never errors.
    '''
    pod = ResultsPod(fake_context, fake_stores, dispatcher=Mock(),
                     source_search_client=yield_search_client_check_source_raises_transport()())

    # Re-raised, not swallowed: the command span stays ERROR and the failure is still visible as
    # one rather than being downgraded to a user message.
    with pytest.raises(ClientConnectionError):
        await pod.enqueuer.generate_media_requests_from_search(fake_envelope(fake_context), 'foo bar')

    assert pod.dispatcher.send_message.call_args[0][2] == 'Error searching input "foo bar", the search backend is unavailable'
    assert await pod.broker_client.list_bundles_for_guild(fake_context['guild'].id) == []


@pytest.mark.asyncio()
async def test_play_of_a_cached_track_queues_it_for_the_guilds_player(mocker, fake_context, fake_stores):  #pylint:disable=redefined-outer-name
    with TemporaryDirectory() as tmp_dir:
        with fake_media_download(tmp_dir, fake_context=fake_context, is_direct_search=True) as sd:
            pod = ResultsPod(fake_context, fake_stores, dispatcher=Mock(),
                             source_search_client=yield_search_client_check_source([sd.media_request])())
            mocker.patch.object(pod.broker_client, 'check_cache', new=AsyncMock(return_value=sd))
            # `!play` has already joined voice by the time the pod searches.
            await pod.open_player(fake_context['guild'].id, fake_context['channel'].id)

            await pod.enqueuer.generate_media_requests_from_search(fake_envelope(fake_context), 'foo bar')

            assert (await pod.broker_client.get_guild_queue(fake_context['guild'].id)).items
            assert pod.woken == [fake_context['guild'].id]


@pytest.mark.asyncio()
async def test_a_track_is_registered_with_the_broker_before_it_is_queued(mocker, fake_context, fake_stores):  #pylint:disable=redefined-outer-name
    """Registration must precede enqueue so the player cannot claim a cache-hit track before the
    broker knows about it (which returns None and crashes the player on the raw S3 key)."""
    pod = ResultsPod(fake_context, fake_stores)
    order = []
    mocker.patch.object(pod.broker_client, 'register_download',
                        new=AsyncMock(side_effect=lambda *_a, **_k: order.append('register')))
    mocker.patch.object(pod.broker_client, 'enqueue_track',
                        new=AsyncMock(side_effect=lambda *_a, **_k: order.append('enqueue') or 'ok'))
    mocker.patch.object(pod.broker_client, 'update_request_status', new=AsyncMock())
    with TemporaryDirectory() as tmp_dir:
        with fake_media_download(tmp_dir, fake_context=fake_context) as media_download:
            assert await pod.enqueuer.enqueue_download(media_download, AsyncMock()) is True
    assert order == ['register', 'enqueue']
