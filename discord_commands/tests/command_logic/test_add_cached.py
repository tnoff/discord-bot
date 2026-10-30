"""A downloaded track handed to the guild's player goes through the broker's real queue code."""
from tempfile import TemporaryDirectory

import pytest

from tests.helpers import fake_media_download
from tests.helpers import fake_engine, fake_context, fake_stores  # pylint: disable=unused-import
from tests.results_pod import ResultsPod, music_settings


async def pod_with_player(fake_context, fake_stores, *overrides, **kwargs):  # pylint: disable=redefined-outer-name
    pod = ResultsPod(fake_context, fake_stores, music_settings(*overrides), **kwargs)
    await pod.open_player(fake_context['guild'].id, fake_context['channel'].id)
    return pod


async def queued_urls(pod, fake_context):  # pylint: disable=redefined-outer-name
    return [item.webpage_url for item in (await pod.broker_client.get_guild_queue(fake_context['guild'].id)).items]


@pytest.mark.asyncio
async def test_add_cached_caches_video(fake_engine, mocker, fake_context, fake_stores):  # pylint: disable=redefined-outer-name
    """Queueing a track with S3 caching enabled records it in the video cache"""
    mocker.patch('tests.fakes.asyncio_broker.get_file', return_value=True)
    pod = await pod_with_player(
        fake_context, fake_stores,
        {'download': {'cache': {'enable_cache_files': True}, 'storage': {'bucket_name': 'test-bucket'}}},
        db_engine=fake_engine)
    with TemporaryDirectory() as tmp_dir:
        with fake_media_download(tmp_dir, fake_context=fake_context, is_direct_search=True) as media_download:
            assert await pod.player_gateway.add_cached(fake_context['guild'].id, media_download) is True
            assert (await pod.broker_client.get_guild_queue(fake_context['guild'].id)).items
            assert await pod.broker_client.local_broker.video_cache.get_webpage_url_item(media_download.media_request)


@pytest.mark.asyncio
async def test_add_cached_queues_for_the_players_guild(fake_context, fake_stores):  # pylint: disable=redefined-outer-name
    """A downloaded track goes into the broker's queue for its guild, ready for the player to claim."""
    pod = await pod_with_player(fake_context, fake_stores)
    assert await queued_urls(pod, fake_context) == []
    with TemporaryDirectory() as tmp_dir:
        with fake_media_download(tmp_dir, fake_context=fake_context) as media_download:
            assert await pod.player_gateway.add_cached(fake_context['guild'].id, media_download) is True
            assert await queued_urls(pod, fake_context) == [media_download.webpage_url]


@pytest.mark.asyncio
async def test_add_cached_refuses_a_closed_queue(fake_context, fake_stores):  # pylint: disable=redefined-outer-name
    """A guild closed while a download was in flight (the bot is going away) does not take new tracks"""
    pod = await pod_with_player(fake_context, fake_stores)
    await pod.broker_client.close_guild(fake_context['guild'].id)
    with TemporaryDirectory() as tmp_dir:
        with fake_media_download(tmp_dir, fake_context=fake_context) as media_download:
            assert await pod.player_gateway.add_cached(fake_context['guild'].id, media_download) is False
            assert await queued_urls(pod, fake_context) == []


@pytest.mark.asyncio
async def test_add_cached_tolerates_a_track_already_queued(fake_context, fake_stores):  # pylint: disable=redefined-outer-name
    """Queued twice (a retried result, say) plays once, and the second add still counts as added"""
    pod = await pod_with_player(fake_context, fake_stores)
    with TemporaryDirectory() as tmp_dir:
        with fake_media_download(tmp_dir, fake_context=fake_context) as media_download:
            assert await pod.player_gateway.add_cached(fake_context['guild'].id, media_download) is True
            assert await pod.player_gateway.add_cached(fake_context['guild'].id, media_download) is True
            assert len(await queued_urls(pod, fake_context)) == 1


@pytest.mark.asyncio
async def test_add_cached_keeps_the_order_the_tracks_arrived_in(fake_context, fake_stores):  # pylint: disable=redefined-outer-name
    """The queue the broker holds is the one read back, in order"""
    pod = await pod_with_player(fake_context, fake_stores)
    with TemporaryDirectory() as tmp_dir:
        with fake_media_download(tmp_dir, fake_context=fake_context) as first:
            with fake_media_download(tmp_dir, fake_context=fake_context) as second:
                await pod.player_gateway.add_cached(fake_context['guild'].id, first)
                await pod.player_gateway.add_cached(fake_context['guild'].id, second)
                assert await queued_urls(pod, fake_context) == [first.webpage_url, second.webpage_url]


@pytest.mark.asyncio
async def test_add_cached_queue_full(fake_context, fake_stores):  # pylint: disable=redefined-outer-name
    """add_cached returns False when the play queue is full, and leaves no registry entry behind."""
    pod = await pod_with_player(fake_context, fake_stores, {'player': {'queue_max_size': 1}})
    with TemporaryDirectory() as tmp_dir:
        with fake_media_download(tmp_dir, fake_context=fake_context) as queued:
            with fake_media_download(tmp_dir, fake_context=fake_context) as refused:
                assert await pod.player_gateway.add_cached(fake_context['guild'].id, queued) is True
                assert await pod.player_gateway.add_cached(fake_context['guild'].id, refused) is False
                assert await queued_urls(pod, fake_context) == [queued.webpage_url]
                assert await pod.broker_client.local_broker.get_entry(str(refused.media_request.uuid)) is None


@pytest.mark.asyncio
async def test_add_cached_queue_full_with_bundle(fake_context, fake_stores):  # pylint: disable=redefined-outer-name
    """A full play queue sets failure_reason on the request."""
    pod = await pod_with_player(fake_context, fake_stores, {'player': {'queue_max_size': 1}})
    with TemporaryDirectory() as tmp_dir:
        with fake_media_download(tmp_dir, fake_context=fake_context) as already_queued:
            assert await pod.player_gateway.add_cached(fake_context['guild'].id, already_queued) is True
        with fake_media_download(tmp_dir, fake_context=fake_context) as media_download:
            bundle_uuid = await pod.broker_client.create_bundle(
                fake_context['guild'].id, fake_context['channel'].id, input_string='test')
            media_download.media_request.bundle_uuid = bundle_uuid
            await pod.broker_client.register_request(media_download.media_request)
            assert await pod.player_gateway.add_cached(fake_context['guild'].id, media_download) is False
            assert media_download.media_request.failure_reason is not None
