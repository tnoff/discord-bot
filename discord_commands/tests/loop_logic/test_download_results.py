from tempfile import TemporaryDirectory
from unittest.mock import AsyncMock

import pytest
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter
from opentelemetry.trace.status import StatusCode

from discord_core.cogs.music_helpers.common import MediaRequestLifecycleStage, SearchType
from discord_core.exceptions import ExitEarlyException
from discord_core.types.playlist_add_request import PlaylistAddRequest
from discord_core.types.search import SearchResult
from discord_core.utils.media_enqueuer import MediaEnqueuer

from tests.fakes.pipeline_fakes import (yield_download_worker_bot_flagged,
                                        yield_download_worker_download_error,
                                        yield_download_worker_download_exception,
                                        yield_download_worker_retry_limit_exceeded,
                                        yield_download_worker_success_no_data,
                                        yield_download_worker_video_too_long,
                                        yield_fake_download_worker)
from tests.helpers import fake_media_download, fake_source_dict, random_string
from tests.helpers import fake_engine, fake_context, fake_stores  #pylint:disable=unused-import
from tests.results_pod import ResultsPod, music_settings

CACHE_MUSIC = music_settings({
    'download': {
        'cache': {
            'enable_cache_files': True,
        },
        'storage': {
            'bucket_name': 'test-bucket',
        }
    }
})


async def download_one(pod, guild_id, media_request):
    '''Submit one request, let the worker finish it, and run one pass of the download-result loop.'''
    await pod.download_client.submit(guild_id, media_request)
    await pod.download_client.run(pod.stop_event)
    await pod.loops.process_download_results()


async def queued_items(pod, guild_id):
    '''What the broker holds in the guild's play queue.'''
    return (await pod.broker_client.get_guild_queue(guild_id)).items


@pytest.mark.asyncio()
async def test_download_result_reaches_the_players_queue(fake_context, fake_stores):  #pylint:disable=redefined-outer-name
    with TemporaryDirectory() as tmp_dir:
        with fake_media_download(tmp_dir, fake_context=fake_context) as sd:
            pod = ResultsPod(fake_context, fake_stores, download_worker_cls=yield_fake_download_worker(sd))
            guild_id = fake_context['guild'].id
            await pod.open_player(guild_id, fake_context['channel'].id)
            await download_one(pod, guild_id, sd.media_request)
            assert await queued_items(pod, guild_id)
            assert pod.woken == [guild_id]


@pytest.mark.asyncio()
async def test_download_result_hits_cache(fake_context, fake_stores, mocker):  #pylint:disable=redefined-outer-name
    with TemporaryDirectory() as tmp_dir:
        with fake_media_download(tmp_dir, fake_context=fake_context, is_direct_search=True) as sd:
            mocker.patch('tests.fakes.asyncio_broker.get_file', return_value=True)
            pod = ResultsPod(fake_context, fake_stores, music=CACHE_MUSIC,
                             download_worker_cls=yield_fake_download_worker(sd))
            guild_id = fake_context['guild'].id
            await pod.open_player(guild_id, fake_context['channel'].id)
            await download_one(pod, guild_id, sd.media_request)
            assert await queued_items(pod, guild_id)


@pytest.mark.asyncio()
@pytest.mark.parametrize('worker', [
    yield_download_worker_bot_flagged(),
    yield_download_worker_download_exception(),
    yield_download_worker_download_error(),
    yield_fake_download_worker(None),
    yield_download_worker_retry_limit_exceeded(),
], ids=['bot-flagged', 'download-exception', 'download-error', 'no-result', 'retry-limit-exceeded'])
async def test_failed_download_queues_nothing(fake_context, fake_stores, worker):  #pylint:disable=redefined-outer-name
    pod = ResultsPod(fake_context, fake_stores, download_worker_cls=worker)
    guild_id = fake_context['guild'].id
    await pod.open_player(guild_id, fake_context['channel'].id)
    await download_one(pod, guild_id, fake_source_dict(fake_context))
    assert not await queued_items(pod, guild_id)
    assert not pod.woken


@pytest.mark.asyncio()
async def test_download_for_a_closed_player_is_discarded(fake_context, fake_stores):  #pylint:disable=redefined-outer-name
    '''The player went away while the track downloaded: nothing is queued.'''
    with TemporaryDirectory() as tmp_dir:
        with fake_media_download(tmp_dir, fake_context=fake_context) as sd:
            pod = ResultsPod(fake_context, fake_stores, download_worker_cls=yield_fake_download_worker(sd))
            guild_id = fake_context['guild'].id
            await pod.open_player(guild_id, fake_context['channel'].id)
            await pod.download_client.submit(guild_id, sd.media_request)
            await pod.broker_client.close_guild(guild_id)
            await pod.download_client.run(pod.stop_event)
            await pod.loops.process_download_results()
            assert not await queued_items(pod, guild_id)
            assert not pod.woken


@pytest.mark.asyncio()
async def test_download_for_a_guild_with_no_player_creates_none(fake_context, fake_stores):  #pylint:disable=redefined-outer-name
    with TemporaryDirectory() as tmp_dir:
        with fake_media_download(tmp_dir, fake_context=fake_context) as sd:
            pod = ResultsPod(fake_context, fake_stores, download_worker_cls=yield_fake_download_worker(sd))
            guild_id = fake_context['guild'].id
            await download_one(pod, guild_id, sd.media_request)
            assert not await pod.player_gateway.has_live_player(guild_id)
            assert not await queued_items(pod, guild_id)


@pytest.mark.asyncio()
async def test_download_loop_exits_early_on_shutdown(fake_context, fake_stores):  #pylint:disable=redefined-outer-name
    """process_download_results raises ExitEarlyException immediately when the stop event is set."""
    pod = ResultsPod(fake_context, fake_stores)
    pod.stop_event.set()
    with pytest.raises(ExitEarlyException):
        await pod.loops.process_download_results()


@pytest.mark.asyncio()
async def test_download_loop_returns_when_no_result_is_ready(fake_context, fake_stores):  #pylint:disable=redefined-outer-name
    """process_download_results returns without error when the result queue is empty."""
    pod = ResultsPod(fake_context, fake_stores)
    await pod.loops.process_download_results()


def playlist_add_request(fake_context, url, playlist_id=1):  #pylint:disable=redefined-outer-name
    '''A PlaylistAddRequest for a direct URL.'''
    return PlaylistAddRequest(
        guild_id=fake_context['guild'].id,
        channel_id=fake_context['channel'].id,
        requester_name=fake_context['author'].display_name,
        requester_id=fake_context['author'].id,
        search_result=SearchResult(search_type=SearchType.DIRECT, raw_search_string=url),
        playlist_id=playlist_id,
    )


@pytest.mark.asyncio()
async def test_playlist_add_request_without_ytdlp_data_fails(fake_context, fake_stores):  #pylint:disable=redefined-outer-name
    """A PlaylistAddRequest whose download carries no metadata is marked failed."""
    pod = ResultsPod(fake_context, fake_stores, download_worker_cls=yield_download_worker_success_no_data())
    req = playlist_add_request(fake_context, f'https://yt.example/{random_string()}')
    # In production the request is registered with the broker before download;
    # register here so the FAILED lifecycle push lands on it.
    await pod.broker_client.register_request(req)
    await download_one(pod, fake_context['guild'].id, req)
    assert req.lifecycle_stage == MediaRequestLifecycleStage.FAILED


@pytest.mark.asyncio()
async def test_playlist_add_request_cache_hit_adds_the_item(fake_context, fake_stores, mocker):  #pylint:disable=redefined-outer-name
    """A cache hit for a PlaylistAddRequest goes down the playlist add path."""
    with TemporaryDirectory() as tmp_dir:
        with fake_media_download(tmp_dir, fake_context=fake_context, is_direct_search=True) as sd:
            pod = ResultsPod(fake_context, fake_stores, music=CACHE_MUSIC,
                             download_worker_cls=yield_fake_download_worker(sd))
            req = playlist_add_request(fake_context, sd.webpage_url)
            await pod.download_client.submit(fake_context['guild'].id, req)
            add_item = mocker.patch.object(MediaEnqueuer, 'add_playlist_item')
            await pod.download_client.run(pod.stop_event)
            await pod.loops.process_download_results()
            add_item.assert_called_once()


@pytest.mark.asyncio()
async def test_download_result_runs_cache_cleanup(fake_context, fake_stores):  #pylint:disable=redefined-outer-name
    '''The loop triggers broker_client.cache_cleanup after queueing the source.'''
    with TemporaryDirectory() as tmp_dir:
        with fake_media_download(tmp_dir, fake_context=fake_context) as sd:
            pod = ResultsPod(fake_context, fake_stores, download_worker_cls=yield_fake_download_worker(sd))
            guild_id = fake_context['guild'].id
            await pod.open_player(guild_id, fake_context['channel'].id)
            pod.broker_client.cache_cleanup = AsyncMock(return_value=True)
            await download_one(pod, guild_id, sd.media_request)
            pod.broker_client.cache_cleanup.assert_awaited_once()


def consumer_span_exporter(mocker):
    """Swap in a recording tracer so span *status* survives to an assertion.

    The suite runs with no global tracer provider, so spans are non-recording
    and drop their status entirely (see tests/utils/test_otel.py).
    """
    exporter = InMemorySpanExporter()
    provider = TracerProvider()
    provider.add_span_processor(SimpleSpanProcessor(exporter))
    mocker.patch('discord_core.utils.otel.TRACER', provider.get_tracer('test'))
    return exporter


def process_results_span_status(exporter):
    """Status of the process_download_results consumer span."""
    spans = [s for s in exporter.get_finished_spans() if s.name.endswith('process_download_results')]
    assert len(spans) == 1
    return spans[0].status.status_code


@pytest.mark.asyncio()
async def test_video_too_long_marks_request_rejected(fake_context, fake_stores, mocker):  #pylint:disable=redefined-outer-name
    """A content-check terminal error (TOO_LONG) lands as a rejection, not a plain failure."""
    pod = ResultsPod(fake_context, fake_stores, download_worker_cls=yield_download_worker_video_too_long())
    guild_id = fake_context['guild'].id
    await pod.open_player(guild_id, fake_context['channel'].id)
    s = fake_source_dict(fake_context)
    await pod.broker_client.register_request(s)
    await pod.download_client.submit(guild_id, s)
    await pod.download_client.run(pod.stop_event)
    exporter = consumer_span_exporter(mocker)
    await pod.loops.process_download_results()
    assert s.lifecycle_stage == MediaRequestLifecycleStage.FAILED
    assert s.rejected is True
    # A declined video is not a fault: the consumer span the error-rate alert
    # watches must not go red for it.
    assert process_results_span_status(exporter) is StatusCode.OK


@pytest.mark.asyncio()
async def test_retry_limit_exceeded_keeps_consumer_span_error(fake_context, fake_stores, mocker):  #pylint:disable=redefined-outer-name
    """A genuine terminal failure still marks the consumer span ERROR."""
    pod = ResultsPod(fake_context, fake_stores, download_worker_cls=yield_download_worker_retry_limit_exceeded())
    guild_id = fake_context['guild'].id
    await pod.open_player(guild_id, fake_context['channel'].id)
    s = fake_source_dict(fake_context)
    await pod.broker_client.register_request(s)
    await pod.download_client.submit(guild_id, s)
    await pod.download_client.run(pod.stop_event)
    exporter = consumer_span_exporter(mocker)
    await pod.loops.process_download_results()
    assert s.rejected is False
    assert process_results_span_status(exporter) is StatusCode.ERROR
