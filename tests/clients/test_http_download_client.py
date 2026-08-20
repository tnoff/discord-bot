'''Tests for HttpDownloadClient — exercised against a real DownloadHttpServer.

Both halves (the cog-side HttpDownloadClient and the worker-side
DownloadHttpServer) are covered together by routing requests through aiohttp's
TestServer + TestClient; the RedisDownloadWorker is replaced by a small fake that
records calls + returns canned responses.  RedisDownloadWorker.status_snapshot
itself is covered against fakeredis in tests/workers/test_redis_download_worker.py.
'''
from unittest.mock import AsyncMock, patch

import pytest
from aiohttp import ClientResponseError
from aiohttp.test_utils import TestClient, TestServer
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter
from opentelemetry.trace import StatusCode

from discord_bot.clients.download_client import HttpDownloadClient
from discord_bot.servers.download_server import DownloadHttpServer
from discord_bot.cogs.music_helpers.common import SearchType
from discord_bot.types.media_request import MediaRequest
from discord_bot.types.playlist_add_request import PlaylistAddRequest
from discord_bot.types.queue import PutsBlocked, SUBMIT_REJECTION_STATUS
from discord_bot.types.search import SearchResult


def _media_request(guild_id: int = 1) -> MediaRequest:
    return MediaRequest(
        guild_id=guild_id, channel_id=2, requester_id=3, requester_name='req',
        search_result=SearchResult(
            search_type=SearchType.DIRECT,
            raw_search_string='https://example.com/v.mp4',
        ),
    )


def _playlist_add_request(guild_id: int = 1, bundle_uuid: str | None = None) -> PlaylistAddRequest:
    return PlaylistAddRequest(
        guild_id=guild_id, channel_id=2, requester_id=3, requester_name='req',
        playlist_id=99,
        bundle_uuid=bundle_uuid,
        search_result=SearchResult(
            search_type=SearchType.SEARCH, raw_search_string='song name',
        ),
    )


class _FakeWorker:
    '''Minimal RedisDownloadWorker stand-in covering the server's call sites.'''

    def __init__(self, *, status=None, clear_items=None):
        self.submit = AsyncMock()
        self.block_guild = AsyncMock(return_value=True)
        self.status_snapshot = AsyncMock(return_value=status or {
            'failure_summary': '0 failures in queue',
            'backoff_seconds_remaining': None,
            'queue_sizes': {},
        })
        self._clear_items = clear_items or []
        self.clear_guild_queue = AsyncMock(side_effect=self._clear)

    async def _clear(self, guild_id, preserve_predicate=None):
        '''Apply the server-supplied predicate exactly as the real worker would,
        so the server's flag→predicate translation is exercised end to end.'''
        del guild_id
        return [mr for mr in self._clear_items
                if not (preserve_predicate and preserve_predicate(mr))]


def _make_server(worker=None):
    worker = worker or _FakeWorker()
    return worker, DownloadHttpServer(worker)


# ---------------------------------------------------------------------------
# Lifecycle
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_start_spawns_poller_and_stop_cancels_it():
    '''start() creates the poller task; stop() cancels it cleanly.'''
    client = HttpDownloadClient('http://localhost:9999')
    await client.start()
    assert client._poller_task is not None  # pylint: disable=protected-access
    assert not client._poller_task.done()  # pylint: disable=protected-access
    await client.stop()
    assert client._poller_task.done()  # pylint: disable=protected-access


@pytest.mark.asyncio
async def test_start_is_idempotent():
    '''Calling start() twice doesn't spawn a second poller task.'''
    client = HttpDownloadClient('http://localhost:9999')
    await client.start()
    first_task = client._poller_task  # pylint: disable=protected-access
    await client.start()
    assert client._poller_task is first_task  # pylint: disable=protected-access
    await client.stop()


@pytest.mark.asyncio
async def test_stop_without_start_is_safe():
    '''stop() with no prior start() doesn't raise.'''
    client = HttpDownloadClient('http://localhost:9999')
    await client.stop()  # no-op


# ---------------------------------------------------------------------------
# submit / clear / block (awaited producer surface)
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_submit_posts_serialised_request():
    '''submit POSTs /downloads with the serialised MediaRequest + priority.'''
    worker, server = _make_server()
    async with TestClient(TestServer(server.build_app())) as tc:
        client = HttpDownloadClient(str(tc.make_url('')), session=tc.session)
        await client.submit(7, _media_request(guild_id=7), priority=2)
    worker.submit.assert_awaited_once()
    args, kwargs = worker.submit.await_args
    assert args[0] == 7
    assert args[1].guild_id == 7
    assert kwargs['priority'] == 2


@pytest.mark.asyncio
async def test_clear_forwards_preserve_flag_and_returns_dropped():
    '''With a predicate, clear preserves non-download items server-side and returns
    only the dropped (downloadable) requests, deserialised back to MediaRequests.'''
    worker = _FakeWorker(clear_items=[
        _media_request(7), _playlist_add_request(7, bundle_uuid='keep-bundle')])
    _, server = _make_server(worker)
    async with TestClient(TestServer(server.build_app())) as tc:
        client = HttpDownloadClient(str(tc.make_url('')), session=tc.session)
        result = await client.clear_guild_queue(7, preserve_predicate=lambda r: not r.download_file)
    # The playlist-add (download_file=False) is preserved; only the download drops.
    assert len(result.dropped) == 1
    assert result.dropped[0].download_file is True
    # The downloader reports the preserved item's bundle_uuid back over HTTP so the
    # cog can skip deleting that bundle in HA (the reconciliation for MR 5).
    assert result.preserved_bundle_uuids == {'keep-bundle'}
    _, kwargs = worker.clear_guild_queue.await_args
    assert kwargs['preserve_predicate'] is not None


@pytest.mark.asyncio
async def test_clear_without_predicate_drops_everything():
    '''No predicate → preserve_playlist_adds=False → the server preserves nothing.'''
    worker = _FakeWorker(clear_items=[_media_request(7), _playlist_add_request(7)])
    _, server = _make_server(worker)
    async with TestClient(TestServer(server.build_app())) as tc:
        client = HttpDownloadClient(str(tc.make_url('')), session=tc.session)
        result = await client.clear_guild_queue(7)
    assert len(result.dropped) == 2
    assert result.preserved_bundle_uuids == set()
    _, kwargs = worker.clear_guild_queue.await_args
    assert kwargs['preserve_predicate'] is None


@pytest.mark.asyncio
async def test_block_guild_posts_to_block_endpoint():
    '''block_guild POSTs /downloads/block and returns True.'''
    worker, server = _make_server()
    async with TestClient(TestServer(server.build_app())) as tc:
        client = HttpDownloadClient(str(tc.make_url('')), session=tc.session)
        result = await client.block_guild(7)
    assert result is True
    worker.block_guild.assert_awaited_once_with(7)


# ---------------------------------------------------------------------------
# Cached read surface (queue_size / failure_summary / backoff)
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_initial_cached_values_are_safe_defaults():
    '''Before the poller has run, the cached reads return safe defaults.'''
    client = HttpDownloadClient('http://localhost:9999')
    assert await client.queue_size(7) == 0
    assert client.failure_summary == '0 failures in queue'
    assert client.backoff_seconds_remaining is None


@pytest.mark.asyncio
async def test_poller_refreshes_cached_values_from_status_endpoint():
    '''After the poller fires once the cached reads reflect the server's response.'''
    worker = _FakeWorker(status={
        'failure_summary': '1 failure in queue',
        'backoff_seconds_remaining': 42,
        'queue_sizes': {'7': 3, '12': 1},
    })
    _, server = _make_server(worker)
    async with TestClient(TestServer(server.build_app())) as tc:
        client = HttpDownloadClient(str(tc.make_url('')), session=tc.session)
        # Drive one iteration directly; start() races the poll cadence.
        await client._poll_status_loop_once()  # pylint: disable=protected-access
    assert await client.queue_size(7) == 3
    assert await client.queue_size(12) == 1
    assert await client.queue_size(99) == 0  # absent → default
    assert client.failure_summary == '1 failure in queue'
    assert client.backoff_seconds_remaining == 42


@pytest.mark.asyncio
async def test_poller_swallows_status_errors_and_keeps_running(mocker):
    '''A transient HTTP error during status refresh is logged, not propagated.'''
    client = HttpDownloadClient('http://localhost:9999')
    mocker.patch.object(client, '_http', side_effect=RuntimeError('boom'))
    await client._poll_status_loop_once()  # pylint: disable=protected-access
    assert client.failure_summary == '0 failures in queue'


@pytest.mark.asyncio
async def test_poller_returns_early_on_empty_status(mocker):
    '''An empty/None status response is treated as a no-op refresh.'''
    client = HttpDownloadClient('http://localhost:9999')
    mocker.patch.object(client, '_http', return_value=None)
    await client._poll_status_loop_once()  # pylint: disable=protected-access
    assert client.failure_summary == '0 failures in queue'
    assert client.backoff_seconds_remaining is None
    assert await client.queue_size(7) == 0


@pytest.mark.asyncio
async def test_poll_status_loop_exits_when_stop_event_set(mocker):
    '''The outer loop exits once _poller_stop is set before entering.'''
    client = HttpDownloadClient('http://localhost:9999')
    mocker.patch.object(client, '_http', return_value=None)
    client._poller_stop.set()  # pylint: disable=protected-access
    await client._poll_status_loop()  # pylint: disable=protected-access


@pytest.mark.asyncio
async def test_poll_status_loop_runs_one_iteration_then_exits(mocker):
    '''The loop runs at least one iteration body before shutdown fires.'''
    client = HttpDownloadClient('http://localhost:9999')
    mocker.patch.object(client, 'POLL_INTERVAL_SECONDS', 0.01)
    iterations = {'n': 0}
    real_iter = client._poll_status_loop_once  # pylint: disable=protected-access

    async def counting_iter():
        iterations['n'] += 1
        await real_iter()
        client._poller_stop.set()  # pylint: disable=protected-access

    mocker.patch.object(client, '_http', return_value=None)
    mocker.patch.object(client, '_poll_status_loop_once', side_effect=counting_iter)
    await client._poll_status_loop()  # pylint: disable=protected-access
    assert iterations['n'] >= 1


@pytest.mark.asyncio
async def test_poll_status_loop_continues_through_wait_timeout(mocker):
    '''When _poller_stop stays clear, wait_for raises TimeoutError and the loop
    `continue`s.  Stop on the second iteration so the loop exits deterministically.'''
    client = HttpDownloadClient('http://localhost:9999')
    mocker.patch.object(client, 'POLL_INTERVAL_SECONDS', 0.001)
    iterations = {'n': 0}

    async def stop_on_second():
        iterations['n'] += 1
        if iterations['n'] >= 2:
            client._poller_stop.set()  # pylint: disable=protected-access

    mocker.patch.object(client, '_poll_status_loop_once', side_effect=stop_on_second)
    await client._poll_status_loop()  # pylint: disable=protected-access
    assert iterations['n'] == 2


# ---------------------------------------------------------------------------
# Status endpoint shape (DownloadHttpServer)
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_status_endpoint_returns_worker_snapshot():
    '''GET /downloads/status returns the worker's status_snapshot verbatim.'''
    worker = _FakeWorker(status={
        'failure_summary': '2 failures in queue',
        'backoff_seconds_remaining': 10,
        'queue_sizes': {'7': 3, '12': 1},
    })
    server = DownloadHttpServer(worker)
    async with TestClient(TestServer(server.build_app())) as tc:
        resp = await tc.get('/downloads/status')
        body = await resp.json()
    assert resp.status == 200
    assert body['failure_summary'] == '2 failures in queue'
    assert body['backoff_seconds_remaining'] == 10
    assert body['queue_sizes'] == {'7': 3, '12': 1}


# ---------------------------------------------------------------------------
# Heartbeat gauge
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_heartbeat_observation_reflects_serving_state():
    '''
    heartbeat_observations reports 0 before serving, tagged job=downloader_server.

    `downloader_server`, NOT `downloader`: this gauge reports is_serving — the TCP
    site is up — and the bare name read like the worker loop's heartbeat, so a pod
    whose consumer loop was wedged still showed 1 until the liveness probe
    restarted it. The loop's own series is `downloader_worker`, published by
    cli/_lib/worker_pod.py, and the two must not collide on this label. Mirrors the
    search server's existing `youtube_music_search_server` convention.
    '''
    _, server = _make_server()
    assert DownloadHttpServer.HEARTBEAT_JOB == 'downloader_server'
    (observation,) = server.heartbeat_observations()
    assert observation.value == 0
    assert observation.attributes == {'background_job': 'downloader_server'}


# ---------------------------------------------------------------------------
# Server-side input validation
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_submit_endpoint_rejects_malformed_body():
    '''Missing guild_id → 422 from /downloads.'''
    _, server = _make_server()
    async with TestClient(TestServer(server.build_app())) as tc:
        resp = await tc.post('/downloads', json={'priority': 1})
    assert resp.status == 422


@pytest.mark.asyncio
async def test_clear_endpoint_rejects_malformed_body():
    '''Missing guild_id → 422 from /downloads/clear.'''
    _, server = _make_server()
    async with TestClient(TestServer(server.build_app())) as tc:
        resp = await tc.post('/downloads/clear', json={})
    assert resp.status == 422


@pytest.mark.asyncio
async def test_block_endpoint_rejects_malformed_body():
    '''Missing guild_id → 422 from /downloads/block.'''
    _, server = _make_server()
    async with TestClient(TestServer(server.build_app())) as tc:
        resp = await tc.post('/downloads/block', json={})
    assert resp.status == 422


# ---------------------------------------------------------------------------
# Submit rejections across the HTTP seam
#
# PutsBlocked/QueueFull are normal control flow the music cog handles at every
# submit site.  Before this was wired they escaped _handle_submit as an
# unhandled exception -> aiohttp 500 -> the bot retried three times and the cog
# saw a ClientResponseError its `except PutsBlocked` could not match, failing
# the whole command (trace fc7c6b0b, 2026-08-17).
# ---------------------------------------------------------------------------

def _recording_tracer():
    '''Throwaway SDK tracer + exporter; the suite has no global provider, so
    span status is dropped without one.'''
    exporter = InMemorySpanExporter()
    provider = TracerProvider()
    provider.add_span_processor(SimpleSpanProcessor(exporter))
    return provider.get_tracer('test'), exporter


@pytest.mark.parametrize('expected', list(SUBMIT_REJECTION_STATUS))
@pytest.mark.asyncio
async def test_submit_rejection_arrives_as_the_in_process_exception(expected):
    '''A worker-side refusal reaches the caller as the same exception type it
    would raise in-process, not as a ClientResponseError.

    Parametrised over SUBMIT_REJECTION_STATUS rather than a hand-written list, so
    this doubles as the drift guard for the server's `except (PutsBlocked,
    QueueFull)`: adding an entry to the mapping without wiring the catch fails
    here instead of silently falling through to a 500.'''
    worker, server = _make_server()
    worker.submit = AsyncMock(side_effect=expected('refused'))
    async with TestClient(TestServer(server.build_app())) as tc:
        client = HttpDownloadClient(str(tc.make_url('')), session=tc.session)
        with pytest.raises(expected):
            await client.submit(7, _media_request(guild_id=7))


@pytest.mark.asyncio
async def test_submit_rejection_is_not_retried():
    '''A refusal is deterministic, so it must propagate on the first attempt.
    Encoded as 5xx it burned three extra retries with exponential backoff before
    returning the identical answer.'''
    worker, server = _make_server()
    worker.submit = AsyncMock(side_effect=PutsBlocked('blocked'))
    async with TestClient(TestServer(server.build_app())) as tc:
        client = HttpDownloadClient(str(tc.make_url('')), session=tc.session)
        with pytest.raises(PutsBlocked):
            await client.submit(7, _media_request(guild_id=7))
    assert worker.submit.await_count == 1


@pytest.mark.asyncio
async def test_submit_genuine_fault_still_raises_and_still_retries():
    '''Only the two queue-contract exceptions are translated. Anything else is
    a real server fault: still a 500, still retried, still surfaces raw.'''
    worker, server = _make_server()
    worker.submit = AsyncMock(side_effect=RuntimeError('boom'))
    async with TestClient(TestServer(server.build_app())) as tc:
        client = HttpDownloadClient(str(tc.make_url('')), session=tc.session)
        with pytest.raises(ClientResponseError):
            await client.submit(7, _media_request(guild_id=7))
    assert worker.submit.await_count > 1


@pytest.mark.asyncio
async def test_submit_rejection_leaves_both_seam_spans_ok():
    '''A refusal is a decision, not a fault. Both the server and client submit
    spans must end OK, or every blocked-guild submit reads as an error on the
    seam error-rate panels and alerts.'''
    worker, server = _make_server()
    worker.submit = AsyncMock(side_effect=PutsBlocked('blocked'))
    tracer, exporter = _recording_tracer()
    with patch('discord_bot.utils.otel.TRACER', tracer):
        async with TestClient(TestServer(server.build_app())) as tc:
            client = HttpDownloadClient(str(tc.make_url('')), session=tc.session)
            with pytest.raises(PutsBlocked):
                await client.submit(7, _media_request(guild_id=7))
    submit_spans = [s for s in exporter.get_finished_spans() if s.name == 'downloader.submit']
    assert len(submit_spans) == 2, [s.name for s in exporter.get_finished_spans()]
    for span in submit_spans:
        assert span.status.status_code is not StatusCode.ERROR
        assert span.attributes['queue.submit_rejection'] == 'PutsBlocked'
