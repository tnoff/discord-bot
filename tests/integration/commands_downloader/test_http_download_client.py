'''Tests for HttpDownloadClient — exercised against a real DownloadHttpServer.

Both halves (the cog-side HttpDownloadClient and the worker-side
DownloadHttpServer) are covered together by routing requests through aiohttp's
TestServer + TestClient; the RedisDownloadWorker is replaced by a small fake that
records calls + returns canned responses.  RedisDownloadWorker.status_snapshot
itself is covered against fakeredis in tests/workers/test_redis_download_worker.py.
'''
import asyncio
from unittest.mock import AsyncMock, patch

import pytest
from aiohttp import ClientResponseError
from aiohttp.test_utils import TestClient, TestServer
from opentelemetry.trace import StatusCode

from discord_core.types.queue import PutsBlocked, SUBMIT_REJECTION_STATUS

from discord_commands.clients.http_download_client import HttpDownloadClient
from discord_commands.clients.http_queue_worker_client import _exception_detail
from discord_downloader.servers.download_server import DownloadHttpServer

from tests.http_queue_client_suite import FakeQueueWorker, QueueClientSuite, QueueServerSuite, media_request
from tests.tracing_support import recording_tracer


def _make_server(worker=None):
    worker = worker or FakeQueueWorker()
    return worker, DownloadHttpServer(worker)


class TestHttpDownloadClient(QueueClientSuite, QueueServerSuite):
    '''The shared queue-client cases, against the download pod.

    The heartbeat job is `downloader_server`, NOT `downloader`: the gauge reports is_serving
    (the TCP site is up), and the bare name read like the worker loop's heartbeat, so a pod
    whose consumer loop was wedged still showed 1 until the liveness probe restarted it. The
    loop's own series is `downloader_worker`, published by cli/_lib/worker_pod.py, and the two
    must not collide on this label.
    '''
    client_cls = HttpDownloadClient
    server_cls = DownloadHttpServer
    prefix = '/downloads'
    heartbeat_job = 'downloader_server'


# ---------------------------------------------------------------------------
# Download-only behaviour
# ---------------------------------------------------------------------------

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


# ---------------------------------------------------------------------------
# Submit rejections across the HTTP seam
#
# PutsBlocked/QueueFull are normal control flow the music cog handles at every
# submit site.  Before this was wired they escaped _handle_submit as an
# unhandled exception -> aiohttp 500 -> the bot retried three times and the cog
# saw a ClientResponseError its `except PutsBlocked` could not match, failing
# the whole command (trace fc7c6b0b, 2026-08-17).
# ---------------------------------------------------------------------------

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
            await client.submit(7, media_request(guild_id=7))


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
            await client.submit(7, media_request(guild_id=7))
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
            await client.submit(7, media_request(guild_id=7))
    assert worker.submit.await_count > 1


@pytest.mark.asyncio
async def test_submit_rejection_leaves_both_seam_spans_ok():
    '''A refusal is a decision, not a fault. Both the server and client submit
    spans must end OK, or every blocked-guild submit reads as an error on the
    seam error-rate panels and alerts.'''
    worker, server = _make_server()
    worker.submit = AsyncMock(side_effect=PutsBlocked('blocked'))
    tracer, exporter = recording_tracer()
    with patch('discord_core.utils.otel.TRACER', tracer):
        async with TestClient(TestServer(server.build_app())) as tc:
            client = HttpDownloadClient(str(tc.make_url('')), session=tc.session)
            with pytest.raises(PutsBlocked):
                await client.submit(7, media_request(guild_id=7))
    submit_spans = [s for s in exporter.get_finished_spans() if s.name == 'downloader.submit']
    assert len(submit_spans) == 2, [s.name for s in exporter.get_finished_spans()]
    for span in submit_spans:
        assert span.status.status_code is not StatusCode.ERROR
        assert span.attributes['queue.submit_rejection'] == 'PutsBlocked'


# ---------------------------------------------------------------------------
# Status-poller span volume
#
# The poller runs every POLL_INTERVAL_SECONDS (1s) whether or not anything
# changed, so its spans are emitted at the poll rate rather than per unit of
# work. Two of these clients made utils.retry_broker_command ~99% of the bot's
# span volume, and while the worker pod was being rescheduled onto a new node
# every tick also landed an ERROR span for a failure _poll_status_loop_once
# already handles by keeping the cached values (2026-08-22 01:34-01:40 UTC).
# ---------------------------------------------------------------------------

def _span_names(exporter):
    '''Names of every span the exporter saw, both halves of the wire.'''
    return [s.name for s in exporter.get_finished_spans()]


@pytest.mark.asyncio
async def test_status_poll_emits_no_spans_on_either_half():
    '''A full status refresh -- client poll through server handler -- is silent.

    Both halves are unspanned deliberately: with the client no longer starting a
    span there is no caller context to continue, so a server span here would be a
    root span per second per worker describing an unremarkable cache refresh.'''
    _, server = _make_server()
    tracer, exporter = recording_tracer()
    async with TestClient(TestServer(server.build_app())) as tc:
        client = HttpDownloadClient(str(tc.make_url('')), session=tc.session)
        with patch('discord_core.utils.otel.TRACER', tracer):
            await client._poll_status_loop_once()  # pylint: disable=protected-access
    assert _span_names(exporter) == []
    assert client.failure_summary == '0 failures in queue'


@pytest.mark.asyncio
async def test_status_poll_failure_emits_no_spans():
    '''An unreachable worker pod -- the node-replacement case -- stays silent in
    traces and leaves the cached values in place.'''
    tracer, exporter = recording_tracer()
    client = HttpDownloadClient('http://127.0.0.1:1')
    with patch('discord_core.utils.otel.TRACER', tracer):
        with patch('discord_core.utils.retry.async_sleep', new_callable=AsyncMock):
            await client._poll_status_loop_once()  # pylint: disable=protected-access
    await client.close()
    assert _span_names(exporter) == []
    assert client.failure_summary == '0 failures in queue'


@pytest.mark.asyncio
async def test_real_work_is_still_traced_on_both_halves():
    '''Only the status poll went quiet.  A submit -- which loses work if it fails
    -- still emits the client retry span and the server handler span.'''
    _, server = _make_server()
    tracer, exporter = recording_tracer()
    async with TestClient(TestServer(server.build_app())) as tc:
        client = HttpDownloadClient(str(tc.make_url('')), session=tc.session)
        with patch('discord_core.utils.otel.TRACER', tracer):
            await client.submit(7, media_request(guild_id=7))
    names = _span_names(exporter)
    assert 'utils.retry_broker_command' in names
    assert 'downloader.submit' in names


@pytest.mark.asyncio
async def test_status_poll_still_updates_the_cache_without_spans():
    '''Dropping the spans must not change what the poll actually does.'''
    _, server = _make_server(FakeQueueWorker(status={
        'failure_summary': '2 failures in queue',
        'failure_count': 0,
        'backoff_seconds_remaining': 11,
        'queue_sizes': {'7': 5},
    }))
    async with TestClient(TestServer(server.build_app())) as tc:
        client = HttpDownloadClient(str(tc.make_url('')), session=tc.session)
        await client._poll_status_loop_once()  # pylint: disable=protected-access
        assert client.failure_summary == '2 failures in queue'
        assert client.backoff_seconds_remaining == 11
        assert await client.queue_size(7) == 5


# ---------------------------------------------------------------------------
# Message-less exceptions in the poller warning
#
# The poller swallows status-refresh errors and keeps the cached values, so the
# warning it logs is the only signal that a worker pod stopped answering. On
# 2026-09-03 20:07 UTC the first line of a downloader roll read
# "downloader status poller error: " with nothing after it: the request had hit
# STATUS_REQUEST_TIMEOUT_SECONDS, and str(asyncio.TimeoutError()) is ''.
#
# caplog is not usable here -- the package sets propagate=False, so records
# never reach pytest's handler and the assertions pass vacuously in isolation
# then fail in the full suite. Patch the module logger and assert the args.
# ---------------------------------------------------------------------------


def test_message_less_exception_logs_its_type():
    '''str() on a timeout is empty; the type name is the only detail it has.'''
    assert _exception_detail(asyncio.TimeoutError()) == 'TimeoutError'
    assert _exception_detail(ConnectionResetError()) == 'ConnectionResetError'


def test_a_described_exception_is_passed_through():
    '''The informative case must stay byte-identical -- no type-name prefix.'''
    detail = _exception_detail(RuntimeError('Cannot connect to host x:8083'))
    assert detail == 'Cannot connect to host x:8083'


@pytest.mark.asyncio
async def test_poller_timeout_warns_with_detail(mocker):
    '''A timed-out status refresh names TimeoutError instead of trailing off.'''
    client = HttpDownloadClient('http://localhost:9999')
    mocker.patch.object(client, '_http', side_effect=asyncio.TimeoutError())
    warn = mocker.patch('discord_commands.clients.http_queue_worker_client.logger.warning')
    await client._poll_status_loop_once()  # pylint: disable=protected-access
    warn.assert_called_once_with('%s status poller error: %s', 'downloader', 'TimeoutError')
    # Still swallowed: the cached values survive the outage.
    assert client.failure_summary == '0 failures in queue'


@pytest.mark.asyncio
async def test_poller_keeps_a_useful_message(mocker):
    '''An aiohttp-style connector message is passed through unchanged.'''
    client = HttpDownloadClient('http://localhost:9999')
    mocker.patch.object(client, '_http', side_effect=RuntimeError('Cannot connect to host'))
    warn = mocker.patch('discord_commands.clients.http_queue_worker_client.logger.warning')
    await client._poll_status_loop_once()  # pylint: disable=protected-access
    warn.assert_called_once_with('%s status poller error: %s', 'downloader',
                                 'Cannot connect to host')


# ---------------------------------------------------------------------------
# Status-poller tracing toggle (monitoring.tracing.trace_queue_worker_status_poll)
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_status_poller_is_untraced_by_default():
    '''The poller starts no span unless asked to.

    This is the behaviour that shipped: two clients on a 1Hz tick made this span
    ~99% of the bot's span volume, and every tick during a worker-pod reschedule
    also stamped an ERROR the method already handles by keeping its cached values.
    '''
    worker = FakeQueueWorker(status={'failure_summary': 'ok', 'failure_count': 0, 'queue_sizes': {}})
    _, server = _make_server(worker)
    tracer, exporter = recording_tracer()
    async with TestClient(TestServer(server.build_app())) as tc:
        client = HttpDownloadClient(str(tc.make_url('')), session=tc.session)
        with patch('discord_core.utils.otel.TRACER', tracer):
            await client._poll_status_loop_once()  # pylint: disable=protected-access
    assert not exporter.get_finished_spans()


@pytest.mark.asyncio
async def test_status_poller_is_traced_when_the_toggle_is_on():
    '''trace_status_poll=True puts the span back, without an image build.

    The half that proves the toggle is wired rather than merely accepted. It is
    also the operationally interesting position: while the downloader pod is
    misbehaving, this span is the per-iteration record of the bot's view of it.

    Asserted on exported spans rather than on the traced= kwarg, so the test
    fails if the flag is stored and then passed to the wrong place.
    '''
    worker = FakeQueueWorker(status={'failure_summary': 'ok', 'failure_count': 0, 'queue_sizes': {}})
    _, server = _make_server(worker)
    tracer, exporter = recording_tracer()
    async with TestClient(TestServer(server.build_app())) as tc:
        client = HttpDownloadClient(str(tc.make_url('')), session=tc.session,
                                    trace_status_poll=True)
        with patch('discord_core.utils.otel.TRACER', tracer):
            await client._poll_status_loop_once()  # pylint: disable=protected-access
    assert [s.name for s in exporter.get_finished_spans()] == ['utils.retry_broker_command']
