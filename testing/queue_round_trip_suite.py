'''
One real client talking to one real server, for the queue-worker seam: what neither end can show alone.

``queue_client_suite`` shows the client sends what the contract accepts and reads every reply it declares;
``queue_server_suite`` shows the server accepts that and answers those replies. What is left is that the two
*mean the same thing*: that the status a refusal rides on is the one the client decodes, that a fault the server
turns into a 500 is one the client retries, and that a trace crosses the wire in one piece. So this keeps a round
trip per route, the refusals, and the span behaviour that only exists when both halves are in the process.

A pod's test module subclasses ``QueueRoundTripSuite`` and names its client and server classes.
'''
from unittest.mock import AsyncMock

import pytest
from aiohttp import ClientResponseError
from aiohttp.test_utils import TestClient, TestServer
from opentelemetry.trace import StatusCode

from discord_core.types.queue import PutsBlocked, SUBMIT_REJECTION_STATUS

from testing.queue_status import status_snapshot
from testing.queue_worker_support import FakeQueueWorker, QueueSuiteBase, playlist_add_request
from testing.tracing_support import record_spans


class QueueRoundTripSuite(QueueSuiteBase):
    '''Cases that need a real client and a real server together.'''

    def make_pod(self, worker=None):
        '''A (worker, TestClient) pair: a real server over a fake worker.'''
        worker = worker or FakeQueueWorker()
        return worker, TestClient(TestServer(self.server_cls(worker).build_app()))

    def make_client(self, tc):
        '''A real client pointed at the TestClient's server.'''
        return self.client_cls(str(tc.make_url('')), session=tc.session)

    @pytest.mark.asyncio
    async def test_a_submit_reaches_the_worker_with_its_priority(self):
        '''What the client sends is what the worker is handed.'''
        worker, tc = self.make_pod()
        async with tc:
            await self.make_client(tc).submit(7, self.request(7), priority=2)
        worker.submit.assert_awaited_once()
        args, kwargs = worker.submit.await_args
        assert (args[0], args[1].search_result.raw_search_string, kwargs['priority']) == (
            7, self.request_search_string, 2)

    @pytest.mark.parametrize('expected', list(SUBMIT_REJECTION_STATUS))
    @pytest.mark.asyncio
    async def test_a_refusal_arrives_as_the_exception_the_worker_raised(self, expected):
        '''The worker's own refusal reaches the cog as the same exception, once: the status the server picks is the
        one the client decodes, and a refusal is not retried. Before this, a blocked guild was a 500 the bot retried
        three times and `except PutsBlocked` never matched (trace fc7c6b0b, 2026-08-17).'''
        worker, tc = self.make_pod()
        worker.submit = AsyncMock(side_effect=expected('refused'))
        async with tc:
            with pytest.raises(expected):
                await self.make_client(tc).submit(7, self.request(7))
        assert worker.submit.await_count == 1

    @pytest.mark.asyncio
    async def test_a_genuine_fault_still_raises_and_is_retried(self):
        '''Only the two refusals are translated. Anything else is a real server fault: a 500, retried, surfacing raw.'''
        worker, tc = self.make_pod()
        worker.submit = AsyncMock(side_effect=RuntimeError('boom'))
        async with tc:
            with pytest.raises(ClientResponseError):
                await self.make_client(tc).submit(7, self.request(7))
        assert worker.submit.await_count > 1

    @pytest.mark.asyncio
    async def test_a_refusal_leaves_both_submit_spans_ok(self, mocker):
        '''The client span and the server span both end OK, or every blocked-guild submit reads as an error.'''
        exporter = record_spans(mocker)
        worker, tc = self.make_pod()
        worker.submit = AsyncMock(side_effect=PutsBlocked('blocked'))
        async with tc:
            with pytest.raises(PutsBlocked):
                await self.make_client(tc).submit(7, self.request(7))
        prefix = self.client_cls.SPAN_PREFIX
        spans = [s for s in exporter.get_finished_spans() if s.name == f'{prefix}.submit']
        assert len(spans) == 2, [s.name for s in exporter.get_finished_spans()]
        for span in spans:
            assert span.status.status_code is not StatusCode.ERROR
            assert span.attributes['queue.submit_rejection'] == 'PutsBlocked'

    @pytest.mark.asyncio
    async def test_real_work_is_traced_on_both_halves(self, mocker):
        '''A submit loses work if it fails, so it emits the client retry span and the server handler span.'''
        exporter = record_spans(mocker)
        _, tc = self.make_pod()
        async with tc:
            await self.make_client(tc).submit(7, self.request(7))
        names = [s.name for s in exporter.get_finished_spans()]
        assert 'utils.retry_broker_command' in names
        assert f'{self.client_cls.SPAN_PREFIX}.submit' in names

    @pytest.mark.asyncio
    async def test_a_clear_round_trip_preserves_what_the_predicate_keeps(self):
        '''The predicate becomes a flag on the wire and a predicate again in the pod: the playlist add is kept (its
        bundle reported back) and only the download is dropped, deserialised back to a request.'''
        _, tc = self.make_pod(FakeQueueWorker(clear_items=[
            self.request(7), playlist_add_request(7, bundle_uuid='keep-bundle')]))
        async with tc:
            result = await self.make_client(tc).clear_guild_queue(7, preserve_predicate=lambda r: not r.download_file)
        assert [item.download_file for item in result.dropped] == [True]
        assert result.preserved_bundle_uuids == {'keep-bundle'}

    @pytest.mark.asyncio
    async def test_a_block_round_trip_blocks_the_guild(self):
        '''block_guild reaches the worker and reports True.'''
        worker, tc = self.make_pod()
        async with tc:
            assert await self.make_client(tc).block_guild(7) is True
        worker.block_guild.assert_awaited_once_with(7)

    @pytest.mark.asyncio
    async def test_a_status_poll_fills_the_cache_and_is_silent_on_both_halves(self, mocker):
        '''The poll refreshes what the cog reads, and neither the client nor the server starts a span for it: at one
        poll a second per worker those would be most of the span volume.'''
        exporter = record_spans(mocker)
        _, tc = self.make_pod(FakeQueueWorker(status=status_snapshot(
            summary='2 failures in queue', backoff=11, queue_sizes={'7': 5})))
        async with tc:
            client = self.make_client(tc)
            await client._poll_status_loop_once()  # pylint: disable=protected-access
        assert not exporter.get_finished_spans()
        assert (client.failure_summary, client.backoff_seconds_remaining, await client.queue_size(7)) == (
            '2 failures in queue', 11, 5)
