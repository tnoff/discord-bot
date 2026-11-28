'''
The cases every HTTP queue-worker client must pass, against the seam's contract and no pod.

HttpDownloadClient and HttpYoutubeMusicSearchClient are the same client pointed at two pods. A client's test module
subclasses ``QueueClientSuite`` and ``QueuePollerSuite``, names its class and the contract table of its prefix, and adds what is specific to
its pod. The client is served by a ``ContractStub``, which fails the test if the client sends a body the request
model refuses, so what is checked here is the client's half of the seam: what it sends, and how it reads each reply
the contract declares. That the pod does the right thing with a body it accepts is the server suite
(``queue_server_suite``), and that the two agree is the round trip (``queue_round_trip_suite``).
'''
import asyncio

import pytest

from discord_core.types import queue_worker_responses as out
from discord_core.types.media_request import MediaRequest
from discord_core.types.playlist_add_request import PlaylistAddRequest
from discord_core.types.queue import SUBMIT_REJECTION_STATUS

from testing.contracts.stub import ContractStub, serving
from testing.queue_status import status_snapshot
from testing.queue_worker_support import UNREACHABLE_URL, QueueSuiteBase, playlist_add_request
from testing.tracing_support import record_spans


def _snapshot(**kwargs) -> out.StatusSnapshotResponse:
    return out.StatusSnapshotResponse.model_validate(status_snapshot(**kwargs))


class QueueClientBase(QueueSuiteBase):
    '''The contract stub and the client under test; not collected itself.'''
    # The contract table of this pod's prefix (testing.contracts.queue_worker.DOWNLOADS or YTMUSIC).
    contracts: dict

    @property
    def routes(self):
        '''The route group the client under test calls.'''
        return self.client_cls.ROUTES

    def make_stub(self) -> ContractStub:
        '''A stub serving this pod's contract.'''
        return ContractStub(self.contracts)

    def make_client(self, tc=None, **kwargs):
        '''A client pointed at the stub's server, or at an address nothing listens on.'''
        if tc is None:
            return self.client_cls(UNREACHABLE_URL, **kwargs)
        return self.client_cls(str(tc.make_url('')), session=tc.session, **kwargs)


class QueueClientSuite(QueueClientBase):
    '''The producer half every HTTP queue-worker client must pass: lifecycle, submit, clear and block.'''

    # -- lifecycle ---------------------------------------------------------------------------

    @pytest.mark.asyncio
    async def test_start_spawns_poller_and_stop_cancels_it(self):
        '''start() creates the poller task; stop() cancels it cleanly.'''
        client = self.make_client()
        await client.start()
        assert client._poller_task is not None  # pylint: disable=protected-access
        assert not client._poller_task.done()  # pylint: disable=protected-access
        await client.stop()
        assert client._poller_task.done()  # pylint: disable=protected-access

    @pytest.mark.asyncio
    async def test_start_is_idempotent(self):
        '''Calling start() twice doesn't spawn a second poller task.'''
        client = self.make_client()
        await client.start()
        first_task = client._poller_task  # pylint: disable=protected-access
        await client.start()
        assert client._poller_task is first_task  # pylint: disable=protected-access
        await client.stop()

    @pytest.mark.asyncio
    async def test_stop_without_start_is_safe(self):
        '''stop() with no prior start() doesn't raise.'''
        await self.make_client().stop()  # no-op

    # -- submit ------------------------------------------------------------------------------

    @pytest.mark.asyncio
    @pytest.mark.parametrize('request_factory', ['plain', 'playlist_add'])
    async def test_submit_sends_the_serialised_request_and_priority(self, request_factory):
        '''The body is exactly what the request model serialises: guild, priority and the request, whichever kind.'''
        sent = self.request(7) if request_factory == 'plain' else playlist_add_request(7)
        stub = self.make_stub()
        stub.script(self.routes.submit, out.SubmitAcceptedResponse(), status=202)
        async with serving(stub) as tc:
            await self.make_client(tc).submit(7, sent, priority=2)
        assert stub.calls_to(self.routes.submit) == [
            {'guild_id': 7, 'priority': 2, 'media_request': sent.model_dump(mode='json')}]

    @pytest.mark.asyncio
    async def test_submit_without_a_priority_sends_none(self):
        '''No priority goes out as null, which the server reads as none.'''
        stub = self.make_stub()
        stub.script(self.routes.submit, out.SubmitAcceptedResponse(), status=202)
        async with serving(stub) as tc:
            await self.make_client(tc).submit(7, self.request(7))
        assert stub.calls_to(self.routes.submit)[0]['priority'] is None

    @pytest.mark.parametrize('expected', list(SUBMIT_REJECTION_STATUS))
    @pytest.mark.asyncio
    async def test_a_refusal_arrives_as_the_in_process_exception(self, expected):
        '''A refusal the contract declares (a blocked guild, a full queue) reaches the caller as the exception the
        cog catches in process, not as a ClientResponseError, and is not retried: it is deterministic, so a retry
        only delays the identical answer.'''
        stub = self.make_stub()
        stub.script(self.routes.submit,
                    out.SubmitRejectedResponse(reason=expected.__name__, detail='refused'),
                    status=SUBMIT_REJECTION_STATUS[expected])
        async with serving(stub) as tc:
            with pytest.raises(expected):
                await self.make_client(tc).submit(7, self.request(7))
        assert len(stub.calls_to(self.routes.submit)) == 1

    @pytest.mark.asyncio
    async def test_a_refusal_leaves_the_submit_span_ok(self, mocker):
        '''A refusal is a decision, not a fault: the client span must end OK or every blocked-guild submit reads as
        an error on the seam error-rate panels.'''
        exporter = record_spans(mocker)
        stub = self.make_stub()
        stub.script(self.routes.submit, out.SubmitRejectedResponse(reason='PutsBlocked', detail='x'), status=409)
        async with serving(stub) as tc:
            with pytest.raises(Exception):
                await self.make_client(tc).submit(7, self.request(7))
        (span,) = [s for s in exporter.get_finished_spans() if s.name == f'{self.client_cls.SPAN_PREFIX}.submit']
        assert span.status.status_code.name != 'ERROR'
        assert span.attributes['queue.submit_rejection'] == 'PutsBlocked'

    # -- clear / block -----------------------------------------------------------------------

    @pytest.mark.asyncio
    async def test_clear_with_a_predicate_asks_the_pod_to_preserve_playlist_adds(self):
        '''A predicate cannot cross HTTP, so supplying one sends the flag; what the pod dropped comes back as the
        requests it was, and the bundles it kept as a set.'''
        stub = self.make_stub()
        stub.script(self.routes.clear, out.ClearGuildResponse(
            dropped=[self.request(7).model_dump(mode='json')], preserved_bundle_uuids=['keep-bundle']))
        async with serving(stub) as tc:
            result = await self.make_client(tc).clear_guild_queue(7, preserve_predicate=lambda r: not r.download_file)
        assert stub.calls_to(self.routes.clear) == [{'guild_id': 7, 'preserve_playlist_adds': True}]
        assert [type(item) for item in result.dropped] == [MediaRequest]
        assert result.preserved_bundle_uuids == {'keep-bundle'}

    @pytest.mark.asyncio
    async def test_clear_without_a_predicate_keeps_nothing(self):
        '''No predicate sends the flag as false, and each dropped request comes back as its own kind.'''
        stub = self.make_stub()
        stub.script(self.routes.clear, out.ClearGuildResponse(
            dropped=[self.request(7).model_dump(mode='json'), playlist_add_request(7).model_dump(mode='json')],
            preserved_bundle_uuids=[]))
        async with serving(stub) as tc:
            result = await self.make_client(tc).clear_guild_queue(7)
        assert stub.calls_to(self.routes.clear) == [{'guild_id': 7, 'preserve_playlist_adds': False}]
        assert [type(item) for item in result.dropped] == [MediaRequest, PlaylistAddRequest]
        assert result.preserved_bundle_uuids == set()

    @pytest.mark.asyncio
    async def test_block_guild_posts_the_guild_and_returns_true(self):
        '''block_guild sends the guild to the block route.'''
        stub = self.make_stub()
        stub.script(self.routes.block, out.BlockResponse())
        async with serving(stub) as tc:
            assert await self.make_client(tc).block_guild(7) is True
        assert stub.calls_to(self.routes.block) == [{'guild_id': 7}]


class QueuePollerSuite(QueueClientBase):
    '''The read half: the cached values the status poller refreshes, and how the poller fails.'''

    # -- cached read surface (queue_size / failure_summary / backoff) ------------------------

    @pytest.mark.asyncio
    async def test_initial_cached_values_are_safe_defaults(self):
        '''Before the poller has run, the cached reads return safe defaults.'''
        client = self.make_client()
        assert await client.queue_size(7) == 0
        assert client.failure_summary == '0 failures in queue'
        assert client.backoff_seconds_remaining is None

    @pytest.mark.asyncio
    async def test_poller_refreshes_cached_values_from_the_status_reply(self):
        '''After the poller fires once the cached reads reflect the pod's status reply.'''
        stub = self.make_stub()
        stub.script(self.routes.status, _snapshot(summary='1 failure in queue', backoff=42,
                                                  queue_sizes={'7': 3, '12': 1}))
        async with serving(stub) as tc:
            client = self.make_client(tc)
            # Drive one iteration directly; start() races the poll cadence.
            await client._poll_status_loop_once()  # pylint: disable=protected-access
        assert await client.queue_size(7) == 3
        assert await client.queue_size(12) == 1
        assert await client.queue_size(99) == 0  # absent -> default
        assert client.failure_summary == '1 failure in queue'
        assert client.backoff_seconds_remaining == 42

    @pytest.mark.asyncio
    async def test_the_status_poll_starts_no_span_by_default(self, mocker):
        '''The poller ticks every second whether or not anything changed, so a span per tick would be most of the
        bot's span volume; it is silent unless trace_status_poll turns it on.'''
        exporter = record_spans(mocker)
        stub = self.make_stub()
        stub.script(self.routes.status, _snapshot())
        async with serving(stub) as tc:
            await self.make_client(tc)._poll_status_loop_once()  # pylint: disable=protected-access
        assert not exporter.get_finished_spans()

    @pytest.mark.asyncio
    async def test_the_status_poll_is_traced_when_the_toggle_is_on(self, mocker):
        '''trace_status_poll=True puts the span back, asserted on the exported span and not on the flag, so it fails
        if the flag is stored and then passed to the wrong place.'''
        exporter = record_spans(mocker)
        stub = self.make_stub()
        stub.script(self.routes.status, _snapshot())
        async with serving(stub) as tc:
            await self.make_client(tc, trace_status_poll=True)._poll_status_loop_once()  # pylint: disable=protected-access
        assert [s.name for s in exporter.get_finished_spans()] == ['utils.retry_broker_command']

    @pytest.mark.asyncio
    async def test_poller_swallows_status_errors_and_keeps_running(self, mocker):
        '''A transient HTTP error during status refresh is logged, not propagated.'''
        client = self.make_client()
        mocker.patch.object(client, '_http', side_effect=RuntimeError('boom'))
        await client._poll_status_loop_once()  # pylint: disable=protected-access
        assert client.failure_summary == '0 failures in queue'

    @pytest.mark.asyncio
    async def test_a_timed_out_poll_warns_with_the_exception_type(self, mocker):
        '''str() of a timeout is empty, so the warning names the type rather than trailing off after the colon.'''
        client = self.make_client()
        mocker.patch.object(client, '_http', side_effect=asyncio.TimeoutError())
        warn = mocker.patch('discord_commands.clients.http_queue_worker_client.logger.warning')
        await client._poll_status_loop_once()  # pylint: disable=protected-access
        warn.assert_called_once_with('%s status poller error: %s', self.client_cls.SPAN_PREFIX, 'TimeoutError')

    @pytest.mark.asyncio
    async def test_a_described_failure_keeps_its_own_message(self, mocker):
        '''A connector message is passed through unchanged, with no type-name prefix.'''
        client = self.make_client()
        mocker.patch.object(client, '_http', side_effect=RuntimeError('Cannot connect to host'))
        warn = mocker.patch('discord_commands.clients.http_queue_worker_client.logger.warning')
        await client._poll_status_loop_once()  # pylint: disable=protected-access
        warn.assert_called_once_with('%s status poller error: %s', self.client_cls.SPAN_PREFIX,
                                     'Cannot connect to host')

    @pytest.mark.asyncio
    async def test_poller_returns_early_on_empty_status(self, mocker):
        '''An empty/None status response is treated as a no-op refresh.'''
        client = self.make_client()
        mocker.patch.object(client, '_http', return_value=None)
        await client._poll_status_loop_once()  # pylint: disable=protected-access
        assert client.failure_summary == '0 failures in queue'
        assert client.backoff_seconds_remaining is None
        assert await client.queue_size(7) == 0

    @pytest.mark.asyncio
    async def test_poll_status_loop_exits_when_stop_event_set(self, mocker):
        '''The outer loop exits once _poller_stop is set before entering.'''
        client = self.make_client()
        mocker.patch.object(client, '_http', return_value=None)
        client._poller_stop.set()  # pylint: disable=protected-access
        await client._poll_status_loop()  # pylint: disable=protected-access

    @pytest.mark.asyncio
    async def test_poll_status_loop_continues_through_wait_timeout(self, mocker):
        '''When _poller_stop stays clear, wait_for raises TimeoutError and the loop `continue`s.  Stop on the second
        iteration so the loop exits deterministically.'''
        client = self.make_client()
        mocker.patch.object(client, 'POLL_INTERVAL_SECONDS', 0.001)
        iterations = {'n': 0}

        async def stop_on_second():
            iterations['n'] += 1
            if iterations['n'] >= 2:
                client._poller_stop.set()  # pylint: disable=protected-access

        mocker.patch.object(client, '_poll_status_loop_once', side_effect=stop_on_second)
        await client._poll_status_loop()  # pylint: disable=protected-access
        assert iterations['n'] == 2

    @pytest.mark.asyncio
    async def test_poller_hanging_status_request_is_abandoned(self, mocker):
        '''A status request that never returns is abandoned after STATUS_REQUEST_TIMEOUT_SECONDS rather than parking
        the poller (aiohttp's own default is 5 minutes).'''
        client = self.make_client()
        mocker.patch.object(client, 'STATUS_REQUEST_TIMEOUT_SECONDS', 0.01)

        async def _hang(*_args, **_kwargs):
            await asyncio.sleep(30)

        mocker.patch.object(client, '_http', side_effect=_hang)
        await asyncio.wait_for(client._poll_status_loop_once(), timeout=5)  # pylint: disable=protected-access
        assert client.failure_summary == '0 failures in queue'
