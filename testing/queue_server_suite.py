'''
The cases every queue-worker HTTP server must pass, over raw HTTP and no client.

DownloadHttpServer and YoutubeMusicSearchHttpServer are the same server on two prefixes. A server's test module
subclasses ``QueueServerSuite`` and names its class and prefix. The worker is a ``FakeQueueWorker``, and requests
are posted as the request models serialise them, so this is the server's half of the seam: it accepts what the
contract says it accepts, hands the worker what was sent, and answers each declared reply. The client's half is
``queue_client_suite``, and that the two agree is ``queue_round_trip_suite``.
'''
import pytest
from aiohttp.test_utils import TestClient, TestServer

from discord_core.types.playlist_add_request import PlaylistAddRequest
from discord_core.types.queue import PutsBlocked, QueueFull, SUBMIT_REJECTION_STATUS
from discord_core.types.queue_worker_requests import SubmitRequestBody

from testing.queue_status import status_snapshot
from testing.queue_worker_support import FakeQueueWorker, QueueSuiteBase, playlist_add_request
from testing.tracing_support import record_spans


class QueueServerSuite(QueueSuiteBase):
    '''Cases every queue-worker HTTP server must pass.'''
    # The server's route prefix, e.g. '/downloads'.
    prefix = ''
    # The server's HEARTBEAT_JOB: how the pod's gauge is told apart from the bot-side loop's.
    heartbeat_job = ''

    def make_server(self, worker=None):
        '''A (worker, server) pair; the worker defaults to a FakeQueueWorker.'''
        worker = worker or FakeQueueWorker()
        return worker, self.server_cls(worker)

    def served_client(self, worker=None):
        '''The (worker, TestClient) pair for a served fake worker; the caller opens the TestClient.'''
        worker, server = self.make_server(worker)
        return worker, TestClient(TestServer(server.build_app()))

    def submit_body(self, request=None, **fields) -> dict:
        '''A submit body, as the request model serialises it.'''
        return SubmitRequestBody(guild_id=7, media_request=request or self.request(7),
                                 **fields).model_dump(mode='json')

    # -- submit ------------------------------------------------------------------------------

    @pytest.mark.asyncio
    @pytest.mark.parametrize('kind', ['plain', 'playlist_add'])
    async def test_submit_hands_the_worker_the_request_and_priority(self, kind):
        '''The worker gets the guild, the priority and the request as the kind it was sent (a plain request or a
        playlist add), and the caller is told it was accepted.'''
        sent = self.request(7) if kind == 'plain' else playlist_add_request(7)
        worker, tc = self.served_client()
        async with tc:
            resp = await tc.post(self.prefix, json=self.submit_body(sent, priority=2))
            body = await resp.json()
        assert (resp.status, body) == (202, {'status': 'ok'})
        worker.submit.assert_awaited_once()
        args, kwargs = worker.submit.await_args
        assert args[0] == 7
        assert type(args[1]) is type(sent)  # pylint: disable=unidiomatic-typecheck
        assert args[1].search_result.raw_search_string == sent.search_result.raw_search_string
        assert kwargs['priority'] == 2
        assert isinstance(args[1], PlaylistAddRequest) is (kind == 'playlist_add')

    @pytest.mark.parametrize('rejection', [PutsBlocked, QueueFull])
    @pytest.mark.asyncio
    async def test_a_refusal_is_its_declared_status_and_names_the_exception(self, rejection):
        '''A refusal is a deterministic answer, not a fault: the status the contract declares for it, with the
        exception's class name for the client to turn back into the exception.

        Parametrised against SUBMIT_REJECTION_STATUS as well, so adding an entry to the mapping without wiring the
        server's catch fails here and not as a silent 500.'''
        assert rejection in SUBMIT_REJECTION_STATUS
        worker, tc = self.served_client()
        worker.submit.side_effect = rejection('refused')
        async with tc:
            resp = await tc.post(self.prefix, json=self.submit_body())
            body = await resp.json()
        assert resp.status == SUBMIT_REJECTION_STATUS[rejection]
        assert body == {'status': 'rejected', 'reason': rejection.__name__, 'detail': 'refused'}

    @pytest.mark.asyncio
    async def test_a_refusal_leaves_the_server_span_ok(self, mocker):
        '''A refusal is a decision, so the server span ends OK or every blocked-guild submit reads as an error.'''
        exporter = record_spans(mocker)
        worker, tc = self.served_client()
        worker.submit.side_effect = PutsBlocked('blocked')
        async with tc:
            await tc.post(self.prefix, json=self.submit_body())
        (span,) = [s for s in exporter.get_finished_spans() if s.name == f'{self.server_cls.SPAN_PREFIX}.submit']
        assert span.status.status_code.name != 'ERROR'
        assert span.attributes['queue.submit_rejection'] == 'PutsBlocked'

    @pytest.mark.asyncio
    async def test_a_genuine_fault_is_still_a_500(self):
        '''Only the two queue-contract exceptions are translated; anything else is a real server fault.'''
        worker, tc = self.served_client()
        worker.submit.side_effect = RuntimeError('boom')
        async with tc:
            resp = await tc.post(self.prefix, json=self.submit_body())
        assert resp.status == 500

    # -- clear / block -----------------------------------------------------------------------

    @pytest.mark.asyncio
    async def test_clear_preserving_playlist_adds_keeps_them_and_reports_their_bundles(self):
        '''The flag becomes a predicate here, since a predicate cannot cross HTTP: playlist adds are kept (and their
        bundles reported so the cog does not delete them) and the downloads are dropped and returned.'''
        worker, tc = self.served_client(FakeQueueWorker(clear_items=[
            self.request(7), playlist_add_request(7, bundle_uuid='keep-bundle')]))
        async with tc:
            resp = await tc.post(f'{self.prefix}/clear', json={'guild_id': 7, 'preserve_playlist_adds': True})
            body = await resp.json()
        assert resp.status == 200
        assert [item['download_file'] for item in body['dropped']] == [True]
        assert body['preserved_bundle_uuids'] == ['keep-bundle']
        assert worker.clear_guild_queue.await_args.kwargs['preserve_predicate'] is not None

    @pytest.mark.asyncio
    async def test_clear_by_default_drops_everything(self):
        '''Without the flag no predicate is passed, so nothing is kept.'''
        worker, tc = self.served_client(FakeQueueWorker(clear_items=[self.request(7), playlist_add_request(7)]))
        async with tc:
            resp = await tc.post(f'{self.prefix}/clear', json={'guild_id': 7})
            body = await resp.json()
        assert len(body['dropped']) == 2
        assert body['preserved_bundle_uuids'] == []
        assert worker.clear_guild_queue.await_args.kwargs['preserve_predicate'] is None

    @pytest.mark.asyncio
    async def test_block_reaches_the_worker(self):
        '''block blocks the guild named in the body.'''
        worker, tc = self.served_client()
        async with tc:
            resp = await tc.post(f'{self.prefix}/block', json={'guild_id': 7})
            body = await resp.json()
        assert (resp.status, body) == (200, {'status': 'ok'})
        worker.block_guild.assert_awaited_once_with(7)

    # -- status ------------------------------------------------------------------------------

    @pytest.mark.asyncio
    async def test_status_endpoint_returns_worker_snapshot(self):
        '''GET <prefix>/status returns the worker's status_snapshot verbatim.'''
        _, tc = self.served_client(FakeQueueWorker(status=status_snapshot(
            summary='2 failures in queue', backoff=10, queue_sizes={'7': 3, '12': 1})))
        async with tc:
            resp = await tc.get(f'{self.prefix}/status')
            body = await resp.json()
        assert resp.status == 200
        assert body['failure_summary'] == '2 failures in queue'
        assert body['backoff_seconds_remaining'] == 10
        assert body['queue_sizes'] == {'7': 3, '12': 1}

    @pytest.mark.asyncio
    async def test_status_emits_no_span(self, mocker):
        '''The bot polls this every second per worker, so a span here would be a root span per tick describing an
        unremarkable cache refresh; routes that mutate queue state keep theirs.'''
        exporter = record_spans(mocker)
        _, tc = self.served_client()
        async with tc:
            await tc.get(f'{self.prefix}/status')
        assert not exporter.get_finished_spans()

    # -- refusing a body, draining, heartbeat ------------------------------------------------

    @pytest.mark.parametrize('suffix, body', [
        ('', {'priority': 1}),
        ('', {'guild_id': 7, 'media_request': {'not': 'a request'}}),
        ('/clear', {}),
        ('/clear', {'guild_id': 7, 'preserve_playlist_adds': 'maybe'}),
        ('/block', {}),
    ])
    @pytest.mark.asyncio
    async def test_endpoint_rejects_malformed_body(self, suffix, body):
        '''A body the request model refuses (a missing guild_id, a request that is not one, a flag that is not a
        bool) is a 422, and nothing reaches the worker.'''
        worker, tc = self.served_client()
        async with tc:
            resp = await tc.post(f'{self.prefix}{suffix}', json=body)
        assert resp.status == 422
        worker.submit.assert_not_awaited()
        worker.clear_guild_queue.assert_not_awaited()
        worker.block_guild.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_draining_server_refuses_new_requests(self):
        '''Once draining, the shared middleware 503s new requests — the rolling-deploy behaviour both worker pods
        rely on.'''
        _, server = self.make_server()
        async with TestClient(TestServer(server.build_app())) as tc:
            server.start_draining()
            resp = await tc.get(f'{self.prefix}/status')
        assert resp.status == 503

    @pytest.mark.asyncio
    async def test_heartbeat_observation_reflects_serving_state(self):
        '''heartbeat_observations reports 0 before serving, tagged with the server's job label.'''
        _, server = self.make_server()
        (observation,) = server.heartbeat_observations()
        assert observation.value == 0
        assert observation.attributes == {'background_job': self.server_cls.HEARTBEAT_JOB}
        assert self.server_cls.HEARTBEAT_JOB == self.heartbeat_job
