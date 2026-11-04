'''
The tests the commands pod's HTTP queue-worker clients share.

HttpDownloadClient and HttpYoutubeMusicSearchClient are the same seam pointed at two pods, so
their behaviour, and the server half each one talks to, is tested by one set of cases.
A client's test module subclasses ``QueueClientSuite`` and ``QueueServerSuite``, fills in the
class attributes and adds whatever is specific to its pod. Both halves are driven together through aiohttp's TestServer +
TestClient, with the worker replaced by ``FakeQueueWorker``.

Test names here must not be exactly 40 characters long: trufflehog's Lob detector matches
`test_` + 35 chars, the shape of a Lob test API key, and fails pr-check:secrets.
'''
import asyncio
from unittest.mock import AsyncMock

import pytest
from aiohttp.test_utils import TestClient, TestServer

from discord_core.cogs.music_helpers.common import SearchType
from discord_core.types.media_request import MediaRequest
from discord_core.types.playlist_add_request import PlaylistAddRequest
from discord_core.types.search import SearchResult

from tests.queue_status import status_snapshot

UNREACHABLE_URL = 'http://localhost:9999'


def media_request(guild_id: int = 1, *, search_type=SearchType.DIRECT,
                  raw_search_string='https://example.com/v.mp4') -> MediaRequest:
    '''A MediaRequest for the given guild.'''
    return MediaRequest(
        guild_id=guild_id, channel_id=2, requester_id=3, requester_name='req',
        search_result=SearchResult(search_type=search_type, raw_search_string=raw_search_string),
    )


def playlist_add_request(guild_id: int = 1, bundle_uuid: str | None = None) -> PlaylistAddRequest:
    '''A PlaylistAddRequest (download_file=False) for the given guild.'''
    return PlaylistAddRequest(
        guild_id=guild_id, channel_id=2, requester_id=3, requester_name='req',
        playlist_id=99,
        bundle_uuid=bundle_uuid,
        search_result=SearchResult(search_type=SearchType.SEARCH, raw_search_string='song name'),
    )


class FakeQueueWorker:
    '''Minimal queue worker stand-in covering the server's call sites.'''

    def __init__(self, *, status=None, clear_items=None):
        self.submit = AsyncMock()
        self.block_guild = AsyncMock(return_value=True)
        self.status_snapshot = AsyncMock(return_value=status or status_snapshot())
        self._clear_items = clear_items or []
        self.clear_guild_queue = AsyncMock(side_effect=self._clear)

    async def _clear(self, guild_id, preserve_predicate=None):
        '''Apply the server-supplied predicate exactly as the real worker would,
        so the server's flag→predicate translation is exercised end to end.'''
        del guild_id
        return [mr for mr in self._clear_items
                if not (preserve_predicate and preserve_predicate(mr))]


class QueueSuiteBase:
    '''What the client and server cases share: the pod under test and how to reach it.

    Not collected itself; a subclass in a ``test_*`` module supplies the attributes below.
    '''
    client_cls: type
    server_cls: type
    # The server's route prefix, e.g. '/downloads'.
    prefix = ''
    request_search_type = SearchType.DIRECT
    request_search_string = 'https://example.com/v.mp4'
    # The server's HEARTBEAT_JOB: how the pod's gauge is told apart from the bot-side loop's.
    heartbeat_job = ''

    def make_server(self, worker=None):
        '''A (worker, server) pair; the worker defaults to a FakeQueueWorker.'''
        worker = worker or FakeQueueWorker()
        return worker, self.server_cls(worker)

    def request(self, guild_id: int = 1) -> MediaRequest:
        '''A request in the shape this pod accepts.'''
        return media_request(guild_id, search_type=self.request_search_type,
                             raw_search_string=self.request_search_string)

    def make_client(self, tc=None, **kwargs):
        '''A client pointed at the TestClient's server, or at an address nothing listens on.'''
        if tc is None:
            return self.client_cls(UNREACHABLE_URL, **kwargs)
        return self.client_cls(str(tc.make_url('')), session=tc.session, **kwargs)

    def served_client(self, worker=None):
        '''The (worker, TestClient) pair for a served fake worker; the caller opens the TestClient.'''
        worker, server = self.make_server(worker)
        return worker, TestClient(TestServer(server.build_app()))



class QueueClientSuite(QueueSuiteBase):
    '''Cases every HTTP queue-worker client must pass.'''

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

    # -- submit / clear / block (awaited producer surface) -----------------------------------

    @pytest.mark.asyncio
    async def test_submit_posts_serialised_request(self):
        '''submit POSTs the prefix route with the serialised MediaRequest + priority.'''
        worker, tc = self.served_client()
        async with tc:
            await self.make_client(tc).submit(7, self.request(7), priority=2)
        worker.submit.assert_awaited_once()
        args, kwargs = worker.submit.await_args
        assert args[0] == 7
        assert args[1].guild_id == 7
        assert args[1].search_result.raw_search_string == self.request_search_string
        assert kwargs['priority'] == 2

    @pytest.mark.asyncio
    async def test_clear_forwards_preserve_flag_and_returns_dropped(self):
        '''With a predicate, clear preserves non-download items server-side and returns
        only the dropped (downloadable) requests, deserialised back to MediaRequests.'''
        worker, tc = self.served_client(FakeQueueWorker(clear_items=[
            self.request(7), playlist_add_request(7, bundle_uuid='keep-bundle')]))
        async with tc:
            result = await self.make_client(tc).clear_guild_queue(
                7, preserve_predicate=lambda r: not r.download_file)
        # The playlist-add (download_file=False) is preserved; only the download drops.
        assert len(result.dropped) == 1
        assert result.dropped[0].download_file is True
        # The pod reports the preserved item's bundle_uuid back over HTTP so the
        # cog can skip deleting that bundle in HA.
        assert result.preserved_bundle_uuids == {'keep-bundle'}
        _, kwargs = worker.clear_guild_queue.await_args
        assert kwargs['preserve_predicate'] is not None

    @pytest.mark.asyncio
    async def test_clear_without_predicate_drops_everything(self):
        '''No predicate → preserve_playlist_adds=False → the server preserves nothing.'''
        worker, tc = self.served_client(FakeQueueWorker(
            clear_items=[self.request(7), playlist_add_request(7)]))
        async with tc:
            result = await self.make_client(tc).clear_guild_queue(7)
        assert len(result.dropped) == 2
        assert result.preserved_bundle_uuids == set()
        _, kwargs = worker.clear_guild_queue.await_args
        assert kwargs['preserve_predicate'] is None

    @pytest.mark.asyncio
    async def test_block_guild_posts_to_the_block_endpoint(self):
        '''block_guild POSTs the block route and returns True.'''
        worker, tc = self.served_client()
        async with tc:
            result = await self.make_client(tc).block_guild(7)
        assert result is True
        worker.block_guild.assert_awaited_once_with(7)

    # -- cached read surface (queue_size / failure_summary / backoff) ------------------------

    @pytest.mark.asyncio
    async def test_initial_cached_values_are_safe_defaults(self):
        '''Before the poller has run, the cached reads return safe defaults.'''
        client = self.make_client()
        assert await client.queue_size(7) == 0
        assert client.failure_summary == '0 failures in queue'
        assert client.backoff_seconds_remaining is None

    @pytest.mark.asyncio
    async def test_poller_refreshes_cached_values_from_status_endpoint(self):
        '''After the poller fires once the cached reads reflect the server's response.'''
        _, tc = self.served_client(FakeQueueWorker(status=status_snapshot(
            summary='1 failure in queue', backoff=42, queue_sizes={'7': 3, '12': 1})))
        async with tc:
            client = self.make_client(tc)
            # Drive one iteration directly; start() races the poll cadence.
            await client._poll_status_loop_once()  # pylint: disable=protected-access
        assert await client.queue_size(7) == 3
        assert await client.queue_size(12) == 1
        assert await client.queue_size(99) == 0  # absent → default
        assert client.failure_summary == '1 failure in queue'
        assert client.backoff_seconds_remaining == 42

    @pytest.mark.asyncio
    async def test_poller_swallows_status_errors_and_keeps_running(self, mocker):
        '''A transient HTTP error during status refresh is logged, not propagated.'''
        client = self.make_client()
        mocker.patch.object(client, '_http', side_effect=RuntimeError('boom'))
        await client._poll_status_loop_once()  # pylint: disable=protected-access
        assert client.failure_summary == '0 failures in queue'

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
        '''When _poller_stop stays clear, wait_for raises TimeoutError and the loop
        `continue`s.  Stop on the second iteration so the loop exits deterministically.'''
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
        '''A status request that never returns is abandoned after STATUS_REQUEST_TIMEOUT_SECONDS
        rather than parking the poller (aiohttp's own default is 5 minutes).'''
        client = self.make_client()
        mocker.patch.object(client, 'STATUS_REQUEST_TIMEOUT_SECONDS', 0.01)

        async def _hang(*_args, **_kwargs):
            await asyncio.sleep(30)

        mocker.patch.object(client, '_http', side_effect=_hang)
        await asyncio.wait_for(client._poll_status_loop_once(), timeout=5)  # pylint: disable=protected-access
        assert client.failure_summary == '0 failures in queue'


class QueueServerSuite(QueueSuiteBase):
    '''Cases every queue-worker HTTP server must pass.'''


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

    @pytest.mark.parametrize('suffix, body', [
        ('', {'priority': 1}),
        ('/clear', {}),
        ('/block', {}),
    ])
    @pytest.mark.asyncio
    async def test_endpoint_rejects_malformed_body(self, suffix, body):
        '''Missing guild_id → 422.'''
        _, tc = self.served_client()
        async with tc:
            resp = await tc.post(f'{self.prefix}{suffix}', json=body)
        assert resp.status == 422

    @pytest.mark.asyncio
    async def test_draining_server_refuses_new_requests(self):
        '''Once draining, the shared middleware 503s new requests — the rolling-deploy
        behaviour both worker pods rely on.'''
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
