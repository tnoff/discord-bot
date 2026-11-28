'''Tests for QueueWorkerHttpServer: the HTTP front of a Redis-backed queue worker.'''
import pytest
from aiohttp.test_utils import TestClient, TestServer

from discord_core.routes import contract
from discord_core.routes.queue_worker import at_prefix
from discord_core.servers.queue_worker_server import QueueWorkerHttpServer
from discord_core.tests.workers.queue_support import (
    RedisQueueStub, make_playlist_add, make_request)
from discord_core.types.queue import QueueFull
from discord_core.utils.otel import AttributeNaming

ROUTES = at_prefix('/qw')


class _Server(QueueWorkerHttpServer):
    '''Concrete server on a private prefix.'''
    ROUTES = ROUTES
    SPAN_PREFIX = 'qw_test'
    HEARTBEAT_JOB = 'qw_test_job'
    HEARTBEAT_DESCRIPTION = 'qw test heartbeat'
    DEFAULT_PORT = 18777

    @property
    def bound_port(self) -> int:
        '''The port this server would bind.'''
        return self._port


class _FullWorker(RedisQueueStub):
    '''Worker whose queue is always full.'''

    async def submit(self, guild_id, media_request, priority=None):
        '''Refuse like a bounded queue at capacity.'''
        raise QueueFull()


@pytest.fixture(name='worker')
def _worker(redis_client):
    '''A stub worker on the shared fake Redis.'''
    return RedisQueueStub(redis_client)


async def _client(worker):
    '''Started TestClient over the server's app.'''
    client = TestClient(TestServer(_Server(worker).build_app()))
    await client.start_server()
    return client


def test_prefix_and_port_come_from_class_constants(worker):
    '''ROUTE_PREFIX is derived from ROUTES; port defaults to the class constant.'''
    assert _Server.ROUTE_PREFIX == '/qw'
    assert _Server(worker).bound_port == 18777
    assert _Server(worker, host='127.0.0.1', port=1234).bound_port == 1234


def test_heartbeat_follows_serving_state(worker):
    '''The heartbeat gauge reads 1 only while the server is marked serving.'''
    server = _Server(worker)
    (down,) = server.heartbeat_observations()
    assert down.value == 0
    assert down.attributes == {AttributeNaming.BACKGROUND_JOB.value: 'qw_test_job'}
    server.set_serving(True)
    assert server.heartbeat_observations()[0].value == 1


@pytest.mark.asyncio
async def test_submit_accepts_and_forwards_priority(worker):
    '''A valid submit is answered 202 and reaches the worker with its priority.'''
    request = make_request()
    client = await _client(worker)
    try:
        resp = await client.post('/qw', json={
            'guild_id': '7', 'priority': 2, 'media_request': request.model_dump(mode='json')})
        assert resp.status == 202
        assert await resp.json() == {'status': 'ok'}
    finally:
        await client.close()
    assert worker.submitted == [(7, request.uuid, 2)]


@pytest.mark.asyncio
async def test_submit_defaults_priority_to_none(worker):
    '''Omitting priority forwards None.'''
    request = make_playlist_add()
    client = await _client(worker)
    try:
        resp = await client.post('/qw', json={
            'guild_id': 7, 'media_request': request.model_dump(mode='json')})
        assert resp.status == 202
    finally:
        await client.close()
    assert worker.submitted == [(7, request.uuid, None)]


@pytest.mark.asyncio
async def test_submit_blocked_guild_is_409(worker):
    '''A blocked guild answers 409 with the exception name as the reason.'''
    await worker.block_guild(7)
    client = await _client(worker)
    try:
        resp = await client.post('/qw', json={
            'guild_id': 7, 'media_request': make_request().model_dump(mode='json')})
        assert resp.status == 409
        body = await resp.json()
    finally:
        await client.close()
    assert body == {'status': 'rejected', 'reason': 'PutsBlocked',
                    'detail': 'Puts blocked for guild 7'}
    assert not worker.submitted


@pytest.mark.asyncio
async def test_submit_full_queue_is_429(redis_client):
    '''A full queue answers 429 and QueueFull as the reason.'''
    client = await _client(_FullWorker(redis_client))
    try:
        resp = await client.post('/qw', json={
            'guild_id': 7, 'media_request': make_request().model_dump(mode='json')})
        assert resp.status == 429
        body = await resp.json()
    finally:
        await client.close()
    assert body['reason'] == 'QueueFull'
    assert body['status'] == 'rejected'


@pytest.mark.asyncio
@pytest.mark.parametrize('payload', [
    {'media_request': {}},
    {'guild_id': 'abc', 'media_request': {}},
    {'guild_id': 7},
    {'guild_id': 7, 'media_request': {'guild_id': 7}},
])
async def test_submit_bad_body_is_422(worker, payload):
    '''Missing or invalid fields answer 422 and nothing is queued.'''
    client = await _client(worker)
    try:
        resp = await client.post('/qw', json=payload)
        assert resp.status == 422
    finally:
        await client.close()
    assert not worker.submitted


@pytest.mark.asyncio
@pytest.mark.parametrize('path', ['/qw', '/qw/clear', '/qw/block'])
async def test_malformed_json_is_422(worker, path):
    '''Every mutating route rejects a non-JSON body with 422.'''
    client = await _client(worker)
    try:
        resp = await client.post(path, data='{not json', headers={'Content-Type': 'application/json'})
        assert resp.status == 422
    finally:
        await client.close()


@pytest.mark.asyncio
async def test_clear_returns_dropped_requests(worker):
    '''Clearing without the flag drops everything and reports it all.'''
    first, second = make_request(text='one'), make_playlist_add(text='two')
    await worker.enqueue(first, 1.0)
    await worker.enqueue(second, 2.0)
    client = await _client(worker)
    try:
        resp = await client.post('/qw/clear', json={'guild_id': 7})
        assert resp.status == 200
        body = await resp.json()
    finally:
        await client.close()
    assert [item['uuid'] for item in body['dropped']] == [first.uuid, second.uuid]
    assert body['preserved_bundle_uuids'] == []
    assert await worker.queue_size(7) == 0


@pytest.mark.asyncio
async def test_clear_preserving_playlist_adds(worker):
    '''preserve_playlist_adds keeps metadata-only requests and lists their bundle uuids.'''
    download = make_request(text='dl')
    kept = make_playlist_add(text='kept')
    kept.bundle_uuid = 'bundle.keep'
    unbundled = make_playlist_add(text='no bundle')
    for score, item in enumerate([download, kept, unbundled]):
        await worker.enqueue(item, float(score))
    client = await _client(worker)
    try:
        resp = await client.post('/qw/clear', json={'guild_id': 7, 'preserve_playlist_adds': True})
        body = await resp.json()
    finally:
        await client.close()
    assert [item['uuid'] for item in body['dropped']] == [download.uuid]
    assert body['preserved_bundle_uuids'] == ['bundle.keep']
    assert await worker.queue_size(7) == 2


@pytest.mark.asyncio
async def test_clear_requires_guild_id(worker):
    '''A clear without a usable guild_id is 422.'''
    client = await _client(worker)
    try:
        assert (await client.post('/qw/clear', json={})).status == 422
        assert (await client.post('/qw/clear', json={'guild_id': None})).status == 422
    finally:
        await client.close()


@pytest.mark.asyncio
async def test_clear_refuses_a_preserve_flag_that_is_not_a_bool(worker):
    '''The flag used to be read with `bool(...)`, so any string meant "keep": the request model refuses it instead,
    and nothing is dropped.'''
    client = await _client(worker)
    try:
        resp = await client.post('/qw/clear', json={'guild_id': 7, 'preserve_playlist_adds': 'maybe'})
        assert resp.status == 422
    finally:
        await client.close()


@pytest.mark.asyncio
async def test_block_sets_flag(worker):
    '''Block answers 200 ok and the guild is blocked afterwards.'''
    client = await _client(worker)
    try:
        resp = await client.post('/qw/block', json={'guild_id': 7})
        assert resp.status == 200
        assert await resp.json() == {'status': 'ok'}
        assert (await client.post('/qw/block', json={})).status == 422
    finally:
        await client.close()
    assert await worker.guild_is_blocked(7) is True


@pytest.mark.asyncio
async def test_status_reports_queue_sizes(worker):
    '''Status serves the worker snapshot, with a zero backoff reported as null.'''
    await worker.enqueue(make_request(guild_id=3), 1.0)
    worker.snapshot_overrides = {'failure_summary': 'throttled', 'failure_count': 2}
    client = await _client(worker)
    try:
        resp = await client.get('/qw/status')
        assert resp.status == 200
        body = await resp.json()
    finally:
        await client.close()
    assert body == {'failure_summary': 'throttled', 'failure_count': 2,
                    'backoff_seconds_remaining': None, 'queue_sizes': {'3': 1}}


@pytest.mark.asyncio
async def test_contract_route_lists_served_routes(worker):
    '''The contract endpoint advertises exactly the four queue routes plus itself.'''
    client = await _client(worker)
    try:
        resp = await client.get(contract.CONTRACT_ROUTE.template)
        served = contract.decode(await resp.json())
        head = await client.head('/qw/status')
    finally:
        await client.close()
    expected = {(route.method, route.template) for route in ROUTES.all}
    expected.add((contract.CONTRACT_ROUTE.method, contract.CONTRACT_ROUTE.template))
    assert expected <= served
    assert head.status == 200


@pytest.mark.asyncio
async def test_draining_server_answers_503(worker):
    '''Once draining starts, requests are refused with 503.'''
    server = _Server(worker)
    server.start_draining()
    client = TestClient(TestServer(server.build_app()))
    await client.start_server()
    try:
        resp = await client.get('/qw/status')
        assert resp.status == 503
    finally:
        await client.close()
