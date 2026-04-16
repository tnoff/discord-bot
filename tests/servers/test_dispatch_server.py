'''
Tests for DispatchHttpServer — the aiohttp HTTP server wrapping MessageDispatcher.
'''
import asyncio

import aiohttp
import pytest
from aiohttp.test_utils import TestClient, TestServer

from discord_bot.servers.base import AiohttpServerBase
from discord_bot.servers.dispatch_server import DispatchHttpServer
from tests.helpers import FakeDispatchServer, FakeRedisDispatchQueue


# ---------------------------------------------------------------------------
# AiohttpServerBase (servers/base.py line 17)
# ---------------------------------------------------------------------------

def test_base_build_app_raises_not_implemented():
    '''AiohttpServerBase.build_app raises NotImplementedError — subclasses must override it.'''
    server = AiohttpServerBase()
    with pytest.raises(NotImplementedError):
        server.build_app()


def _make_server(dispatcher=None, result_store=None):
    if result_store is None:
        result_store = {}
    if dispatcher is None:
        dispatcher = FakeDispatchServer(result_store)
    redis_queue = FakeRedisDispatchQueue(result_store)
    return dispatcher, DispatchHttpServer(dispatcher, redis_queue)


@pytest.mark.asyncio
class TestServe:
    async def test_serve_starts_and_responds(self):
        '''serve() starts the aiohttp server and handles requests until cancelled.'''
        _, server = _make_server()
        task = asyncio.create_task(server.serve())
        deadline = asyncio.get_event_loop().time() + 5.0
        while True:
            try:
                _, writer = await asyncio.open_connection('0.0.0.0', 8082)
                writer.close()
                try:
                    await writer.wait_closed()
                except OSError:
                    pass
                break
            except OSError:
                if asyncio.get_event_loop().time() >= deadline:
                    raise
                await asyncio.sleep(0.02)
        try:
            async with aiohttp.ClientSession() as session:
                resp = await session.get('http://0.0.0.0:8082/dispatch/results/nonexistent')
                assert resp.status == 202
        finally:
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass


@pytest.mark.asyncio
class TestSend:
    async def test_valid_body_calls_dispatcher(self):
        dispatcher, server = _make_server()
        async with TestClient(TestServer(server.build_app())) as client:
            resp = await client.post('/dispatch/send', json={
                'guild_id': 1, 'channel_id': 2, 'content': 'hello',
            })
            assert resp.status == 202
        assert any(c[0] == 'send_message' for c in dispatcher.calls)

    async def test_missing_required_field_returns_422(self):
        _, server = _make_server()
        async with TestClient(TestServer(server.build_app())) as client:
            resp = await client.post('/dispatch/send', json={'guild_id': 1})
            assert resp.status == 422


@pytest.mark.asyncio
class TestDelete:
    async def test_valid_body_calls_dispatcher(self):
        dispatcher, server = _make_server()
        async with TestClient(TestServer(server.build_app())) as client:
            resp = await client.post('/dispatch/delete', json={
                'guild_id': 1, 'channel_id': 2, 'message_id': 3,
            })
            assert resp.status == 202
        assert any(c[0] == 'delete_message' for c in dispatcher.calls)

    async def test_missing_required_field_returns_422(self):
        _, server = _make_server()
        async with TestClient(TestServer(server.build_app())) as client:
            resp = await client.post('/dispatch/delete', json={'guild_id': 1})
            assert resp.status == 422


@pytest.mark.asyncio
class TestUpdateMutable:
    async def test_valid_body_calls_dispatcher(self):
        dispatcher, server = _make_server()
        async with TestClient(TestServer(server.build_app())) as client:
            resp = await client.post('/dispatch/update_mutable', json={
                'key': 'k', 'guild_id': 1, 'content': ['msg'], 'channel_id': 2,
            })
            assert resp.status == 202
        assert any(c[0] == 'update_mutable' for c in dispatcher.calls)

    async def test_missing_required_field_returns_422(self):
        _, server = _make_server()
        async with TestClient(TestServer(server.build_app())) as client:
            resp = await client.post('/dispatch/update_mutable', json={'key': 'k'})
            assert resp.status == 422


@pytest.mark.asyncio
class TestRemoveMutable:
    async def test_valid_body_calls_dispatcher(self):
        dispatcher, server = _make_server()
        async with TestClient(TestServer(server.build_app())) as client:
            resp = await client.post('/dispatch/remove_mutable', json={'key': 'k'})
            assert resp.status == 202
        assert any(c[0] == 'remove_mutable' for c in dispatcher.calls)

    async def test_missing_required_field_returns_422(self):
        _, server = _make_server()
        async with TestClient(TestServer(server.build_app())) as client:
            resp = await client.post('/dispatch/remove_mutable', json={})
            assert resp.status == 422


@pytest.mark.asyncio
class TestUpdateMutableChannel:
    async def test_valid_body_calls_dispatcher(self):
        dispatcher, server = _make_server()
        async with TestClient(TestServer(server.build_app())) as client:
            resp = await client.post('/dispatch/update_mutable_channel', json={
                'key': 'k', 'guild_id': 1, 'new_channel_id': 99,
            })
            assert resp.status == 202
        assert any(c[0] == 'update_mutable_channel' for c in dispatcher.calls)

    async def test_missing_required_field_returns_422(self):
        _, server = _make_server()
        async with TestClient(TestServer(server.build_app())) as client:
            resp = await client.post('/dispatch/update_mutable_channel', json={'key': 'k'})
            assert resp.status == 422


@pytest.mark.asyncio
class TestFetchHistory:
    async def test_valid_body_returns_request_id(self):
        _, server = _make_server()
        async with TestClient(TestServer(server.build_app())) as client:
            resp = await client.post('/dispatch/fetch_history', json={
                'guild_id': 1, 'channel_id': 2, 'limit': 50,
            })
            assert resp.status == 202
            data = await resp.json()
            assert 'request_id' in data
            assert isinstance(data['request_id'], str)

    async def test_deterministic_request_id(self):
        '''Same params always produce the same request_id.'''
        _, server = _make_server()
        async with TestClient(TestServer(server.build_app())) as client:
            body = {'guild_id': 1, 'channel_id': 2, 'limit': 50}
            resp1 = await client.post('/dispatch/fetch_history', json=body)
            resp2 = await client.post('/dispatch/fetch_history', json=body)
            data1 = await resp1.json()
            data2 = await resp2.json()
            assert data1['request_id'] == data2['request_id']

    async def test_missing_required_field_returns_422(self):
        _, server = _make_server()
        async with TestClient(TestServer(server.build_app())) as client:
            resp = await client.post('/dispatch/fetch_history', json={'guild_id': 1})
            assert resp.status == 422


@pytest.mark.asyncio
class TestFetchEmojis:
    async def test_valid_body_returns_request_id(self):
        _, server = _make_server()
        async with TestClient(TestServer(server.build_app())) as client:
            resp = await client.post('/dispatch/fetch_emojis', json={'guild_id': 1})
            assert resp.status == 202
            data = await resp.json()
            assert 'request_id' in data

    async def test_missing_required_field_returns_422(self):
        _, server = _make_server()
        async with TestClient(TestServer(server.build_app())) as client:
            resp = await client.post('/dispatch/fetch_emojis', json={})
            assert resp.status == 422


@pytest.mark.asyncio
class TestGetResult:
    async def test_pending_returns_202(self):
        '''No result stored → 202 pending.'''
        _, server = _make_server()
        async with TestClient(TestServer(server.build_app())) as client:
            resp = await client.get('/dispatch/results/nonexistent')
            assert resp.status == 202
            data = await resp.json()
            assert data['status'] == 'pending'

    async def test_result_available_returns_200(self):
        '''Result pre-stored in queue → 200 with result JSON.'''
        result_store = {'req-123': {'guild_id': 1, 'channel_id': 2, 'messages': []}}
        _, server = _make_server(result_store=result_store)
        async with TestClient(TestServer(server.build_app())) as client:
            resp = await client.get('/dispatch/results/req-123')
            assert resp.status == 200
            data = await resp.json()
            assert data['guild_id'] == 1
