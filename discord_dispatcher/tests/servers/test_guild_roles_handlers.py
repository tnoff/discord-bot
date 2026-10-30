'''
The guild-roles routes on DispatchHttpServer (servers/guild_roles_handlers.py): each answers 202 with a
request id, the id depends on the call_id, and a bad body is the seam's 422.
'''
import pytest
from aiohttp.test_utils import TestClient, TestServer

from discord_dispatcher.servers.dispatch_server import DispatchHttpServer

from tests.helpers import FakeDispatchServer, FakeRedisDispatchQueue

BODIES = {
    '/dispatch/fetch_roles': ('fetch_roles', {'guild_id': 1}),
    '/dispatch/fetch_member': ('fetch_member', {'guild_id': 1, 'user_id': 2}),
    '/dispatch/fetch_role_members': ('fetch_role_members', {'guild_id': 1, 'role_id': 3}),
    '/dispatch/change_member_role': ('change_member_role',
                                     {'guild_id': 1, 'user_id': 2, 'role_id': 3, 'action': 'add'}),
}


def _make_server():
    dispatcher = FakeDispatchServer()
    return dispatcher, DispatchHttpServer(dispatcher, FakeRedisDispatchQueue())


@pytest.mark.asyncio
@pytest.mark.parametrize('path', sorted(BODIES))
async def test_a_call_is_taken_with_a_request_id(path):
    kind, params = BODIES[path]
    dispatcher, server = _make_server()
    async with TestClient(TestServer(server.build_app())) as client:
        resp = await client.post(path, json={**params, 'call_id': 'a'})
        assert resp.status == 202
        request_id = (await resp.json())['request_id']
    assert dispatcher.calls == [('enqueue_guild_roles_call', request_id, kind, params)]


@pytest.mark.asyncio
@pytest.mark.parametrize('path', sorted(BODIES))
async def test_the_call_id_decides_whether_two_calls_share_a_request(path):
    _, params = BODIES[path]
    _, server = _make_server()
    async with TestClient(TestServer(server.build_app())) as client:
        ids = []
        for call_id in ('a', 'a', 'b'):
            resp = await client.post(path, json={**params, 'call_id': call_id})
            ids.append((await resp.json())['request_id'])
    assert ids[0] == ids[1]
    assert ids[0] != ids[2]


@pytest.mark.asyncio
@pytest.mark.parametrize('path', sorted(BODIES))
async def test_a_body_without_a_call_id_is_a_422(path):
    _, params = BODIES[path]
    _, server = _make_server()
    async with TestClient(TestServer(server.build_app())) as client:
        resp = await client.post(path, json=params)
        assert resp.status == 422


@pytest.mark.asyncio
async def test_an_unknown_action_is_a_422():
    _, server = _make_server()
    async with TestClient(TestServer(server.build_app())) as client:
        resp = await client.post('/dispatch/change_member_role', json={
            'guild_id': 1, 'user_id': 2, 'role_id': 3, 'action': 'toggle', 'call_id': 'a'})
        assert resp.status == 422


@pytest.mark.asyncio
async def test_the_same_call_for_a_different_member_is_a_different_request():
    _, server = _make_server()
    async with TestClient(TestServer(server.build_app())) as client:
        first = await client.post('/dispatch/fetch_member', json={'guild_id': 1, 'user_id': 2, 'call_id': 'a'})
        second = await client.post('/dispatch/fetch_member', json={'guild_id': 1, 'user_id': 9, 'call_id': 'a'})
        assert (await first.json())['request_id'] != (await second.json())['request_id']
