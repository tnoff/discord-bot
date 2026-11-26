'''Tests for the shared helpers on AiohttpServerBase: body parsing, route registration, contract.'''
import pytest
from aiohttp import web
from aiohttp.test_utils import TestClient, TestServer

from discord_core.routes import contract
from discord_core.routes.route import Route
from discord_core.servers.base import AiohttpServerBase
from discord_core.utils.otel import AttributeNaming


class _EchoServer(AiohttpServerBase):
    '''Server exposing an echo handler built on _read_body.'''

    def build_app(self) -> web.Application:
        '''Register a GET and a POST route through the seam helper.'''
        app = web.Application()
        self.register_seam_routes(app, {
            Route('GET', '/things'): self._handle_get,
            Route('POST', '/things'): self._handle_echo,
            Route('PUT', '/things'): self._handle_echo,
        })
        self.add_contract_route(app)
        return app

    def heartbeat(self, job_name: str):
        '''Expose the protected heartbeat body.'''
        return self._serving_heartbeat_observations(job_name)

    async def _handle_get(self, _request):
        return web.Response(text='got')

    async def _handle_echo(self, request):
        ctx, body = await self._read_body(request)
        return web.json_response({'body': body, 'has_ctx': ctx is not None})


async def _started(server):
    '''Started TestClient for the server's app.'''
    client = TestClient(TestServer(server.build_app()))
    await client.start_server()
    return client


@pytest.mark.asyncio
async def test_read_body_parses_json():
    '''A JSON body comes back as a dict alongside a trace context.'''
    client = await _started(_EchoServer())
    try:
        resp = await client.post('/things', json={'a': 1})
        assert resp.status == 200
        assert await resp.json() == {'body': {'a': 1}, 'has_ctx': True}
    finally:
        await client.close()


@pytest.mark.asyncio
async def test_read_body_malformed_is_422():
    '''A body that is not JSON raises HTTP 422.'''
    client = await _started(_EchoServer())
    try:
        resp = await client.post('/things', data='nope', headers={'Content-Type': 'application/json'})
        assert resp.status == 422
    finally:
        await client.close()


@pytest.mark.asyncio
async def test_seam_routes_register_methods_and_head():
    '''GET routes also answer HEAD; other methods register as given.'''
    client = await _started(_EchoServer())
    try:
        assert (await client.get('/things')).status == 200
        assert (await client.head('/things')).status == 200
        assert (await client.put('/things', json={})).status == 200
        assert (await client.delete('/things')).status == 405
    finally:
        await client.close()


@pytest.mark.asyncio
async def test_contract_route_lists_live_routes():
    '''The contract body lists the registered routes, and is sorted by method then template.'''
    client = await _started(_EchoServer())
    try:
        resp = await client.get(contract.CONTRACT_ROUTE.template)
        payload = await resp.json()
    finally:
        await client.close()
    served = contract.decode(payload)
    assert ('POST', '/things') in served
    assert ('PUT', '/things') in served
    assert ('GET', '/things') in served
    pairs = [(item[contract.METHOD_KEY], item[contract.TEMPLATE_KEY])
             for item in payload[contract.ROUTES_KEY]]
    assert pairs == sorted(pairs)


def test_serving_heartbeat_value_and_label():
    '''The shared heartbeat body reports 0 then 1 under the given job label.'''
    server = _EchoServer()
    (before,) = server.heartbeat('job-x')
    assert before.value == 0
    server.set_serving(True)
    (after,) = server.heartbeat('job-x')
    assert after.value == 1
    assert after.attributes == {AttributeNaming.BACKGROUND_JOB.value: 'job-x'}
    assert server.is_serving is True


def test_start_draining_clears_serving():
    '''Draining flips serving off and marks the server draining.'''
    server = _EchoServer()
    server.set_serving(True)
    server.start_draining()
    assert server.is_serving is False


def test_build_app_must_be_overridden():
    '''The base build_app is abstract.'''
    with pytest.raises(NotImplementedError):
        AiohttpServerBase.build_app(_EchoServer())
