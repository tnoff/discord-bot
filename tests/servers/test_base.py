'''
Tests for AiohttpServerBase (discord_bot/servers/base.py).

Uses a minimal concrete subclass (SimpleServer) with /ping and /slow endpoints.
TestClient is used for middleware tests that don't need the full serve() lifecycle;
real ports (18100-18105) are used for serve() / drain integration tests.
'''
import asyncio
import logging

import aiohttp
import pytest
from aiohttp import web
from aiohttp.test_utils import TestClient, TestServer

from discord_bot.servers.base import AiohttpServerBase


# ---------------------------------------------------------------------------
# Minimal concrete server
# ---------------------------------------------------------------------------

class SimpleServer(AiohttpServerBase):
    '''Concrete AiohttpServerBase for testing: /ping returns immediately, /slow blocks.'''

    def __init__(self, host: str = '127.0.0.1', port: int = 18100):
        super().__init__()
        self._host = host
        self._port = port
        self.slow_release: asyncio.Event = asyncio.Event()

    def build_app(self) -> web.Application:
        app = web.Application(middlewares=[self._get_drain_middleware()])
        app.router.add_get('/ping', self._handle_ping)
        app.router.add_get('/slow', self._handle_slow)
        return app

    async def _handle_ping(self, _request: web.Request) -> web.Response:
        return web.Response(text='pong')

    async def _handle_slow(self, _request: web.Request) -> web.Response:
        await self.slow_release.wait()
        return web.Response(text='done')


async def _wait_for_port(host: str, port: int, timeout: float = 5.0) -> None:
    '''Poll until a TCP port accepts connections.'''
    deadline = asyncio.get_event_loop().time() + timeout
    while True:
        try:
            _, writer = await asyncio.open_connection(host, port)
            writer.close()
            try:
                await writer.wait_closed()
            except OSError:
                pass
            return
        except OSError:
            if asyncio.get_event_loop().time() >= deadline:
                raise
            await asyncio.sleep(0.02)


# ---------------------------------------------------------------------------
# build_app
# ---------------------------------------------------------------------------

class TestBuildApp:
    def test_raises_not_implemented(self):
        server = AiohttpServerBase()
        with pytest.raises(NotImplementedError):
            server.build_app()


# ---------------------------------------------------------------------------
# Drain middleware (no real server needed — TestClient suffices)
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
class TestDrainMiddleware:
    async def test_normal_request_returns_200(self):
        server = SimpleServer()
        async with TestClient(TestServer(server.build_app())) as client:
            resp = await client.get('/ping')
            assert resp.status == 200
            assert await resp.text() == 'pong'

    async def test_start_draining_causes_503(self):
        server = SimpleServer()
        async with TestClient(TestServer(server.build_app())) as client:
            server.start_draining()
            resp = await client.get('/ping')
            assert resp.status == 503

    async def test_start_draining_is_idempotent(self):
        server = SimpleServer()
        async with TestClient(TestServer(server.build_app())) as client:
            server.start_draining()
            server.start_draining()
            resp = await client.get('/ping')
            assert resp.status == 503

    async def test_drain_response_body_is_informative(self):
        server = SimpleServer()
        async with TestClient(TestServer(server.build_app())) as client:
            server.start_draining()
            resp = await client.get('/ping')
            assert resp.status == 503
            body = await resp.text()
            assert 'draining' in body.lower()

    async def test_active_count_zero_after_request_completes(self):
        '''
        After requests finish, drain_and_stop() completes immediately —
        confirming _active_requests returns to zero between requests.
        '''
        server = SimpleServer()
        async with TestClient(TestServer(server.build_app())) as client:
            await client.get('/ping')
            await client.get('/ping')
        await asyncio.wait_for(server.drain_and_stop(timeout=1.0), timeout=2.0)


# ---------------------------------------------------------------------------
# drain_and_stop (no in-flight — needs only the asyncio loop, not a real port)
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
class TestDrainAndStopNoServer:
    async def test_completes_immediately_with_no_inflight(self):
        server = SimpleServer()
        await asyncio.wait_for(server.drain_and_stop(timeout=5.0), timeout=2.0)

    async def test_sets_draining_flag(self):
        '''After drain_and_stop, new requests via middleware return 503.'''
        server = SimpleServer()
        await server.drain_and_stop(timeout=1.0)
        async with TestClient(TestServer(server.build_app())) as client:
            resp = await client.get('/ping')
            assert resp.status == 503


# ---------------------------------------------------------------------------
# serve() + drain_and_stop() integration (requires real ports)
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
class TestServeLifecycle:
    async def test_serve_responds_to_requests(self):
        server = SimpleServer(port=18100)
        task = asyncio.create_task(server.serve())
        await _wait_for_port('127.0.0.1', 18100)
        try:
            async with aiohttp.ClientSession() as session:
                resp = await session.get('http://127.0.0.1:18100/ping')
                assert resp.status == 200
        finally:
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass

    async def test_serve_cleanup_on_cancel(self):
        '''Cancelling serve() runs runner.cleanup() without raising unexpected errors.'''
        server = SimpleServer(port=18101)
        task = asyncio.create_task(server.serve())
        await _wait_for_port('127.0.0.1', 18101)
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass  # expected — nothing else should propagate

    async def test_serve_exits_cleanly_after_drain(self):
        '''drain_and_stop() unblocks serve() so the task exits without cancellation.'''
        server = SimpleServer(port=18102)
        task = asyncio.create_task(server.serve())
        await _wait_for_port('127.0.0.1', 18102)
        await asyncio.wait_for(server.drain_and_stop(timeout=5.0), timeout=5.0)
        result = await asyncio.wait_for(task, timeout=2.0)
        assert result is None  # normal return, not CancelledError

    async def test_drain_rejects_new_requests_after_start(self):
        '''Once draining, the live server returns 503 to new requests.'''
        server = SimpleServer(port=18103)
        task = asyncio.create_task(server.serve())
        await _wait_for_port('127.0.0.1', 18103)
        try:
            server.start_draining()
            async with aiohttp.ClientSession() as session:
                resp = await session.get('http://127.0.0.1:18103/ping')
                assert resp.status == 503
        finally:
            await server.drain_and_stop(timeout=1.0)
            await asyncio.wait_for(task, timeout=2.0)


# ---------------------------------------------------------------------------
# drain_and_stop() with in-flight requests
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
class TestDrainWithInflight:
    async def test_waits_for_inflight_before_stopping(self):
        '''drain_and_stop() blocks until the slow in-flight request completes.'''
        server = SimpleServer(port=18104)
        serve_task = asyncio.create_task(server.serve())
        await _wait_for_port('127.0.0.1', 18104)
        try:
            async with aiohttp.ClientSession() as session:
                slow_task = asyncio.create_task(
                    session.get('http://127.0.0.1:18104/slow')
                )
                await asyncio.sleep(0.1)  # let request reach the handler

                drain_task = asyncio.create_task(server.drain_and_stop(timeout=5.0))
                await asyncio.sleep(0.05)
                assert not drain_task.done(), 'drain_and_stop must wait for in-flight request'

                server.slow_release.set()  # let the handler return

                await asyncio.wait_for(drain_task, timeout=3.0)
                await asyncio.wait_for(serve_task, timeout=3.0)

                resp = await asyncio.wait_for(slow_task, timeout=3.0)
                assert resp.status == 200
        except Exception:
            serve_task.cancel()
            try:
                await serve_task
            except asyncio.CancelledError:
                pass
            raise

    async def test_timeout_exits_despite_inflight(self):
        '''drain_and_stop() gives up after timeout even if requests are still active.'''
        server = SimpleServer(port=18105)
        serve_task = asyncio.create_task(server.serve())
        await _wait_for_port('127.0.0.1', 18105)
        try:
            async with aiohttp.ClientSession() as session:
                slow_task = asyncio.create_task(
                    session.get('http://127.0.0.1:18105/slow')
                )
                await asyncio.sleep(0.1)
                # Key assertion: drain_and_stop returns despite the blocked in-flight request.
                await asyncio.wait_for(server.drain_and_stop(timeout=0.2), timeout=2.0)
                slow_task.cancel()
                try:
                    await slow_task
                except (asyncio.CancelledError, aiohttp.ClientError):
                    pass
        finally:
            # runner.cleanup() hangs while the handler is blocked, so cancel the task.
            serve_task.cancel()
            try:
                await serve_task
            except asyncio.CancelledError:
                pass

    async def test_timeout_logs_warning(self, caplog):
        '''drain_and_stop() emits a WARNING when the timeout expires with active requests.'''
        server = SimpleServer(port=18106)
        serve_task = asyncio.create_task(server.serve())
        await _wait_for_port('127.0.0.1', 18106)
        try:
            async with aiohttp.ClientSession() as session:
                slow_task = asyncio.create_task(
                    session.get('http://127.0.0.1:18106/slow')
                )
                await asyncio.sleep(0.1)
                with caplog.at_level(logging.WARNING, logger='discord_bot.servers.base'):
                    await asyncio.wait_for(server.drain_and_stop(timeout=0.2), timeout=2.0)
                assert 'drain timeout reached' in caplog.text
                slow_task.cancel()
                try:
                    await slow_task
                except (asyncio.CancelledError, aiohttp.ClientError):
                    pass
        finally:
            serve_task.cancel()
            try:
                await serve_task
            except asyncio.CancelledError:
                pass
