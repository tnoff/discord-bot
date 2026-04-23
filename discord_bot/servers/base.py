'''Shared base classes and mixins for HTTP servers.'''
import asyncio
import json
import logging
from abc import ABC, abstractmethod

from aiohttp import web

logger = logging.getLogger(__name__)


class _DbPingMixin:
    '''Mixin that adds an async database ping helper to health server subclasses.'''

    _db_engine = None
    _last_db_ok: bool | None = None

    async def _db_ping(self) -> bool:
        '''Ping the database. Returns True on success, False on any exception.'''
        try:
            async with self._db_engine.connect():
                pass
            return True
        except Exception:
            return False


class BaseHealthServer(ABC):
    '''
    Asyncio-based health endpoint.

    Subclasses implement _check_health() to determine the 200/503 response.
    Everything else — TCP accept, header drain, response write, writer close — is shared.
    '''

    def __init__(self, port: int):
        self.port = port

    @abstractmethod
    async def _check_health(self) -> bool:
        '''Return True for 200 OK, False for 503 Service Unavailable.'''

    async def _extra_body(self) -> dict:
        '''Optional extra fields merged into the health response JSON. Default: empty.'''
        return {}

    async def serve(self) -> None:
        '''Asyncio coroutine — schedule with asyncio.create_task().'''
        server = await asyncio.start_server(self._handle, '0.0.0.0', self.port)
        logger.info(f'{self.__class__.__name__} listening on port {self.port}')
        async with server:
            await server.serve_forever()

    async def _handle(self, reader, writer) -> None:
        '''Handle a single HTTP request and write the response.'''
        try:
            await reader.readline()
            while True:
                line = await reader.readline()
                if line in (b'\r\n', b'\n', b''):
                    break

            healthy = await self._check_health()
            body_dict = {'status': 'ok' if healthy else 'unavailable'}
            body_dict.update(await self._extra_body())
            if healthy:
                status_line = b'HTTP/1.1 200 OK\r\n'
            else:
                status_line = b'HTTP/1.1 503 Service Unavailable\r\n'
            body = json.dumps(body_dict).encode()

            headers = (
                b'Content-Type: application/json\r\n'
                + b'Content-Length: ' + str(len(body)).encode() + b'\r\n'
                + b'Connection: close\r\n'
                + b'\r\n'
            )
            writer.write(status_line + headers + body)
            await writer.drain()
        except Exception as e:
            logger.debug(f'{self.__class__.__name__} handler error: {e}')
        finally:
            writer.close()
            try:
                await writer.wait_closed()
            except Exception:
                pass


class AiohttpServerBase:
    '''Base class for aiohttp HTTP servers with a standard serve() lifecycle.'''
    _host: str
    _port: int

    def __init__(self):
        self._draining: bool = False
        self._active_requests: int = 0
        self._shutdown_event: asyncio.Event = asyncio.Event()

    def _get_drain_middleware(self):
        '''
        Return an aiohttp middleware that enforces drain behaviour.

        While draining, new requests receive 503. In-flight requests are tracked
        via _active_requests so drain_and_stop() can wait for them to finish.
        '''
        @web.middleware
        async def drain_middleware(request, handler):
            if self._draining:
                return web.Response(status=503, reason='Service Draining',
                                    text='Service is draining, try another instance')
            self._active_requests += 1
            try:
                return await handler(request)
            finally:
                self._active_requests -= 1
        return drain_middleware

    def start_draining(self) -> None:
        '''Begin refusing new requests without waiting for in-flight ones to finish.
        Use drain_and_stop() to also wait and shut down.'''
        self._draining = True

    async def drain_and_stop(self, timeout: float = 30.0) -> None:
        '''
        Stop accepting new requests, wait for in-flight ones to complete, then
        unblock serve(). Call from a shutdown handler after serve() is running.
        '''
        self.start_draining()
        loop = asyncio.get_running_loop()
        deadline = loop.time() + timeout
        logger.info('%s :: draining, %d active requests in flight',
                    self.__class__.__name__, self._active_requests)
        while self._active_requests > 0:
            if loop.time() >= deadline:
                logger.warning('%s :: drain timeout reached with %d requests still active',
                               self.__class__.__name__, self._active_requests)
                break
            await asyncio.sleep(0.05)
        self._shutdown_event.set()

    def build_app(self) -> web.Application:
        '''Build and return the aiohttp Application.'''
        raise NotImplementedError

    async def serve(self) -> None:
        '''Asyncio coroutine — schedule with asyncio.create_task().'''
        app = self.build_app()
        runner = web.AppRunner(app)
        await runner.setup()
        site = web.TCPSite(runner, self._host, self._port)
        await site.start()
        logger.info('%s listening on %s:%s', self.__class__.__name__, self._host, self._port)
        try:
            await self._shutdown_event.wait()
        finally:
            await runner.cleanup()
