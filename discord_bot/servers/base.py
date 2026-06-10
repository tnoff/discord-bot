'''Shared base class for aiohttp HTTP servers.'''
import asyncio
import logging

from aiohttp import web

logger = logging.getLogger(__name__)


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
