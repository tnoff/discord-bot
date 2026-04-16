'''Shared base class for aiohttp HTTP servers.'''
import asyncio
import logging

from aiohttp import web

logger = logging.getLogger(__name__)


class AiohttpServerBase:
    '''Base class for aiohttp HTTP servers with a standard serve() lifecycle.'''
    _host: str
    _port: int

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
            while True:
                await asyncio.sleep(3600)
        finally:
            await runner.cleanup()
