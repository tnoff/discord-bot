'''Tests for HealthServerBase paths not reached by probes: serve(), error handling, abstract checks.'''
import asyncio
import logging
import socket

import pytest

from discord_core.servers.health_server_base import HealthServerBase, close_writer
from testing.net_support import cancel_task, wait_for_port


class _Healthy(HealthServerBase):
    '''Health server that is always ok.'''

    async def _check(self):
        return True, {'who': 'me'}

    async def handle(self, reader, writer):
        '''Expose the protected request handler.'''
        await self._handle(reader, writer)


class _Unchecked(HealthServerBase):
    '''Health server that forgot to implement _check.'''

    async def _check(self):
        return await super()._check()

    async def liveness(self):
        '''Expose the protected liveness check.'''
        return await self._check()

    async def readiness(self):
        '''Expose the protected readiness check.'''
        return await self._readiness_check()


class _ClosingWriter:
    '''Writer double whose wait_closed fails like a reset connection.'''

    def __init__(self):
        self.closed = False

    def close(self):
        '''Record the close.'''
        self.closed = True

    async def wait_closed(self):
        '''Fail the way a peer reset does.'''
        raise ConnectionResetError('peer went away')


class _BrokenReader:
    '''Reader double that fails on the first line.'''

    async def readline(self):
        '''Fail before any request is parsed.'''
        raise RuntimeError('socket exploded')


def _free_port() -> int:
    '''A currently unused localhost port.'''
    with socket.socket() as sock:
        sock.bind(('127.0.0.1', 0))
        return sock.getsockname()[1]


@pytest.mark.asyncio
async def test_close_writer_swallows_oserror():
    '''A reset during wait_closed does not escape; the writer is still closed.'''
    writer = _ClosingWriter()
    await close_writer(writer)
    assert writer.closed is True


@pytest.mark.asyncio
async def test_check_must_be_implemented():
    '''The base liveness and readiness checks are abstract.'''
    server = _Unchecked(1, '127.0.0.1')
    with pytest.raises(NotImplementedError):
        await server.liveness()
    with pytest.raises(NotImplementedError):
        await server.readiness()


@pytest.mark.asyncio
async def test_handler_error_is_logged_and_writer_closed(caplog):
    '''An exception while reading the request is swallowed, logged, and the writer closed.'''
    writer = _ClosingWriter()
    with caplog.at_level(logging.DEBUG):
        await _Healthy(1, '127.0.0.1').handle(_BrokenReader(), writer)
    assert writer.closed is True
    assert '_Healthy handler error: socket exploded' in caplog.text


@pytest.mark.asyncio
async def test_serve_answers_http_over_a_real_socket():
    '''serve() binds the port and answers a probe with the 200 JSON payload.'''
    port = _free_port()
    task = asyncio.create_task(_Healthy(port, '127.0.0.1').serve())
    try:
        await wait_for_port(port)
        reader, writer = await asyncio.open_connection('127.0.0.1', port)
        writer.write(b'GET /health HTTP/1.1\r\nHost: x\r\n\r\n')
        await writer.drain()
        raw = await reader.read()
        writer.close()
    finally:
        await cancel_task(task)
    head, _, body = raw.partition(b'\r\n\r\n')
    assert head.startswith(b'HTTP/1.1 200 OK')
    assert b'"who": "me"' in body
    assert b'"status": "ok"' in body
