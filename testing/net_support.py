'''Start and stop the background servers the tests drive: wait for a port, cancel a task.'''
import asyncio

from discord_core.servers.health_server_base import close_writer


async def wait_for_port(port: int, *, host: str = '127.0.0.1', timeout: float = 5.0,
                        interval: float = 0.02) -> None:
    '''Poll until the port accepts a TCP connection, or raise the last OSError at the timeout.'''
    deadline = asyncio.get_event_loop().time() + timeout
    while True:
        try:
            _, writer = await asyncio.open_connection(host, port)
            await close_writer(writer)
            return
        except OSError:
            if asyncio.get_event_loop().time() >= deadline:
                raise
            await asyncio.sleep(interval)


async def cancel_task(task: asyncio.Task) -> None:
    '''Cancel a background task and wait for it to finish, swallowing its CancelledError.'''
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass


async def raw_request(port: int, path: str = '/health') -> str:
    """Open a raw TCP connection and send a minimal HTTP GET, return the full response."""
    reader, writer = await asyncio.open_connection('127.0.0.1', port)
    writer.write(f'GET {path} HTTP/1.1\r\nHost: localhost\r\n\r\n'.encode())
    await writer.drain()
    response = b''
    try:
        response = await asyncio.wait_for(reader.read(4096), timeout=3)
    finally:
        await close_writer(writer)
    return response.decode()
