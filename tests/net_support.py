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
