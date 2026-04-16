from pathlib import Path
from typing import Protocol

import aiohttp
from opentelemetry.propagate import inject

from discord_bot.cogs.music_helpers.media_broker import MediaBroker
from discord_bot.types.download import DownloadResult, DownloadStatusUpdate
from discord_bot.types.media_download import MediaDownload
from discord_bot.utils.discord_retry import async_retry_broker_command


class BrokerClient(Protocol):
    '''
    Interface for interacting with the MediaBroker.

    Two implementations exist:
      InMemoryBrokerClient  — wraps MediaBroker directly (same process)
      HttpBrokerClient      — forwards calls to BrokerHttpServer over HTTP
    '''
    async def update_request_status(self, uuid: str, update: DownloadStatusUpdate) -> None:
        '''Apply a lifecycle status update from the download worker.'''
    async def register_download_result(self, result: DownloadResult) -> MediaDownload | None:
        '''Register a completed DownloadResult; returns a MediaDownload or None for HTTP clients.'''
    async def checkout(self, uuid: str, guild_id: int, guild_path: str | None = None) -> str | None:
        '''Mark a request CHECKED_OUT; optionally stage the file and return the path string.'''
    async def release(self, uuid: str) -> None:
        '''Release a CHECKED_OUT entry and clean up the guild-specific file.'''
    async def prefetch(self, queue_items: list, guild_id: int, guild_path: str | None, limit: int) -> None:
        '''Pre-stage the next limit items from the queue to local disk.'''


class InMemoryBrokerClient:
    '''
    BrokerClient backed by a local MediaBroker instance.
    Used when all components run in the same process.
    '''
    def __init__(self, broker: MediaBroker):
        self._broker = broker

    async def update_request_status(self, uuid: str, update: DownloadStatusUpdate) -> None:
        '''Delegate to broker.update_request_status.'''
        self._broker.update_request_status(uuid, update)

    async def register_download_result(self, result: DownloadResult) -> MediaDownload | None:
        '''Delegate to broker.register_download_result.'''
        return await self._broker.register_download_result(result)

    async def checkout(self, uuid: str, guild_id: int, guild_path: str | None = None) -> str | None:
        '''Delegate to broker.checkout, converting path to/from str.'''
        path = self._broker.checkout(uuid, guild_id, Path(guild_path) if guild_path else None)
        return str(path) if path else None

    async def release(self, uuid: str) -> None:
        '''Delegate to broker.release.'''
        self._broker.release(uuid)

    async def prefetch(self, queue_items: list, guild_id: int, guild_path: str | None, limit: int) -> None:
        '''Delegate to broker.prefetch.'''
        self._broker.prefetch(queue_items, guild_id, Path(guild_path) if guild_path else None, limit)


class HttpBrokerClient:
    '''
    BrokerClient that forwards calls to a remote BrokerHttpServer over HTTP.
    Used when the broker runs in a separate process.
    '''
    def __init__(self, base_url: str, session: aiohttp.ClientSession | None = None):
        self._base_url = base_url.rstrip('/')
        self._session = session

    def _get_session(self) -> aiohttp.ClientSession:
        '''Return the shared session, creating it lazily on first use.'''
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession()
        return self._session

    async def close(self) -> None:
        '''Close the underlying aiohttp session.'''
        if self._session and not self._session.closed:
            await self._session.close()

    def _trace_headers(self) -> dict[str, str]:
        '''Return headers dict with W3C traceparent injected from the active span, if any.'''
        headers: dict[str, str] = {}
        inject(headers)
        return headers

    async def update_request_status(self, uuid: str, update: DownloadStatusUpdate) -> None:
        '''PUT /requests/{uuid}/status.'''
        session = self._get_session()
        async def _call():
            async with session.put(
                f'{self._base_url}/requests/{uuid}/status',
                headers=self._trace_headers(),
                json=update.model_dump(),
            ) as resp:
                resp.raise_for_status()
        await async_retry_broker_command(_call)

    async def register_download_result(self, result: DownloadResult) -> MediaDownload | None:
        '''POST /downloads — returns None; the broker processes the result server-side.'''
        session = self._get_session()
        async def _call():
            async with session.post(
                f'{self._base_url}/downloads',
                headers=self._trace_headers(),
                json=result.model_dump(mode='json'),
            ) as resp:
                resp.raise_for_status()
        await async_retry_broker_command(_call)
        return None

    async def checkout(self, uuid: str, guild_id: int, guild_path: str | None = None) -> str | None:
        '''POST /requests/{uuid}/checkout — returns staged file path string or None.'''
        session = self._get_session()
        body: dict = {'guild_id': guild_id}
        if guild_path:
            body['guild_path'] = guild_path
        async def _call():
            async with session.post(
                f'{self._base_url}/requests/{uuid}/checkout',
                headers=self._trace_headers(),
                json=body,
            ) as resp:
                resp.raise_for_status()
                return await resp.json()
        data = await async_retry_broker_command(_call)
        return data.get('guild_file_path')

    async def release(self, uuid: str) -> None:
        '''POST /requests/{uuid}/release.'''
        session = self._get_session()
        async def _call():
            async with session.post(
                f'{self._base_url}/requests/{uuid}/release',
                headers=self._trace_headers(),
            ) as resp:
                resp.raise_for_status()
        await async_retry_broker_command(_call)

    async def prefetch(self, queue_items: list, guild_id: int, guild_path: str | None, limit: int) -> None:
        '''POST /prefetch — sends UUIDs extracted from queue_items.'''
        session = self._get_session()
        uuids = [str(item.media_request.uuid) for item in queue_items]
        body: dict = {
            'uuids': uuids,
            'guild_id': guild_id,
            'guild_path': guild_path,
            'limit': limit,
        }
        async def _call():
            async with session.post(
                f'{self._base_url}/prefetch',
                headers=self._trace_headers(),
                json=body,
            ) as resp:
                resp.raise_for_status()
        await async_retry_broker_command(_call)
