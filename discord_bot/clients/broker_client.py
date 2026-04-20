import asyncio
from pathlib import Path
from typing import Protocol

import aiohttp

from discord_bot.cogs.music_helpers.media_broker import MediaBroker
from discord_bot.types.download import DownloadResult, DownloadStatusUpdate
from discord_bot.types.media_download import MediaDownload
from discord_bot.clients.http_client_base import HttpClientMixin


class BrokerClient(Protocol):
    '''
    Interface for interacting with the MediaBroker.

    Two implementations exist:
      InMemoryBrokerClient  — wraps MediaBroker directly (same process)
      HttpBrokerClient      — forwards calls to BrokerHttpServer over HTTP
    '''
    async def register_request(self, media_request) -> None:
        '''Register a new MediaRequest entering the pipeline.'''
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

    result_queue: asyncio.Queue that completed DownloadResults are pushed onto so
    that Music.process_download_results can consume them. The queue is injected here
    (and into BrokerHttpServer) rather than held by DownloadClient, which means the
    same routing path works whether the download worker is in-process or remote.
    '''
    def __init__(self, broker: MediaBroker, result_queue: asyncio.Queue):
        self._broker = broker
        self._result_queue = result_queue

    async def register_request(self, media_request) -> None:
        '''Delegate to broker.register_request.'''
        await self._broker.register_request(media_request)

    async def update_request_status(self, uuid: str, update: DownloadStatusUpdate) -> None:
        '''Delegate to broker.update_request_status.'''
        await self._broker.update_request_status(uuid, update)

    async def register_download_result(self, result: DownloadResult) -> MediaDownload | None:
        '''Enqueue the raw DownloadResult for Music.process_download_results to route.'''
        self._result_queue.put_nowait(result)
        return None

    async def checkout(self, uuid: str, guild_id: int, guild_path: str | None = None) -> str | None:
        '''Delegate to broker.checkout, converting path to/from str.'''
        path = await self._broker.checkout(uuid, guild_id, Path(guild_path) if guild_path else None)
        return str(path) if path else None

    async def release(self, uuid: str) -> None:
        '''Delegate to broker.release.'''
        await self._broker.release(uuid)

    async def prefetch(self, queue_items: list, guild_id: int, guild_path: str | None, limit: int) -> None:
        '''Delegate to broker.prefetch.'''
        await self._broker.prefetch(queue_items, guild_id, Path(guild_path) if guild_path else None, limit)


class HttpBrokerClient(HttpClientMixin):
    '''
    BrokerClient that forwards calls to a remote BrokerHttpServer over HTTP.
    Used when the broker runs in a separate process.
    '''
    def __init__(self, base_url: str, session: aiohttp.ClientSession | None = None):
        self._base_url = base_url.rstrip('/')
        self._session = session

    async def register_request(self, media_request) -> None:
        '''POST /requests/{uuid} — register a new MediaRequest with the remote broker.'''
        await self._http('POST', f'{self._base_url}/requests/{media_request.uuid}',
                         media_request.serialize())

    async def update_request_status(self, uuid: str, update: DownloadStatusUpdate) -> None:
        '''PUT /requests/{uuid}/status.'''
        await self._http('PUT', f'{self._base_url}/requests/{uuid}/status', update.model_dump())

    async def register_download_result(self, result: DownloadResult) -> MediaDownload | None:
        '''POST /downloads — returns None; the broker processes the result server-side.'''
        await self._http('POST', f'{self._base_url}/downloads', result.model_dump(mode='json'))
        return None

    async def checkout(self, uuid: str, guild_id: int, guild_path: str | None = None) -> str | None:
        '''POST /requests/{uuid}/checkout — returns staged file path string or None.'''
        body: dict = {'guild_id': guild_id}
        if guild_path:
            body['guild_path'] = guild_path
        data = await self._http('POST', f'{self._base_url}/requests/{uuid}/checkout', body)
        return data.get('guild_file_path') if data else None

    async def release(self, uuid: str) -> None:
        '''POST /requests/{uuid}/release.'''
        await self._http('POST', f'{self._base_url}/requests/{uuid}/release')

    async def prefetch(self, queue_items: list, guild_id: int, guild_path: str | None, limit: int) -> None:
        '''POST /prefetch — sends UUIDs extracted from queue_items.'''
        uuids = [str(item.media_request.uuid) for item in queue_items]
        await self._http('POST', f'{self._base_url}/prefetch', {
            'uuids': uuids, 'guild_id': guild_id, 'guild_path': guild_path, 'limit': limit,
        })
