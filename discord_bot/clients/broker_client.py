import asyncio
from dataclasses import dataclass
from pathlib import Path
from typing import Protocol

import aiohttp
from opentelemetry.trace import SpanKind

from discord_bot.interfaces.broker_protocols import MediaBrokerBase
from discord_bot.types.download import DownloadResult, DownloadStatusUpdate
from discord_bot.types.media_request import MediaRequest
from discord_bot.types.media_download import MediaDownload
from discord_bot.clients.http_client_base import HttpClientMixin
from discord_bot.utils.otel import async_otel_span_wrapper


@dataclass
class CheckoutResult:
    '''
    Result of a broker checkout operation.

    Exactly one of local_path or s3_key will be set. local_path means the file
    is already staged on local disk and ready to play. s3_key means the file
    lives in S3; bucket_name is set alongside it so the caller can download
    without needing separate S3 configuration.
    '''
    local_path: Path | None = None
    s3_key: str | None = None
    bucket_name: str | None = None


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
    async def checkout(self, uuid: str, guild_id: int, guild_path: str | None = None) -> CheckoutResult | None:
        '''Mark a request CHECKED_OUT; returns a CheckoutResult with local_path or s3_key set.'''
    async def release(self, uuid: str) -> None:
        '''Release a CHECKED_OUT entry and clean up the guild-specific file.'''
    async def remove(self, uuid: str) -> None:
        '''Remove an entry from the registry without touching any files.'''
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
    def __init__(self, broker: MediaBrokerBase, result_queue: asyncio.Queue):
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

    async def checkout(self, uuid: str, guild_id: int, guild_path: str | None = None) -> CheckoutResult | None:
        '''Delegate to broker.checkout; wraps the local path in CheckoutResult.'''
        path = await self._broker.checkout(uuid, guild_id, Path(guild_path) if guild_path else None)
        if path is None:
            return None
        return CheckoutResult(local_path=path)

    async def release(self, uuid: str) -> None:
        '''Delegate to broker.release.'''
        await self._broker.release(uuid)

    async def remove(self, uuid: str) -> None:
        '''Delegate to broker.remove.'''
        await self._broker.remove(uuid)

    async def prefetch(self, queue_items: list, guild_id: int, guild_path: str | None, limit: int) -> None:
        '''Delegate to broker.prefetch.'''
        await self._broker.prefetch(queue_items, guild_id, Path(guild_path) if guild_path else None, limit)


class HttpBrokerClient(HttpClientMixin):
    '''
    BrokerClient that forwards calls to a remote BrokerHttpServer over HTTP.
    Used when the broker runs in a separate process.

    In HA mode checkout returns a CheckoutResult with s3_key set. The caller
    (MusicPlayer) is responsible for downloading the file from S3 before playback.
    '''
    def __init__(self, base_url: str, bucket_name: str | None = None,
                 session: aiohttp.ClientSession | None = None):
        self._base_url = base_url.rstrip('/')
        self._bucket_name = bucket_name
        self._session = session

    async def register_request(self, media_request: MediaRequest) -> None:
        '''POST /requests/{uuid} — register a new MediaRequest with the remote broker.'''
        async with async_otel_span_wrapper(
            'broker.register_request', kind=SpanKind.CLIENT,
            attributes={'music.media_request.uuid': str(media_request.uuid)},
        ):
            await self._http('POST', f'{self._base_url}/requests/{media_request.uuid}',
                             media_request.model_dump(mode='json'))

    async def update_request_status(self, uuid: str, update: DownloadStatusUpdate) -> None:
        '''PUT /requests/{uuid}/status.'''
        async with async_otel_span_wrapper(
            'broker.update_status', kind=SpanKind.CLIENT,
            attributes={'music.media_request.uuid': uuid},
        ):
            await self._http('PUT', f'{self._base_url}/requests/{uuid}/status', update.model_dump())

    async def register_download_result(self, result: DownloadResult) -> MediaDownload | None:
        '''POST /downloads — returns None; the broker processes the result server-side.'''
        async with async_otel_span_wrapper('broker.register_download', kind=SpanKind.CLIENT):
            await self._http('POST', f'{self._base_url}/downloads', result.model_dump(mode='json'))
        return None

    async def checkout(self, uuid: str, guild_id: int, guild_path: str | None = None) -> CheckoutResult | None:
        '''
        POST /requests/{uuid}/checkout — returns a CheckoutResult or None.

        In non-HA mode the broker stages the file itself and responds with guild_file_path;
        returns CheckoutResult(local_path=...). In HA mode the broker responds with an
        s3_key; returns CheckoutResult(s3_key=...) and leaves the S3 download to the caller.
        '''
        body: dict = {'guild_id': guild_id}
        if guild_path:
            body['guild_path'] = guild_path
        async with async_otel_span_wrapper(
            'broker.checkout', kind=SpanKind.CLIENT,
            attributes={'music.media_request.uuid': uuid, 'music.guild_id': guild_id},
        ):
            data = await self._http('POST', f'{self._base_url}/requests/{uuid}/checkout', body)
            if data is None:
                return None
            s3_key = data.get('s3_key')
            if s3_key:
                return CheckoutResult(s3_key=s3_key, bucket_name=self._bucket_name)
            guild_file_path = data.get('guild_file_path')
            if guild_file_path:
                return CheckoutResult(local_path=Path(guild_file_path))
            return None

    async def release(self, uuid: str) -> None:
        '''POST /requests/{uuid}/release.'''
        async with async_otel_span_wrapper(
            'broker.release', kind=SpanKind.CLIENT,
            attributes={'music.media_request.uuid': uuid},
        ):
            await self._http('POST', f'{self._base_url}/requests/{uuid}/release')

    async def remove(self, uuid: str) -> None:
        '''POST /requests/{uuid}/remove.'''
        async with async_otel_span_wrapper(
            'broker.remove', kind=SpanKind.CLIENT,
            attributes={'music.media_request.uuid': uuid},
        ):
            await self._http('POST', f'{self._base_url}/requests/{uuid}/remove')

    async def prefetch(self, queue_items: list, guild_id: int, guild_path: str | None, limit: int) -> None:
        '''POST /prefetch — sends UUIDs extracted from queue_items.'''
        uuids = [str(item.media_request.uuid) for item in queue_items]
        async with async_otel_span_wrapper(
            'broker.prefetch', kind=SpanKind.CLIENT,
            attributes={'music.guild_id': guild_id, 'music.prefetch_limit': limit},
        ):
            await self._http('POST', f'{self._base_url}/prefetch', {
                'uuids': uuids, 'guild_id': guild_id, 'guild_path': guild_path, 'limit': limit,
            })
