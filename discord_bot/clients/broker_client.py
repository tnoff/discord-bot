from pathlib import Path
from typing import Protocol

import aiohttp

from discord_bot.interfaces.broker_protocols import CheckoutResult, MediaBrokerBase
from discord_bot.types.download import DownloadResult, LifecycleStatusUpdate
from discord_bot.types.media_request import MediaRequest
from discord_bot.types.media_download import MediaDownload
from discord_bot.clients.http_client_base import HttpClientMixin


class BrokerClient(Protocol):
    '''
    Interface for interacting with the MediaBroker.

    Two implementations exist:
      InMemoryBrokerClient  — wraps MediaBroker directly (same process)
      HttpBrokerClient      — forwards calls to BrokerHttpServer over HTTP
    '''
    async def update_request_status(self, uuid: str, update: LifecycleStatusUpdate) -> None:
        '''Apply a lifecycle status update from the download worker.'''
    async def register_download_result(self, result: DownloadResult) -> MediaDownload | None:
        '''Register a completed DownloadResult; returns a MediaDownload or None for HTTP clients.'''
    async def checkout(self, uuid: str, guild_id: int, guild_path: str | None = None) -> CheckoutResult | None:
        '''Mark a request CHECKED_OUT; returns a CheckoutResult with local_path or s3_key set.'''
    async def release(self, uuid: str) -> None:
        '''Release a CHECKED_OUT entry and clean up the guild-specific file.'''
    async def prefetch(self, queue_items: list, guild_id: int, guild_path: str | None, limit: int) -> None:
        '''Pre-stage the next limit items from the queue to local disk.'''
    async def create_bundle(self, guild_id: int, channel_id: int,
                            input_string: str | None = None,
                            has_search_banner: bool = False) -> str:
        '''Create a new bundle on the broker; returns the bundle uuid.'''
    async def finalize_bundle(self, bundle_uuid: str) -> None:
        '''Lock pagination and trigger the first full render of a bundle.'''
    async def delete_bundle(self, bundle_uuid: str) -> None:
        '''Tear down a bundle (broker also drops its Discord messages).'''
    async def list_bundles_for_guild(self, guild_id: int) -> list[str]:
        '''Return uuids of every bundle currently stored for this guild.'''


class InMemoryBrokerClient:
    '''
    BrokerClient backed by a local MediaBrokerBase engine (AsyncioBroker).
    Used when all components run in the same process.
    '''
    def __init__(self, broker: MediaBrokerBase):
        self._broker = broker

    async def update_request_status(self, uuid: str, update: LifecycleStatusUpdate) -> None:
        '''Delegate to broker.update_request_status.'''
        await self._broker.update_request_status(uuid, update)

    async def register_download_result(self, result: DownloadResult) -> MediaDownload | None:
        '''Delegate to broker.register_download_result.'''
        return await self._broker.register_download_result(result)

    async def checkout(self, uuid: str, guild_id: int, guild_path: str | None = None) -> CheckoutResult | None:
        '''Delegate to broker.checkout, which already returns a CheckoutResult.'''
        return await self._broker.checkout(uuid, guild_id, Path(guild_path) if guild_path else None)

    async def release(self, uuid: str) -> None:
        '''Delegate to broker.release.'''
        await self._broker.release(uuid)

    async def prefetch(self, queue_items: list, guild_id: int, guild_path: str | None, limit: int) -> None:
        '''Delegate to broker.prefetch.'''
        await self._broker.prefetch(queue_items, guild_id, Path(guild_path) if guild_path else None, limit)

    async def create_bundle(self, guild_id: int, channel_id: int,
                            input_string: str | None = None,
                            has_search_banner: bool = False) -> str:
        '''Delegate to broker.create_bundle.'''
        return await self._broker.create_bundle(
            guild_id, channel_id,
            input_string=input_string, has_search_banner=has_search_banner,
        )

    async def finalize_bundle(self, bundle_uuid: str) -> None:
        '''Delegate to broker.finalize_bundle.'''
        await self._broker.finalize_bundle(bundle_uuid)

    async def delete_bundle(self, bundle_uuid: str) -> None:
        '''Delegate to broker.delete_bundle.'''
        await self._broker.delete_bundle(bundle_uuid)

    async def list_bundles_for_guild(self, guild_id: int) -> list[str]:
        '''Delegate to broker.list_bundles_for_guild.'''
        return await self._broker.list_bundles_for_guild(guild_id)


class HttpBrokerClient(HttpClientMixin):
    '''
    BrokerClient that forwards calls to a remote BrokerHttpServer over HTTP.
    Used when the broker runs in a separate process.
    '''
    def __init__(self, base_url: str, bucket_name: str | None = None,
                 session: aiohttp.ClientSession | None = None):
        self._base_url = base_url.rstrip('/')
        self._bucket_name = bucket_name
        self._session = session

    async def register_request(self, media_request: MediaRequest) -> None:
        '''POST /requests/{uuid} — register a new MediaRequest with the remote broker.'''
        await self._http('POST', f'{self._base_url}/requests/{media_request.uuid}',
                         media_request.model_dump(mode='json'))

    async def update_request_status(self, uuid: str, update: LifecycleStatusUpdate) -> None:
        '''PUT /requests/{uuid}/status.'''
        await self._http('PUT', f'{self._base_url}/requests/{uuid}/status', update.model_dump())

    async def register_download_result(self, result: DownloadResult) -> MediaDownload | None:
        '''POST /downloads — returns None; the broker processes the result server-side.'''
        await self._http('POST', f'{self._base_url}/downloads', result.model_dump(mode='json'))
        return None

    async def checkout(self, uuid: str, guild_id: int, guild_path: str | None = None) -> CheckoutResult | None:
        '''POST /requests/{uuid}/checkout — returns a CheckoutResult or None.

        Non-HA brokers stage the file themselves and respond with guild_file_path
        (→ CheckoutResult(local_path=...)). HA brokers respond with an s3_key
        (→ CheckoutResult(s3_key=..., bucket_name=...)) and leave the S3 download
        to the caller (MusicPlayer).'''
        body: dict = {'guild_id': guild_id}
        if guild_path:
            body['guild_path'] = guild_path
        data = await self._http('POST', f'{self._base_url}/requests/{uuid}/checkout', body)
        if not data:
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
        await self._http('POST', f'{self._base_url}/requests/{uuid}/release')

    async def prefetch(self, queue_items: list, guild_id: int, guild_path: str | None, limit: int) -> None:
        '''POST /prefetch — sends UUIDs extracted from queue_items.'''
        uuids = [str(item.media_request.uuid) for item in queue_items]
        await self._http('POST', f'{self._base_url}/prefetch', {
            'uuids': uuids, 'guild_id': guild_id, 'guild_path': guild_path, 'limit': limit,
        })
