'''
Cog-facing broker clients.

InMemoryBrokerClient wraps a local MediaBrokerBase (AsyncioBroker) for
single-process deployments; HttpBrokerClient forwards every call to a remote
BrokerHttpServer for HA deployments.  Both satisfy the BrokerClient Protocol
(interfaces/broker_protocols) so the cog depends only on the Protocol and lets
config decide which is constructed.
'''
import logging
from pathlib import Path

from discord_bot.interfaces.broker_protocols import (
    BrokerClient,
    CheckoutResult,
    DownloadResultQueue,
    SearchResultQueue,
    MediaBrokerBase,
)
from discord_bot.types.download import DownloadResult, LifecycleStatusUpdate
from discord_bot.clients.http_broker_client import HttpBrokerClient
from discord_bot.types.media_download import MediaDownload
from discord_bot.types.search_resolution import SearchResolution
from discord_bot.workers.asyncio_queues import AsyncioDownloadResultQueue, AsyncioSearchResultQueue

# Re-exported so `from discord_bot.clients.broker_client import CheckoutResult`
# / `BrokerClient` / `HttpBrokerClient` keep working.  Canonical homes are
# types/checkout_result.py, interfaces/broker_protocols.py and
# clients/http_broker_client.py respectively.
__all__ = ['BrokerClient', 'CheckoutResult', 'HttpBrokerClient', 'InMemoryBrokerClient']

logger = logging.getLogger(__name__)

class InMemoryBrokerClient: #pylint:disable=too-many-public-methods
    '''
    BrokerClient backed by a local MediaBroker instance.  Used when all
    components run in the same process.

    Wide by design: it implements the full BrokerClient Protocol (~19 methods)
    plus the result_queue / search_result_queue / local_broker accessors an
    embedded BrokerHttpServer and the cog's depth gauges read — so it trips
    too-many-public-methods, disabled here as on the Music cog.

    The internal result_queue holds DownloadResults reported by the local
    DownloadClient so next_result can hand them off to the cog's
    process_download_results router.  Pass an explicit queue if it needs to
    be shared with an embedded BrokerHttpServer (so external download workers
    POSTing in land on the same queue the cog drains).
    '''
    def __init__(self, broker: MediaBrokerBase,
                 result_queue: DownloadResultQueue | None = None,
                 search_result_queue: SearchResultQueue | None = None):
        self._broker = broker
        self._result_queue: DownloadResultQueue = (
            result_queue if result_queue is not None else AsyncioDownloadResultQueue()
        )
        self._search_result_queue: SearchResultQueue = (
            search_result_queue if search_result_queue is not None else AsyncioSearchResultQueue()
        )

    @property
    def result_queue(self) -> DownloadResultQueue:
        '''Internal queue exposed so an embedded BrokerHttpServer can share it.'''
        return self._result_queue

    @property
    def search_result_queue(self) -> SearchResultQueue:
        '''Internal search-result queue, exposed so an embedded BrokerHttpServer
        can share it (and so the cog's queue-depth gauge can read it).'''
        return self._search_result_queue

    @property
    def local_broker(self) -> MediaBrokerBase:
        '''The wrapped MediaBrokerBase instance.

        Only meaningful in single-process mode — there is no equivalent on
        HttpBrokerClient because HA mode doesn't host a local broker.  The cog
        uses this to attach an embedded BrokerHttpServer for external workers.
        '''
        return self._broker

    async def register_request(self, media_request) -> None:
        '''Delegate to broker.register_request.'''
        await self._broker.register_request(media_request)

    async def update_request_status(self, uuid: str, update: LifecycleStatusUpdate) -> None:
        '''Delegate to broker.update_request_status.'''
        await self._broker.update_request_status(uuid, update)

    async def register_download_result(self, result: DownloadResult) -> MediaDownload | None:
        '''Persist a successful DownloadResult on the local broker (zone=AVAILABLE)
        and push the raw result onto the bot-ready queue for next_result.

        Results without file_name (e.g. PlaylistAddRequest results that only
        carry metadata) are queued for the cog to route but not persisted —
        there's no media file to track.'''
        if result.status.success and result.file_name is not None:
            await self._broker.register_download_result(result)
        await self._result_queue.put(result)
        return None

    async def next_result(self) -> DownloadResult | None:
        '''Pop the next ready DownloadResult; returns None if the queue is empty.'''
        return await self._result_queue.get_nowait()

    async def register_search_result(self, resolution: SearchResolution) -> None:
        '''Push a resolved search onto the local bot-ready queue for
        next_search_result.  No broker-engine call — search is passthrough.'''
        await self._search_result_queue.put(resolution)

    async def next_search_result(self) -> SearchResolution | None:
        '''Pop the next ready SearchResolution; None if the queue is empty.'''
        return await self._search_result_queue.get_nowait()

    async def checkout(self, uuid: str, guild_id: int, guild_path: str | None = None) -> CheckoutResult | None:
        '''Delegate to broker.checkout, which already returns a CheckoutResult.'''
        return await self._broker.checkout(uuid, guild_id, Path(guild_path) if guild_path else None)

    async def release(self, uuid: str) -> None:
        '''Delegate to broker.release.'''
        await self._broker.release(uuid)

    async def remove(self, uuid: str) -> None:
        '''Delegate to broker.remove.'''
        await self._broker.remove(uuid)

    async def discard(self, uuid: str) -> None:
        '''Delegate to broker.discard.'''
        await self._broker.discard(uuid)

    async def register_download(self, media_download: MediaDownload) -> None:
        '''Delegate to broker.register_download.'''
        await self._broker.register_download(media_download)

    async def check_cache(self, media_request) -> MediaDownload | None:
        '''Delegate to broker.check_cache.'''
        return await self._broker.check_cache(media_request)

    async def cache_cleanup(self) -> bool:
        '''Delegate to broker.cache_cleanup.'''
        return await self._broker.cache_cleanup()

    async def get_cache_count(self) -> int:
        '''Delegate to broker.get_cache_count.'''
        return await self._broker.get_cache_count()

    async def prefetch(self, queue_items: list, guild_id: int, guild_path: str | None, limit: int) -> None:
        '''Delegate to broker.prefetch.'''
        await self._broker.prefetch(queue_items, guild_id, Path(guild_path) if guild_path else None, limit)

    async def create_bundle(self, guild_id: int, channel_id: int,
                            input_string: str | None = None,
                            has_search_banner: bool = False) -> str:
        '''Delegate to broker.create_bundle.'''
        return await self._broker.create_bundle(
            guild_id, channel_id,
            input_string=input_string,
            has_search_banner=has_search_banner,
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
