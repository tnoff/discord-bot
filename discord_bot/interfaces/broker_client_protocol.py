'''
The cog-facing BrokerClient Protocol, on its own.

Split out of interfaces/broker_protocols.py so that annotating a broker handle
does not import the broker ENGINE.  broker_protocols also defines
MediaBrokerBase, whose class-level annotations pull VideoCacheClient
(sqlalchemy), and the module itself pulls integrations.s3 (boto3) — so every
module that only wanted this Protocol was dragging the engine's dependencies
into its process.  interfaces/download_protocols was doing exactly that, which
is how sqlalchemy reached the downloader pod.

Same split, and the same reason, as CheckoutResult and ClearGuildResult moving to
types/.  Re-exported from broker_protocols, so existing imports keep working.
'''
from typing import Protocol

from discord_bot.interfaces.player_session_store import PlayerSessionClient
from discord_bot.types.checkout_result import CheckoutResult
from discord_bot.types.download import DownloadResult, LifecycleStatusUpdate
from discord_bot.types.media_download import MediaDownload
from discord_bot.types.search_resolution import SearchResolution

__all__ = ['BrokerClient']


class BrokerClient(PlayerSessionClient, Protocol):
    '''
    Cog-facing handle for the MediaBroker.  Two implementations exist:

      InMemoryBrokerClient (clients/broker_client.py) — wraps a local
        MediaBrokerBase directly.  Used in single-process deployments.

      HttpBrokerClient (clients/broker_client.py) — forwards calls to a
        remote BrokerHttpServer over HTTP.  Used in HA deployments where
        the broker pod runs separately.

    Both shapes satisfy this Protocol; the cog only depends on the Protocol
    and lets config decide which impl is constructed.
    '''
    async def register_request(self, media_request) -> None:
        '''Register a new MediaRequest entering the pipeline.'''
    async def update_request_status(self, uuid: str, update: LifecycleStatusUpdate) -> None:
        '''Apply a lifecycle status update from the download worker.'''
    async def register_download(self, media_download: MediaDownload) -> None:
        '''Persist a downloaded MediaDownload on the broker (zone=AVAILABLE).'''
    async def register_download_result(self, result: DownloadResult) -> MediaDownload | None:
        '''Register a completed DownloadResult.  For success the broker stores the
        entry as AVAILABLE; in either case the result is pushed onto the bot-ready
        queue served by next_result.  Returns None — consumers build their own
        MediaDownload from the result they receive.'''
    async def next_result(self) -> DownloadResult | None:
        '''Pop the next bot-ready DownloadResult, or None if nothing is ready.
        Non-blocking — callers poll on their own cadence.'''
    async def register_search_result(self, resolution: SearchResolution) -> None:
        '''Push a resolved search onto the bot-ready search-result queue served
        by next_search_result.  Pure passthrough — the broker engine is not
        involved (search resolves nothing on the broker).'''
    async def next_search_result(self) -> SearchResolution | None:
        '''Pop the next bot-ready SearchResolution, or None if nothing is ready.
        Non-blocking — callers poll on their own cadence.'''
    async def checkout(self, uuid: str, guild_id: int, guild_path: str | None = None) -> CheckoutResult | None:
        '''Mark a request CHECKED_OUT; returns a CheckoutResult with local_path or s3_key set.'''
    async def release(self, uuid: str) -> None:
        '''Release a CHECKED_OUT entry and clean up the guild-specific file.'''
    async def remove(self, uuid: str) -> None:
        '''Remove an entry from the registry without touching any files.'''
    async def discard(self, uuid: str) -> None:
        '''Drop an entry that was registered but cannot be enqueued; deletes the
        underlying file unless a VideoCache is keeping it.'''
    async def prefetch(self, queue_items: list, guild_id: int, guild_path: str | None, limit: int) -> None:
        '''Pre-stage the next limit items from the queue to local disk.'''
    async def check_cache(self, media_request) -> MediaDownload | None:
        '''Look up a cached MediaDownload by webpage URL; returns None on miss.'''
    async def cache_cleanup(self) -> bool:
        '''Evict stale cache entries.  Returns True if at least one was removed.'''
    async def get_cache_count(self) -> int:
        '''Return the current VideoCache entry count (0 if no cache configured).'''
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
