'''
Asyncio BrokerClient: a thin wrapper around a MediaBrokerBase.

A test double only.
The pod-facing implementation is HttpBrokerClient (clients/http_broker_client.py);
music.broker_client is required config, so nothing a deployment can be
configured with reaches this class. tests.helpers.attach_in_process_broker
builds this stack so the suite can drive real broker behaviour without
standing up a pod behind an aiohttp server.

It satisfies the BrokerClient Protocol (interfaces/broker_client_protocol),
and must keep satisfying it: a double that drifts from the Protocol takes the
meaning out of every test built on it rather than failing them.

The guild queue half is NOT a second implementation. It delegates to the real
GuildQueueBroker (Lua scripts and all, on fakeredis), so a test of the gateway
exercises the same queue the broker pod runs, and only the HTTP hop is missing.
That hop is what tests/clients/test_asyncio_guild_queue_contract.py checks: the same
scenario through this class and through HttpBrokerClient -> BrokerHttpServer must give
identical answers.
'''
import logging

from discord_core.types.download import DownloadResult, LifecycleStatusUpdate
from discord_core.types.media_download import MediaDownload
from discord_core.types.search_resolution import SearchResolution

from discord_core.types.guild_queue import ClaimedDownload, GuildQueueSnapshot, PlayingSnapshot
from discord_core.types.player_session import PlayerSession

from discord_broker.interfaces.broker_protocols import (
    CheckoutResult,
    DownloadResultQueue,
    SearchResultQueue,
    MediaBrokerBase,
)
from tests.fakes.asyncio_queues import AsyncioDownloadResultQueue, AsyncioSearchResultQueue

logger = logging.getLogger(__name__)


class AsyncioBrokerClient: #pylint:disable=too-many-public-methods
    '''
    BrokerClient backed by a local MediaBroker instance.  Used when all
    components run in the same process.

    Wide by design: it implements the full BrokerClient Protocol (~19 methods)
    plus the result_queue / search_result_queue / local_broker accessors tests
    read to reach the engine directly — so it trips too-many-public-methods,
    disabled here as on the Music cog.

    The internal result_queue holds DownloadResults reported by the local
    DownloadClient so next_result can hand them off to the cog's
    process_download_results router.
    '''
    def __init__(self, broker: MediaBrokerBase,
                 result_queue: DownloadResultQueue | None = None,
                 search_result_queue: SearchResultQueue | None = None,
                 guild_queue=None):
        '''
        guild_queue : a GuildQueueBroker over the same engine as `broker`.  Without one the
                      guild-queue methods raise, so a test that did not ask for a queue cannot
                      silently get an empty one.
        '''
        self._broker = broker
        self._guild_queue = guild_queue
        self._result_queue: DownloadResultQueue = (
            result_queue if result_queue is not None else AsyncioDownloadResultQueue()
        )
        self._search_result_queue: SearchResultQueue = (
            search_result_queue if search_result_queue is not None else AsyncioSearchResultQueue()
        )

    @property
    def result_queue(self) -> DownloadResultQueue:
        '''Internal queue, exposed so a test can hand it to a BrokerHttpServer.'''
        return self._result_queue

    @property
    def search_result_queue(self) -> SearchResultQueue:
        '''Internal search-result queue, exposed so a test can hand it to a
        BrokerHttpServer.'''
        return self._search_result_queue

    @property
    def local_broker(self) -> MediaBrokerBase:
        '''The wrapped MediaBrokerBase instance.

        Test-only: there is no equivalent on HttpBrokerClient, which talks to a
        broker pod rather than hosting an engine.
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

    async def checkout(self, uuid: str, guild_id: int) -> CheckoutResult | None:
        '''Delegate to broker.checkout, which already returns a CheckoutResult.'''
        return await self._broker.checkout(uuid, guild_id)

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

    async def save_player_session(self, session: PlayerSession) -> None:
        '''Delegate to broker.save_player_session.'''
        await self._broker.save_player_session(session)

    async def list_player_sessions(self) -> list[PlayerSession]:
        '''Delegate to broker.list_player_sessions.'''
        return await self._broker.list_player_sessions()

    async def delete_player_session(self, guild_id: int) -> None:
        '''Delegate to broker.delete_player_session.'''
        await self._broker.delete_player_session(guild_id)

    # ------------------------------------------------------------------
    # Guild queue: delegates to the real GuildQueueBroker, converting its
    # BrokerEntry results into the views HttpBrokerClient returns.
    # ------------------------------------------------------------------

    @property
    def guild_queue(self):
        '''The GuildQueueBroker behind the guild-queue methods.

        Test-only, like local_broker: lets a test reach the engine (to expire a heartbeat, say).'''
        return self._queue()

    def _queue(self):
        if self._guild_queue is None:
            raise RuntimeError('This AsyncioBrokerClient was built without a guild_queue; '
                               'pass one (tests.helpers.attach_in_process_broker does)')
        return self._guild_queue

    async def enqueue_track(self, guild_id: int, uuid: str, max_size: int = 0) -> str:
        '''Delegate to GuildQueueBroker.enqueue.'''
        return await self._queue().enqueue(guild_id, uuid, max_size)

    async def get_guild_queue(self, guild_id: int) -> GuildQueueSnapshot:
        '''Read the queue and convert its entries to MediaDownloads.'''
        queue = await self._queue().get_queue(guild_id)
        playing = None
        if queue.playing:
            entry = queue.playing.entry
            playing = PlayingSnapshot(
                uuid=queue.playing.uuid, started_at=queue.playing.started_at,
                gateway_id=queue.playing.gateway_id,
                download=entry.download if entry else None)
        return GuildQueueSnapshot(
            version=queue.version,
            items=[entry.download for entry in queue.items if entry.download],
            playing=playing, skip_for=queue.skip_for, closed=queue.closed,
            text_channel_id=queue.text_channel_id)

    async def remove_queued_track(self, guild_id: int, uuid: str) -> MediaDownload | None:
        '''Delegate to GuildQueueBroker.remove; the removed track, or None.'''
        entry = await self._queue().remove(guild_id, uuid)
        return entry.download if entry else None

    async def bump_queued_track(self, guild_id: int, uuid: str) -> MediaDownload | None:
        '''Delegate to GuildQueueBroker.bump; the bumped track, or None.'''
        entry = await self._queue().bump(guild_id, uuid)
        return entry.download if entry else None

    async def shuffle_queue(self, guild_id: int) -> bool:
        '''Delegate to GuildQueueBroker.shuffle.'''
        return await self._queue().shuffle(guild_id)

    async def clear_queue(self, guild_id: int) -> int:
        '''Delegate to GuildQueueBroker.clear.'''
        return await self._queue().clear(guild_id)

    async def poll_guild_queue(self, guild_id: int, since: int | None = None) -> tuple[int, str | None] | None:
        '''Same contract as the HTTP poll: None when nothing changed since `since`.'''
        version, skip_for = await self._queue().poll(guild_id)
        if since is not None and since == version and skip_for is None:
            return None
        return version, skip_for

    async def claim_next_track(self, guild_id: int, gateway_id: str) -> ClaimedDownload | None:
        '''Delegate to GuildQueueBroker.claim_next.'''
        claimed = await self._queue().claim_next(guild_id, gateway_id)
        if claimed is None:
            return None
        return ClaimedDownload(download=claimed.entry.download, checkout=claimed.checkout)

    async def playing_heartbeat(self, guild_id: int, uuid: str) -> bool:
        '''Delegate to GuildQueueBroker.heartbeat.'''
        return await self._queue().heartbeat(guild_id, uuid)

    async def skip_track(self, guild_id: int, uuid: str) -> str:
        '''Delegate to GuildQueueBroker.skip.'''
        return await self._queue().skip(guild_id, uuid)

    async def finish_track(self, guild_id: int, uuid: str, skipped: bool, history_cap: int) -> None:
        '''Delegate to GuildQueueBroker.finish.'''
        await self._queue().finish(guild_id, uuid, skipped, history_cap)

    async def get_guild_history(self, guild_id: int) -> list[dict]:
        '''Delegate to GuildQueueBroker.get_history.'''
        return await self._queue().get_history(guild_id)

    async def close_guild(self, guild_id: int) -> int:
        '''Delegate to GuildQueueBroker.close.'''
        return await self._queue().close(guild_id)

    async def open_guild(self, guild_id: int, text_channel_id: int) -> str | None:
        '''Delegate to GuildQueueBroker.open.'''
        return await self._queue().open(guild_id, text_channel_id)
