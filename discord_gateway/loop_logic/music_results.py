from asyncio import QueueFull
from logging import Logger
from typing import Callable

from opentelemetry.trace import SpanKind
from opentelemetry.trace.status import StatusCode

from discord_core.interfaces.broker_client_protocol import BrokerClient
from discord_core.interfaces.database_protocols import PlaylistStore
from discord_core.interfaces.download_client_protocol import DownloadClient
from discord_core.types.download import LifecycleEvent, is_rejection
from discord_core.types.media_download import MediaDownload, media_download_attributes
from discord_core.types.media_request import media_request_attributes
from discord_core.types.playlist_add_request import PlaylistAddRequest
from discord_core.types.queue import PutsBlocked
from discord_core.types.search_resolution import SearchResolution
from discord_core.utils.otel import async_otel_span_wrapper, span_links_from_context

from discord_gateway.command_logic.enqueue import MediaEnqueuer
from discord_gateway.command_logic.player_gateway import PlayerGateway
from discord_gateway.types.playlist_add_result import PlaylistAddResult

OTEL_SPAN_PREFIX = 'music'

# What enqueue_track answers when it does not accept a track (the broker's own spellings).
ENQUEUE_FULL = 'full'
ENQUEUE_CLOSED = 'closed'
ENQUEUE_DUPLICATE = 'duplicate'


class MusicResultsLoop:
    '''
    The work done for one search result and one download result. Discord-free: see ``loop_logic``.

    The cog keeps the loops themselves (the shutdown check, the broker pop, the idle sleep and the
    heartbeat); this class does what happens to what was popped. The player is reached only through
    ``PlayerGateway``: ``has_live_player`` to drop a result whose guild has gone, and ``add_cached``
    to queue the track. ``add_cached`` comes back into ``enqueue_download``, the broker half of
    queueing a track, with the player's own wake-up passed in as ``on_queued``.
    '''

    def __init__(self, logger: Logger, broker_client: BrokerClient, download_client: DownloadClient,
                 enqueue: MediaEnqueuer, player_gateway: PlayerGateway,
                 playlist_store: PlaylistStore | None, queue_max_size: int,
                 server_queue_priority: dict[int, int]):
        self.logger = logger
        self.broker_client = broker_client
        self.download_client = download_client
        self.enqueue = enqueue
        self.player_gateway = player_gateway
        self.playlist_store = playlist_store
        self.queue_max_size = queue_max_size
        self.server_queue_priority = server_queue_priority

    async def enqueue_download(self, media_download: MediaDownload, on_queued: Callable[[], None]) -> bool:
        '''
        Queue a downloaded track in the broker for the guild's player.

        media_download : Standard MediaDownload for post download
        on_queued : Called once the broker has the track, before the request is marked completed
            (the player's wake-up and prefetch)
        '''
        attributes = media_download_attributes(media_download)
        async with async_otel_span_wrapper(f'{OTEL_SPAN_PREFIX}.add_source_to_player', kind=SpanKind.INTERNAL, attributes=attributes, links=span_links_from_context(media_download.media_request.span_context)):
            media_uuid = str(media_download.media_request.uuid)
            # Register with the broker BEFORE the track becomes visible in the queue. For a
            # cache hit this is the only point the broker learns of the entry, and the
            # player can claim the track the instant it is queued. If that race puts the
            # claim ahead of registration the broker has no entry to check out, and the
            # player would fall back to the raw S3 cache key (crashing on open()).
            # Registering first closes the window.
            await self.broker_client.register_download(media_download)
            result = await self.broker_client.enqueue_track(
                media_download.media_request.guild_id, media_uuid, self.queue_max_size)
            if result == ENQUEUE_FULL:
                self.logger.info(f'Play queue full, aborting download of item "{str(media_download.media_request)}"')
                reason = (f'Cannot add item "{media_download.title}" to play queue, play queue is full'
                          if media_download.media_request.bundle_uuid else None)
                await self.enqueue.push_state(media_download.media_request, LifecycleEvent.FAILED, failure_reason=reason)
                await self.broker_client.discard(media_uuid)
                return False
                # Dont return to loop, file was downloaded so we can iterate on cache at least
            if result == ENQUEUE_CLOSED:
                self.logger.info(f'Queue closed in guild "{media_download.media_request.guild_id}", assuming shutdown')
                await self.enqueue.push_state(media_download.media_request, LifecycleEvent.DISCARDED)
                await self.broker_client.discard(media_uuid)
                return False
            if result == ENQUEUE_DUPLICATE:
                # Already queued, so the track will play; nothing to undo.
                self.logger.warning(f'"{media_download.webpage_url}" was already queued in guild '
                                    f'{media_download.media_request.guild_id}')
            self.logger.info(f'Adding "{media_download.webpage_url}" '
                             f'to queue in guild {media_download.media_request.guild_id}')
            on_queued()
            await self.enqueue.push_state(media_download.media_request, LifecycleEvent.COMPLETED)
            return True

    async def requeue_search_result(self, resolution: SearchResolution) -> None:
        '''
        Put a resolution back on the broker's search-result queue.

        next_search_result is a destructive pop: the resolution exists nowhere
        else once this loop holds it, so any failure between that pop and a
        successful submit destroys the request outright -- the media request is
        never downloaded and its bundle row strands on QUEUED forever, with no
        error the user can see.  The downloader deploys under Recreate (one
        Mullvad key, so no rolling overlap is possible), which makes a hard gap
        on its Service a routine event rather than an exotic one.

        A failure to requeue is the one case where the request really is lost,
        so it is logged as such rather than folded into the caller's traceback.
        '''
        try:
            await self.broker_client.register_search_result(resolution)
        except Exception:
            self.logger.exception(
                f'Failed to requeue search result for media_request '
                f'{resolution.media_request.uuid}; the request is lost')

    async def process_search_result(self, resolution: SearchResolution) -> None:
        '''
        Route one resolved search into the download pipeline: cache-check, then download submit.
        Resolution happens on the search pod; this is the tail that can only run where the
        download client and cache live.
        '''
        media_request = resolution.media_request
        async with async_otel_span_wrapper(f'{OTEL_SPAN_PREFIX}.process_search_results', kind=SpanKind.CONSUMER,
                                           attributes=media_request_attributes(media_request),
                                           links=span_links_from_context(media_request.span_context) + span_links_from_context(resolution.span_context)) as span:
            await self.enqueue.push_state(media_request, LifecycleEvent.QUEUED)

            # Check if cache item exists already
            if await self.enqueue.enqueue_from_cache(media_request):
                # Cache hit: _enqueue_media_download_from_cache already pushed COMPLETED
                # on this same media_request (check_cache binds the cached download to
                # this request object), which advances the bundle row toward teardown.
                # A second COMPLETED here is redundant and trails an extra render behind
                # the remove — the source of the stranded "Media request queued for
                # download" status message — so it is intentionally omitted.
                span.set_status(StatusCode.OK)
                return

            try:
                self.logger.debug(f'Handing off media_request "{str(media_request)}" to download queue, uuid: {media_request.uuid}')
                await self.download_client.submit(media_request.guild_id, media_request, priority=self.server_queue_priority.get(media_request.guild_id, None))
            except PutsBlocked:
                self.logger.info(f'Puts to queue in guild {media_request.guild_id} are currently blocked, assuming shutdown')
                await self.enqueue.push_state(media_request, LifecycleEvent.DISCARDED)
                return
            except QueueFull:
                self.logger.info(f'Queue full in guild {media_request.guild_id}, cannot add more media requests')
                await self.enqueue.push_state(media_request, LifecycleEvent.DISCARDED)
            except Exception:
                # PutsBlocked and QueueFull are the downloader *answering*; anything
                # else means it never got the request. Most often that is a
                # ClientConnectorError against a downloader pod that is mid-Recreate,
                # which downloader-app.yaml calls harmless on the grounds that
                # "downloads queue in Redis and resume when the new pod's tunnel is
                # up". That holds for work already on the downloader's queue and not
                # for work in this handoff, which is why the resolution goes back
                # before the exception propagates.
                #
                # Re-raised rather than swallowed so the loop runner's capped backoff
                # applies. Returning normally here would re-pop the same resolution
                # immediately and spin against a Service that is still down.
                await self.requeue_search_result(resolution)
                raise
            span.set_status(StatusCode.OK)

    async def _return_bad_video(self, media_request, user_message: str | None, rejected: bool) -> None:
        await self.enqueue.push_state(media_request, LifecycleEvent.FAILED, failure_reason=user_message,
                                      rejected=rejected)
        if media_request.history_playlist_item_id:
            self.logger.info(f'Unable to find playlist item {media_request.history_playlist_item_id} '
                             f'from history playlist, deleting')
            await self.playlist_store.delete_item(media_request.history_playlist_item_id)

    async def process_download_result(self, result) -> None:
        '''
        Route one completed DownloadResult to the guild's player or the playlist handler.
        Retryable errors are handled inside download_client.run(); only successes and
        terminal failures reach this method.
        '''
        media_request = result.media_request
        is_playlist_add = isinstance(media_request, PlaylistAddRequest)
        attributes = media_request_attributes(media_request)

        async with async_otel_span_wrapper(f'{OTEL_SPAN_PREFIX}.process_download_results', kind=SpanKind.CONSUMER, attributes=attributes, links=span_links_from_context(media_request.span_context) + span_links_from_context(result.span_context)) as span:
            if not result.status.success:
                rejected = is_rejection(result.status.error_type)
                self.logger.info(f'Terminal error on "{str(media_request)}": {result.status.error_detail or ""}')
                # A rejection is a decision, not a fault — the pipeline looked at the
                # video and declined it (too long, banned, private, age restricted).
                # Marking those spans ERROR made the Consumer Span Error Rate alert
                # page on ordinary user input, so only genuine failures stay ERROR.
                span.set_status(StatusCode.OK if rejected else StatusCode.ERROR)
                await self._return_bad_video(media_request, result.status.user_message, rejected)
                return

            self.logger.info(f'Successfully fetched media request "{str(media_request)}" in guild "{media_request.guild_id}"')

            if is_playlist_add:
                data = result.ytdlp_data
                if not data:
                    await self.enqueue.push_state(media_request, LifecycleEvent.FAILED,
                                                  failure_reason=f'No metadata returned for "{str(media_request)}"')
                    span.set_status(StatusCode.ERROR)
                    return
                playlist_result = PlaylistAddResult(
                    webpage_url=data.get('webpage_url', ''),
                    title=data.get('title', ''),
                    uploader=data.get('uploader', ''),
                )
                span.set_status(StatusCode.OK)
                await self.enqueue.add_playlist_item(media_request, playlist_result)
                return

            if not await self.player_gateway.has_live_player(media_request.guild_id):
                self.logger.info(f'Player gone after download for guild {media_request.guild_id}, discarding "{str(media_request)}"')
                await self.enqueue.push_state(media_request, LifecycleEvent.DISCARDED)
                span.set_status(StatusCode.OK)
                return

            # The broker pod already persisted this result when the download
            # client reported it — build the MediaDownload the player needs from
            # the result rather than from a return value the HTTP client can't
            # round-trip.
            media_download = MediaDownload(result.file_name, result.ytdlp_data, media_request)
            media_download.file_size_bytes = result.file_size_bytes
            span.set_status(StatusCode.OK)
            await self.player_gateway.add_cached(media_request.guild_id, media_download)
            await self.broker_client.cache_cleanup()
