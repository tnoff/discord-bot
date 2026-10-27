from asyncio import QueueFull
from logging import Logger
from typing import Awaitable, Callable

from opentelemetry.trace import SpanKind

from discord_core.interfaces.broker_client_protocol import BrokerClient
from discord_core.interfaces.database_protocols import PlaylistStore
from discord_core.interfaces.download_client_protocol import DownloadClient
from discord_core.interfaces.youtube_music_search_protocols import YoutubeMusicSearchClient
from discord_core.clients.dispatch_client_base import DispatchClientBase
from discord_core.cogs.music_helpers.common import SearchType
from discord_core.types.download import LifecycleEvent, LifecycleStatusUpdate
from discord_core.types.media_download import MediaDownload, media_download_attributes
from discord_core.types.media_request import MediaRequest
from discord_core.types.playlist import PlaylistItemAddStatus, PlaylistItemWrite
from discord_core.types.playlist_add_request import PlaylistAddRequest
from discord_core.types.queue import PutsBlocked
from discord_core.utils.otel import async_otel_span_wrapper, capture_span_context, span_links_from_context
from discord_core.types.command_envelope import CommandEnvelope
from discord_core.utils.playlist_view import playlist_public_view

from discord_core.clients.search_client import SearchClient, SearchException
from discord_core.interfaces.player_gateway import PlayerGateway
from discord_core.types.playlist_add_result import PlaylistAddResult


# What enqueue_track answers when it does not accept a track (the broker's own spellings).
ENQUEUE_FULL = 'full'
ENQUEUE_CLOSED = 'closed'
ENQUEUE_DUPLICATE = 'duplicate'

OTEL_SPAN_PREFIX = 'music'


class TrackQueuer:
    '''
    The broker half of putting a track on a guild's play queue. It needs the broker and nothing
    else, so a ``PlayerGateway`` with no player of its own (the commands pod's) can queue a cache
    hit with it, and ``MediaEnqueuer`` shares the same copy.
    '''

    def __init__(self, broker_client: BrokerClient, logger: Logger, queue_max_size: int):
        self.broker_client = broker_client
        self.logger = logger
        self.queue_max_size = queue_max_size

    async def push_state(self, media_request: MediaRequest, event: LifecycleEvent, **details) -> None:
        '''Send a lifecycle transition to the broker, which marks it and re-renders the bundle.'''
        await self.broker_client.update_request_status(
            str(media_request.uuid), LifecycleStatusUpdate(event=event, **details))

    async def enqueue_download(self, media_download: MediaDownload, on_queued: Callable[[], Awaitable[None]]) -> bool:
        '''
        Queue a downloaded track in the broker for the guild's player.

        media_download : Standard MediaDownload for post download
        on_queued : Awaited once the broker has the track, before the request is marked completed
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
                await self.push_state(media_download.media_request, LifecycleEvent.FAILED, failure_reason=reason)
                await self.broker_client.discard(media_uuid)
                return False
                # Dont return to loop, file was downloaded so we can iterate on cache at least
            if result == ENQUEUE_CLOSED:
                self.logger.info(f'Queue closed in guild "{media_download.media_request.guild_id}", assuming shutdown')
                await self.push_state(media_download.media_request, LifecycleEvent.DISCARDED)
                await self.broker_client.discard(media_uuid)
                return False
            if result == ENQUEUE_DUPLICATE:
                # Already queued, so the track will play; nothing to undo.
                self.logger.warning(f'"{media_download.webpage_url}" was already queued in guild '
                                    f'{media_download.media_request.guild_id}')
            self.logger.info(f'Adding "{media_download.webpage_url}" '
                             f'to queue in guild {media_download.media_request.guild_id}')
            await on_queued()
            await self.push_state(media_download.media_request, LifecycleEvent.COMPLETED)
            return True



class MediaEnqueuer:
    '''
    Turns a search into media requests and routes them: to the search queue, to the download
    queue, or straight to the player on a cache hit. Discord-free: see ``discord_core.types.command_envelope``.

    The result loops that stay on the gateway (``process_search_results``,
    ``process_download_results``) call ``enqueue_from_cache`` and ``add_playlist_item`` too, so
    there is one copy of the cache-hit and playlist-add paths. The player is reached only through
    ``PlayerGateway.add_cached``.
    '''

    def __init__(self, broker_client: BrokerClient, search_client: SearchClient,
                 youtube_music_search_client: YoutubeMusicSearchClient, download_client: DownloadClient,
                 playlist_store: PlaylistStore | None, player_gateway: PlayerGateway,
                 dispatcher: DispatchClientBase, logger: Logger, queue_max_size: int,
                 server_playlist_max_size: int, message_delete_after: int,
                 server_queue_priority: dict[int, int]):
        self.broker_client = broker_client
        self.search_client = search_client
        self.youtube_music_search_client = youtube_music_search_client
        self.download_client = download_client
        self.playlist_store = playlist_store
        self.player_gateway = player_gateway
        self.dispatcher = dispatcher
        self.logger = logger
        self.queue_max_size = queue_max_size
        self.server_playlist_max_size = server_playlist_max_size
        self.message_delete_after = message_delete_after
        self.server_queue_priority = server_queue_priority
        self.queuer = TrackQueuer(broker_client, logger, queue_max_size)

    def _send(self, env: CommandEnvelope, content: str) -> None:
        self.dispatcher.send_message(env.guild_id, env.channel_id, content,
                                     delete_after=self.message_delete_after)

    async def push_state(self, media_request: MediaRequest, event: LifecycleEvent, **details) -> None:
        '''Send a lifecycle transition to the broker, which marks it and re-renders the bundle.'''
        await self.queuer.push_state(media_request, event, **details)

    async def add_playlist_item(self, request: PlaylistAddRequest, result: PlaylistAddResult) -> None:
        '''
        Insert a playlist item using the lightweight PlaylistAddResult metadata.

        request : PlaylistAddRequest carrying playlist_id
        result : PlaylistAddResult with webpage_url, title, uploader
        '''
        self.logger.info(f'Adding video_url "{result.webpage_url}" to playlist "{request.playlist_id}"'
                         f' in guild {request.guild_id}')
        # A batch of one. The store reports why rather than raising, so the
        # full-playlist case is an outcome to branch on instead of an exception
        # whose type cannot survive a network hop.
        outcomes = await self.playlist_store.add_items(
            request.playlist_id,
            [PlaylistItemWrite(video_url=result.webpage_url, title=result.title,
                               uploader=result.uploader)],
            self.server_playlist_max_size)
        outcome = outcomes[0]
        if outcome.status == PlaylistItemAddStatus.PLAYLIST_FULL:
            await self.push_state(request, LifecycleEvent.FAILED,
                                  failure_reason='Unable to add item to playlist, playlist too long',
                                  rejected=True)
            return
        playlist_public_view_id = await playlist_public_view(self.playlist_store, request.playlist_id, request.guild_id)
        if outcome.status == PlaylistItemAddStatus.ADDED:
            await self.push_state(request, LifecycleEvent.COMPLETED)
            return
        await self.push_state(request, LifecycleEvent.FAILED,
                              failure_reason=f'Item "{result.title}" already exists in playlist {playlist_public_view_id}',
                              rejected=True)

    async def enqueue_from_cache(self, media_request: MediaRequest) -> bool:
        '''
        Serve a request from the cache if it is there, returning whether it was.

        A hit goes to the playlist for a ``PlaylistAddRequest`` and to the guild's player
        otherwise (nothing happens to it if the guild has no player).
        '''
        media_download = await self.broker_client.check_cache(media_request)
        if not media_download:
            return False
        # check_cache binds the cached file to THIS media_request
        # (video_cache_client.get_webpage_url_item passes it straight into the
        # MediaDownload), so media_download.media_request is the same object —
        # marking it COMPLETED advances this request's own bundle row toward
        # teardown.  add_source_to_player and the SEARCH caller re-push
        # COMPLETED on the same request; the transitions are idempotent.
        await self.push_state(media_download.media_request, LifecycleEvent.COMPLETED)
        if isinstance(media_request, PlaylistAddRequest):
            playlist_result = PlaylistAddResult(
                webpage_url=media_download.webpage_url or '',
                title=media_download.title,
                uploader=media_download.uploader,
            )
            await self.add_playlist_item(media_request, playlist_result)
            return True
        await self.player_gateway.add_cached(media_request.guild_id, media_download)
        return True

    async def enqueue_download(self, media_download: MediaDownload, on_queued: Callable[[], Awaitable[None]]) -> bool:
        '''Queue a downloaded track in the broker for the guild's player; see ``TrackQueuer``.'''
        return await self.queuer.enqueue_download(media_download, on_queued)

    async def enqueue_media_requests(self, env: CommandEnvelope, entries: list[MediaRequest],
                                     bundle_uuid: str) -> bool:
        '''
        Enqueue media requests to the search or download queue, or the player on a cache hit.

        entries: Each one is registered with the broker (which auto-attaches it to bundle_uuid)
            and routed to either the search or download queue based on its search type.

        Returns true if all items added, false if some were not.
        '''
        ctx_span_context = capture_span_context()
        for media_request in entries:
            if media_request.span_context is None:
                media_request.span_context = ctx_span_context
            media_request.bundle_uuid = bundle_uuid
            await self.broker_client.register_request(media_request)
            self.logger.debug(f'Running enqueue for media request "{str(media_request)}, uuid: {media_request.uuid}, bundle: {bundle_uuid}')
            # Unless a direct or youtube url, pass into the search queue
            if media_request.search_result.search_type not in [SearchType.DIRECT, SearchType.YOUTUBE]:
                try:
                    await self.youtube_music_search_client.submit(media_request.guild_id, media_request, priority=self.server_queue_priority.get(media_request.guild_id, None))
                except PutsBlocked:
                    self.logger.info(f'Puts to search queue in guild {env.guild_id} are currently blocked, assuming shutdown')
                    await self.broker_client.delete_bundle(bundle_uuid)
                    return False
                except QueueFull:
                    self.logger.info(f'Search Queue full in guild {env.guild_id}, cannot add more media requests')
                    await self.push_state(media_request, LifecycleEvent.DISCARDED)
                    break
                continue
            # Else directly add to download queue
            if await self.enqueue_from_cache(media_request):
                # Cache hit: mark the current request completed (broker bundle counts it)
                await self.push_state(media_request, LifecycleEvent.COMPLETED)
                continue
            try:
                await self.download_client.submit(media_request.guild_id, media_request)
                await self.push_state(media_request, LifecycleEvent.QUEUED)
            except PutsBlocked:
                await self.broker_client.delete_bundle(bundle_uuid)
                self.logger.info(f'Puts to download queue in guild {env.guild_id} are currently blocked, assuming shutdown')
                return False
            except QueueFull:
                self.logger.info(f'Download Queue full in guild {env.guild_id}, cannot add more media requests')
                await self.push_state(media_request, LifecycleEvent.DISCARDED)
                break

        # Lock pagination on the bundle and trigger a final render.
        await self.broker_client.finalize_bundle(bundle_uuid)
        return True

    async def generate_media_requests_from_search(self, env: CommandEnvelope, search: str,
                                                  add_to_playlist: int = None) -> None:
        '''
        Generate media requests and a broker-owned bundle from a search input.

        search: Original Search string
        add_to_playlist: If came from playlist item-add, pass the playlist id here
        '''
        # Single-search bundle — broker renders the placeholder row immediately.
        bundle_uuid = await self.broker_client.create_bundle(
            env.guild_id, env.channel_id, input_string=search, has_search_banner=False)

        try:
            collection = await self.search_client.check_source(search, self.queue_max_size)
        except SearchException as exc:
            self.logger.info(f'Received download client exception for search "{search}", {str(exc)}')
            await self.broker_client.delete_bundle(bundle_uuid)
            self._send(env, f'Error searching input "{search}", message: {str(exc.user_message)}')
            return
        except Exception:
            # check_source stopped being an in-process call at the media_search
            # cutover: it is a round trip to the search pod now, so it can fail with
            # a transport error that is not a SearchException and that the clause
            # above was never written to see. The bundle above is already created
            # and rendered, so letting one through leaves a placeholder row that
            # never resolves -- the same stranded-row failure as a dropped search
            # resolution, reached from the other end of the pipeline.
            await self.broker_client.delete_bundle(bundle_uuid)
            self._send(env, f'Error searching input "{search}", the search backend is unavailable')
            raise

        # Multi-item collection — drop the single-search bundle and create a
        # multi-track bundle whose banner persists across renders.
        if collection.collection_name:
            await self.broker_client.delete_bundle(bundle_uuid)
            bundle_uuid = await self.broker_client.create_bundle(
                env.guild_id, env.channel_id,
                input_string=collection.collection_name, has_search_banner=True,
            )

        media_requests = []
        for search_result in collection.search_results:
            if add_to_playlist:
                mr = PlaylistAddRequest(guild_id=env.guild_id, channel_id=env.channel_id, requester_name=env.author_display_name, requester_id=env.author_id,
                                        search_result=search_result, playlist_id=add_to_playlist)
            else:
                mr = MediaRequest(guild_id=env.guild_id, channel_id=env.channel_id, requester_name=env.author_display_name, requester_id=env.author_id,
                                  search_result=search_result)
            media_requests.append(mr)
        await self.enqueue_media_requests(env, media_requests, bundle_uuid)
