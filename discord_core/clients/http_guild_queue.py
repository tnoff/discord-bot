'''
HTTP half of the guild-queue surface, split out of http_broker_client.

Same shape and reasoning as http_player_session: this is player state the broker pod hosts, so
it gets its own mixin rather than widening HttpBrokerClient further.  Mixed into
HttpBrokerClient, which supplies _base_url, _bucket_name, _call_route, _route_url, _validate
and _get_session.

Its routes are declared in routes/guild_queue.py and are part of the broker seam: they are in
routes/broker.py's ALL, so in HttpBrokerClient.ROUTES_CALLED, and the peer route check covers them.

Unlike the session mixin, a 404 is NOT swallowed.  A session that cannot be saved costs the
next startup nothing it could not do without; a queue that cannot be reached means no track
plays.  A broker that predates these routes should fail loudly, not look like an empty queue.
'''
from opentelemetry.trace import SpanKind

from discord_core.routes import guild_queue as guild_queue_routes
from discord_core.types.broker_responses import (
    BumpQueuedTrackResponse, ClaimTrackHitResponse, ClaimTrackMissResponse, ClearQueueResponse,
    CloseGuildResponse, EnqueueTrackResponse, FinishTrackResponse, GuildHistoryResponse,
    GuildQueueResponse, OpenGuildResponse, PlayingHeartbeatResponse, PollGuildQueueResponse,
    QueuedDownload, RemoveQueuedTrackResponse, ShuffleQueueResponse, SkipTrackResponse,
)
from discord_core.types.checkout_result import CheckoutResult
from discord_core.types.guild_queue import ClaimedDownload, GuildQueueSnapshot, PlayingSnapshot
from discord_core.types.media_download import MediaDownload, media_download_from_dict
from discord_core.types.playlist_add_request import parse_media_request
from discord_core.utils.otel import async_otel_span_wrapper

_NO_CONTENT_STATUS = 204


def _download(body: QueuedDownload) -> MediaDownload:
    '''Rebuild a MediaDownload from a wire track.'''
    return media_download_from_dict(body.model_dump(), parse_media_request(body.request))


def _guild(guild_id: int) -> dict:
    return {'music.guild_id': guild_id}


class HttpGuildQueueMixin:
    '''Guild-queue calls against a remote BrokerHttpServer.'''

    async def enqueue_track(self, guild_id: int, uuid: str, max_size: int = 0) -> str:
        '''POST /guilds/{guild_id}/queue — queue an AVAILABLE entry.  ok, closed, full or duplicate.'''
        async with async_otel_span_wrapper(
            'broker.enqueue_track', kind=SpanKind.CLIENT,
            attributes={**_guild(guild_id), 'music.media_request.uuid': uuid},
        ):
            payload = await self._call_route(guild_queue_routes.ENQUEUE_TRACK,
                                             {'uuid': uuid, 'max_size': max_size},
                                             guild_id=guild_id)
        return self._validate(EnqueueTrackResponse, payload).result

    async def get_guild_queue(self, guild_id: int) -> GuildQueueSnapshot:
        '''GET /guilds/{guild_id}/queue — the queue, the playing track and the pending skip.'''
        async with async_otel_span_wrapper('broker.get_guild_queue', kind=SpanKind.CLIENT,
                                           attributes=_guild(guild_id)):
            payload = await self._call_route(guild_queue_routes.GET_GUILD_QUEUE, guild_id=guild_id)
        body = self._validate(GuildQueueResponse, payload)
        playing = None
        if body.playing:
            playing = PlayingSnapshot(
                uuid=body.playing.uuid,
                started_at=body.playing.started_at,
                gateway_id=body.playing.gateway_id,
                download=_download(body.playing.download) if body.playing.download else None,
            )
        return GuildQueueSnapshot(
            version=body.version,
            items=[_download(item) for item in body.items],
            playing=playing,
            skip_for=body.skip_for,
            closed=body.closed,
            text_channel_id=body.text_channel_id,
        )

    async def remove_queued_track(self, guild_id: int, uuid: str) -> MediaDownload | None:
        '''POST /guilds/{guild_id}/queue/remove — drop a queued track.  None if it was not queued.'''
        async with async_otel_span_wrapper(
            'broker.remove_queued_track', kind=SpanKind.CLIENT,
            attributes={**_guild(guild_id), 'music.media_request.uuid': uuid},
        ):
            payload = await self._call_route(guild_queue_routes.REMOVE_QUEUED_TRACK, {'uuid': uuid},
                                             guild_id=guild_id)
        body = self._validate(RemoveQueuedTrackResponse, payload)
        return _download(body.download) if body.removed and body.download else None

    async def bump_queued_track(self, guild_id: int, uuid: str) -> MediaDownload | None:
        '''POST /guilds/{guild_id}/queue/bump — move a queued track first.  None if not queued.'''
        async with async_otel_span_wrapper(
            'broker.bump_queued_track', kind=SpanKind.CLIENT,
            attributes={**_guild(guild_id), 'music.media_request.uuid': uuid},
        ):
            payload = await self._call_route(guild_queue_routes.BUMP_QUEUED_TRACK, {'uuid': uuid},
                                             guild_id=guild_id)
        body = self._validate(BumpQueuedTrackResponse, payload)
        return _download(body.download) if body.bumped and body.download else None

    async def shuffle_queue(self, guild_id: int) -> bool:
        '''POST /guilds/{guild_id}/queue/shuffle.'''
        async with async_otel_span_wrapper('broker.shuffle_queue', kind=SpanKind.CLIENT,
                                           attributes=_guild(guild_id)):
            payload = await self._call_route(guild_queue_routes.SHUFFLE_QUEUE, guild_id=guild_id)
        return self._validate(ShuffleQueueResponse, payload).shuffled

    async def clear_queue(self, guild_id: int) -> int:
        '''POST /guilds/{guild_id}/queue/clear — returns how many tracks were dropped.'''
        async with async_otel_span_wrapper('broker.clear_queue', kind=SpanKind.CLIENT,
                                           attributes=_guild(guild_id)):
            payload = await self._call_route(guild_queue_routes.CLEAR_QUEUE, guild_id=guild_id)
        return self._validate(ClearQueueResponse, payload).cleared

    async def poll_guild_queue(self, guild_id: int, since: int | None = None) -> tuple[int, str | None] | None:
        '''
        GET /guilds/{guild_id}/queue/state[?since=N] — (version, uuid a skip is pending for),
        or None when nothing changed since `since`.

        The unchanged (204) path intentionally mints NO span and skips the retry wrapper, like
        next_result: this is polled about once a second per active guild even when idle, and a
        span per empty poll churns allocations for no signal.  The span is only opened once
        there is a body to parse.
        '''
        url = self._route_url(guild_queue_routes.POLL_GUILD_QUEUE, guild_id=guild_id)
        if since is not None:
            url = f'{url}?since={since}'
        session = self._get_session()
        async with session.request(guild_queue_routes.POLL_GUILD_QUEUE.method, url,
                                   headers=self._trace_headers()) as resp:
            if resp.status == _NO_CONTENT_STATUS:
                return None
            resp.raise_for_status()
            payload = await resp.json()
        async with async_otel_span_wrapper('broker.poll_guild_queue', kind=SpanKind.CLIENT,
                                           attributes=_guild(guild_id)):
            body = self._validate(PollGuildQueueResponse, payload)
        return body.version, body.skip_for

    async def claim_next_track(self, guild_id: int, gateway_id: str) -> ClaimedDownload | None:
        '''POST /guilds/{guild_id}/claim — take the next track and mark it as playing.'''
        async with async_otel_span_wrapper('broker.claim_next_track', kind=SpanKind.CLIENT,
                                           attributes=_guild(guild_id)):
            payload = await self._call_route(guild_queue_routes.CLAIM_TRACK, {'gateway_id': gateway_id},
                                             guild_id=guild_id)
            # Two models for one route, matching the two shapes the broker sends, like checkout:
            # validating the branch we took names the peer if either shape drifts.
            if payload and payload.get('claimed'):
                hit = self._validate(ClaimTrackHitResponse, payload)
                return ClaimedDownload(
                    download=_download(hit.download),
                    checkout=CheckoutResult(s3_key=hit.s3_key, bucket_name=self._bucket_name),
                )
            self._validate(ClaimTrackMissResponse, payload)
            return None

    async def playing_heartbeat(self, guild_id: int, uuid: str) -> bool:
        '''POST /guilds/{guild_id}/playing/heartbeat.'''
        async with async_otel_span_wrapper(
            'broker.playing_heartbeat', kind=SpanKind.CLIENT,
            attributes={**_guild(guild_id), 'music.media_request.uuid': uuid},
        ):
            payload = await self._call_route(guild_queue_routes.PLAYING_HEARTBEAT, {'uuid': uuid},
                                             guild_id=guild_id)
        return self._validate(PlayingHeartbeatResponse, payload).alive

    async def skip_track(self, guild_id: int, uuid: str) -> str:
        '''POST /guilds/{guild_id}/skip — ok, no_player or not_current.'''
        async with async_otel_span_wrapper(
            'broker.skip_track', kind=SpanKind.CLIENT,
            attributes={**_guild(guild_id), 'music.media_request.uuid': uuid},
        ):
            payload = await self._call_route(guild_queue_routes.SKIP_TRACK, {'uuid': uuid},
                                             guild_id=guild_id)
        return self._validate(SkipTrackResponse, payload).result

    async def finish_track(self, guild_id: int, uuid: str, skipped: bool, history_cap: int) -> None:
        '''POST /guilds/{guild_id}/finish.'''
        async with async_otel_span_wrapper(
            'broker.finish_track', kind=SpanKind.CLIENT,
            attributes={**_guild(guild_id), 'music.media_request.uuid': uuid},
        ):
            payload = await self._call_route(
                guild_queue_routes.FINISH_TRACK,
                {'uuid': uuid, 'skipped': skipped, 'history_cap': history_cap},
                guild_id=guild_id)
        self._validate(FinishTrackResponse, payload)

    async def get_guild_history(self, guild_id: int) -> list[dict]:
        '''GET /guilds/{guild_id}/history — oldest first.'''
        async with async_otel_span_wrapper('broker.get_guild_history', kind=SpanKind.CLIENT,
                                           attributes=_guild(guild_id)):
            payload = await self._call_route(guild_queue_routes.GET_GUILD_HISTORY, guild_id=guild_id)
        return self._validate(GuildHistoryResponse, payload).items

    async def close_guild(self, guild_id: int) -> int:
        '''POST /guilds/{guild_id}/close — returns how many entries were released.'''
        async with async_otel_span_wrapper('broker.close_guild', kind=SpanKind.CLIENT,
                                           attributes=_guild(guild_id)):
            payload = await self._call_route(guild_queue_routes.CLOSE_GUILD, guild_id=guild_id)
        return self._validate(CloseGuildResponse, payload).released

    async def open_guild(self, guild_id: int, text_channel_id: int) -> str | None:
        '''
        POST /guilds/{guild_id}/open — take ownership of the guild's player.

        Reopens a closed guild, points its play-order message at text_channel_id (a second call
        with another channel moves it), and requeues a track the previous owner started and never
        finished.  Returns that track's uuid, or None.
        '''
        async with async_otel_span_wrapper('broker.open_guild', kind=SpanKind.CLIENT,
                                           attributes={**_guild(guild_id), 'music.text_channel_id': text_channel_id}):
            payload = await self._call_route(guild_queue_routes.OPEN_GUILD,
                                             {'text_channel_id': text_channel_id}, guild_id=guild_id)
        return self._validate(OpenGuildResponse, payload).recovered
