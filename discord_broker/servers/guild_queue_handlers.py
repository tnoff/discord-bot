'''
HTTP handlers for the guild-queue routes, as a mixin for BrokerHttpServer.

Split out of broker_server for size and for the same reason the client side is a mixin
(clients/http_guild_queue): this is player state the broker pod hosts, not media lifecycle.
Mixed into BrokerHttpServer, which supplies _guild_queue (a GuildQueueBroker or None) and
inherits _read_body from the server base.

Routes are declared in discord_core/routes/guild_queue.py.  Rejections and misses are 200 bodies,
never error statuses (see that module); the only error statuses here are 422, for a request the
caller got wrong.
'''
from typing import Callable

from aiohttp import web
from opentelemetry.propagate import extract
from opentelemetry.trace import SpanKind

from discord_core.routes import guild_queue as guild_queue_routes
from discord_core.routes.route import Route
from discord_core.types import broker_responses
from discord_core.types.media_download import MediaDownload, media_download_to_dict
from discord_core.utils.otel import otel_span_wrapper


def _queued_download(download: MediaDownload) -> broker_responses.QueuedDownload:
    '''A MediaDownload as the wire carries it.'''
    return broker_responses.QueuedDownload.model_validate(media_download_to_dict(download))


def _guild_id(request: web.Request) -> int:
    try:
        return int(request.match_info['guild_id'])
    except ValueError as exc:
        raise web.HTTPUnprocessableEntity() from exc


def _uuid(body: dict) -> str:
    try:
        return str(body['uuid'])
    except Exception as exc:
        raise web.HTTPUnprocessableEntity() from exc


class GuildQueueHandlersMixin:
    '''Guild-queue route handlers, for a server that holds a GuildQueueBroker as _guild_queue.'''

    def guild_queue_handlers(self) -> dict[Route, Callable]:
        '''Route -> handler for every guild-queue route; empty when no queue is configured.'''
        if self._guild_queue is None:
            return {}
        return {
            guild_queue_routes.ENQUEUE_TRACK: self._handle_enqueue_track,
            guild_queue_routes.GET_GUILD_QUEUE: self._handle_get_guild_queue,
            guild_queue_routes.REMOVE_QUEUED_TRACK: self._handle_remove_queued_track,
            guild_queue_routes.BUMP_QUEUED_TRACK: self._handle_bump_queued_track,
            guild_queue_routes.SHUFFLE_QUEUE: self._handle_shuffle_queue,
            guild_queue_routes.CLEAR_QUEUE: self._handle_clear_queue,
            guild_queue_routes.POLL_GUILD_QUEUE: self._handle_poll_guild_queue,
            guild_queue_routes.CLAIM_TRACK: self._handle_claim_track,
            guild_queue_routes.PLAYING_HEARTBEAT: self._handle_playing_heartbeat,
            guild_queue_routes.SKIP_TRACK: self._handle_skip_track,
            guild_queue_routes.FINISH_TRACK: self._handle_finish_track,
            guild_queue_routes.GET_GUILD_HISTORY: self._handle_get_guild_history,
            guild_queue_routes.CLOSE_GUILD: self._handle_close_guild,
            guild_queue_routes.OPEN_GUILD: self._handle_open_guild,
        }

    async def _handle_enqueue_track(self, request: web.Request) -> web.Response:
        ctx, body = await self._read_body(request)
        guild_id = _guild_id(request)
        uuid = _uuid(body)
        try:
            max_size = int(body.get('max_size', 0))
        except Exception as exc:
            raise web.HTTPUnprocessableEntity() from exc
        with otel_span_wrapper('broker.enqueue_track', context=ctx, kind=SpanKind.SERVER):
            result = await self._guild_queue.enqueue(guild_id, uuid, max_size)
        return web.json_response(broker_responses.EnqueueTrackResponse(result=result).model_dump())

    async def _handle_get_guild_queue(self, request: web.Request) -> web.Response:
        ctx = extract(request.headers)
        guild_id = _guild_id(request)
        with otel_span_wrapper('broker.get_guild_queue', context=ctx, kind=SpanKind.SERVER):
            queue = await self._guild_queue.get_queue(guild_id)
        playing = None
        if queue.playing:
            playing = broker_responses.PlayingTrackBody(
                uuid=queue.playing.uuid,
                started_at=queue.playing.started_at,
                gateway_id=queue.playing.gateway_id,
                download=_queued_download(queue.playing.entry.download)
                if queue.playing.entry and queue.playing.entry.download else None,
            )
        return web.json_response(broker_responses.GuildQueueResponse(
            version=queue.version,
            # An entry with no download cannot be played; it is dead weight in the list.
            items=[_queued_download(entry.download) for entry in queue.items if entry.download],
            playing=playing,
            skip_for=queue.skip_for,
            closed=queue.closed,
            text_channel_id=queue.text_channel_id,
        ).model_dump())

    async def _handle_remove_queued_track(self, request: web.Request) -> web.Response:
        ctx, body = await self._read_body(request)
        guild_id = _guild_id(request)
        uuid = _uuid(body)
        with otel_span_wrapper('broker.remove_queued_track', context=ctx, kind=SpanKind.SERVER):
            entry = await self._guild_queue.remove(guild_id, uuid)
        removed = entry is not None and entry.download is not None
        return web.json_response(broker_responses.RemoveQueuedTrackResponse(
            removed=removed,
            download=_queued_download(entry.download) if removed else None,
        ).model_dump())

    async def _handle_bump_queued_track(self, request: web.Request) -> web.Response:
        ctx, body = await self._read_body(request)
        guild_id = _guild_id(request)
        uuid = _uuid(body)
        with otel_span_wrapper('broker.bump_queued_track', context=ctx, kind=SpanKind.SERVER):
            entry = await self._guild_queue.bump(guild_id, uuid)
        bumped = entry is not None and entry.download is not None
        return web.json_response(broker_responses.BumpQueuedTrackResponse(
            bumped=bumped,
            download=_queued_download(entry.download) if bumped else None,
        ).model_dump())

    async def _handle_shuffle_queue(self, request: web.Request) -> web.Response:
        ctx = extract(request.headers)
        guild_id = _guild_id(request)
        with otel_span_wrapper('broker.shuffle_queue', context=ctx, kind=SpanKind.SERVER):
            shuffled = await self._guild_queue.shuffle(guild_id)
        return web.json_response(broker_responses.ShuffleQueueResponse(shuffled=shuffled).model_dump())

    async def _handle_clear_queue(self, request: web.Request) -> web.Response:
        ctx = extract(request.headers)
        guild_id = _guild_id(request)
        with otel_span_wrapper('broker.clear_queue', context=ctx, kind=SpanKind.SERVER):
            cleared = await self._guild_queue.clear(guild_id)
        return web.json_response(broker_responses.ClearQueueResponse(cleared=cleared).model_dump())

    async def _handle_poll_guild_queue(self, request: web.Request) -> web.Response:
        '''GET /guilds/{guild_id}/queue/state[?since=N] — 204 when nothing changed since N.

        The unchanged answer opens no span: this is polled about once a second per active guild,
        and a span per empty poll is allocation churn with no signal (see _handle_next_result).
        '''
        guild_id = _guild_id(request)
        try:
            since = int(request.query['since']) if 'since' in request.query else None
        except ValueError as exc:
            raise web.HTTPUnprocessableEntity() from exc
        version, skip_for = await self._guild_queue.poll(guild_id)
        if since is not None and since == version and skip_for is None:
            return web.Response(status=204)
        with otel_span_wrapper('broker.poll_guild_queue', context=extract(request.headers),
                               kind=SpanKind.SERVER):
            return web.json_response(broker_responses.PollGuildQueueResponse(
                version=version, skip_for=skip_for).model_dump())

    async def _handle_claim_track(self, request: web.Request) -> web.Response:
        ctx, body = await self._read_body(request)
        guild_id = _guild_id(request)
        try:
            gateway_id = str(body['gateway_id'])
        except Exception as exc:
            raise web.HTTPUnprocessableEntity() from exc
        with otel_span_wrapper('broker.claim_track', context=ctx, kind=SpanKind.SERVER):
            claimed = await self._guild_queue.claim_next(guild_id, gateway_id)
        if claimed is None:
            return web.json_response(broker_responses.ClaimTrackMissResponse().model_dump())
        return web.json_response(broker_responses.ClaimTrackHitResponse(
            download=_queued_download(claimed.entry.download),
            s3_key=claimed.checkout.s3_key,
        ).model_dump())

    async def _handle_playing_heartbeat(self, request: web.Request) -> web.Response:
        ctx, body = await self._read_body(request)
        guild_id = _guild_id(request)
        uuid = _uuid(body)
        with otel_span_wrapper('broker.playing_heartbeat', context=ctx, kind=SpanKind.SERVER):
            alive = await self._guild_queue.heartbeat(guild_id, uuid)
        return web.json_response(broker_responses.PlayingHeartbeatResponse(alive=alive).model_dump())

    async def _handle_skip_track(self, request: web.Request) -> web.Response:
        ctx, body = await self._read_body(request)
        guild_id = _guild_id(request)
        uuid = _uuid(body)
        with otel_span_wrapper('broker.skip_track', context=ctx, kind=SpanKind.SERVER):
            result = await self._guild_queue.skip(guild_id, uuid)
        return web.json_response(broker_responses.SkipTrackResponse(result=result).model_dump())

    async def _handle_finish_track(self, request: web.Request) -> web.Response:
        ctx, body = await self._read_body(request)
        guild_id = _guild_id(request)
        uuid = _uuid(body)
        try:
            skipped = bool(body['skipped'])
            history_cap = int(body['history_cap'])
        except Exception as exc:
            raise web.HTTPUnprocessableEntity() from exc
        with otel_span_wrapper('broker.finish_track', context=ctx, kind=SpanKind.SERVER):
            await self._guild_queue.finish(guild_id, uuid, skipped, history_cap)
        return web.json_response(broker_responses.FinishTrackResponse().model_dump())

    async def _handle_get_guild_history(self, request: web.Request) -> web.Response:
        ctx = extract(request.headers)
        guild_id = _guild_id(request)
        with otel_span_wrapper('broker.get_guild_history', context=ctx, kind=SpanKind.SERVER):
            items = await self._guild_queue.get_history(guild_id)
        return web.json_response(broker_responses.GuildHistoryResponse(items=items).model_dump())

    async def _handle_close_guild(self, request: web.Request) -> web.Response:
        ctx = extract(request.headers)
        guild_id = _guild_id(request)
        with otel_span_wrapper('broker.close_guild', context=ctx, kind=SpanKind.SERVER):
            released = await self._guild_queue.close(guild_id)
        return web.json_response(broker_responses.CloseGuildResponse(released=released).model_dump())

    async def _handle_open_guild(self, request: web.Request) -> web.Response:
        ctx, body = await self._read_body(request)
        guild_id = _guild_id(request)
        try:
            text_channel_id = int(body['text_channel_id'])
        except Exception as exc:
            raise web.HTTPUnprocessableEntity() from exc
        with otel_span_wrapper('broker.open_guild', context=ctx, kind=SpanKind.SERVER):
            recovered = await self._guild_queue.open(guild_id, text_channel_id)
        return web.json_response(broker_responses.OpenGuildResponse(recovered=recovered).model_dump())
