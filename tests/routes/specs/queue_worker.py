'''
The queue-worker seam, served twice: by the downloader at /downloads and by the search pod at
/search/ytmusic. The four routes are the same shape, so one table is built per prefix.
'''
from unittest.mock import AsyncMock

import fakeredis.aioredis
from pydantic import BaseModel

from discord_core.clients.redis_client import RedisManager
from discord_core.routes import queue_worker as queue_worker_routes
from discord_core.types import queue_worker_responses as out
from discord_core.types.playlist_add_request import AnyMediaRequest
from discord_core.types.queue import QueueFull
from discord_core.utils.failure_queue import FailureQueue

from discord_downloader.servers.download_server import DownloadHttpServer
from discord_downloader.workers.redis_download_worker import RedisDownloadWorker
from discord_search.servers.youtube_music_search_server import YoutubeMusicSearchHttpServer
from discord_search.workers.redis_youtube_music_search_worker import RedisYoutubeMusicSearchWorker

from tests.fakes.stub_youtube_music_client import StubYoutubeMusicClient
from tests.fakes.worker_requests import queue_request
from tests.routes.specs._model import Call, Operation, reply, serving

GUILD = 7


class SubmitBody(BaseModel):
    '''POST {prefix}'''
    guild_id: int
    media_request: AnyMediaRequest
    priority: int | None = None


class ClearBody(BaseModel):
    '''POST {prefix}/clear. `preserve_playlist_adds` keeps the requests that only add to a playlist.'''
    guild_id: int
    preserve_playlist_adds: bool = False


class BlockBody(BaseModel):
    '''POST {prefix}/block'''
    guild_id: int


def _manager() -> RedisManager:
    return RedisManager.from_client(fakeredis.aioredis.FakeRedis(decode_responses=True))


def downloads_environment():
    '''A downloader pod's HTTP server over a download worker on fakeredis.'''
    worker = RedisDownloadWorker(None, None, redis_manager=_manager(), wait_period_minimum=10,
                                 wait_period_max_variance=2, max_retries=2)
    return serving(DownloadHttpServer(worker), worker=worker)


def ytmusic_environment():
    '''A search pod's HTTP server over a YouTube Music search worker on fakeredis.'''
    worker = RedisYoutubeMusicSearchWorker(None, StubYoutubeMusicClient(),
                                           FailureQueue(max_size=10, max_age_seconds=600), 10, 2,
                                           redis_manager=_manager())
    return serving(YoutubeMusicSearchHttpServer(worker), worker=worker)


def submit_body() -> dict:
    '''A request for the test guild.'''
    return {'guild_id': GUILD, 'media_request': queue_request(guild_id=GUILD).model_dump(mode='json')}


async def blocked(env) -> dict:
    '''A guild that is refusing new submissions.'''
    await env.parts.worker.block_guild(GUILD)
    return {}


async def full(env) -> dict:
    '''A queue that is at its limit.'''
    env.parts.worker.submit = AsyncMock(side_effect=QueueFull('queue is full'))
    return {}


async def one_queued(env) -> dict:
    '''One request waiting for the test guild.'''
    await env.parts.worker.submit(GUILD, queue_request(guild_id=GUILD))
    return {}


def operations(routes: queue_worker_routes.QueueWorkerRoutes, noun: str) -> dict:
    '''The four routes of one prefix, described for the `noun` the pod works through.'''
    return {
        routes.submit: Operation(
            f'Queue a request for {noun}.', request=SubmitBody,
            replies=(reply(202, out.SubmitAcceptedResponse),
                     reply(409, out.SubmitRejectedResponse, doc='The guild is blocked.'),
                     reply(429, out.SubmitRejectedResponse, doc='The queue is full.'),
                     reply(422, doc='guild_id or media_request is missing or invalid.')),
            calls=(Call(202, body=submit_body()),
                   Call(409, body=submit_body(), arrange=blocked),
                   Call(429, body=submit_body(), arrange=full),
                   Call(422, body={}))),
        routes.clear: Operation(
            "Drop a guild's pending requests, returning what was dropped.", request=ClearBody,
            replies=(reply(200, out.ClearGuildResponse), reply(422, doc='guild_id is missing.')),
            calls=(Call(200, body={'guild_id': GUILD}, arrange=one_queued), Call(422, body={}))),
        routes.block: Operation(
            'Refuse new requests for a guild.', request=BlockBody,
            replies=(reply(200, out.BlockResponse), reply(422, doc='guild_id is missing.')),
            calls=(Call(200, body={'guild_id': GUILD}), Call(422, body={}))),
        routes.status: Operation(
            "A snapshot of the pod's queues, failures and backoff, for the gateway's poller.",
            replies=(reply(200, out.StatusSnapshotResponse),),
            calls=(Call(200),)),
    }


DOWNLOADS_OPERATIONS = operations(queue_worker_routes.DOWNLOADS, 'download')
YTMUSIC_OPERATIONS = operations(queue_worker_routes.YTMUSIC, 'a YouTube Music search')
