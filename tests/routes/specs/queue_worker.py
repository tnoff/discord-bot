'''
The queue-worker seam, served twice: by the downloader at /downloads and by the search pod at
/search/ytmusic. The four routes are the same shape, so one table is built per prefix.
'''
from unittest.mock import AsyncMock

import fakeredis.aioredis

from discord_core.clients.redis_client import RedisManager
from discord_core.routes import queue_worker as queue_worker_routes
from discord_core.types.queue import QueueFull
from discord_core.utils.failure_queue import FailureQueue

from discord_downloader.servers.download_server import DownloadHttpServer
from discord_downloader.workers.redis_download_worker import RedisDownloadWorker
from discord_search.servers.youtube_music_search_server import YoutubeMusicSearchHttpServer
from discord_search.workers.redis_youtube_music_search_worker import RedisYoutubeMusicSearchWorker

from testing.fakes.stub_youtube_music_client import StubYoutubeMusicClient
from testing.fakes.worker_requests import queue_request
from testing.contracts.queue_worker import DOWNLOADS, YTMUSIC
from tests.routes.specs._model import Call, operation, serving

GUILD = 7


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


def operations(contracts: dict, routes: queue_worker_routes.QueueWorkerRoutes) -> dict:
    '''The shared contract of one prefix, with the calls that prove it on the serving pod.'''
    return {
        routes.submit: operation(contracts[routes.submit], (
            Call(202, body=submit_body()),
            Call(409, body=submit_body(), arrange=blocked),
            Call(429, body=submit_body(), arrange=full),
            Call(422, body={}))),
        routes.clear: operation(contracts[routes.clear], (
            Call(200, body={'guild_id': GUILD}, arrange=one_queued), Call(422, body={}))),
        routes.block: operation(contracts[routes.block], (
            Call(200, body={'guild_id': GUILD}), Call(422, body={}))),
        routes.status: operation(contracts[routes.status], (Call(200),)),
    }


DOWNLOADS_OPERATIONS = operations(DOWNLOADS, queue_worker_routes.DOWNLOADS)
YTMUSIC_OPERATIONS = operations(YTMUSIC, queue_worker_routes.YTMUSIC)
