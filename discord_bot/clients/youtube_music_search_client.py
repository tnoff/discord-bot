'''
Cog-facing YouTube-Music search client.

HttpYoutubeMusicSearchClient forwards the submit/clear/block half of the
cog-facing YoutubeMusicSearchClient Protocol surface to a standalone search pod,
over the shared HttpQueueWorkerClient base.  It is the only implementation any
deployment builds.

The single-process sibling that used to live here moved to
tests/fakes/in_memory_youtube_music_search_client.py with the rest of the
retired in-process stack: keeping it beside the live client is what kept
clients/in_memory_queue_worker_client.py inside every image, reachable on paper
while every one of its importers was dead.
'''
from typing import ClassVar

from discord_bot.clients.http_queue_worker_client import HttpQueueWorkerClient
from discord_bot.seams.media_search.interfaces.youtube_music_search_protocols import (
    YoutubeMusicSearchClient, YoutubeMusicSearchWorkerBase,
)
from discord_bot.seams.queue_worker.routes import queue_worker as queue_worker_routes
from discord_bot.seams.queue_worker.routes.queue_worker import QueueWorkerRoutes

__all__ = [
    'YoutubeMusicSearchClient',
    'YoutubeMusicSearchWorkerBase',
    'HttpYoutubeMusicSearchClient',
]


class HttpYoutubeMusicSearchClient(HttpQueueWorkerClient):
    '''
    YoutubeMusicSearchClient that forwards the cog-facing surface to a remote
    search pod.

    Producer calls (submit / block_guild / clear_guild_queue) POST to the pod's
    YoutubeMusicSearchHttpServer; the cached read surface (failure_summary /
    backoff_seconds_remaining / queue_size) is refreshed by the shared background
    poller from GET /search/ytmusic/status.  Everything comes from HttpQueueWorkerClient —
    only the route and span prefixes differ from the downloader's client.

    **Deliberately narrower than the Protocol**: no resolve / get_input_nowait /
    backoff_wait / set_wait_timestamp.  Those four belong to whoever drives the
    search loop, and that is the search pod: it owns the ytmusicapi client, the
    queue it pops from, and the shared 429 window.  The bot receives resolutions
    back through the broker's search-result queue, not through this client, and
    registers no search loop at all — the cog used to run one itself when no
    search url was configured, and that in-process shape is gone
    (projects/discord-bot-ha-only).  Calling one of the four here is a programming error,
    not a runtime fallback, so they are absent rather than raising stubs.
    '''

    ROUTES: ClassVar[QueueWorkerRoutes] = queue_worker_routes.YTMUSIC
    SPAN_PREFIX: ClassVar[str] = 'youtube_music_search'
