'''
HTTP server for the YouTube-Music search worker pod.

Fronts a RedisYoutubeMusicSearchWorker so bot pods (via
HttpYoutubeMusicSearchClient) can submit / clear / block searches and poll
queue/backoff status over HTTP.  The search pod drives its own consumer loop
in-process to drain the Redis queue and hands resolutions back to the bot through
the broker's search-result queue (the seam MR 2 built) — resolutions never come
back through this server, so there is no pop/resolve route here.

All four handlers, the serve()/drain lifecycle, and the heartbeat gauge come from
QueueWorkerHttpServer; this module only supplies the routes/labels that differ
from the downloader's.
'''
import logging
from typing import ClassVar

from discord_bot.servers.queue_worker_server import QueueWorkerHttpServer
from discord_bot.workers.redis_youtube_music_search_worker import RedisYoutubeMusicSearchWorker

logger = logging.getLogger(__name__)


class YoutubeMusicSearchHttpServer(QueueWorkerHttpServer):
    '''
    aiohttp HTTP server fronting a RedisYoutubeMusicSearchWorker.

    Routes:
        POST /search/ytmusic          submit
        POST /search/ytmusic/clear    clear_guild_queue (preserve_playlist_adds flag)
        POST /search/ytmusic/block    block_guild
        GET  /search/ytmusic/status   queue_size + failure_summary + backoff snapshot

    The routes are namespaced under a per-provider segment rather than owning a
    bare /search, because the search tier is meant to co-host providers: the
    media_search subsystem (Spotify + the YouTube Data API) is a set of thin
    HTTP clients like this one, not a yt-dlp-sized dependency, so it belongs in
    the same slim image and pod rather than one Deployment, image pin, OCIR repo
    and netpol tier each.  Every pod is another pin that a revert-then-auto-bump
    can strand out of step with the bot, which is what broke this very seam
    twice (findings/2026-07-31-discord-search-seam-deploy-skew.md).  Reserving
    /search as the pod namespace keeps /search/spotify and /search/youtube free,
    and keeps a later split to a new Deployment plus one config URL — renaming a
    live cross-pod route would itself be a skew-sensitive change.

    The heartbeat is labelled `youtube_music_search_server`, not
    `youtube_music_search`: the latter is already the bot cog's search-loop
    heartbeat (cogs/music.py LOOP_YOUTUBE_MUSIC_SEARCH), and reusing it would put
    two different things — a cog loop and a pod's HTTP listener — on one series.
    '''

    ROUTE_PREFIX: ClassVar[str] = '/search/ytmusic'
    SPAN_PREFIX: ClassVar[str] = 'youtube_music_search'
    HEARTBEAT_JOB: ClassVar[str] = 'youtube_music_search_server'
    HEARTBEAT_DESCRIPTION: ClassVar[str] = 'Youtube music search HTTP server heartbeat'
    DEFAULT_PORT: ClassVar[int] = 8084

    def __init__(self, worker: RedisYoutubeMusicSearchWorker, host: str = '0.0.0.0',  # nosec B104
                 port: int | None = None):
        '''Typed constructor — the base accepts any worker with the queue surface.'''
        super().__init__(worker, host=host, port=port)
