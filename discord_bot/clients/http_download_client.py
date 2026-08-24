'''
HttpDownloadClient on its own — the client half, without the engine.

Split out of clients/download_client.py for the same reason HttpBrokerClient was
split out of clients/broker_client.py (discord-bot!213): that module also defines
InMemoryDownloadClient and re-exports the whole of interfaces/download_protocols,
so importing it pulls yt_dlp and boto3 into any process that only wanted
to submit a download over HTTP.  The bot is that process since the download dual
path was collapsed (projects/discord-bot-ha-only), and the downloader image's
import boundary is what would catch a regression here.

Re-exported from clients/download_client, so existing imports keep working.
'''
from typing import ClassVar

from discord_bot.clients.http_queue_worker_client import HttpQueueWorkerClient

__all__ = ['HttpDownloadClient']


class HttpDownloadClient(HttpQueueWorkerClient):
    '''
    DownloadClient that forwards the cog-facing surface to a remote downloader pod.

    The whole surface — producer calls (submit / block_guild / clear_guild_queue)
    against the downloader's DownloadHttpServer, plus the cached read surface
    (failure_summary / backoff_seconds_remaining / queue_size) a background poller
    refreshes from GET /downloads/status — lives on HttpQueueWorkerClient, which the
    search pod's client shares.  Only the route and span prefixes differ.

    There is no `run` or `local_worker`: the download consumer loop runs in the
    downloader pod (owned by its CLI entrypoint), not on the bot side.  start() /
    stop() drive the status poller, mirroring the local client's run() lifecycle.
    '''

    ROUTE_PREFIX: ClassVar[str] = '/downloads'
    SPAN_PREFIX: ClassVar[str] = 'downloader'
