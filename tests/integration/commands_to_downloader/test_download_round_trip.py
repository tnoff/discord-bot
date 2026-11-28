'''The commands pod's client against the downloader pod's real server.

What each end does alone is tested against the contract (discord_commands/tests/clients/test_http_download_client.py,
discord_downloader/tests/servers/test_download_server.py); this is the part that needs both. The cases are
`testing/queue_round_trip_suite.py`.
'''
from discord_commands.clients.http_download_client import HttpDownloadClient
from discord_downloader.servers.download_server import DownloadHttpServer

from testing.queue_round_trip_suite import QueueRoundTripSuite


class TestDownloadRoundTrip(QueueRoundTripSuite):
    '''The shared round trips, between the bot's client and the downloader.'''
    client_cls = HttpDownloadClient
    server_cls = DownloadHttpServer
