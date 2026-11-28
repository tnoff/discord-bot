'''DownloadHttpServer over raw HTTP, with a fake worker and no client.

The shared cases are `testing/queue_server_suite.py`: the server is held to the same contract the commands pod's
client is tested against, so neither needs the other pod. That the two agree is
tests/integration/commands_to_downloader/test_download_round_trip.py.
'''
from discord_downloader.servers.download_server import DownloadHttpServer

from testing.queue_server_suite import QueueServerSuite


class TestDownloadHttpServer(QueueServerSuite):
    '''The shared queue-server cases, against the downloader pod.

    The heartbeat job is `downloader_server`, NOT `downloader`: the gauge reports is_serving (the TCP site is up),
    and the bare name read like the worker loop's heartbeat, so a pod whose consumer loop was wedged still showed 1
    until the liveness probe restarted it. The loop's own series is `downloader_worker`, published by
    cli/_lib/worker_pod.py, and the two must not collide on this label.
    '''
    server_cls = DownloadHttpServer
    prefix = '/downloads'
    heartbeat_job = 'downloader_server'
