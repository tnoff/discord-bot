'''YoutubeMusicSearchHttpServer over raw HTTP, with a fake worker and no client.

The shared cases are `testing/queue_server_suite.py`: the server is held to the same contract the commands pod's
client is tested against, so neither needs the other pod. That the two agree is
tests/integration/commands_to_search/test_youtube_music_search_round_trip.py.
'''
from discord_core.cogs.music_helpers.common import SearchType

from discord_search.servers.youtube_music_search_server import YoutubeMusicSearchHttpServer

from testing.queue_server_suite import QueueServerSuite
from testing.queue_worker_support import FakeQueueWorker


class TestYoutubeMusicSearchHttpServer(QueueServerSuite):
    '''The shared queue-server cases, against the search pod.

    The heartbeat job is the search pod's own label, NOT `youtube_music_search`, which the bot cog's search loop
    already publishes.
    '''
    server_cls = YoutubeMusicSearchHttpServer
    prefix = '/search/ytmusic'
    request_search_type = SearchType.SEARCH
    request_search_string = 'some song name'
    heartbeat_job = 'youtube_music_search_server'


def test_routes_sit_under_a_per_provider_segment():
    '''ytmusic owns /search/ytmusic, not a bare /search. The search tier co-hosts the media_search providers
    (Spotify, the YouTube Data API) in the same pod, so /search stays the pod namespace and /search/spotify +
    /search/youtube stay free. Renaming this route once the pod ships would be a cross-pod protocol change, the
    skew-sensitive kind that broke this seam twice.'''
    assert YoutubeMusicSearchHttpServer.ROUTE_PREFIX == '/search/ytmusic'


def test_default_port_is_8084():
    '''The search pod listens on 8084 (the downloader has 8083).'''
    assert YoutubeMusicSearchHttpServer(FakeQueueWorker())._port == 8084  # pylint: disable=protected-access


def test_explicit_port_overrides_the_class_default():
    '''An explicit port wins over DEFAULT_PORT.'''
    assert YoutubeMusicSearchHttpServer(FakeQueueWorker(), port=9111)._port == 9111  # pylint: disable=protected-access
