'''The commands pod's client against the search pod's real YouTube Music server.

What each end does alone is tested against the contract
(discord_commands/tests/clients/test_http_youtube_music_search_client.py,
discord_search/tests/servers/test_youtube_music_search_server.py); this is the part that needs both. The cases are
`testing/queue_round_trip_suite.py`.
'''
from discord_core.cogs.music_helpers.common import SearchType

from discord_commands.clients.http_youtube_music_search_client import HttpYoutubeMusicSearchClient
from discord_search.servers.youtube_music_search_server import YoutubeMusicSearchHttpServer

from testing.queue_round_trip_suite import QueueRoundTripSuite


class TestYoutubeMusicSearchRoundTrip(QueueRoundTripSuite):
    '''The shared round trips, between the bot's client and the search pod.'''
    client_cls = HttpYoutubeMusicSearchClient
    server_cls = YoutubeMusicSearchHttpServer
    request_search_type = SearchType.SEARCH
    request_search_string = 'some song name'


def test_client_and_server_share_one_route_group():
    '''The two halves are deployed as separate pods, so a prefix that drifts between them is a silent 404 across a
    version skew, not a test failure.

    Asserts the two read the SAME registry group object, not merely that their prefixes are equal: both derive
    ROUTE_PREFIX from ROUTES, so comparing prefixes would pass by construction. Identity is what holds the seam
    together, and it fails if either side is repointed.
    '''
    assert HttpYoutubeMusicSearchClient.ROUTES is YoutubeMusicSearchHttpServer.ROUTES
