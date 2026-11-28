'''HttpYoutubeMusicSearchClient, tested against the search pod's queue-worker contract and nothing else.

The shared cases are `testing/queue_client_suite.py`; what the search pod does with a body it accepts is
discord_search/tests/servers/test_youtube_music_search_server.py, and that the two agree is
tests/integration/commands_to_search/test_youtube_music_search_round_trip.py.
'''
# NOTE: test names here deliberately avoid being exactly 40 characters long.
# trufflehog's Lob detector matches `test_` + 35 chars, so a 40-char test
# function name is reported as a VERIFIED secret and fails pr-check:secrets.
from discord_core.cogs.music_helpers.common import SearchType

from discord_commands.clients.http_youtube_music_search_client import HttpYoutubeMusicSearchClient

from testing.contracts.queue_worker import YTMUSIC
from testing.queue_client_suite import QueueClientSuite, QueuePollerSuite


class TestHttpYoutubeMusicSearchClient(QueueClientSuite, QueuePollerSuite):
    '''The shared queue-client cases, against the search pod's contract.'''
    client_cls = HttpYoutubeMusicSearchClient
    contracts = YTMUSIC
    request_search_type = SearchType.SEARCH
    request_search_string = 'some song name'


def test_client_omits_the_pop_and_resolve_surface():
    '''resolve / get_input_nowait / backoff_wait / set_wait_timestamp are absent by design: under HA the search loop
    runs in the search pod, and resolutions come back to the bot through the broker's search-result queue, not this
    client.  A call site that reaches for one is a wiring bug and should fail loudly.'''
    client = HttpYoutubeMusicSearchClient('http://localhost:9999')
    for attribute in ('resolve', 'get_input_nowait', 'backoff_wait', 'set_wait_timestamp', 'local_worker'):
        assert not hasattr(client, attribute)
