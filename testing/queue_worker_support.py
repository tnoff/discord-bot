'''
What the queue-worker suites share: a stand-in worker and the requests they put through it.

The downloader's and the search pod's queue-worker seam is one seam pointed at two pods, so its tests are suites
(``queue_client_suite``, ``queue_server_suite``, ``queue_round_trip_suite``) that a pod's test module subclasses and
fills in. This module holds the pieces they have in common.

Test names in the suites must not be exactly 40 characters long: trufflehog's Lob detector matches
`test_` + 35 chars, the shape of a Lob test API key, and fails pr-check:secrets.
'''
from unittest.mock import AsyncMock

from discord_core.cogs.music_helpers.common import SearchType
from discord_core.types.media_request import MediaRequest
from discord_core.types.playlist_add_request import PlaylistAddRequest
from discord_core.types.search import SearchResult

from testing.queue_status import status_snapshot

UNREACHABLE_URL = 'http://localhost:9999'


def media_request(guild_id: int = 1, *, search_type=SearchType.DIRECT,
                  raw_search_string='https://example.com/v.mp4') -> MediaRequest:
    '''A MediaRequest for the given guild.'''
    return MediaRequest(
        guild_id=guild_id, channel_id=2, requester_id=3, requester_name='req',
        search_result=SearchResult(search_type=search_type, raw_search_string=raw_search_string),
    )


def playlist_add_request(guild_id: int = 1, bundle_uuid: str | None = None) -> PlaylistAddRequest:
    '''A PlaylistAddRequest (download_file=False) for the given guild.'''
    return PlaylistAddRequest(
        guild_id=guild_id, channel_id=2, requester_id=3, requester_name='req',
        playlist_id=99,
        bundle_uuid=bundle_uuid,
        search_result=SearchResult(search_type=SearchType.SEARCH, raw_search_string='song name'),
    )


class FakeQueueWorker:
    '''Minimal queue worker stand-in covering the server's call sites.'''

    def __init__(self, *, status=None, clear_items=None):
        self.submit = AsyncMock()
        self.block_guild = AsyncMock(return_value=True)
        self.status_snapshot = AsyncMock(return_value=status or status_snapshot())
        self._clear_items = clear_items or []
        self.clear_guild_queue = AsyncMock(side_effect=self._clear)

    async def _clear(self, guild_id, preserve_predicate=None):
        '''Apply the server-supplied predicate exactly as the real worker would,
        so the server's flag→predicate translation is exercised end to end.'''
        del guild_id
        return [mr for mr in self._clear_items
                if not (preserve_predicate and preserve_predicate(mr))]


class QueueSuiteBase:
    '''What every queue-worker suite needs to know about the pod under test.

    Not collected itself; a subclass in a ``test_*`` module supplies the attributes.
    '''
    # The pod's pieces, as the classes a test module imports.
    client_cls: type
    server_cls: type
    # The request shape this pod accepts.
    request_search_type = SearchType.DIRECT
    request_search_string = 'https://example.com/v.mp4'

    def request(self, guild_id: int = 1) -> MediaRequest:
        '''A request in the shape this pod accepts.'''
        return media_request(guild_id, search_type=self.request_search_type,
                             raw_search_string=self.request_search_string)
