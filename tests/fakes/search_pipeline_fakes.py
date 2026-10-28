'''
Helpers shared by the tests of the music search pipeline: a broker-owned bundle to assert on, the
ytmusic client stand-in and the request builders.
'''
from discord_core.cogs.music_helpers.common import SearchType
from discord_core.types.media_request import MediaRequest
from discord_core.types.playlist_add_request import PlaylistAddRequest
from discord_core.types.search import SearchResult

from discord_broker.workers.media_bundle import BundleRenderer


class BrokerBundleProxy:
    '''Read-through view of a broker-owned bundle for test assertions.

    Mirrors the small surface the old MultiMediaRequestBundle exposed
    (uuid / bundled_requests / completed / finished) but resolves everything
    from the broker's live BundleState so tests assert broker-side state.
    '''
    def __init__(self, broker, uuid):
        self._broker = broker
        self.uuid = uuid

    @property
    def _renderer(self):
        state = self._broker.get_bundle_state(self.uuid)
        renderer = BundleRenderer(state)
        renderer.update_request_status()
        return renderer

    @property
    def bundled_requests(self):
        '''Live BundledRequestState list off the broker state.'''
        return self._broker.get_bundle_state(self.uuid).bundled_requests

    @property
    def completed(self):
        '''Completed counter after reconciling lifecycle stages.'''
        return self._renderer.state.completed

    @property
    def finished(self):
        '''True once all requests reach a terminal stage.'''
        return self._renderer.finished


async def make_broker_bundle(cog, test_context, request=None, all_added=False,
                             has_search_banner=False):
    '''Create a broker-owned bundle (optionally with one registered request).

    Returns (BrokerBundleProxy, request).  Mirrors the old test pattern of
    constructing a MultiMediaRequestBundle, attaching it to the cog, and adding
    a request — now routed through the broker so the cog owns nothing.
    '''
    bundle_uuid = await cog.broker_client.create_bundle(
        test_context['guild'].id, test_context['channel'].id,
        has_search_banner=has_search_banner,
    )
    if request is not None:
        request.bundle_uuid = bundle_uuid
        await cog.broker_client.register_request(request)
    if all_added:
        await cog.broker_client.finalize_bundle(bundle_uuid)
    return BrokerBundleProxy(cog.broker_client.local_broker, bundle_uuid), request



class MockYoutubeMusicClient:
    """Mock YouTube Music client for testing"""
    def __init__(self, youtube_music_result='test-video-id'):
        self.youtube_music_result = youtube_music_result

    def search(self, search_string): #pylint:disable=unused-argument
        """Mock YouTube Music search that returns a video ID"""
        return self.youtube_music_result


def create_test_media_request(test_context, search_string='test search', bundle_uuid=None, search_type=SearchType.SEARCH):
    """Helper to create test media requests"""
    request = MediaRequest(
        guild_id=test_context['guild'].id,
        channel_id=test_context['channel'].id,
        requester_name=test_context['author'].display_name,
        requester_id=test_context['author'].id,
        search_result=SearchResult(search_type=search_type, raw_search_string=search_string)
    )
    if bundle_uuid:
        request.bundle_uuid = bundle_uuid
    return request


def create_test_playlist_add_request(test_context, playlist_id, search_string='test search', bundle_uuid=None, search_type=SearchType.SEARCH):
    """Helper to create test PlaylistAddRequest objects"""
    request = PlaylistAddRequest(
        guild_id=test_context['guild'].id,
        channel_id=test_context['channel'].id,
        requester_name=test_context['author'].display_name,
        requester_id=test_context['author'].id,
        search_result=SearchResult(search_type=search_type, raw_search_string=search_string),
        playlist_id=playlist_id,
    )
    if bundle_uuid:
        request.bundle_uuid = bundle_uuid
    return request
