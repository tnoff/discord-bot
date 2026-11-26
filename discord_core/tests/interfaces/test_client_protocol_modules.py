'''Tests for the small cog-facing Protocol and ABC modules in interfaces/.'''
import dataclasses
import inspect

import pytest

from discord_core.interfaces.broker_client_protocol import BrokerClient
from discord_core.interfaces.download_client_protocol import DownloadClient, RETRY_BACKOFF_SECONDS_MINIMUM
from discord_core.interfaces.gateway_intent_client import GatewayIntentClient
from discord_core.interfaces.guild_queue_client import GuildQueueClient
from discord_core.interfaces.media_search_protocols import MediaSearchClient
from discord_core.interfaces.player_gateway import EnsureResult, PlayerGateway, PlayerStatus
from discord_core.interfaces.player_session_store import PlayerSessionClient, PlayerSessionStore
from discord_core.types.catalog import CatalogResponse
from discord_core.types.player_session import PlayerSession


def async_methods(protocol) -> set[str]:
    '''Names of the coroutine functions the protocol class itself defines.'''
    return {name for name, member in vars(protocol).items() if inspect.iscoroutinefunction(member)}


def test_broker_client_inherits_the_three_narrower_protocols():
    '''BrokerClient is the union of session, queue and intent surfaces plus its own'''
    for parent in (PlayerSessionClient, GuildQueueClient, GatewayIntentClient):
        assert parent in BrokerClient.__mro__


def test_broker_client_declares_the_request_lifecycle_and_bundle_calls():
    '''The cog-facing broker surface names every call the cog makes'''
    expected = {
        'register_request', 'update_request_status', 'register_download',
        'register_download_result', 'next_result', 'register_search_result',
        'next_search_result', 'checkout', 'release', 'remove', 'discard',
        'check_cache', 'cache_cleanup', 'report_corrupt_cache_object',
        'get_cache_count', 'create_bundle', 'finalize_bundle', 'delete_bundle',
        'list_bundles_for_guild',
    }
    assert async_methods(BrokerClient) == expected
    parameters = inspect.signature(BrokerClient.create_bundle).parameters
    assert parameters['input_string'].default is None
    assert parameters['has_search_banner'].default is False


def test_download_client_surface_and_backoff_default():
    '''DownloadClient has async producer calls and sync read-only properties'''
    assert async_methods(DownloadClient) == {'submit', 'block_guild', 'clear_guild_queue', 'queue_size', 'run'}
    assert isinstance(DownloadClient.failure_summary, property)
    assert isinstance(DownloadClient.backoff_seconds_remaining, property)
    assert RETRY_BACKOFF_SECONDS_MINIMUM == 30


class FakeMediaSearch:
    '''Implements both provider calls.'''

    async def spotify_source(self, playlist_id=None, album_id=None, track_id=None):
        '''Return a one-item catalog naming whichever id was given.'''
        chosen = playlist_id or album_id or track_id
        return CatalogResponse(items=[{'search_string': chosen}])

    async def youtube_source(self, playlist_id):
        '''Return an empty catalog.'''
        return CatalogResponse(collection_name=playlist_id)


class OnlySpotify:
    '''Missing the youtube call.'''

    async def spotify_source(self, playlist_id=None, album_id=None, track_id=None):
        '''Return an empty catalog named after the ids.'''
        return CatalogResponse(collection_name=f'{playlist_id}{album_id}{track_id}')


@pytest.mark.asyncio
async def test_media_search_protocol_is_runtime_checkable():
    '''A class with both calls conforms; one missing a call does not'''
    assert isinstance(FakeMediaSearch(), MediaSearchClient)
    assert not isinstance(OnlySpotify(), MediaSearchClient)
    catalog = await FakeMediaSearch().spotify_source(track_id='t1')
    assert [item.search_string for item in catalog.items] == ['t1']


def test_player_status_defaults_describe_a_missing_player():
    '''A bare PlayerStatus is not connected, not playing, with nothing pending'''
    status = PlayerStatus(exists=False)
    assert (status.voice_connected, status.is_playing, status.pending_downloads) == (False, False, 0)
    assert status.text_channel_id is None


def test_player_gateway_dataclasses_are_frozen():
    '''Snapshots cannot be mutated by command logic'''
    status = PlayerStatus(exists=True, is_playing=True, text_channel_id=5)
    with pytest.raises(dataclasses.FrozenInstanceError):
        status.is_playing = False
    result = EnsureResult(ok=False, error='no voice')
    assert result.error == 'no voice'
    assert EnsureResult(ok=True).error is None
    with pytest.raises(dataclasses.FrozenInstanceError):
        result.ok = True


def test_player_gateway_declares_the_player_calls():
    '''The gateway surface is exactly the calls command logic may make'''
    assert async_methods(PlayerGateway) == {
        'status', 'has_live_player', 'ensure', 'stop', 'move_messages', 'wake', 'add_cached'}


class MemorySessionStore(PlayerSessionStore):
    '''Dict-backed engine implementation of the session store.'''

    def __init__(self):
        '''Start empty.'''
        self.sessions = {}

    async def save_player_session(self, session):
        '''Replace any existing session for the guild.'''
        self.sessions[session.guild_id] = session

    async def list_player_sessions(self):
        '''Snapshot every session.'''
        return list(self.sessions.values())

    async def delete_player_session(self, guild_id):
        '''Drop the session if present.'''
        self.sessions.pop(guild_id, None)


@pytest.mark.asyncio
async def test_session_store_abc_requires_all_three_methods_and_round_trips():
    '''A concrete store works; the abstract base and a partial subclass cannot be built'''
    assert PlayerSessionStore.__abstractmethods__ == {
        'save_player_session', 'list_player_sessions', 'delete_player_session'}
    store = MemorySessionStore()
    session = PlayerSession(guild_id=1, voice_channel_id=2, text_channel_id=3)
    await store.save_player_session(session)
    assert await store.list_player_sessions() == [session]
    await store.delete_player_session(1)
    await store.delete_player_session(1)
    assert await store.list_player_sessions() == []
    assert async_methods(PlayerSessionClient) == PlayerSessionStore.__abstractmethods__
