'''
The database seam: the persistence tier's stores, one POST route per store method.

What each route takes and answers is `testing/contracts/database.py`, which the store clients are also
tested against. The table below is the proof: one row per route. Each row's store method is replaced by a
mock that returns the row's sample, and the handler's answer is checked against the contract's typed model,
so a row whose ``result`` type disagrees with what the handler serialises fails.
'''
from datetime import datetime, timezone
from unittest.mock import AsyncMock

from discord_core.routes import database as database_routes
from discord_core.types.guild_analytics import GuildAnalyticsEntry
from discord_core.types.markov import MarkovChannelEntry
from discord_core.types.media_download import media_download_from_dict
from discord_core.types.media_request import MediaRequest
from discord_core.types.playlist import (PlaylistEntry, PlaylistItemAddOutcome, PlaylistItemAddStatus,
                                         PlaylistItemEntry)
from discord_core.types.video_cache import VideoCacheEntry

from discord_db.servers.database_server import DatabaseHttpServer

from testing.fakes.worker_requests import direct_request
from testing.contracts.database import CONTRACTS, GA, MK, PL, VC
from tests.routes.specs._model import Call, operation, serving

GUILD = 5
CHANNEL = 2
NOW = datetime(2026, 1, 1, tzinfo=timezone.utc)


# ---------------------------------------------------------------------------
# Samples: what a store returns, and the body that asks for it
# ---------------------------------------------------------------------------

MARKOV_CHANNEL = MarkovChannelEntry(id=1, channel_id=CHANNEL, server_id=GUILD, last_message_id=9)
PLAYLIST = PlaylistEntry(id=3, name='mix', server_id=GUILD, last_queued=NOW, created_at=NOW, is_history=False)
ITEM = PlaylistItemEntry(id=4, title='a', video_url='https://example.com/a', uploader='u',
                         playlist_id=3, created_at=NOW)
ITEM_WRITE = {'video_url': 'https://example.com/a', 'title': 'a', 'uploader': 'u'}
CACHE_ENTRY = VideoCacheEntry(id=8, video_id='a', video_url='https://example.com/a', title='a',
                              uploader='u', duration=90, extractor='youtube', last_iterated_at=NOW,
                              created_at=NOW, count=1, ready_for_deletion=False, file_size_bytes=10,
                              base_path='cache/a', storage_type='s3')
DOWNLOAD = {'request': direct_request(GUILD, CHANNEL).model_dump(mode='json'), 'file_path': 'a.mp3',
            'file_size_bytes': 10, 'cache_hit': True,
            'ytdl_data': {'id': 'a', 'title': 'a', 'webpage_url': 'https://example.com/a',
                          'uploader': 'u', 'duration': 90, 'extractor': 'youtube'}}
MEDIA_REQUEST = direct_request(GUILD, CHANNEL).model_dump(mode='json')
CACHED = media_download_from_dict(DOWNLOAD, MediaRequest.model_validate(MEDIA_REQUEST))


def environment():
    '''A db pod serving every group, each over a mock store.'''
    stores = {group.name: AsyncMock() for group in database_routes.GROUPS}
    server = DatabaseHttpServer(
        guild_analytics_store=stores['guild_analytics'], markov_store=stores['markov'],
        playlist_store=stores['playlist'], video_cache_store=stores['video_cache'])
    return serving(server, **stores)


def returns(group: str, method: str, value):
    '''An arrangement that makes the store method answer ``value``.'''
    async def arrange(env) -> dict:
        getattr(getattr(env.parts, group), method).return_value = value
        return {}
    return arrange


def fails(group: str, method: str):
    '''An arrangement that makes the store method raise, as a database that is down does.'''
    async def arrange(env) -> dict:
        getattr(getattr(env.parts, group), method).side_effect = RuntimeError('database is down')
        return {}
    return arrange


def row(group, name: str, body: dict, sample):
    '''One route: the calls that prove its contract on the live handler.

    `sample` is what the mock store returns; the handler turns it into the wire form. A body of
    `{}` has nothing to omit, so the 422 call sends text that is not JSON instead.
    '''
    unreadable = Call(422, body='not json', raw=True) if not body else Call(422, body={})
    route = group.routes[name]
    return route, operation(CONTRACTS[route], (
        Call(200, body=body, arrange=returns(group.name, name, sample)),
        Call(200, body=body, arrange=fails(group.name, name)),
        unreadable))


GUILD_BODY = {'guild_id': GUILD}
CHANNEL_BODY = {'guild_id': GUILD, 'channel_id': CHANNEL}
PLAYLIST_BODY = {'playlist_id': 3}
NAME_BODY = {'guild_id': GUILD, 'name': 'mix'}
ADD_OUTCOME = PlaylistItemAddOutcome(video_url='https://example.com/a', title='a',
                                     status=PlaylistItemAddStatus.ADDED, item_id=4)

OPERATIONS = dict([
    row(GA, 'get_analytics', GUILD_BODY,
        GuildAnalyticsEntry(total_plays=3, cached_plays=1, total_duration_days=0,
                            total_duration_seconds=500, created_at=NOW)),
    row(GA, 'record_play', {**GUILD_BODY, 'duration_seconds': 90, 'cache_hit': True},
        True),
    row(MK, 'list_channels', {}, [MARKOV_CHANNEL]),
    row(MK, 'list_guild_channel_ids', GUILD_BODY, [CHANNEL]),
    row(MK, 'get_channel', CHANNEL_BODY, MARKOV_CHANNEL),
    row(MK, 'add_channel', CHANNEL_BODY, MARKOV_CHANNEL),
    row(MK, 'remove_channel', CHANNEL_BODY, True),
    row(MK, 'reset_channel', CHANNEL_BODY, True),
    row(MK, 'save_messages', {
        **CHANNEL_BODY,
        'messages': [{'word_pairs': [['a', 'b']], 'message_timestamp': NOW.isoformat(), 'last_message_id': 9}]},
        2),
    row(MK, 'generate_words', {**GUILD_BODY, 'count': 5}, ['a', 'b']),
    row(MK, 'prune_relations_before', {'cutoff': NOW.isoformat()}, True),
    row(PL, 'list_playlists', GUILD_BODY, [PLAYLIST]),
    row(PL, 'count_playlists', GUILD_BODY, 2),
    row(PL, 'get_playlist', PLAYLIST_BODY, PLAYLIST),
    row(PL, 'get_playlist_by_name', NAME_BODY, PLAYLIST),
    row(PL, 'get_history_playlist', GUILD_BODY, PLAYLIST),
    row(PL, 'ensure_history_playlist', GUILD_BODY, 3),
    row(PL, 'create_playlist', NAME_BODY, PLAYLIST),
    row(PL, 'delete_playlist', PLAYLIST_BODY, True),
    row(PL, 'rename_playlist', {**PLAYLIST_BODY, 'name': 'new'}, True),
    row(PL, 'mark_queued', PLAYLIST_BODY, True),
    row(PL, 'get_playlist_size', PLAYLIST_BODY, 4),
    row(PL, 'list_items', PLAYLIST_BODY, [ITEM]),
    row(PL, 'add_items',
        {**PLAYLIST_BODY, 'items': [ITEM_WRITE], 'max_size': 64}, [ADD_OUTCOME]),
    row(PL, 'delete_item', {'item_id': 4}, True),
    row(PL, 'delete_item_by_index', {**PLAYLIST_BODY, 'index': 0}, ITEM),
    row(PL, 'record_history_item',
        {**PLAYLIST_BODY, 'item': ITEM_WRITE, 'max_size': 100}, True),
    row(VC, 'iterate_file', {'media_download': DOWNLOAD}, True),
    row(VC, 'get_webpage_url_item', {'media_request': MEDIA_REQUEST}, CACHED),
    row(VC, 'remove_video_cache', {'video_cache_ids': [8]}, True),
    row(VC, 'ready_remove', {}, True),
    row(VC, 'get_deletable_entries', {}, [CACHE_ENTRY]),
    row(VC, 'get_cache_count', {}, 7),
])
