'''
The database seam: the persistence tier's stores, one POST route per store method.

Every route answers 200, failures included: the body is a ``DatabaseResponse`` with a ``result``
or an ``error``. ``result`` is ``Any`` on the wire model, so each route here gets a response model
of its own (``result`` typed to what the store method returns), built from the shared envelope.
Only a request that cannot be read is a 422.

The table below is the whole description: one row per route. Each row's store method is replaced
by a mock that returns the row's sample, and the handler's answer is checked against the typed
model, so a row whose ``result`` type disagrees with what the handler serialises fails.
'''
from datetime import datetime, timezone
from unittest.mock import AsyncMock

from pydantic import BaseModel, create_model

from discord_core.routes import database as database_routes
from discord_core.types.broker_responses import CachedDownload
from discord_core.types.database_wire import DatabaseResponse
from discord_core.types.guild_analytics import GuildAnalyticsEntry
from discord_core.types.markov import MarkovChannelEntry, MarkovMessageWrite
from discord_core.types.media_download import media_download_from_dict
from discord_core.types.media_request import MediaRequest
from discord_core.types.playlist import (PlaylistEntry, PlaylistItemAddOutcome, PlaylistItemAddStatus,
                                         PlaylistItemEntry, PlaylistItemWrite)
from discord_core.types.video_cache import VideoCacheEntry

from discord_db.servers.database_server import DatabaseHttpServer

from tests.fakes.worker_requests import direct_request
from tests.routes.specs._model import Call, Operation, reply, serving

GUILD = 5
CHANNEL = 2
NOW = datetime(2026, 1, 1, tzinfo=timezone.utc)


# ---------------------------------------------------------------------------
# Request bodies
# ---------------------------------------------------------------------------

class Nothing(BaseModel):
    '''A route that reads no field; the body is an empty object.'''


class GuildBody(BaseModel):
    '''A body naming a guild.'''
    guild_id: int


class GuildChannelBody(GuildBody):
    '''A body naming a channel of a guild.'''
    channel_id: int


class RecordPlayBody(GuildBody):
    '''guild_analytics/record_play'''
    duration_seconds: int
    cache_hit: bool


class SaveMessagesBody(GuildChannelBody):
    '''markov/save_messages'''
    messages: list[MarkovMessageWrite]


class GenerateWordsBody(GuildBody):
    '''markov/generate_words. `first_word` is optional: `!markov speak` with no seed sends null.'''
    count: int
    first_word: str | None = None


class PruneBody(BaseModel):
    '''markov/prune_relations_before'''
    cutoff: datetime


class PlaylistBody(BaseModel):
    '''A body naming a playlist.'''
    playlist_id: int


class GuildNameBody(GuildBody):
    '''A body naming a playlist by guild and name.'''
    name: str


class RenameBody(PlaylistBody):
    '''playlist/rename_playlist'''
    name: str


class AddItemsBody(PlaylistBody):
    '''playlist/add_items'''
    items: list[PlaylistItemWrite]
    max_size: int


class ItemBody(BaseModel):
    '''playlist/delete_item'''
    item_id: int


class IndexBody(PlaylistBody):
    '''playlist/delete_item_by_index. Zero is the first item, not a missing argument.'''
    index: int


class RecordHistoryBody(PlaylistBody):
    '''playlist/record_history_item'''
    item: PlaylistItemWrite
    max_size: int


class MediaDownloadBody(BaseModel):
    '''A finished download, as the video-cache routes carry it.'''
    request: MediaRequest
    file_path: str | None = None
    file_size_bytes: int | None = None
    cache_hit: bool = False
    ytdl_data: dict = {}


class IterateFileBody(BaseModel):
    '''video_cache/iterate_file'''
    media_download: MediaDownloadBody


class MediaRequestBody(BaseModel):
    '''video_cache/get_webpage_url_item'''
    media_request: MediaRequest


class RemoveCacheBody(BaseModel):
    '''video_cache/remove_video_cache. `base_paths` also drops the rows holding those object keys.'''
    video_cache_ids: list[int]
    base_paths: list[str] = []


class ReadyRemoveBody(BaseModel):
    '''video_cache/ready_remove. `evict_all` flags every row rather than applying the policy.'''
    evict_all: bool = False


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


def _title(name: str) -> str:
    return ''.join(part.capitalize() for part in name.split('_'))


def row(group, name: str, request: type[BaseModel], result, body: dict, sample, summary: str):
    '''One route: its request model, the type of its ``result``, a body, and what the store returns.

    `sample` is what the mock store returns; the handler turns it into the wire form. A body of
    `{}` has nothing to omit, so the 422 call sends text that is not JSON instead.
    '''
    response = create_model(f'{_title(group.name)}{_title(name)}Response', __base__=DatabaseResponse,
                            result=(result | None, None))
    unreadable = Call(422, body='not json', raw=True) if not body else Call(422, body={})
    return group.routes[name], Operation(
        summary, request=request,
        replies=(reply(200, response, doc='The `result`, or an `error` when the store failed.'),
                 reply(422, doc='A required field is missing or has the wrong shape.')),
        calls=(Call(200, body=body, arrange=returns(group.name, name, sample)),
               Call(200, body=body, arrange=fails(group.name, name)),
               unreadable))


GA, MK, PL, VC = (database_routes.GUILD_ANALYTICS, database_routes.MARKOV, database_routes.PLAYLIST,
                  database_routes.VIDEO_CACHE)
GUILD_BODY = {'guild_id': GUILD}
CHANNEL_BODY = {'guild_id': GUILD, 'channel_id': CHANNEL}
PLAYLIST_BODY = {'playlist_id': 3}
NAME_BODY = {'guild_id': GUILD, 'name': 'mix'}
ADD_OUTCOME = PlaylistItemAddOutcome(video_url='https://example.com/a', title='a',
                                     status=PlaylistItemAddStatus.ADDED, item_id=4)

OPERATIONS = dict([
    row(GA, 'get_analytics', GuildBody, GuildAnalyticsEntry, GUILD_BODY,
        GuildAnalyticsEntry(total_plays=3, cached_plays=1, total_duration_days=0,
                            total_duration_seconds=500, created_at=NOW), "A guild's play totals."),
    row(GA, 'record_play', RecordPlayBody, bool, {**GUILD_BODY, 'duration_seconds': 90, 'cache_hit': True},
        True, 'Add one play to the totals.'),
    row(MK, 'list_channels', Nothing, list[MarkovChannelEntry], {}, [MARKOV_CHANNEL],
        'Every tracked channel.'),
    row(MK, 'list_guild_channel_ids', GuildBody, list[int], GUILD_BODY, [CHANNEL],
        "A guild's tracked channel ids."),
    row(MK, 'get_channel', GuildChannelBody, MarkovChannelEntry, CHANNEL_BODY, MARKOV_CHANNEL,
        'One tracked channel, or null.'),
    row(MK, 'add_channel', GuildChannelBody, MarkovChannelEntry, CHANNEL_BODY, MARKOV_CHANNEL,
        'Start tracking a channel.'),
    row(MK, 'remove_channel', GuildChannelBody, bool, CHANNEL_BODY, True, 'Stop tracking a channel.'),
    row(MK, 'reset_channel', GuildChannelBody, bool, CHANNEL_BODY, True,
        "Forget a channel's read position."),
    row(MK, 'save_messages', SaveMessagesBody, int, {
        **CHANNEL_BODY,
        'messages': [{'word_pairs': [['a', 'b']], 'message_timestamp': NOW.isoformat(), 'last_message_id': 9}]},
        2, 'Write a batch of messages. The result is how many were written, or null if the channel is not tracked.'),
    row(MK, 'generate_words', GenerateWordsBody, list[str], {**GUILD_BODY, 'count': 5}, ['a', 'b'],
        'Walk the chain for up to `count` words.'),
    row(MK, 'prune_relations_before', PruneBody, bool, {'cutoff': NOW.isoformat()}, True,
        'Drop relations older than the cutoff.'),
    row(PL, 'list_playlists', GuildBody, list[PlaylistEntry], GUILD_BODY, [PLAYLIST],
        "A guild's playlists, newest first."),
    row(PL, 'count_playlists', GuildBody, int, GUILD_BODY, 2, 'How many playlists a guild has.'),
    row(PL, 'get_playlist', PlaylistBody, PlaylistEntry, PLAYLIST_BODY, PLAYLIST,
        'One playlist by id, or null.'),
    row(PL, 'get_playlist_by_name', GuildNameBody, PlaylistEntry, NAME_BODY, PLAYLIST,
        'One playlist by name, or null.'),
    row(PL, 'get_history_playlist', GuildBody, PlaylistEntry, GUILD_BODY, PLAYLIST,
        "A guild's history playlist, or null."),
    row(PL, 'ensure_history_playlist', GuildBody, int, GUILD_BODY, 3,
        "The id of a guild's history playlist, creating it if absent."),
    row(PL, 'create_playlist', GuildNameBody, PlaylistEntry, NAME_BODY, PLAYLIST,
        'Make a playlist and return it.'),
    row(PL, 'delete_playlist', PlaylistBody, bool, PLAYLIST_BODY, True, 'Drop a playlist and its items.'),
    row(PL, 'rename_playlist', RenameBody, bool, {**PLAYLIST_BODY, 'name': 'new'}, True, 'Rename a playlist.'),
    row(PL, 'mark_queued', PlaylistBody, bool, PLAYLIST_BODY, True, 'Stamp when a playlist was last queued.'),
    row(PL, 'get_playlist_size', PlaylistBody, int, PLAYLIST_BODY, 4, 'How many items a playlist holds.'),
    row(PL, 'list_items', PlaylistBody, list[PlaylistItemEntry], PLAYLIST_BODY, [ITEM],
        "A playlist's items, oldest first."),
    row(PL, 'add_items', AddItemsBody, list[PlaylistItemAddOutcome],
        {**PLAYLIST_BODY, 'items': [ITEM_WRITE], 'max_size': 64}, [ADD_OUTCOME],
        'Add items, stopping when the playlist is full. One outcome per item attempted.'),
    row(PL, 'delete_item', ItemBody, bool, {'item_id': 4}, True, 'Drop one item by id.'),
    row(PL, 'delete_item_by_index', IndexBody, PlaylistItemEntry, {**PLAYLIST_BODY, 'index': 0}, ITEM,
        'Drop the item at a zero-based position and return it, or null if out of range.'),
    row(PL, 'record_history_item', RecordHistoryBody, bool,
        {**PLAYLIST_BODY, 'item': ITEM_WRITE, 'max_size': 100}, True,
        'Write one played track to the history playlist, evicting the oldest to make room.'),
    row(VC, 'iterate_file', IterateFileBody, bool, {'media_download': DOWNLOAD}, True,
        'Catalog a finished download.'),
    row(VC, 'get_webpage_url_item', MediaRequestBody, CachedDownload, {'media_request': MEDIA_REQUEST}, CACHED,
        'Look up a cache hit for a request. A miss is a null result.'),
    row(VC, 'remove_video_cache', RemoveCacheBody, bool, {'video_cache_ids': [8]}, True,
        'Drop cache rows by id.'),
    row(VC, 'ready_remove', ReadyRemoveBody, bool, {}, True,
        'Apply the eviction policy, or flag every row when `evict_all` is set.'),
    row(VC, 'get_deletable_entries', Nothing, list[VideoCacheEntry], {}, [CACHE_ENTRY],
        'The rows flagged for deletion.'),
    row(VC, 'get_cache_count', Nothing, int, {}, 7, 'How many rows the catalog holds.'),
])
