'''
The database seam's contract: the persistence tier's stores, one POST route per store method.

Every route answers 200, failures included: the body is a ``DatabaseResponse`` with a ``result`` or an ``error``.
``result`` is ``Any`` on the wire model, so each route here gets a response model of its own with ``result`` typed to
what the store method returns. Only a request that cannot be read is a 422, and it is never retried.

The request bodies are `discord_core` types (`types/database_requests.py`, keyed by `REQUESTS`), so this table only
adds the summaries and the type of each ``result``. Both ends read it: the db pod's server is proved against it
(tests/routes/specs/database.py runs every reply) and the commands and broker pods' store clients are tested against a
stub that serves it, so neither needs the other pod.
'''
from pydantic import BaseModel, create_model

from discord_core.routes import database as database_routes
from discord_core.types.broker_responses import CachedDownload
from discord_core.types.database_requests import REQUESTS
from discord_core.types.database_wire import DatabaseResponse
from discord_core.types.guild_analytics import GuildAnalyticsEntry
from discord_core.types.markov import MarkovChannelEntry
from discord_core.types.playlist import PlaylistEntry, PlaylistItemAddOutcome, PlaylistItemEntry
from discord_core.types.video_cache import VideoCacheEntry

from testing.contracts.model import Contract, reply

GA, MK, PL, VC = (database_routes.GUILD_ANALYTICS, database_routes.MARKOV, database_routes.PLAYLIST,
                  database_routes.VIDEO_CACHE)


def _title(name: str) -> str:
    return ''.join(part.capitalize() for part in name.split('_'))


def _row(group, name: str, result, summary: str):
    '''One route: its request model from core, the type of its ``result``, and what it does.'''
    response: type[BaseModel] = create_model(
        f'{_title(group.name)}{_title(name)}Response', __base__=DatabaseResponse, result=(result | None, None))
    return group.routes[name], Contract(
        summary, request=REQUESTS[(group.name, name)],
        replies=(reply(200, response, doc='The `result`, or an `error` when the store failed.'),
                 reply(422, doc='A required field is missing or has the wrong shape.')))


CONTRACTS = dict([
    _row(GA, 'get_analytics', GuildAnalyticsEntry, "A guild's play totals."),
    _row(GA, 'record_play', bool, 'Add one play to the totals.'),
    _row(MK, 'list_channels', list[MarkovChannelEntry], 'Every tracked channel.'),
    _row(MK, 'list_guild_channel_ids', list[int], "A guild's tracked channel ids."),
    _row(MK, 'get_channel', MarkovChannelEntry, 'One tracked channel, or null.'),
    _row(MK, 'add_channel', MarkovChannelEntry, 'Start tracking a channel.'),
    _row(MK, 'remove_channel', bool, 'Stop tracking a channel.'),
    _row(MK, 'reset_channel', bool, "Forget a channel's read position."),
    _row(MK, 'save_messages', int,
         'Write a batch of messages. The result is how many were written, or null if the channel is not tracked.'),
    _row(MK, 'generate_words', list[str], 'Walk the chain for up to `count` words.'),
    _row(MK, 'prune_relations_before', bool, 'Drop relations older than the cutoff.'),
    _row(PL, 'list_playlists', list[PlaylistEntry], "A guild's playlists, newest first."),
    _row(PL, 'count_playlists', int, 'How many playlists a guild has.'),
    _row(PL, 'get_playlist', PlaylistEntry, 'One playlist by id, or null.'),
    _row(PL, 'get_playlist_by_name', PlaylistEntry, 'One playlist by name, or null.'),
    _row(PL, 'get_history_playlist', PlaylistEntry, "A guild's history playlist, or null."),
    _row(PL, 'ensure_history_playlist', int, "The id of a guild's history playlist, creating it if absent."),
    _row(PL, 'create_playlist', PlaylistEntry, 'Make a playlist and return it.'),
    _row(PL, 'delete_playlist', bool, 'Drop a playlist and its items.'),
    _row(PL, 'rename_playlist', bool, 'Rename a playlist.'),
    _row(PL, 'mark_queued', bool, 'Stamp when a playlist was last queued.'),
    _row(PL, 'get_playlist_size', int, 'How many items a playlist holds.'),
    _row(PL, 'list_items', list[PlaylistItemEntry], "A playlist's items, oldest first."),
    _row(PL, 'add_items', list[PlaylistItemAddOutcome],
         'Add items, stopping when the playlist is full. One outcome per item attempted.'),
    _row(PL, 'delete_item', bool, 'Drop one item by id.'),
    _row(PL, 'delete_item_by_index', PlaylistItemEntry,
         'Drop the item at a zero-based position and return it, or null if out of range.'),
    _row(PL, 'record_history_item', bool,
         'Write one played track to the history playlist, evicting the oldest to make room.'),
    _row(VC, 'iterate_file', bool, 'Catalog a finished download.'),
    _row(VC, 'get_webpage_url_item', CachedDownload, 'Look up a cache hit for a request. A miss is a null result.'),
    _row(VC, 'remove_video_cache', bool, 'Drop cache rows by id.'),
    _row(VC, 'ready_remove', bool, 'Apply the eviction policy, or flag every row when `evict_all` is set.'),
    _row(VC, 'get_deletable_entries', list[VideoCacheEntry], 'The rows flagged for deletion.'),
    _row(VC, 'get_cache_count', int, 'How many rows the catalog holds.'),
])
