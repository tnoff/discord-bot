'''
The request bodies of the database seam (routes/database.py), served by the db pod and called by the bot's
store clients and the broker's video-cache store.

Here so the clients build and the server reads the SAME model rather than each writing out the field names,
and so the contract tests read them. The envelope every route answers with is `types/database_wire.py`; the
rest of the answers are the DTOs each Protocol method returns.

A route that takes no arguments (`list_channels`, `get_deletable_entries`, `get_cache_count`) has no model: its
body is ignored.

One model serves every route that takes the same arguments, so the names say what is sent, not which route
sends it (`GuildRequest` is six routes). Every field is required unless it says otherwise, and a missing or
null one is a 422 rather than an error envelope: the request never reached the database, and sending the same
bytes again cannot fix it.
'''
from datetime import datetime
from typing import Any

from pydantic import BaseModel, Field

from discord_core.types.markov import MarkovMessageWrite
from discord_core.types.media_request import MediaRequest
from discord_core.types.playlist import PlaylistItemWrite


class GuildRequest(BaseModel):
    '''guild_analytics/get_analytics, markov/list_guild_channel_ids, playlist/{list,count}_playlists,
    playlist/{get,ensure}_history_playlist'''
    guild_id: int


class RecordPlayRequest(BaseModel):
    '''guild_analytics/record_play'''
    guild_id: int
    duration_seconds: int
    cache_hit: bool


class GuildChannelRequest(BaseModel):
    '''markov/{get,add,remove,reset}_channel'''
    guild_id: int
    channel_id: int


class SaveMessagesRequest(BaseModel):
    '''markov/save_messages'''
    guild_id: int
    channel_id: int
    messages: list[MarkovMessageWrite]


class GenerateWordsRequest(BaseModel):
    '''markov/generate_words'''
    guild_id: int
    count: int
    # `!markov speak` with no seed sends null.
    first_word: str | None = None


class PruneRelationsRequest(BaseModel):
    '''markov/prune_relations_before'''
    cutoff: datetime


class PlaylistRequest(BaseModel):
    '''playlist/{get,delete}_playlist, mark_queued, get_playlist_size, list_items'''
    playlist_id: int


class GuildNameRequest(BaseModel):
    '''playlist/get_playlist_by_name, create_playlist'''
    guild_id: int
    name: str


class RenamePlaylistRequest(BaseModel):
    '''playlist/rename_playlist'''
    playlist_id: int
    name: str


class AddItemsRequest(BaseModel):
    '''playlist/add_items: a batch, so a bad item fails the request instead of half-writing it.'''
    playlist_id: int
    items: list[PlaylistItemWrite]
    max_size: int


class DeleteItemRequest(BaseModel):
    '''playlist/delete_item'''
    item_id: int


class DeleteItemByIndexRequest(BaseModel):
    '''playlist/delete_item_by_index. Index 0 is the first item, not a missing argument.'''
    playlist_id: int
    index: int


class RecordHistoryItemRequest(BaseModel):
    '''playlist/record_history_item'''
    playlist_id: int
    item: PlaylistItemWrite
    max_size: int


class MediaDownloadPayload(BaseModel):
    '''A finished download, as `media_download_to_dict` writes it and `media_download_from_dict` reads it.'''
    request: MediaRequest
    file_path: str | None = None
    file_size_bytes: int | None = None
    cache_hit: bool = False
    ytdl_data: dict[str, Any] = Field(default_factory=dict)


class IterateFileRequest(BaseModel):
    '''video_cache/iterate_file. The download's own request rides inside it: the row is written from that
    download, so the far side has no other copy.'''
    media_download: MediaDownloadPayload


class GetWebpageUrlItemRequest(BaseModel):
    '''video_cache/get_webpage_url_item. The caller supplies the request it is asking about.'''
    media_request: MediaRequest


class RemoveVideoCacheRequest(BaseModel):
    '''video_cache/remove_video_cache: rows by id, and by object key.'''
    video_cache_ids: list[int]
    base_paths: list[str] = Field(default_factory=list)


class ReadyRemoveRequest(BaseModel):
    '''video_cache/ready_remove: apply the eviction policy, or flag every row.'''
    evict_all: bool = False


#: Every route's request model, keyed `(group, route)`; None for a route that takes no arguments.
#: Not trusted by hand: `tests/types/test_database_requests.py` checks it covers every route in
#: `routes/database.py` and that each model's fields are the arguments of the Protocol method the route is named for.
REQUESTS: dict[tuple[str, str], type[BaseModel] | None] = {
    ('guild_analytics', 'get_analytics'): GuildRequest,
    ('guild_analytics', 'record_play'): RecordPlayRequest,
    ('markov', 'list_channels'): None,
    ('markov', 'list_guild_channel_ids'): GuildRequest,
    ('markov', 'get_channel'): GuildChannelRequest,
    ('markov', 'add_channel'): GuildChannelRequest,
    ('markov', 'remove_channel'): GuildChannelRequest,
    ('markov', 'reset_channel'): GuildChannelRequest,
    ('markov', 'save_messages'): SaveMessagesRequest,
    ('markov', 'generate_words'): GenerateWordsRequest,
    ('markov', 'prune_relations_before'): PruneRelationsRequest,
    ('playlist', 'list_playlists'): GuildRequest,
    ('playlist', 'count_playlists'): GuildRequest,
    ('playlist', 'get_playlist'): PlaylistRequest,
    ('playlist', 'get_playlist_by_name'): GuildNameRequest,
    ('playlist', 'get_history_playlist'): GuildRequest,
    ('playlist', 'ensure_history_playlist'): GuildRequest,
    ('playlist', 'create_playlist'): GuildNameRequest,
    ('playlist', 'delete_playlist'): PlaylistRequest,
    ('playlist', 'rename_playlist'): RenamePlaylistRequest,
    ('playlist', 'mark_queued'): PlaylistRequest,
    ('playlist', 'get_playlist_size'): PlaylistRequest,
    ('playlist', 'list_items'): PlaylistRequest,
    ('playlist', 'add_items'): AddItemsRequest,
    ('playlist', 'delete_item'): DeleteItemRequest,
    ('playlist', 'delete_item_by_index'): DeleteItemByIndexRequest,
    ('playlist', 'record_history_item'): RecordHistoryItemRequest,
    ('video_cache', 'iterate_file'): IterateFileRequest,
    ('video_cache', 'get_webpage_url_item'): GetWebpageUrlItemRequest,
    ('video_cache', 'remove_video_cache'): RemoveVideoCacheRequest,
    ('video_cache', 'ready_remove'): ReadyRemoveRequest,
    ('video_cache', 'get_deletable_entries'): None,
    ('video_cache', 'get_cache_count'): None,
}
