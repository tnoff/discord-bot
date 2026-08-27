'''
Protocols for the persistence tier — one process owns postgres, callers become
clients of it (projects/discord-db-tier-extraction).

Postgres access is spread across two processes that were never designed to
share it: the bot holds playlists, guild analytics and markov, the broker holds
the video-cache catalog. Neither owns the schema. The end state is a
`discord-db` pod that does, with both of today's holders talking to it over
HTTP.

The surface is grouped by **table**, not by caller, and lands one group at a
time. Grouping by caller would mint two overlapping cache interfaces the moment
a second consumer appeared, and the video cache already has two: the broker
engine writes it, and `MediaBrokerBase.cache_cleanup` evicts from it.

Groups, in the order they cross the seam:

  VideoCacheStore    (here) — the `video_cache` catalog.
  MarkovStore        — `markov_channel` / `markov_relation`.
  PlaylistStore      — `playlist` / `playlist_item`.
  GuildAnalyticsStore — `guild` / `server_video_analytics`.

**Every method here returns a value that survives a network hop.** That is the
single rule the Protocol exists to enforce, and it is why `VideoCacheEntry`
exists rather than the signatures naming `database.VideoCache`: an ORM instance
is bound to the session that loaded it, and a store answering over HTTP has no
session to bind to. A signature written against the ORM class is one only the
in-memory implementation can satisfy, which makes the Protocol decoration
rather than a contract.

The corollary is the media_search error-envelope lesson, and it applies to
every method added here: "no such row" is an **answer**, returned as None or an
empty list. It is not an error condition, and once these calls are HTTP it must
not become a non-2xx -- `async_retry_broker_command` would re-run a query whose
answer cannot change, and `raise_for_status()` would discard the body that said
so.

Implementations:

  VideoCacheClient (cogs/music_helpers/video_cache_client.py) — in-process,
  backed by a session generator over the local engine. What the broker runs
  today. It keeps its name until an HTTP sibling exists to be distinguished
  from; renaming it now would churn three repos to no effect.

  HttpVideoCacheStore — not yet written. Forwards to the db pod's routes.
'''
from typing import List, Protocol, runtime_checkable

from discord_bot.types.media_download import MediaDownload
from discord_bot.types.media_request import MediaRequest
from discord_bot.types.video_cache import VideoCacheEntry


@runtime_checkable
class VideoCacheStore(Protocol):
    '''
    The `video_cache` catalog: what has been downloaded, how big, how stale.

    Rows only. Every file operation on the objects these rows describe belongs
    to the broker -- it holds the bucket name and the Redis checkout registry,
    and `cache_cleanup` interleaves all three stores (catalog row, checkout
    state, S3 object) in one loop. That interleave is why the orchestration
    stays in the broker and only the catalog moves: pulling `cache_cleanup`
    across would make the persistence tier need the broker's Redis registry,
    leaking the boundary in the opposite direction.
    '''

    async def iterate_file(self, media_download: MediaDownload) -> bool:
        '''
        Record a completed download: insert a row, or bump an existing one.

        media_download : The finished download to catalog
        '''

    async def get_webpage_url_item(self, media_request: MediaRequest) -> MediaDownload | None:
        '''
        Return a cache hit for the request's resolved URL, or None.

        None is the miss answer and covers both "no such row" and "the row was
        written under a different storage_type" -- the latter also flags the
        stale row for eviction. Neither is an error.

        media_request : The request whose resolved search string to look up
        '''

    async def remove_video_cache(self, video_cache_ids: List[int]) -> bool:
        '''
        Delete catalog rows by id.

        The caller deletes the S3 objects first; this method never touches
        storage.

        video_cache_ids : Row ids to delete
        '''

    async def ready_remove(self) -> bool:
        '''
        Apply the eviction policy, flagging excess rows `ready_for_deletion`.

        Marks only -- nothing is deleted here, and the flagged rows come back
        from `get_deletable_entries` so the caller can drop their files first.
        '''

    async def get_deletable_entries(self) -> List[VideoCacheEntry]:
        '''
        Return the rows currently flagged `ready_for_deletion`.

        Entries, not ORM rows: the caller reads `base_path` and `id` after the
        loading session has closed, which a live instance would refuse.
        '''

    async def get_cache_count(self) -> int:
        '''Return the number of rows in the catalog.'''
