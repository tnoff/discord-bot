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
  MarkovStore        (here) — `markov_channel` / `markov_relation`.
  PlaylistStore      (here) — `playlist` / `playlist_item`.
  GuildAnalyticsStore (here) — `guild` / `server_video_analytics`.

All four groups have crossed. `cogs/music.py` and `cogs/markov.py` no
longer open a database session at all, which is the property MR 2 needs:
swapping in an HTTP implementation is a constructor change in the cog and
nothing else.

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

  MarkovClient (clients/markov_client.py) — in-process, same shape: a session
  generator over the local engine. What the bot runs today.

  PlaylistClient (clients/playlist_client.py) — in-process, same shape.

  GuildAnalyticsClient (clients/guild_analytics_client.py) — in-process,
  same shape.

  HttpMarkovStore (clients/http_markov_store.py) and HttpPlaylistStore
  (clients/http_playlist_store.py) — forward to the db pod's routes. Inert
  until MR 4's cutover; each is a constructor change in the cog and nothing
  else, which is the whole point of the Protocol.

  HttpVideoCacheStore — not yet written. The one group whose signatures name
  MediaDownload rather than a view type, so what crosses the wire is an open
  question rather than a repeat of the two above.

**A second rule the markov group forced: one call per unit of work the caller
actually has, not per row.** Each method here is sized so the in-process
implementation opens one session and the eventual HTTP one makes one request.
That is not a performance nicety, it is the difference between this seam and the
two defects that had to be fixed before it could be built -- a commit per word
pair, and a query per word of a sentence. A protocol written per-row bakes that
shape back in at a layer where it is much harder to see: `save_messages` takes a
batch and `generate_words` returns a whole sentence for exactly this reason.
'''
from datetime import datetime
from typing import List, Protocol, runtime_checkable

from discord_bot.types.guild_analytics import GuildAnalyticsEntry
from discord_bot.types.markov import MarkovChannelEntry, MarkovMessageWrite
from discord_bot.types.playlist import (
    PlaylistEntry,
    PlaylistItemAddOutcome,
    PlaylistItemEntry,
    PlaylistItemWrite,
)
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


@runtime_checkable
class MarkovStore(Protocol):
    '''
    The markov chain tables: which channels are tracked, and the word graph.

    Channel rows are values here, never live instances. The cog used to receive
    `MarkovChannel` objects and both mutate and delete them through the session
    that loaded them; those are `save_messages`, `remove_channel` and
    `reset_channel` now, because attribute assignment has no remote equivalent.
    '''

    async def list_channels(self) -> List[MarkovChannelEntry]:
        '''
        Return every tracked channel, across all guilds.

        Entries, not rows: the producer loop iterates these while awaiting
        Discord dispatches for each one, and a live-row version would hold a
        postgres connection open for that entire fan-out.
        '''

    async def list_guild_channel_ids(self, guild_id: int) -> List[int]:
        '''
        Return the Discord channel ids tracked in one guild.

        Ids, not one-column Row tuples -- the caller used to index `row[0]`,
        which is a driver artifact rather than an answer.

        guild_id : Discord guild to list
        '''

    async def get_channel(self, guild_id: int, channel_id: int) -> MarkovChannelEntry | None:
        '''
        Return the tracked channel, or None when markov is off for it.

        None is the answer, not an error.

        guild_id : Discord guild id
        channel_id : Discord channel id
        '''

    async def add_channel(self, guild_id: int, channel_id: int) -> MarkovChannelEntry:
        '''
        Start tracking a channel and return its new row.

        guild_id : Discord guild id
        channel_id : Discord channel id
        '''

    async def remove_channel(self, guild_id: int, channel_id: int) -> bool:
        '''
        Stop tracking a channel, dropping its relations with it.

        Returns False when the channel was not tracked -- again an answer, and
        the caller's cue to say so rather than to retry.

        guild_id : Discord guild id
        channel_id : Discord channel id
        '''

    async def reset_channel(self, guild_id: int, channel_id: int) -> bool:
        '''
        Clear a channel's relations and its `last_message_id` together.

        The recovery path for a `last_message_id` Discord no longer knows: the
        next producer pass re-requests from the retention cutoff instead of
        pinning to a message that no longer exists. Both halves in one
        transaction, because a clear that loses the id but keeps the relations
        would double every word it re-gathers.

        guild_id : Discord guild id
        channel_id : Discord channel id
        '''

    async def save_messages(self, guild_id: int, channel_id: int,
                            messages: List[MarkovMessageWrite]) -> int | None:
        '''
        Persist a batch of gathered messages, committing one message at a time.

        Returns the number of messages written, or None when the channel is not
        tracked. The commit boundary stays per message so each one's relations
        and the channel's new `last_message_id` land together; the batch is what
        keeps that from costing a connection -- or a round trip -- per message.

        guild_id : Discord guild id
        channel_id : Discord channel id
        messages : Word pairs and message id, oldest first
        '''

    async def generate_words(self, guild_id: int, count: int,
                             first_word: str | None = None) -> List[str]:
        '''
        Walk the chain and return up to `count` words.

        The whole walk, not one step: each word is chosen from the previous
        one, so a per-word method would be a round trip per word -- the shape
        `!markov speak` was just fixed for having. Postgres picks each word.

        An empty list means the guild has nothing to say, either because it has
        no relations at all or because none lead with `first_word`. A short list
        means the chain dead-ended, which retention makes reachable: it can
        delete every relation in which a word leads while keeping one where it
        follows.

        guild_id : Discord guild id
        count : Maximum words to return
        first_word : Constrain the opening word, if given
        '''

    async def prune_relations_before(self, cutoff: datetime) -> bool:
        '''
        Delete relations older than the retention cutoff.

        cutoff : Relations created before this are dropped
        '''


@runtime_checkable
class PlaylistStore(Protocol):
    '''
    The `playlist` and `playlist_item` tables.

    The largest group, and the one where the per-unit-of-work rule earns the
    most: three separate loops in the music cog add items one at a time, and
    the post-play history write is six queries the caller happens to run in
    sequence. Those are `add_items` and `record_history_item` here.

    Ordering is part of this interface, not an implementation detail. The public
    playlist index users type (`!playlist 1`) is a position in `list_playlists`,
    and the history playlist evicts the oldest item by the order `list_items`
    promises.

    The two orders differ, and the difference is evidence rather than taste.
    `playlist_item` really did have `created_at` NULL on every row -- 1463 of
    them in production -- so its order was heap order and `asc` with an id
    tiebreak is a fix with no prior behaviour to preserve. `playlist` did not:
    all 32 rows carried distinct timestamps, so `desc` had been in effect all
    along and the index has always been newest-first. Assuming the second table
    matched the first, on one measurement of an empty database, reversed the
    numbering for every guild with more than one playlist.
    '''

    async def list_playlists(self, guild_id: int) -> List[PlaylistEntry]:
        '''
        Return a guild's non-history playlists, newest first.

        The order defines the public index, so it is a promise rather than an
        observation -- and one that was broken once by assuming what production
        held rather than reading it.

        guild_id : Discord guild id
        '''

    async def count_playlists(self, guild_id: int) -> int:
        '''
        Return how many non-history playlists a guild has.

        guild_id : Discord guild id
        '''

    async def get_playlist(self, playlist_id: int) -> PlaylistEntry | None:
        '''
        Return one playlist by row id, or None.

        playlist_id : Playlist row id
        '''

    async def get_playlist_by_name(self, guild_id: int, name: str) -> PlaylistEntry | None:
        '''
        Return a guild's playlist with this name, or None.

        guild_id : Discord guild id
        name : Playlist name to look for
        '''

    async def get_history_playlist(self, guild_id: int) -> PlaylistEntry | None:
        '''
        Return a guild's history playlist, or None when it has none yet.

        guild_id : Discord guild id
        '''

    async def ensure_history_playlist(self, guild_id: int) -> int:
        '''
        Return the guild's history playlist id, creating it if absent.

        One call rather than a read followed by a conditional write: over HTTP
        the two-step version is a race between any two players starting at once,
        and the table's unique constraint would turn that into an error on a
        path that has no way to report one.

        guild_id : Discord guild id
        '''

    async def create_playlist(self, guild_id: int, name: str) -> PlaylistEntry:
        '''
        Create a playlist and return it.

        guild_id : Discord guild id
        name : Playlist name
        '''

    async def delete_playlist(self, playlist_id: int) -> bool:
        '''
        Delete a playlist and every item in it.

        playlist_id : Playlist row id
        '''

    async def rename_playlist(self, playlist_id: int, name: str) -> bool:
        '''
        Rename a playlist. False when there is no such playlist.

        playlist_id : Playlist row id
        name : New name
        '''

    async def mark_queued(self, playlist_id: int) -> bool:
        '''
        Record that a playlist was just queued.

        playlist_id : Playlist row id
        '''

    async def get_playlist_size(self, playlist_id: int) -> int:
        '''
        Return how many items a playlist holds.

        playlist_id : Playlist row id
        '''

    async def list_items(self, playlist_id: int) -> List[PlaylistItemEntry]:
        '''
        Return a playlist's items, oldest first.

        Entries, not rows: `!playlist queue` reads these while dispatching
        searches and enqueuing downloads, which is a long network-bound stretch
        the loading session has no business staying open for.

        playlist_id : Playlist row id
        '''

    async def add_items(self, playlist_id: int, items: List[PlaylistItemWrite],
                        max_size: int) -> List[PlaylistItemAddOutcome]:
        '''
        Add items to a playlist, stopping when it is full.

        One outcome per item attempted, in order, so the caller can say
        something different for added, duplicate and full -- which all three of
        its loops do. Items after a full playlist are not attempted and get no
        outcome, matching the loops this replaces.

        Enforcing max_size inside the store rather than around it is what makes
        the ceiling hold: the check and the insert are one transaction, where
        the caller's version was a count, then a decision, then a write.

        playlist_id : Playlist row id
        items : Items to add, in order
        max_size : Ceiling on the playlist's item count
        '''

    async def delete_item(self, item_id: int) -> bool:
        '''
        Delete one item by row id. False when it is already gone.

        playlist_id is not needed: the id identifies the row. False rather than
        an error because the caller reaching for this has already found the
        item missing from somewhere else.

        item_id : PlaylistItem row id
        '''

    async def delete_item_by_index(self, playlist_id: int,
                                   index: int) -> PlaylistItemEntry | None:
        '''
        Delete the item at a zero-based position, returning what was deleted.

        Position is defined by `list_items`, which is what the user saw when
        they read the index off `!playlist show`. None when the index is out of
        range.

        playlist_id : Playlist row id
        index : Zero-based position in list order
        '''

    async def record_history_item(self, playlist_id: int, item: PlaylistItemWrite,
                                  max_size: int) -> bool:
        '''
        Write one played track to the history playlist, evicting to make room.

        Deduplicate by url, drop as many of the oldest items as the new one
        needs, then insert -- one call for what was six queries the post-play
        loop ran in sequence. Returns False when the playlist no longer exists.

        The eviction is why `list_items`' order is a promise: this deletes "the
        oldest", and until `created_at` was populated that meant whichever rows
        postgres happened to return first.

        playlist_id : History playlist row id
        item : The track that just played
        max_size : Ceiling on the playlist's item count
        '''


@runtime_checkable
class GuildAnalyticsStore(Protocol):
    '''
    Per-guild play totals: `guild` and `server_video_analytics`.

    Two rows, two methods, and both of them ensure the rows exist before doing
    anything else. That was already true -- `update_video_guild_analytics`
    called `ensure_guild_video_analytics`, which called `ensure_guild` -- but
    the ensure ran in the caller's session, so a first play in a new guild was
    three statements plus the update, interleaved with whatever else that
    session held open.

    The pair also shows the two rules above in their plainest form. `record_play`
    is a read-modify-write -- load the row, increment four fields, write them
    back -- and the old code ran it inside a session the post-play loop was
    holding open across a Discord dispatch. One call, one transaction, and the
    implementation takes a row lock, because the whole point of a db pod is that
    a second caller can exist and READ COMMITTED will happily let two of them
    write back the same increment.

    And `get_analytics` returns an entry rather than the row, because
    `!music-stats` reads six columns off the result and formats `created_at`.
    Every one of those reads was legal only inside the session block.
    '''

    async def get_analytics(self, guild_id: int) -> GuildAnalyticsEntry:
        '''
        Return a guild's play totals, creating the rows on first call.

        Never None: a guild with no plays has zeroes, not a missing row, and
        that is what `!music-stats` prints for a quiet server.

        guild_id : Discord guild id (the `server_id` column, not `guild.id`)
        '''

    async def record_play(self, guild_id: int, duration_seconds: int,
                          cache_hit: bool) -> bool:
        '''
        Add one play to a guild's totals.

        Increments the play count, adds the duration (carrying whole days into
        `total_duration_days`), and counts the play as cached when it was
        served from the video cache.

        guild_id : Discord guild id (the `server_id` column, not `guild.id`)
        duration_seconds : Length of the track that just played
        cache_hit : True when the download was served from cache
        '''
