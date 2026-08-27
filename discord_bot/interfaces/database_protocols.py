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

  MarkovClient (clients/markov_client.py) — in-process, same shape: a session
  generator over the local engine. What the bot runs today.

  HttpVideoCacheStore / HttpMarkovStore — not yet written. Forward to the db
  pod's routes.

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

from discord_bot.types.markov import MarkovChannelEntry, MarkovMessageWrite
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
