'''
Response bodies for the broker seam — one model per route.

Measured before this existed: 22 routes, 25 `json_response` calls, **3 through
`model_dump()` and 22 dict literals**. [[http-seam-contract]] recorded "broker 4
named models across 22 routes", which is true about the models existing and
misleading about coverage — `MediaDownload` and `SearchResolution` were returned
through models; everything else was hand-built dicts.

**Thirteen of these are `{'status': 'ok'}` and stay separate.** Same rule as the
dispatch seam: a seam package is a versioned contract, and merging two routes'
models means a change to one route's shape edits the other's type. `release`,
`remove` and `discard` agree today; that is a fact about today. Do not
deduplicate them.

**Field declaration order is the wire.** `web.json_response` serialises a dict
in insertion order and these models replace dict literals, so the order here is
the byte order that used to be written by hand.

**Status codes are not carried here.** 201/202/200 stay at the `json_response`
call, because the model describes the body and the code describes the outcome,
and folding them together would mean a body model could silently change a
status.
'''
from typing import Any, Literal

from pydantic import BaseModel

from discord_core.types.player_session import PlayerSession


class _Ok(BaseModel):
    '''
    Shared base for the thirteen `{'status': 'ok'}` bodies.

    A base class, NOT a shared model: each route still has its own named type
    that can gain a field without touching the other twelve. This exists only so
    thirteen identical three-line bodies do not have to be typed out, and it
    carries no routes of its own.
    '''
    status: Literal['ok'] = 'ok'


class RegisterRequestResponse(_Ok):
    '''201 from the register-request route.'''


class UpdateStatusResponse(_Ok):
    '''200 from the update-status route.'''


class RegisterDownloadResponse(_Ok):
    '''202 from the register-download route.'''


class RegisterDownloadDirectResponse(_Ok):
    '''200 from the direct register-download route.'''


class RegisterSearchResultResponse(_Ok):
    '''202 from the register-search-result route.'''


class ReleaseResponse(_Ok):
    '''200 from the release route.'''


class RemoveResponse(_Ok):
    '''200 from the remove route.'''


class DiscardResponse(_Ok):
    '''200 from the discard route.'''


class FinalizeBundleResponse(_Ok):
    '''200 from the finalize-bundle route.'''


class DeleteBundleResponse(_Ok):
    '''200 from the delete-bundle route.'''


class SavePlayerSessionResponse(_Ok):
    '''201 from the save-player-session route.'''


class DeletePlayerSessionResponse(_Ok):
    '''200 from the delete-player-session route.'''


class CheckoutEmptyResponse(BaseModel):
    '''
    Checkout found nothing to hand out (unknown entry, or no download attached).

    Two models for one route, because the two answers are different BYTES, not
    one body with optional fields: a hit sends `{'s3_key': ...}` and a miss sends
    `{}`. (Brokers from before local staging was removed sent
    `{'guild_file_path': null}` for a miss; the extra key is ignored on read.)
    '''


class CheckoutS3Response(BaseModel):
    '''Checkout answered by an HA broker: the file is in S3, caller fetches it.

    `bucket_name` is deliberately absent. The client fills it from its own
    config, because the bot already knows which bucket it is configured against
    and the broker telling it would be a second source for one fact.
    '''
    s3_key: str


class CheckCacheMissResponse(BaseModel):
    '''Cache miss. `hit` is the discriminator the client branches on.'''
    hit: Literal[False] = False


class CachedYtdlData(BaseModel):
    '''The yt-dlp metadata carried with a cache hit.'''
    id: Any = None
    title: Any = None
    webpage_url: Any = None
    uploader: Any = None
    duration: Any = None
    extractor: Any = None


class CachedDownload(BaseModel):
    '''The download record carried with a cache hit.

    `request` stays a `dict` rather than a nested `MediaRequest`: the handler
    builds it with `model_dump(mode='json')`, and re-parsing it into the model
    only to dump it again risks the datetime class of bug that bit the dispatch
    seam, where a plain `model_dump()` returns objects `json_response` cannot
    serialise.
    '''
    request: dict
    file_path: str | None = None
    file_size_bytes: Any = None
    cache_hit: Any = None
    ytdl_data: CachedYtdlData


class CheckCacheHitResponse(BaseModel):
    '''Cache hit, with the cached download record.'''
    hit: Literal[True] = True
    download: CachedDownload


class CacheCleanupResponse(BaseModel):
    '''Whether the cleanup pass removed anything.'''
    removed: bool


class GetCacheCountResponse(BaseModel):
    '''Number of entries in the video cache.'''
    count: int


class CreateBundleResponse(BaseModel):
    '''201 with the new bundle's uuid.'''
    uuid: str


class ListPlayerSessionsResponse(BaseModel):
    '''Every stored player session.'''
    sessions: list[PlayerSession]


class ListBundlesForGuildResponse(BaseModel):
    '''The bundle uuids a guild currently has.'''
    uuids: list[str]


# ---------------------------------------------------------------------------
# Guild queue
# ---------------------------------------------------------------------------

class QueuedDownload(BaseModel):
    '''
    A track in a guild's queue, in the shape `media_download_to_dict` writes.

    Same fields as `CachedDownload`, deliberately a separate type: the cache seam and the queue
    seam version independently. `request` stays a `dict` for the same reason it does there
    (the server builds it with `model_dump(mode='json')`; re-parsing it to dump it again is
    where the dispatch seam's datetime bug came from).
    '''
    request: dict
    file_path: str | None = None
    file_size_bytes: Any = None
    cache_hit: Any = None
    ytdl_data: CachedYtdlData


class PlayingTrackBody(BaseModel):
    '''
    What the broker knows about a guild's playing track.

    `download` is None when the entry has expired out from under the now-playing record, which
    is rare and means the track keeps playing but its details are gone.
    '''
    uuid: str
    started_at: float
    gateway_id: str
    download: QueuedDownload | None = None


class EnqueueTrackResponse(BaseModel):
    '''
    Outcome of queueing a track: ok, or why not.

    closed: the guild's player is shut down. full: the queue is at its cap. duplicate: the track
    is already queued. Rejections are results, not errors; see the module comment on the routes.
    '''
    result: Literal['ok', 'closed', 'full', 'duplicate']


class GuildQueueResponse(BaseModel):
    '''A guild's queue in play order, its playing track, and the markers a poller watches.'''
    version: int
    items: list[QueuedDownload]
    playing: PlayingTrackBody | None = None
    skip_for: str | None = None
    closed: bool


class RemoveQueuedTrackResponse(BaseModel):
    '''`removed` is False when the track was not queued; `download` is what was removed.'''
    removed: bool
    download: QueuedDownload | None = None


class BumpQueuedTrackResponse(BaseModel):
    '''`bumped` is False when the track was not queued; `download` is the track moved.'''
    bumped: bool
    download: QueuedDownload | None = None


class ShuffleQueueResponse(BaseModel):
    '''False only if the queue kept changing under every attempt to shuffle it.'''
    shuffled: bool


class ClearQueueResponse(BaseModel):
    '''How many queued tracks were dropped.'''
    cleared: int


class PollGuildQueueResponse(BaseModel):
    '''
    The cheap change check: the queue version and the track a skip is pending for.

    Answered 204 with no body when `?since=` equals the current version and no skip is pending,
    so an idle poll is an empty response.
    '''
    version: int
    skip_for: str | None = None


class ClaimTrackMissResponse(BaseModel):
    '''Nothing to claim. `claimed` is the discriminator the client branches on.'''
    claimed: Literal[False] = False


class ClaimTrackHitResponse(BaseModel):
    '''
    A track claimed and marked as playing.

    `bucket_name` is absent for the same reason it is from `CheckoutS3Response`: the client
    already knows which bucket it is configured against.
    '''
    claimed: Literal[True] = True
    download: QueuedDownload
    s3_key: str


class PlayingHeartbeatResponse(BaseModel):
    '''False if the track named is no longer the one playing.'''
    alive: bool


class SkipTrackResponse(BaseModel):
    '''
    ok: the skip is recorded and the gateway will act on it. no_player: nothing is playing.
    not_current: the track named is not the one playing (it finished, or was already skipped).
    '''
    result: Literal['ok', 'no_player', 'not_current']


class FinishTrackResponse(_Ok):
    '''200 from the finish-track route.'''


class GuildHistoryResponse(BaseModel):
    '''Tracks that played to the end, oldest first. Each item is the history record as stored.'''
    items: list[dict]


class CloseGuildResponse(BaseModel):
    '''How many entries were released when the guild's player state was shut down.'''
    released: int


class OpenGuildResponse(_Ok):
    '''200 from the open-guild route.'''
