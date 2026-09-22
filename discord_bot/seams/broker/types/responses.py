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

from discord_bot.seams.broker.types.player_session import PlayerSession


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


class PrefetchResponse(_Ok):
    '''200 from the prefetch route.'''


class FinalizeBundleResponse(_Ok):
    '''200 from the finalize-bundle route.'''


class DeleteBundleResponse(_Ok):
    '''200 from the delete-bundle route.'''


class SavePlayerSessionResponse(_Ok):
    '''201 from the save-player-session route.'''


class DeletePlayerSessionResponse(_Ok):
    '''200 from the delete-player-session route.'''


class CheckoutStagedResponse(BaseModel):
    '''
    Checkout answered by a non-HA broker: the file is staged on local disk.

    Two models for one route, because the two answers are different BYTES, not
    one body with optional fields: an HA broker sends `{'s3_key': ...}` with no
    `guild_file_path` key at all, and the client branches on which key is
    present. A single model with both optional would emit both keys and change
    what the client sees.
    '''
    guild_file_path: str | None = None


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
