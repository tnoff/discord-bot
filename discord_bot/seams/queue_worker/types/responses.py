'''
Response bodies for the queue_worker seam — one model per route.

The seam the downloader and search pods serve and the bot polls. Five response
sites, all dict literals before this.

Same rule as the broker and dispatch seams: one model per route, and the two
`{'status': 'ok'}` bodies stay separate because a seam package is a versioned
contract and `submit` agreeing with `block` today is a fact about today.

**`SubmitRejectedResponse` is not an error envelope.** It ships under a non-2xx
chosen by `submit_rejection_status`, unlike the broker and database seams where
errors ride inside a 200. That is deliberate and predates this: a queue refusal
is an expected answer the cog handles, and `async_retry_broker_command`
propagates 4xx immediately rather than laddering, so the status code is doing
real work here. Nothing in this module changes it.
'''
from typing import Literal

from pydantic import BaseModel


class SubmitAcceptedResponse(BaseModel):
    '''202 — the request is queued.'''
    status: Literal['ok'] = 'ok'


class SubmitRejectedResponse(BaseModel):
    '''
    Non-2xx — the queue refused the request.

    `reason` is the exception CLASS NAME, not a message: the cog branches on it,
    and `detail` carries the human-readable half.
    '''
    status: Literal['rejected'] = 'rejected'
    reason: str
    detail: str


class BlockResponse(BaseModel):
    '''200 — the guild is now blocked.'''
    status: Literal['ok'] = 'ok'


class ClearGuildResponse(BaseModel):
    '''
    200 — what `clear_guild_queue` dropped and what it kept.

    This is the body `ClearGuildResult` is assembled FROM, and the two are not
    the same shape: `preserved_bundle_uuids` is a sorted LIST here and becomes a
    `set` in the DTO. That is why typing the DTO would have typed nothing on the
    wire -- the wire is this.
    '''
    dropped: list[dict]
    preserved_bundle_uuids: list[str]


class StatusSnapshotResponse(BaseModel):
    '''
    200 — the poller's view of a worker pod.

    Mirrors `build_status_snapshot`, which is the single place this dict is
    assembled for both the downloader and search workers.

    `backoff_seconds_remaining` is `int | None` and the None is meaningful: the
    builder writes `backoff_seconds or None`, so a zero backoff is reported as
    absent rather than as 0. Typing it plain `int` would change what the poller
    sees the first time a backoff expires.
    '''
    failure_summary: str
    failure_count: int
    backoff_seconds_remaining: int | None = None
    queue_sizes: dict[str, int]
