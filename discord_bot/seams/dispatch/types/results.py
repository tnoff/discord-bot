'''
Result bodies for `GET /dispatch/results/{request_id}` — the last untyped
dispatch surface.

The 200 body is whatever the worker stored, and it is three shapes, not one:
a channel-history result, a guild-emoji result, or a failure. The route is
polymorphic because a request_id can belong to either fetch kind, and the
client discriminates on the presence of `error` — which is why the error stays
*inside* a 200 body rather than becoming a status code. `database_wire` states
the same rule for its own seam: `HttpClientMixin._http` calls
`raise_for_status()` inside `async_retry_broker_command`, so a non-2xx is
retried three times with 1/2/4s backoff **and its body discarded**. An error
encoded as 5xx would be re-run three times and the caller would never see the
`error_detail` that said why.

**Field order is the wire.** `web.json_response` serialises a dict in insertion
order, and these models replace dict literals, so the field declaration order
here IS the byte order that used to be written by hand. Reordering a field is a
wire change, not a tidy-up.
'''
from typing import Any

from pydantic import BaseModel

from discord_bot.seams.dispatch.types.fetched_message import FetchedMessage


class GuildEmojiBody(BaseModel):
    '''One emoji in a guild-emoji result.'''
    id: int
    name: str
    animated: bool


class ChannelHistoryResultBody(BaseModel):
    '''200 body for a completed fetch_history.'''
    guild_id: int
    channel_id: int
    after_message_id: int | None = None
    messages: list[FetchedMessage]


class GuildEmojisResultBody(BaseModel):
    '''200 body for a completed fetch_emojis.'''
    guild_id: int
    emojis: list[GuildEmojiBody]


class DispatchErrorDetailBody(BaseModel):
    '''
    The typed half of a failure, as `encode_error` has always built it.

    `status` and `code` are what let a caller tell a recoverable Discord 404
    from a real failure -- `is_not_found_error` duck-types on `.status` rather
    than on an isinstance check, because a result delivered through the
    dispatcher carries a rebuilt DispatchRemoteError, never the original
    discord.NotFound. Typing them `Any` keeps that: Discord returns ints, but
    the field is whatever `getattr(exc, 'status', None)` found, and narrowing it
    would 422 on an exception type that happens to carry a string.
    '''
    message: str
    type: str
    status: Any = None
    code: Any = None


class DispatchErrorResultBody(BaseModel):
    '''
    200 body for a failed fetch of either kind.

    `error` stays a plain string for wire compatibility -- it is what the client
    checks for with `if 'error' in payload` -- and `error_detail` is what makes
    the failure matchable on the far side.

    **`error_detail` is OPTIONAL, and that is a rolling-deploy requirement, not
    laxness.** It was added after `error`, and `DispatchRemoteError.from_payload`
    documents the reason it tolerates its absence: "so a bot talking to a
    not-yet-rolled dispatcher still gets the message, just without the
    status/code". Making it required here would turn every result stored by an
    older dispatcher into a validation failure the moment a new bot pod rolled
    first -- a self-inflicted outage during exactly the window this seam exists
    to survive. Two existing tests encode that shape; they caught this.
    '''
    error: str
    error_detail: DispatchErrorDetailBody | None = None
