'''
Request bodies for the dispatch seam — one model per route.

Until now these were read field by field out of `await request.json()` with
`int(...)`/`str(...)` coercion and a bare `except Exception -> 422`, and written
on the client as dict literals. Nothing tied the two together, and they had
already drifted: `POST /dispatch/send` has **two** client call sites sending
different bodies — `_handle_send(SendRequest)` omits `allow_404`, while
`send_message()` includes it — so the typed path could not express a field the
server reads. That is the drift projects/seam-body-typing exists to remove.

**One model per route, and the five that are structurally identical stay
separate.** `remove_mutable` and `update_mutable_channel` share no fields, but
five *responses* are all `{'status': 'ok'}` (see `responses.py`); the same
reasoning applies on both sides. A seam package is a versioned contract, and
merging two routes' models means a change to one route's shape edits the other's
type. Do not deduplicate these.

**Coercion is preserved, not tightened.** The handlers coerced with `int()` and
`str()`, so a client sending `"123"` for a guild_id was accepted. pydantic's
default (non-strict) mode coerces the same way. Tightening to strict here would
be a wire change wearing a refactor's clothes.
'''
from pydantic import BaseModel


class SendRequestBody(BaseModel):
    '''POST /dispatch/send'''
    guild_id: int
    channel_id: int
    content: str
    delete_after: int | None = None
    allow_404: bool = False
    span_context: dict | None = None


class DeleteRequestBody(BaseModel):
    '''POST /dispatch/delete'''
    guild_id: int
    channel_id: int
    message_id: int
    span_context: dict | None = None


class UpdateMutableRequestBody(BaseModel):
    '''POST /dispatch/update_mutable'''
    key: str
    guild_id: int
    content: list
    channel_id: int | None = None
    sticky: bool = True
    delete_after: int | None = None


class RemoveMutableRequestBody(BaseModel):
    '''POST /dispatch/remove_mutable'''
    key: str


class UpdateMutableChannelRequestBody(BaseModel):
    '''POST /dispatch/update_mutable_channel'''
    key: str
    guild_id: int
    new_channel_id: int


class FetchHistoryRequestBody(BaseModel):
    '''
    POST /dispatch/fetch_history

    `after` is typed `str`, not `datetime`, and that is load-bearing. The handler
    folds these fields into `dispatch_request_id(params)`, which is
    `sha256(json.dumps(params, sort_keys=True, default=str))` — so a value whose
    *type* changes changes the digest. Parsing `after` into a datetime here would
    make `default=str` render it differently from the raw ISO string the client
    sends (`request.after.isoformat()` in `dispatch_client_base`), every fetch
    would get a new request_id, and result reuse would silently stop working.
    Key ORDER is safe (`sort_keys=True`); value types are not.
    '''
    guild_id: int
    channel_id: int
    limit: int
    after: str | None = None
    after_message_id: int | None = None
    oldest_first: bool = True
    span_context: dict | None = None


class FetchEmojisRequestBody(BaseModel):
    '''POST /dispatch/fetch_emojis'''
    guild_id: int
    max_retries: int = 3
    span_context: dict | None = None
