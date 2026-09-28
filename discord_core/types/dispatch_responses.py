'''
Response bodies for the dispatch seam — one model per route.

**Five of these are structurally identical and are deliberately not merged.**
`send`, `delete`, `update_mutable`, `remove_mutable` and `update_mutable_channel`
all answer `{'status': 'ok'}` with HTTP 202 today. Collapsing them into one
`AcceptedResponse` would mean that the day `send` starts returning a message id,
the edit lands on `remove_mutable`'s type too. A seam package is a versioned
contract and these are five contracts that happen to agree right now. Do not
deduplicate them.

**The 202s are not a mistake.** Every fire-and-forget route answers 202
Accepted, because the dispatcher has queued the work rather than done it. The
two fetch routes answer 202 with a `request_id` the caller then polls. Only
`GET /dispatch/results/{request_id}` ever answers 200.

**Errors stay inside a 200 body.** `_handle_get_result` returns the stored result
verbatim, and a failed fetch is stored as `{'error': ..., 'error_detail': ...}`.
That is the same rule `database_wire` states for its own seam and for the same
reason: `HttpClientMixin._http` calls `raise_for_status()` inside
`async_retry_broker_command`, so a non-2xx is retried three times with 1/2/4s
backoff *and its body discarded* — an error encoded as a status code would be
re-run three times and the caller would never see the detail that said why.
Malformed *requests* still 422, which is correct: `async_retry_broker_command`
propagates 4xx immediately rather than laddering.
'''
from typing import Literal

from pydantic import BaseModel


class SendResponse(BaseModel):
    '''202 from POST /dispatch/send'''
    status: Literal['ok'] = 'ok'


class DeleteResponse(BaseModel):
    '''202 from POST /dispatch/delete'''
    status: Literal['ok'] = 'ok'


class UpdateMutableResponse(BaseModel):
    '''202 from POST /dispatch/update_mutable'''
    status: Literal['ok'] = 'ok'


class RemoveMutableResponse(BaseModel):
    '''202 from POST /dispatch/remove_mutable'''
    status: Literal['ok'] = 'ok'


class UpdateMutableChannelResponse(BaseModel):
    '''202 from POST /dispatch/update_mutable_channel'''
    status: Literal['ok'] = 'ok'


class FetchHistoryResponse(BaseModel):
    '''202 from POST /dispatch/fetch_history — poll GET /dispatch/results/{id}.'''
    request_id: str


class FetchEmojisResponse(BaseModel):
    '''202 from POST /dispatch/fetch_emojis — poll GET /dispatch/results/{id}.'''
    request_id: str


class ResultPendingResponse(BaseModel):
    '''
    202 from GET /dispatch/results/{request_id} — the worker has not finished.

    Distinct from the fire-and-forget `{'status': 'ok'}` despite also being a
    one-field status: 'ok' means accepted, 'pending' means not answered yet, and
    a client that confused them would stop polling.
    '''
    status: Literal['pending'] = 'pending'
