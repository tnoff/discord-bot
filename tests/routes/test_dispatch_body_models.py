'''
Criterion: every dispatch seam body is a named model, and it stays one.

projects/seam-body-typing. The dispatch seam was the only one whose bodies were
untyped -- read field by field out of `await request.json()` on the server and
written as dict literals on the client -- so it is where that project starts.

These tests are about the CONTRACT, not about the handlers: that a model exists
for every route, that it serialises to the bytes the seam already sends, and
that the 422 behaviour the retry ladder depends on is unchanged.
'''
import inspect

import pytest
from pydantic import BaseModel

from discord_bot.seams.dispatch.routes import dispatch as dispatch_routes
from discord_bot.seams.dispatch.types import requests, responses

#: Route constant -> (request model, response model). Written out rather than
#: derived, because deriving it from the modules would make this test agree with
#: whatever they happen to contain -- which is the tautology projects/
#: per-image-code-split spent six steps escaping on the layout rules.
ROUTE_MODELS = {
    'SEND': (requests.SendRequestBody, responses.SendResponse),
    'DELETE': (requests.DeleteRequestBody, responses.DeleteResponse),
    'UPDATE_MUTABLE': (requests.UpdateMutableRequestBody, responses.UpdateMutableResponse),
    'REMOVE_MUTABLE': (requests.RemoveMutableRequestBody, responses.RemoveMutableResponse),
    'UPDATE_MUTABLE_CHANNEL': (requests.UpdateMutableChannelRequestBody,
                               responses.UpdateMutableChannelResponse),
    'FETCH_HISTORY': (requests.FetchHistoryRequestBody, responses.FetchHistoryResponse),
    'FETCH_EMOJIS': (requests.FetchEmojisRequestBody, responses.FetchEmojisResponse),
    'GET_RESULT': (None, responses.ResultPendingResponse),
}


def _route_constants():
    return {name for name, value in vars(dispatch_routes).items()
            if isinstance(value, dispatch_routes.Route)}


def test_every_dispatch_route_has_body_models():
    '''Equality, not containment: a new route fails here, and so does a stale entry.'''
    routes = _route_constants()
    assert routes, 'no Route constants found in the dispatch seam -- the detector is broken'
    assert routes == set(ROUTE_MODELS), (
        f'dispatch routes and body models disagree: {routes ^ set(ROUTE_MODELS)}. '
        f'A route added without a body model is the untyped state this project removed.'
    )


def test_every_body_model_is_a_pydantic_model():
    '''
    A `@dataclass` would pass "has a model" while being unable to round-trip
    through a schema, which is the whole point of typing these.
    '''
    checked = [m for pair in ROUTE_MODELS.values() for m in pair if m is not None]
    assert len(checked) >= 15, f'only {len(checked)} models in the map -- it has been gutted'
    for model in checked:
        assert inspect.isclass(model) and issubclass(model, BaseModel), (
            f'{model!r} is not a pydantic model'
        )


@pytest.mark.parametrize('model,expected', [
    (responses.SendResponse, {'status': 'ok'}),
    (responses.DeleteResponse, {'status': 'ok'}),
    (responses.UpdateMutableResponse, {'status': 'ok'}),
    (responses.RemoveMutableResponse, {'status': 'ok'}),
    (responses.UpdateMutableChannelResponse, {'status': 'ok'}),
    (responses.ResultPendingResponse, {'status': 'pending'}),
])
def test_response_models_serialise_to_the_bytes_the_seam_already_sent(model, expected):
    '''
    The wire does not move. These are the literals the handlers returned before
    the models existed, asserted against `model_dump()` rather than against each
    other, so a model that gains a field fails here rather than at a peer.
    '''
    assert model().model_dump() == expected


@pytest.mark.parametrize('model', [
    responses.FetchHistoryResponse, responses.FetchEmojisResponse,
])
def test_fetch_responses_carry_only_the_request_id(model):
    assert model(request_id='abc').model_dump() == {'request_id': 'abc'}


def test_request_bodies_keep_the_coercion_the_handlers_had():
    '''
    The handlers coerced with `int()`/`str()`, so `"123"` was an acceptable
    guild_id. pydantic's default mode coerces the same way. Tightening to strict
    would be a wire change wearing a refactor's clothes, so it is pinned here.
    '''
    body = requests.SendRequestBody(guild_id='123', channel_id='4', content='hi')
    assert body.guild_id == 123 and body.channel_id == 4


def test_fetch_history_keeps_after_a_string():
    '''
    `after` must not become a datetime. `dispatch_request_id` hashes
    `json.dumps(params, sort_keys=True, default=str)`, so a parsed datetime
    renders differently from the ISO string the client sends and every fetch
    would get a fresh request_id -- silently ending result reuse.
    '''
    raw = '2026-09-21T00:00:00'
    body = requests.FetchHistoryRequestBody(guild_id=1, channel_id=2, limit=3, after=raw)
    assert body.after == raw and isinstance(body.after, str)


def test_send_body_can_express_allow_404():
    '''
    The field the typed path could not reach. `_handle_send(SendRequest)` posted
    a body without `allow_404` while `send_message()` posted one with it, and the
    core `SendRequest` dataclass had no such field -- so a caller going through
    the typed transport hook could not ask for it, and the server silently
    defaulted it to False. One model per route is what makes that visible.
    '''
    assert requests.SendRequestBody(guild_id=1, channel_id=2, content='x').allow_404 is False
    assert requests.SendRequestBody(guild_id=1, channel_id=2, content='x',
                                    allow_404=True).allow_404 is True
