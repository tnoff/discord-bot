'''
ContractStub holds a client test to the contract in both directions, so that is what is pinned here: it refuses a
reply the contract does not declare, fails the test when the client sends a body the request model refuses or
asks a route nobody scripted, and plays a sequence in order with the last reply repeating.
'''
import pytest

from discord_core.routes import dispatch as dispatch_routes
from discord_core.routes import guild_roles as routes
from discord_core.types import guild_roles as bodies
from discord_core.types.dispatch_responses import ResultPendingResponse

from testing.contracts.dispatch_fetches import CONTRACTS
from testing.contracts.stub import ContractStub, serving

ACCEPTED = bodies.FetchRolesResponse(request_id='r-1')
VALID = {'guild_id': 1, 'call_id': 'c'}


def test_it_will_not_play_a_status_the_contract_does_not_declare():
    '''A test cannot make the pod answer something the pod never would.'''
    with pytest.raises(AssertionError, match='declares no 500 reply'):
        ContractStub(CONTRACTS).script(routes.FETCH_ROLES, status=500)


def test_it_will_not_play_a_body_that_is_not_the_declared_model():
    '''The body must be exactly the model: another route's model, or an extra field, is refused.'''
    stub = ContractStub(CONTRACTS)
    with pytest.raises(AssertionError, match='is none of'):
        stub.script(routes.FETCH_ROLES, {'request_id': 'r-1', 'extra': 1}, status=202)
    with pytest.raises(AssertionError, match='is none of'):
        stub.script(routes.FETCH_ROLES, ResultPendingResponse(), status=202)


def test_it_will_not_play_a_body_for_a_reply_that_has_none():
    '''A bodyless status stays bodyless.'''
    with pytest.raises(AssertionError, match='declares no body'):
        ContractStub(CONTRACTS).script(routes.FETCH_ROLES, {'detail': 'x'}, status=422)


@pytest.mark.asyncio
async def test_a_body_the_request_model_refuses_fails_the_test_on_exit():
    '''The pod would answer 422, and a client that never reads the answer must not pass by ignoring it.'''
    stub = ContractStub(CONTRACTS)
    stub.script(routes.FETCH_ROLES, ACCEPTED, status=202)
    with pytest.raises(AssertionError, match='FetchRolesRequestBody refuses'):
        async with serving(stub) as tc:
            async with tc.post(routes.FETCH_ROLES.template, json={'guild_id': 'nope'}) as resp:
                assert resp.status == 422
    assert not stub.calls


@pytest.mark.asyncio
async def test_a_route_nobody_scripted_fails_the_test_on_exit():
    '''A call the test did not expect is a failure, not a default answer.'''
    stub = ContractStub(CONTRACTS)
    with pytest.raises(AssertionError, match='scripted no reply'):
        async with serving(stub) as tc:
            async with tc.post(routes.FETCH_ROLES.template, json=VALID) as resp:
                assert resp.status == 500


@pytest.mark.asyncio
async def test_a_sequence_plays_in_order_and_the_last_reply_repeats():
    '''A poll answers pending until the result is there, and keeps answering the result.'''
    stub = ContractStub(CONTRACTS)
    stub.script_sequence(dispatch_routes.GET_RESULT,
                         (202, ResultPendingResponse()),
                         (200, bodies.GuildRolesResultBody(guild_id=1, roles=[])))
    async with serving(stub) as tc:
        seen = []
        for _ in range(3):
            async with tc.get('/dispatch/results/r-1') as resp:
                seen.append(resp.status)
    assert seen == [202, 200, 200]
    assert stub.paths_to(dispatch_routes.GET_RESULT) == [{'request_id': 'r-1'}] * 3


@pytest.mark.asyncio
async def test_it_records_the_body_a_route_was_sent():
    '''What the client sent is what the test asserts on.'''
    stub = ContractStub(CONTRACTS)
    stub.script(routes.FETCH_ROLES, ACCEPTED, status=202)
    async with serving(stub) as tc:
        async with tc.post(routes.FETCH_ROLES.template, json=VALID) as resp:
            assert await resp.json() == {'request_id': 'r-1'}
    assert stub.calls_to(routes.FETCH_ROLES) == [VALID]
