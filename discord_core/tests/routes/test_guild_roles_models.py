'''
The guild-roles contract (routes/guild_roles.py, types/guild_roles.py): every route has a request and
a 202 answer, a result body per route, and the one rule that keeps results from being shared.
'''
import inspect

import pytest
from pydantic import BaseModel, ValidationError

from discord_core.routes import dispatch as dispatch_routes, guild_roles
from discord_core.routes.route import Route
from discord_core.types import guild_roles as bodies

#: Route constant -> (request model, 202 answer, result body). Written out, not derived, so a route
#: added without its models fails here instead of agreeing with itself.
ROUTE_MODELS = {
    'FETCH_ROLES': (bodies.FetchRolesRequestBody, bodies.FetchRolesResponse, bodies.GuildRolesResultBody),
    'FETCH_MEMBER': (bodies.FetchMemberRequestBody, bodies.FetchMemberResponse, bodies.MemberResultBody),
    'FETCH_ROLE_MEMBERS': (bodies.FetchRoleMembersRequestBody, bodies.FetchRoleMembersResponse,
                           bodies.RoleMembersResultBody),
    'CHANGE_MEMBER_ROLE': (bodies.ChangeMemberRoleRequestBody, bodies.ChangeMemberRoleResponse,
                           bodies.ChangeMemberRoleResultBody),
}


def test_every_guild_roles_route_has_its_models():
    declared = {name for name, value in vars(guild_roles).items() if isinstance(value, Route)}
    assert declared == set(ROUTE_MODELS)
    for models in ROUTE_MODELS.values():
        for model in models:
            assert inspect.isclass(model) and issubclass(model, BaseModel)


def test_the_routes_are_posts_under_the_dispatch_prefix_and_not_yet_in_dispatch_all():
    '''Outside `dispatch.ALL` until the dispatcher serves them (routes/guild_roles.py).'''
    assert len(guild_roles.ALL) == len(ROUTE_MODELS)
    for route in guild_roles.ALL:
        assert route.method == 'POST'
        assert route.template.startswith(f'{dispatch_routes.ROUTE_PREFIX}/')
        assert route not in dispatch_routes.ALL


@pytest.mark.parametrize('name', sorted(ROUTE_MODELS))
def test_every_request_carries_a_call_id(name):
    '''Without it, two identical calls share one stored result (routes/guild_roles.py).'''
    request_model = ROUTE_MODELS[name][0]
    assert 'call_id' in request_model.model_fields
    assert request_model.model_fields['call_id'].is_required()


@pytest.mark.parametrize('name', sorted(ROUTE_MODELS))
def test_every_answer_carries_only_the_request_id(name):
    assert ROUTE_MODELS[name][1](request_id='abc').model_dump() == {'request_id': 'abc'}


def test_change_member_role_accepts_only_add_and_remove():
    kwargs = {'guild_id': 1, 'user_id': 2, 'role_id': 3, 'call_id': 'c'}
    assert bodies.ChangeMemberRoleRequestBody(action='add', **kwargs).action == 'add'
    assert bodies.ChangeMemberRoleRequestBody(action='remove', **kwargs).action == 'remove'
    with pytest.raises(ValidationError):
        bodies.ChangeMemberRoleRequestBody(action='toggle', **kwargs)


def test_request_ids_coerce_like_the_other_dispatch_bodies():
    body = bodies.FetchMemberRequestBody(guild_id='11', user_id='22', call_id='c')
    assert (body.guild_id, body.user_id) == (11, 22)


def test_a_member_who_left_is_a_result_not_a_failure():
    assert bodies.MemberResultBody(guild_id=1, user_id=2).model_dump() == {
        'guild_id': 1, 'user_id': 2, 'member': None}


def test_results_serialise_in_declaration_order():
    '''Field order is the wire (types/guild_roles.py).'''
    member = bodies.MemberBody(id=2, display_name='Ann', role_ids=[5, 6])
    assert list(bodies.RoleMembersResultBody(guild_id=1, role_id=5, members=[member]).model_dump()) == [
        'guild_id', 'role_id', 'members']
    assert list(member.model_dump()) == ['id', 'display_name', 'role_ids']
    assert bodies.GuildRolesResultBody(guild_id=1, roles=[bodies.RoleBody(id=5, name='dj')]).model_dump() == {
        'guild_id': 1, 'roles': [{'id': 5, 'name': 'dj'}]}
    assert bodies.ChangeMemberRoleResultBody(guild_id=1, user_id=2, role_id=3, action='add').model_dump() == {
        'guild_id': 1, 'user_id': 2, 'role_id': 3, 'action': 'add'}
