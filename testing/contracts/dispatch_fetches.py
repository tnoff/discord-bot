'''
The dispatch seam's fetches: asking the dispatcher a question Discord has to answer, and polling for the answer.

Each fetch is a POST that answers 202 with a ``request_id``; the caller then polls
``GET /dispatch/results/{request_id}``, which answers 202 until the worker has stored a result and then 200 with
it. A failed fetch is a 200 holding a ``DispatchErrorResultBody``, never a status code. The request bodies, the
202 answers and every result body are `discord_core` types (the dispatch client builds from them and the
dispatcher serves them), so this table only adds what core does not hold: the summary and which statuses a route
may answer.

Both ends read this: the dispatcher's server is proved against it (tests/routes/specs/dispatch.py runs every
reply), and the commands pod's guild-roles client is tested against a stub that serves it
(discord_commands/tests/clients/test_http_guild_roles_client.py), so neither needs the other pod.
'''
from discord_core.routes import dispatch as dispatch_routes
from discord_core.routes import guild_roles as guild_roles_routes
from discord_core.types import dispatch_responses as out
from discord_core.types import guild_roles as roles
from discord_core.types import requests
from discord_core.types.results import (ChannelHistoryResultBody, DispatchErrorResultBody, GuildEmojisResultBody)

from testing.contracts.model import Contract, reply

#: Every body ``GET /dispatch/results/{request_id}`` may answer 200 with, whichever fetch it answers.
RESULT_MODELS = (ChannelHistoryResultBody, GuildEmojisResultBody, roles.GuildRolesResultBody,
                 roles.MemberResultBody, roles.RoleMembersResultBody, roles.ChangeMemberRoleResultBody,
                 DispatchErrorResultBody)


def _fetch(summary: str, model, response) -> Contract:
    '''A fetch: the body is accepted and the answer is the id to poll.'''
    return Contract(summary, request=model,
                    replies=(reply(202, response), reply(422, doc='The body is missing a field or is not JSON.')))


CONTRACTS = {
    dispatch_routes.FETCH_HISTORY: _fetch(
        "Ask for a channel's recent messages. Poll the result route with the `request_id`.",
        requests.FetchHistoryRequestBody, out.FetchHistoryResponse),
    dispatch_routes.FETCH_EMOJIS: _fetch(
        "Ask for a guild's emoji. Poll the result route with the `request_id`.",
        requests.FetchEmojisRequestBody, out.FetchEmojisResponse),
    guild_roles_routes.FETCH_ROLES: _fetch(
        "Ask for a guild's roles. Poll the result route with the `request_id`.",
        roles.FetchRolesRequestBody, roles.FetchRolesResponse),
    guild_roles_routes.FETCH_MEMBER: _fetch(
        'Ask for one member and their roles. Poll the result route with the `request_id`.',
        roles.FetchMemberRequestBody, roles.FetchMemberResponse),
    guild_roles_routes.FETCH_ROLE_MEMBERS: _fetch(
        'Ask for the members holding a role. Poll the result route with the `request_id`.',
        roles.FetchRoleMembersRequestBody, roles.FetchRoleMembersResponse),
    guild_roles_routes.CHANGE_MEMBER_ROLE: _fetch(
        'Add a role to a member or take it away. Poll the result route with the `request_id`.',
        roles.ChangeMemberRoleRequestBody, roles.ChangeMemberRoleResponse),
    dispatch_routes.GET_RESULT: Contract(
        'The stored result of a fetch, or 202 while there is none yet.', path={'request_id': str},
        replies=(reply(200, *RESULT_MODELS, doc='The result as the worker stored it, whichever fetch it answers.'),
                 reply(202, out.ResultPendingResponse, doc='No result yet; ask again.'))),
}
