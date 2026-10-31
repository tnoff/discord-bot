'''
The guild-roles routes: how the commands pod reads and changes a guild's roles (#1030).

The `role` commands need four things from Discord: a guild's roles, one member, the members who hold
a role, and to add or remove a role. The gateway answered them from its guild cache. The commands
pod has no gateway connection, so it asks the dispatcher, the REST-only pod that already holds the
bot token (design: PHASE2-DESIGN.md section 3).

They are awaitable fetches, the same shape as `routes/dispatch.py` FETCH_HISTORY and FETCH_EMOJIS:
each POST answers 202 with a `request_id`, and the caller polls `GET /dispatch/results/{request_id}`
(`dispatch.GET_RESULT`) for the outcome. A failure comes back as a 200 holding
`DispatchErrorResultBody`, never as a status code, for the reason `types/results.py` records.

Every body carries a `call_id`, the caller's one-per-call nonce. The dispatcher derives `request_id`
from the body, and keeps a result for a day, so two identical bodies would share one result: a
second `role add` for the same member and role would be answered with the first one's outcome
without touching Discord. `call_id` makes each call its own request, while a retried POST (same
`call_id`) still lands on the one request.

Declared in its own module, and part of `routes/dispatch.py`'s ALL: they shipped ahead of the
dispatcher for the deploy-order reason routes/gateway_intents.py records (every `HttpDispatchClient`
demands everything in `dispatch.ALL` of its peer, so they could not join it before the dispatcher
served them), and the dispatcher has served them since #1166.
'''
from discord_core.routes.route import Route, collect

ROUTE_PREFIX = '/dispatch'

FETCH_ROLES = Route('POST', f'{ROUTE_PREFIX}/fetch_roles')
FETCH_MEMBER = Route('POST', f'{ROUTE_PREFIX}/fetch_member')
FETCH_ROLE_MEMBERS = Route('POST', f'{ROUTE_PREFIX}/fetch_role_members')
CHANGE_MEMBER_ROLE = Route('POST', f'{ROUTE_PREFIX}/change_member_role')

ALL = collect(globals())
