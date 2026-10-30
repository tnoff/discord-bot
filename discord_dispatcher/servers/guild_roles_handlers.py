'''
HTTP handlers for the guild-roles routes, as a mixin for DispatchHttpServer.

Split out of dispatch_server for the reason the broker's and the commands pod's handler mixins are:
the routes are declared in discord_core/routes/guild_roles.py, ahead of ``dispatch.ALL``, so the
seam-topology probe must not read this module's routes import as a second seam on the server class.
Mixed into DispatchHttpServer, which supplies ``_dispatcher`` and ``_body``.

Each route is an awaitable fetch: 202 with a ``request_id``, then the caller polls
``GET /dispatch/results/{request_id}``. The id is a hash of the call's parameters AND its
``call_id`` (types/guild_roles.py), so two calls never share a stored result.
'''
from dataclasses import dataclass
from typing import Awaitable, Callable

from aiohttp import web
from opentelemetry.propagate import extract
from opentelemetry.trace import SpanKind

from discord_core.routes import guild_roles as guild_roles_routes
from discord_core.routes.route import Route
from discord_core.types import guild_roles as bodies
from discord_core.utils.dispatch_queue import dispatch_request_id
from discord_core.utils.otel import otel_span_wrapper

from discord_dispatcher.workers import guild_roles as calls


@dataclass(frozen=True)
class _Call:
    '''One route: the call it runs, the models it speaks, and the body fields that parameterise it.'''
    kind: str
    request_model: type
    response_model: type
    fields: tuple[str, ...]


_CALLS = {
    guild_roles_routes.FETCH_ROLES: _Call(
        calls.FETCH_ROLES, bodies.FetchRolesRequestBody, bodies.FetchRolesResponse, ('guild_id',)),
    guild_roles_routes.FETCH_MEMBER: _Call(
        calls.FETCH_MEMBER, bodies.FetchMemberRequestBody, bodies.FetchMemberResponse,
        ('guild_id', 'user_id')),
    guild_roles_routes.FETCH_ROLE_MEMBERS: _Call(
        calls.FETCH_ROLE_MEMBERS, bodies.FetchRoleMembersRequestBody, bodies.FetchRoleMembersResponse,
        ('guild_id', 'role_id')),
    guild_roles_routes.CHANGE_MEMBER_ROLE: _Call(
        calls.CHANGE_MEMBER_ROLE, bodies.ChangeMemberRoleRequestBody, bodies.ChangeMemberRoleResponse,
        ('guild_id', 'user_id', 'role_id', 'action')),
}


class GuildRolesHandlersMixin:
    '''The guild-roles routes' handlers, for a server that holds a ``_dispatcher`` and a ``_body``.'''

    _dispatcher: object
    _body: Callable[..., Awaitable]

    def guild_roles_handlers(self) -> dict[Route, Callable]:
        '''Route -> handler for every guild-roles route.'''
        return {route: self._handler_for(call) for route, call in _CALLS.items()}

    def _handler_for(self, call: _Call) -> Callable:
        async def handle(request: web.Request) -> web.Response:
            ctx = extract(request.headers)
            body = await self._body(request, call.request_model)
            params = {field: getattr(body, field) for field in call.fields}
            # call_id is hashed, span_context is not: the id must differ per call and not per trace.
            request_id = dispatch_request_id({'kind': call.kind, **params, 'call_id': body.call_id})
            with otel_span_wrapper(f'dispatch.{call.kind}', context=ctx, kind=SpanKind.SERVER):
                await self._dispatcher.enqueue_guild_roles_call(
                    request_id, call.kind, params, span_context=body.span_context)
            return web.json_response(call.response_model(request_id=request_id).model_dump(), status=202)
        return handle
