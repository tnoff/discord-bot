'''
The dispatch seam: sending and editing Discord messages, and fetches whose answer is polled for.

Everything but ``GET /dispatch/results/{request_id}`` is accepted with a 202 and carried out later.
The fetch routes answer with a ``request_id``; the result route answers 202 until the worker has
stored a result, then 200 with that result as stored: a history, an emoji list, a guild-roles
answer, or the error that stopped the call.
'''
from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock

import fakeredis.aioredis

from discord_core.routes import dispatch as dispatch_routes
from discord_core.routes import guild_roles as guild_roles_routes
from discord_core.types import dispatch_responses as out
from discord_core.types import guild_roles as roles
from discord_core.types import requests
from discord_core.types.fetched_message import FetchedMessage
from discord_core.types.results import (ChannelHistoryResultBody, DispatchErrorDetailBody,
                                        DispatchErrorResultBody, GuildEmojiBody, GuildEmojisResultBody)
from discord_core.utils.dispatch_queue import RedisDispatchQueue

from discord_dispatcher.servers.dispatch_server import DispatchHttpServer

from testing.contracts.dispatch_fetches import CONTRACTS
from tests.routes.specs._model import Call, Operation, operation, reply, serving

GUILD = 5
CHANNEL = 2
REQUEST_ID = 'req-1'


def environment():
    '''A dispatcher pod over a recording dispatcher and a real result queue on fakeredis.'''
    dispatcher = MagicMock()
    for name in ('enqueue_fetch_history', 'enqueue_fetch_emojis', 'enqueue_guild_roles_call'):
        setattr(dispatcher, name, AsyncMock())
    queue = RedisDispatchQueue(fakeredis.aioredis.FakeRedis(decode_responses=True), shard_id=0,
                               pod_id='spec')
    return serving(DispatchHttpServer(dispatcher, queue), dispatcher=dispatcher, queue=queue)


def stored(body):
    """An arrangement that leaves ``body`` as the worker's stored result."""
    async def arrange(env) -> dict:
        await env.parts.queue.store_result(REQUEST_ID, body.model_dump(mode='json'))
        return {}
    return arrange


RESULTS = (
    ChannelHistoryResultBody(guild_id=GUILD, channel_id=CHANNEL, messages=[FetchedMessage(
        id=3, content='hi', created_at=datetime(2026, 1, 1, tzinfo=timezone.utc), author_bot=False)]),
    GuildEmojisResultBody(guild_id=GUILD, emojis=[GuildEmojiBody(id=4, name='blob', animated=False)]),
    roles.GuildRolesResultBody(guild_id=GUILD, roles=[roles.RoleBody(id=6, name='mods')]),
    roles.MemberResultBody(guild_id=GUILD, user_id=7, member=roles.MemberBody(
        id=7, display_name='tyler', role_ids=[6])),
    roles.RoleMembersResultBody(guild_id=GUILD, role_id=6, members=[]),
    roles.ChangeMemberRoleResultBody(guild_id=GUILD, user_id=7, role_id=6, action='add'),
    DispatchErrorResultBody(error='nope', error_detail=DispatchErrorDetailBody(
        message='nope', type='NotFound', status=404, code=10008)),
)


def accepted(route, model, summary, response, body, status=202):
    """A fire-and-forget route: the body is accepted and nothing else is checked."""
    return route, Operation(
        summary, request=model,
        replies=(reply(status, response), reply(422, doc='The body is missing a field or is not JSON.')),
        calls=(Call(status, body=body), Call(422, body={})))


def fetch(route, body):
    """A fetch route: the shared contract, proved with a body it accepts and one it refuses."""
    return route, operation(CONTRACTS[route], (Call(202, body=body), Call(422, body={})))


OPERATIONS = dict([
    accepted(dispatch_routes.SEND, requests.SendRequestBody, 'Send a message.', out.SendResponse,
             {'guild_id': GUILD, 'channel_id': CHANNEL, 'content': 'hello'}),
    accepted(dispatch_routes.DELETE, requests.DeleteRequestBody, 'Delete a message.', out.DeleteResponse,
             {'guild_id': GUILD, 'channel_id': CHANNEL, 'message_id': 9}),
    accepted(dispatch_routes.UPDATE_MUTABLE, requests.UpdateMutableRequestBody,
             'Create or edit the message kept under a key.', out.UpdateMutableResponse,
             {'key': 'k', 'guild_id': GUILD, 'content': ['line']}),
    accepted(dispatch_routes.REMOVE_MUTABLE, requests.RemoveMutableRequestBody,
             'Remove the message kept under a key.', out.RemoveMutableResponse, {'key': 'k'}),
    accepted(dispatch_routes.UPDATE_MUTABLE_CHANNEL, requests.UpdateMutableChannelRequestBody,
             'Move the message kept under a key to another channel.', out.UpdateMutableChannelResponse,
             {'key': 'k', 'guild_id': GUILD, 'new_channel_id': 8}),
    fetch(dispatch_routes.FETCH_HISTORY, {'guild_id': GUILD, 'channel_id': CHANNEL, 'limit': 10}),
    fetch(dispatch_routes.FETCH_EMOJIS, {'guild_id': GUILD}),
    fetch(guild_roles_routes.FETCH_ROLES, {'guild_id': GUILD, 'call_id': 'c-1'}),
    fetch(guild_roles_routes.FETCH_MEMBER, {'guild_id': GUILD, 'user_id': 7, 'call_id': 'c-1'}),
    fetch(guild_roles_routes.FETCH_ROLE_MEMBERS, {'guild_id': GUILD, 'role_id': 6, 'call_id': 'c-1'}),
    fetch(guild_roles_routes.CHANGE_MEMBER_ROLE,
          {'guild_id': GUILD, 'user_id': 7, 'role_id': 6, 'action': 'add', 'call_id': 'c-1'}),
    (dispatch_routes.GET_RESULT, operation(CONTRACTS[dispatch_routes.GET_RESULT], (
        *[Call(200, path={'request_id': REQUEST_ID}, arrange=stored(body), variant=type(body))
          for body in RESULTS],
        Call(202, path={'request_id': REQUEST_ID})))),
])
