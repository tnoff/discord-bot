'''
How command logic talks back to the channel.
'''
from typing import Awaitable, Callable

from discord_core.clients.dispatch_client_base import DispatchClientBase
from discord_core.types.dispatch_request import SendRequest
from discord_core.utils.otel import capture_span_context

# (guild_id, channel_id, content, delete_after=None) -> content. The pod passes ``dispatcher_reply``,
# which sends through the dispatcher and returns the text it sent, so a handler can end with
# ``return await self.reply(...)``. ``delete_after`` is seconds until the message is removed;
# None keeps it. Ellipsis because Callable cannot express the optional argument.
Reply = Callable[..., Awaitable[str]]


def dispatcher_reply(dispatcher: DispatchClientBase) -> Reply:
    '''A Reply that sends through the dispatcher pod, linked to the span the command runs in.'''
    async def _reply(guild_id: int, channel_id: int, content: str, delete_after: int | None = None) -> str:
        await dispatcher.submit_request(SendRequest(
            guild_id=guild_id, channel_id=channel_id, content=content, delete_after=delete_after,
            span_context=capture_span_context(),
        ))
        return content
    return _reply
