from unittest.mock import AsyncMock

import pytest

from discord_core.types.dispatch_request import SendRequest

from discord_commands.command_logic.reply import dispatcher_reply


@pytest.mark.asyncio
async def test_dispatcher_reply_submits_a_send_and_returns_the_content():
    dispatcher = AsyncMock()
    assert await dispatcher_reply(dispatcher)(1, 2, 'hi') == 'hi'
    request = dispatcher.submit_request.await_args.args[0]
    assert isinstance(request, SendRequest)
    assert (request.guild_id, request.channel_id, request.content, request.delete_after) == (1, 2, 'hi', None)
