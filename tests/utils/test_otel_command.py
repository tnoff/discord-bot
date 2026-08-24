'''
Tests for the gateway-only OTel command wrapper.

Split out of test_otel.py alongside the module itself: command_wrapper is the
only OTel helper that needs discord.py at runtime, so it lives apart from the
span/metric helpers every image imports.
'''
from unittest.mock import MagicMock

import pytest
from discord.ext.commands import Context

from discord_bot.utils.otel_command import command_wrapper


def _make_ctx():
    '''Return a minimal discord Context instance without calling __init__'''
    ctx = Context.__new__(Context)
    ctx.author = MagicMock()
    ctx.author.id = 1001
    ctx.channel = MagicMock()
    ctx.channel.id = 2002
    ctx.guild = MagicMock()
    ctx.guild.id = 3003
    ctx.command = MagicMock()
    ctx.command.name = 'testcmd'
    ctx.command.cog = MagicMock()
    ctx.command.cog.qualified_name = 'TestCog'
    ctx.message = MagicMock()
    ctx.message.content = '!testcmd arg1'
    return ctx


@pytest.mark.asyncio
async def test_command_wrapper_finds_ctx_and_builds_span_name():
    '''command_wrapper locates the Context arg and derives span_name from it'''
    ctx = _make_ctx()

    async def _dummy(_self, _ctx):
        return 'ok'

    wrapped = command_wrapper(_dummy)
    result = await wrapped(None, ctx)
    assert result == 'ok'


@pytest.mark.asyncio
async def test_command_wrapper_no_ctx_uses_default_span_name():
    '''command_wrapper uses fallback span name when no Context arg is present'''
    async def _dummy(_self):
        return 'ok'

    wrapped = command_wrapper(_dummy)
    result = await wrapped(None)
    assert result == 'ok'
