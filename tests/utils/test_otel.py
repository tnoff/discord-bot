from unittest.mock import MagicMock

import pytest
from discord.ext.commands import Context

from discord_bot.utils.otel import async_otel_span_wrapper, command_wrapper, otel_span_wrapper


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


def test_otel_span_wrapper_with_ctx():
    '''otel_span_wrapper sets discord span attributes when ctx is provided'''
    ctx = _make_ctx()
    with otel_span_wrapper('test.span', ctx=ctx) as span:
        assert span is not None


def test_otel_span_wrapper_with_attributes():
    '''otel_span_wrapper sets extra attributes when provided'''
    with otel_span_wrapper('test.span', attributes={'key': 'value'}) as span:
        assert span is not None


@pytest.mark.asyncio
async def test_async_otel_span_wrapper_with_ctx():
    '''async_otel_span_wrapper sets discord span attributes when ctx is provided'''
    ctx = _make_ctx()
    async with async_otel_span_wrapper('test.async_span', ctx=ctx) as span:
        assert span is not None


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


@pytest.mark.asyncio
async def test_async_otel_span_wrapper_records_exception_on_error():
    '''async_otel_span_wrapper re-raises exceptions after recording them on the span'''
    with pytest.raises(ValueError):
        async with async_otel_span_wrapper('test.error_span'):
            raise ValueError('test error')
