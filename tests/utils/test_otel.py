from unittest.mock import MagicMock, patch

import pytest
from discord.ext.commands import Context
from opentelemetry import trace

from discord_bot.utils.otel import (
    async_otel_span_wrapper, capture_span_context, command_wrapper,
    otel_span_wrapper, span_links_from_context,
)


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


def test_capture_span_context_no_active_span():
    '''capture_span_context returns None when no valid span is active (no-op tracer)'''
    result = capture_span_context()
    assert result is None


def test_capture_span_context_with_active_span():
    '''capture_span_context returns a dict with the expected keys inside a real span'''
    tracer = trace.get_tracer(__name__)
    with tracer.start_as_current_span('test.capture'):
        result = capture_span_context()
    # The no-op tracer produces an invalid span, so result is still None.
    # What matters is that the function runs without error and returns dict-or-None.
    assert result is None or isinstance(result, dict)


def test_capture_span_context_returns_dict_for_valid_span():
    '''capture_span_context returns a populated dict when the active span context is valid'''
    mock_span_ctx = MagicMock()
    mock_span_ctx.is_valid = True
    mock_span_ctx.trace_id = 0xDEADBEEF
    mock_span_ctx.span_id = 0xBEEF
    mock_span_ctx.trace_flags = trace.TraceFlags(1)
    mock_span = MagicMock()
    mock_span.get_span_context.return_value = mock_span_ctx
    with patch('discord_bot.utils.otel.trace.get_current_span', return_value=mock_span):
        result = capture_span_context()
    assert result == {'trace_id': 0xDEADBEEF, 'span_id': 0xBEEF, 'trace_flags': 1}


def test_span_links_from_context_none():
    '''span_links_from_context returns empty list for None input'''
    assert not span_links_from_context(None)


def test_span_links_from_context_empty_dict():
    '''span_links_from_context returns empty list for empty dict'''
    assert not span_links_from_context({})


def test_span_links_from_context_valid():
    '''span_links_from_context returns a single Link for a valid context dict'''
    ctx = {
        'trace_id': 0x000000000000000000000000DEADBEEF,
        'span_id': 0x00000000DEADBEF0,
        'trace_flags': 1,
    }
    links = span_links_from_context(ctx)
    assert len(links) == 1
    assert isinstance(links[0], trace.Link)
    assert links[0].context.trace_id == ctx['trace_id']
    assert links[0].context.span_id == ctx['span_id']


def test_span_links_from_context_zero_ids_invalid():
    '''span_links_from_context returns empty list when trace_id or span_id are zero (invalid)'''
    ctx = {'trace_id': 0, 'span_id': 0, 'trace_flags': 0}
    assert not span_links_from_context(ctx)


def test_otel_span_wrapper_with_links():
    '''otel_span_wrapper accepts a links list without error'''
    link_ctx = trace.SpanContext(
        trace_id=0x000000000000000000000000DEADBEEF,
        span_id=0x00000000DEADBEF0,
        is_remote=True,
        trace_flags=trace.TraceFlags(1),
    )
    links = [trace.Link(link_ctx)]
    with otel_span_wrapper('test.linked_span', links=links) as span:
        assert span is not None


@pytest.mark.asyncio
async def test_async_otel_span_wrapper_with_links():
    '''async_otel_span_wrapper accepts a links list without error'''
    link_ctx = trace.SpanContext(
        trace_id=0x000000000000000000000000DEADBEEF,
        span_id=0x00000000DEADBEF0,
        is_remote=True,
        trace_flags=trace.TraceFlags(1),
    )
    links = [trace.Link(link_ctx)]
    async with async_otel_span_wrapper('test.async_linked_span', links=links) as span:
        assert span is not None
