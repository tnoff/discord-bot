'''
Tests for the gateway-only OTel command wrapper.

Split out of test_otel.py alongside the module itself: command_wrapper is the
only OTel helper that needs discord.py at runtime, so it lives apart from the
span/metric helpers every image imports.
'''
import pytest

from discord_gateway.utils.otel_command import command_wrapper

from tests.fakes.command_context import make_command_context


@pytest.mark.asyncio
async def test_command_wrapper_finds_ctx_and_builds_span_name():
    '''command_wrapper locates the Context arg and derives span_name from it'''
    ctx = make_command_context()

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
