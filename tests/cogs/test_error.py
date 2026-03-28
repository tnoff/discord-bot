from unittest.mock import MagicMock, patch

import pytest
from discord.ext import commands

from discord_bot.cogs.error import CommandErrorHandler

from tests.helpers import fake_context  #pylint:disable=unused-import


def make_cog(ctx_fixture):
    return CommandErrorHandler(ctx_fixture['bot'], {})


def make_ctx(fake_context):  #pylint:disable=redefined-outer-name
    '''MagicMock ctx wired to the fake_context guild and channel so dispatch lands in messages_sent.'''
    ctx = MagicMock()
    ctx.guild.id = fake_context['guild'].id
    ctx.channel.id = fake_context['channel'].id
    ctx.command = MagicMock(spec=[])
    ctx.cog = None
    return ctx


@pytest.mark.asyncio
async def test_on_command_error_returns_early_when_command_has_on_error(fake_context):  #pylint:disable=redefined-outer-name
    '''Returns immediately if the command has its own on_error handler'''
    cog = make_cog(fake_context)
    ctx = make_ctx(fake_context)
    ctx.command = MagicMock(spec=['on_error'])
    ctx.command.on_error = MagicMock()

    await cog.on_command_error(ctx, commands.CommandNotFound('!foo'))

    assert len(fake_context['channel'].messages_sent) == 0


@pytest.mark.asyncio
async def test_on_command_error_returns_early_when_cog_overrides_error(fake_context):  #pylint:disable=redefined-outer-name
    '''Returns immediately if the cog overrides cog_command_error'''
    cog = make_cog(fake_context)
    ctx = make_ctx(fake_context)
    ctx.cog = MagicMock()
    setattr(ctx.cog, '_get_overridden_method', MagicMock(return_value=MagicMock()))

    await cog.on_command_error(ctx, commands.CommandNotFound('!foo'))

    assert len(fake_context['channel'].messages_sent) == 0


@pytest.mark.asyncio
async def test_on_command_error_command_not_found(fake_context):  #pylint:disable=redefined-outer-name
    '''Sends help message for CommandNotFound via dispatch'''
    cog = make_cog(fake_context)
    ctx = make_ctx(fake_context)
    error = commands.CommandNotFound('!unknown')

    await cog.on_command_error(ctx, error)

    assert len(fake_context['channel'].messages_sent) == 1
    assert 'use !help' in fake_context['channel'].messages_sent[0]


@pytest.mark.asyncio
async def test_on_command_error_missing_required_argument(fake_context):  #pylint:disable=redefined-outer-name
    '''Sends missing-argument message for MissingRequiredArgument via dispatch'''
    cog = make_cog(fake_context)
    ctx = make_ctx(fake_context)

    param = MagicMock()
    param.name = 'query'
    error = commands.MissingRequiredArgument(param)

    await cog.on_command_error(ctx, error)

    assert len(fake_context['channel'].messages_sent) == 1
    assert 'Missing required arguments' in fake_context['channel'].messages_sent[0]


@pytest.mark.asyncio
async def test_on_command_error_unknown_error_logs(fake_context):  #pylint:disable=redefined-outer-name
    '''Unknown errors are logged via logger.error with exc_info; no message dispatched'''
    cog = make_cog(fake_context)
    ctx = make_ctx(fake_context)
    error = RuntimeError('something broke')

    with patch.object(cog.logger, 'error') as mock_log:
        await cog.on_command_error(ctx, error)

    mock_log.assert_called_once()
    assert len(fake_context['channel'].messages_sent) == 0


@pytest.mark.asyncio
async def test_on_command_error_unwraps_original(fake_context):  #pylint:disable=redefined-outer-name
    '''Uses error.original when present to determine the error type'''
    cog = make_cog(fake_context)
    ctx = make_ctx(fake_context)

    wrapper = MagicMock()
    wrapper.original = commands.CommandNotFound('!wrapped')

    await cog.on_command_error(ctx, wrapper)

    assert len(fake_context['channel'].messages_sent) == 1
    assert 'use !help' in fake_context['channel'].messages_sent[0]
