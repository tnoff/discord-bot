from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from discord.ext import commands

from discord_bot.cogs.error import CommandErrorHandler
from discord_bot.utils.common import GeneralConfig

from tests.helpers import fake_context  #pylint:disable=unused-import


def make_cog(ctx_fixture):
    config = GeneralConfig(discord_token='fake-token')
    return CommandErrorHandler(ctx_fixture['bot'], config)


@pytest.mark.asyncio
async def test_on_command_error_returns_early_when_command_has_on_error(fake_context):  #pylint:disable=redefined-outer-name
    '''Returns immediately if the command has its own on_error handler'''
    cog = make_cog(fake_context)
    ctx = MagicMock()
    ctx.command = MagicMock(spec=['on_error'])
    ctx.command.on_error = MagicMock()
    ctx.send = AsyncMock()

    await cog.on_command_error(ctx, commands.CommandNotFound('!foo'))

    ctx.send.assert_not_called()


@pytest.mark.asyncio
async def test_on_command_error_returns_early_when_cog_overrides_error(fake_context):  #pylint:disable=redefined-outer-name
    '''Returns immediately if the cog overrides cog_command_error'''
    cog = make_cog(fake_context)
    ctx = MagicMock()
    ctx.command = MagicMock(spec=[])  # no on_error attribute
    mock_cog = MagicMock()
    setattr(mock_cog, '_get_overridden_method', MagicMock(return_value=MagicMock()))
    ctx.cog = mock_cog
    ctx.send = AsyncMock()

    await cog.on_command_error(ctx, commands.CommandNotFound('!foo'))

    ctx.send.assert_not_called()


@pytest.mark.asyncio
async def test_on_command_error_command_not_found(fake_context):  #pylint:disable=redefined-outer-name
    '''Sends help message for CommandNotFound'''
    cog = make_cog(fake_context)
    ctx = MagicMock()
    ctx.command = MagicMock(spec=[])
    ctx.cog = None
    ctx.send = AsyncMock()
    error = commands.CommandNotFound('!unknown')

    await cog.on_command_error(ctx, error)

    ctx.send.assert_called_once()
    assert 'use !help' in ctx.send.call_args[0][0]


@pytest.mark.asyncio
async def test_on_command_error_missing_required_argument(fake_context):  #pylint:disable=redefined-outer-name
    '''Sends missing-argument message for MissingRequiredArgument'''
    cog = make_cog(fake_context)
    ctx = MagicMock()
    ctx.command = MagicMock(spec=[])
    ctx.cog = None
    ctx.send = AsyncMock()

    param = MagicMock()
    param.name = 'query'
    error = commands.MissingRequiredArgument(param)

    await cog.on_command_error(ctx, error)

    ctx.send.assert_called_once()
    assert 'Missing required arguments' in ctx.send.call_args[0][0]


@pytest.mark.asyncio
async def test_on_command_error_unknown_error_logs(fake_context):  #pylint:disable=redefined-outer-name
    '''Unknown errors are logged via logger.exception'''
    cog = make_cog(fake_context)
    ctx = MagicMock()
    ctx.command = MagicMock(spec=[])
    ctx.cog = None
    ctx.send = AsyncMock()
    error = RuntimeError('something broke')

    with patch.object(cog.logger, 'exception') as mock_log:
        await cog.on_command_error(ctx, error)

    mock_log.assert_called_once()
    ctx.send.assert_not_called()


@pytest.mark.asyncio
async def test_on_command_error_unwraps_original(fake_context):  #pylint:disable=redefined-outer-name
    '''Uses error.original when present to determine the error type'''
    cog = make_cog(fake_context)
    ctx = MagicMock()
    ctx.command = MagicMock(spec=[])
    ctx.cog = None
    ctx.send = AsyncMock()

    wrapper = MagicMock()
    wrapper.original = commands.CommandNotFound('!wrapped')

    await cog.on_command_error(ctx, wrapper)

    ctx.send.assert_called_once()
    assert 'use !help' in ctx.send.call_args[0][0]
