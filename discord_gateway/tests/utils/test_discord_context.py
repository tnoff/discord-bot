'''
command_span_attributes, asserted by value.

These replace two tests in test_otel.py that passed a Context into the span
wrappers and then only checked `span is not None` — so the attribute names they
existed to protect were never actually read.
'''
from unittest.mock import MagicMock

from discord.ext.commands import Context

from discord_core.utils.discord_context import DiscordContextNaming
from discord_gateway.utils.otel_command import command_span_attributes


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


def test_command_span_attributes_reads_every_field_off_the_context():
    '''Each attribute comes from the Context, under its enum name.'''
    assert command_span_attributes(_make_ctx()) == {
        DiscordContextNaming.AUTHOR.value: 1001,
        DiscordContextNaming.CHANNEL.value: 2002,
        DiscordContextNaming.GUILD.value: 3003,
        DiscordContextNaming.COMMAND.value: 'testcmd',
        DiscordContextNaming.MESSAGE.value: 'arg1',
    }


def test_command_span_attributes_drops_the_invocation_from_the_message():
    '''
    The command word itself is stripped; only the arguments are recorded.

    `'!testcmd arg1'` becomes `'arg1'` — the span already carries the command
    name under its own attribute, so repeating it in the message would be the
    same value twice with one of them harder to query.
    '''
    ctx = _make_ctx()
    ctx.message.content = '!play never gonna give you up'
    assert command_span_attributes(ctx)[DiscordContextNaming.MESSAGE.value] == 'never gonna give you up'
