'''
The one piece of OTel wrapping that genuinely needs discord.py.

``command_wrapper`` decorates cog commands, and it finds the invoking Context by
``isinstance(arg, Context)`` — a runtime check, so unlike the ``ctx:`` annotations
on the span wrappers it cannot be moved under ``TYPE_CHECKING``. Keeping it in
``utils/otel.py`` pulled discord.py into *every* image, because every entrypoint
imports that module for spans and metrics.

Only the gateway process registers cogs, so only the gateway process needs this.
Splitting it out lets the broker, downloader and search images drop discord.py
entirely; the dispatcher keeps it on its own merits, since it sends and edits
real messages (see workers/message_dispatcher).

Same move as CheckoutResult, ClearGuildResult, the BrokerClient Protocol and the
DownloadClient Protocol before it: when a light consumer needs one name from a
heavy module, the name moves rather than the dependency spreading.
'''
import functools

from opentelemetry import trace

from discord.ext.commands import Context

from discord_core.utils.discord_context import DiscordContextNaming
from discord_core.utils.otel import async_otel_span_wrapper


def command_span_attributes(ctx: Context) -> dict:
    '''
    Span attributes for a discord.py command Context.

    This block was duplicated verbatim inside `otel_span_wrapper` and
    `async_otel_span_wrapper`. One copy now, at the single call site that has a
    Context to read.
    '''
    return {
        DiscordContextNaming.AUTHOR.value: ctx.author.id,
        DiscordContextNaming.CHANNEL.value: ctx.channel.id,
        DiscordContextNaming.GUILD.value: ctx.guild.id,
        DiscordContextNaming.COMMAND.value: ctx.command.name,
        DiscordContextNaming.MESSAGE.value: ' '.join(i for i in ctx.message.content.split(' ')[1:]),
    }


def command_wrapper(function):
    '''
    Wrap a discord command function
    '''
    @functools.wraps(function)
    async def _wrapper(*args, **kwargs):
        ctx = None
        for arg in args:
            if isinstance(arg, Context):
                ctx = arg
                break
        span_name = 'unamed_command_wrapper'
        if ctx:
            span_name = f'{ctx.command.cog.qualified_name.lower()}.{ctx.command.name}'
        attributes = command_span_attributes(ctx) if ctx else None
        async with async_otel_span_wrapper(span_name, attributes=attributes, kind=trace.SpanKind.SERVER):
            return await function(*args, **kwargs)
    return _wrapper
