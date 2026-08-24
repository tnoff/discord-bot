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

from discord_bot.utils.otel import async_otel_span_wrapper


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
        async with async_otel_span_wrapper(span_name, ctx=ctx, kind=trace.SpanKind.SERVER):
            return await function(*args, **kwargs)
    return _wrapper
