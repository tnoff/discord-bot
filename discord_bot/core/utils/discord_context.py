'''
Discord command context, as span attributes.

Split out of `utils/otel.py` on 2026-09-18. The enum and the function that reads
it are reached by the bot and the db tier alone; `otel.py` is reached by all
six, so every edit to a Discord-specific attribute name rebuilt the dispatcher,
the downloader and the broker — none of which can see a `Context`.

The move only works because the span wrappers stopped building these attributes
themselves. While they did, the enum could not leave: `otel.py` would have had
to import it back, and an all-six module importing a two-image one makes that
module all-six again. That is the trap to remember if another naming enum looks
movable — check whether `otel.py` uses it, not just who else does.
'''
from enum import Enum
from typing import TYPE_CHECKING

if TYPE_CHECKING:  # pragma: no cover - typing only
    from discord.ext.commands import Context


class DiscordContextNaming(Enum):
    '''
    Context attribute constants
    '''
    AUTHOR = 'discord.author'
    CHANNEL = 'discord.channel'
    GUILD = 'discord.guild'
    COMMAND = 'discord.context.command'
    MESSAGE = 'discord.context.message'


def command_span_attributes(ctx: 'Context') -> dict:
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
