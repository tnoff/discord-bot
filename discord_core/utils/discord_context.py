'''
Discord command context, as span attributes.

Split out of `utils/otel.py` on 2026-09-18. The enum is reached by the bot and
the db tier alone; `otel.py` is reached by all six, so every edit to a Discord-specific attribute name rebuilt the dispatcher,
the downloader and the broker — none of which can see a `Context`.

The move only works because the span wrappers stopped building these attributes
themselves. While they did, the enum could not leave: `otel.py` would have had
to import it back, and an all-six module importing a two-image one makes that
module all-six again. That is the trap to remember if another naming enum looks
movable — check whether `otel.py` uses it, not just who else does.
'''
from enum import Enum


class DiscordContextNaming(Enum):
    '''
    Context attribute constants
    '''
    AUTHOR = 'discord.author'
    CHANNEL = 'discord.channel'
    GUILD = 'discord.guild'
    COMMAND = 'discord.context.command'
    MESSAGE = 'discord.context.message'
