'''
The slice of a command invocation that command logic is allowed to see.

Logic under ``command_logic`` takes this instead of a discord.py ``Context`` so it
can run without discord.py: the cog stub builds the envelope (``envelope_from_ctx``
in ``cogs/common.py``), the logic reads ids and names off it. Plain data, so it can
later cross a process boundary unchanged (#1030).
'''
from dataclasses import dataclass


@dataclass(frozen=True)
class CommandEnvelope:
    '''Who invoked a command, and from where.'''
    guild_id: int
    channel_id: int
    author_id: int
    author_display_name: str
    channel_name: str | None = None
    author_voice_channel_id: int | None = None
    author_voice_channel_name: str | None = None
    author_voice_guild_id: int | None = None
    # Ids of the roles the author holds; the role commands check permissions against them.
    author_role_ids: tuple[int, ...] = ()
