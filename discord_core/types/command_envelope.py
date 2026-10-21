'''
The slice of a command invocation that command logic is allowed to see.

Logic takes this instead of a discord.py ``Context`` so it can run without discord.py: the
gateway's cog stub builds it from the ``Context`` (``envelope_from_ctx``), the logic reads ids and
names off it. It is also the wire form: the same model is the ``envelope`` in
``POST /commands/{command}``, so there is one definition for the gateway to fill and the commands
pod to read (#1030).

Immutable, so a command's logic cannot change who invoked it. ``author_role_ids`` is a tuple in
Python and a list in JSON; pydantic converts on the way in, so a body parses straight into it.
'''
from pydantic import BaseModel, ConfigDict, Field


class CommandEnvelope(BaseModel):
    '''Who invoked a command, and from where.'''
    model_config = ConfigDict(frozen=True)

    guild_id: int = Field(description='Guild the command was issued in')
    channel_id: int = Field(description='Text channel the command was issued in; replies go here')
    author_id: int = Field(description='User who issued the command')
    author_display_name: str = Field(description='How the author is shown in the guild')
    channel_name: str | None = Field(default=None, description='Name of the text channel')
    author_voice_channel_id: int | None = Field(
        default=None, description='Voice channel the author is in, if any')
    author_voice_channel_name: str | None = Field(
        default=None, description='Name of the author\'s voice channel')
    author_voice_guild_id: int | None = Field(
        default=None, description='Guild of the author\'s voice channel')
    author_role_ids: tuple[int, ...] = Field(
        default=(), description='Ids of the roles the author holds; the role commands check them')
