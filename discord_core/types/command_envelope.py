'''
The slice of a command invocation that command logic is allowed to see.

Logic takes this instead of a discord.py ``Context`` so it can run without discord.py: the
gateway's cog stub builds it from the ``Context`` (``envelope_from_ctx``), the logic reads ids and
names off it. Plain data, so it crosses the gateway-to-commands-pod boundary unchanged (#1030).

``CommandEnvelopeBody`` is its wire form. The two are separate because the dataclass is immutable
and holds ``author_role_ids`` as a tuple, which JSON cannot express.
'''
# pylint: disable=duplicate-code
# (discord_gateway/command_logic/envelope.py is the same dataclass until the gateway imports this one.)
from dataclasses import dataclass

from pydantic import BaseModel


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

    def to_body(self) -> 'CommandEnvelopeBody':
        '''The wire form of this envelope.'''
        return CommandEnvelopeBody(**{**self.__dict__, 'author_role_ids': list(self.author_role_ids)})


class CommandEnvelopeBody(BaseModel):
    '''A CommandEnvelope as it travels in POST /commands/{command}.'''
    guild_id: int
    channel_id: int
    author_id: int
    author_display_name: str
    channel_name: str | None = None
    author_voice_channel_id: int | None = None
    author_voice_channel_name: str | None = None
    author_voice_guild_id: int | None = None
    author_role_ids: list[int] = []

    def to_envelope(self) -> CommandEnvelope:
        '''The envelope command logic takes.'''
        return CommandEnvelope(**{**self.model_dump(), 'author_role_ids': tuple(self.author_role_ids)})
