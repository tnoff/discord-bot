'''
What the gateway needs to hand a command to the commands pod.

A request that the pod cannot take is `CommandsUnavailable`, and that is the one exception a
caller is expected to catch: the stub answers a fixed "commands are unavailable" message and does
not retry (commands are not idempotent). Anything else is a bug in the sender and propagates.
'''
from typing import Protocol

from discord_core.types.command_envelope import CommandEnvelope
from discord_core.types.command_wire import CommandAcceptedResponse
from discord_core.types.guild_teardown import GuildTeardownAccepted

__all__ = ['CommandsClient', 'CommandsUnavailable']


class CommandsUnavailable(Exception):
    '''The commands pod did not take the command: down, too slow, or its breaker is open.'''


class CommandsClient(Protocol):
    '''Handle for forwarding a parsed command to the commands pod.'''
    async def forward(self, command: str, envelope: CommandEnvelope, args: dict | None = None,
                      request_id: str | None = None,
                      span_context: dict | None = None) -> CommandAcceptedResponse:
        '''
        Send one command and return once the pod has accepted it. `command` is a key of
        COMMAND_ARGS and `args` must fit its model. `request_id` defaults to a fresh uuid.
        Raises CommandsUnavailable; never retries.
        '''

    async def teardown_guild(self, guild_id: int) -> GuildTeardownAccepted:
        '''
        Ask the pod to clear the guild's queued downloads and searches, refuse new ones for a moment
        and take down the progress messages that track them. Returns once the pod has taken the
        job. Idempotent, so unlike `forward` a caller may repeat it. Raises CommandsUnavailable.
        '''
