'''
Client-side views of a gateway intent, as the broker seam returns them.

An intent is a request for a gateway to do something only a gateway can: it owns the voice
client.  The kinds and the `params` each one carries are part of the contract between the pod
that asks and the gateway that acts:

- `join`: `voice_channel_id`, `text_channel_id`.  Create the guild's player if needed and join.
  The only kind whose result is read back.
- `stop`: `text_channel_id`.  Tear the guild's player down at the user's request.
- `move_messages`: `text_channel_id`.  Point the player, and the session a restart would rejoin
  from, at a new text channel.
- `wake`: no params.  A track was queued; claim it now instead of at the next claim poll.

Every kind is idempotent, which is what makes redelivery after a lease expires safe.
'''
from dataclasses import dataclass, field
from enum import Enum


class IntentKind(str, Enum):
    '''What a gateway is asked to do.'''
    JOIN = 'join'
    STOP = 'stop'
    MOVE_MESSAGES = 'move_messages'
    WAKE = 'wake'


@dataclass
class GatewayIntent:
    '''One request, in the order the broker took it.'''
    id: str
    guild_id: int
    kind: IntentKind
    params: dict = field(default_factory=dict)


@dataclass
class IntentResult:
    '''
    Where an intent got to.

    done is False until the gateway completes it, or if the intent expired unclaimed (the caller
    cannot tell the two apart and should not need to: both mean "did not happen in time").
    ok and error are only set once done; error is the text to tell the channel.
    '''
    done: bool
    ok: bool | None = None
    error: str | None = None
