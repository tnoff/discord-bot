from pydantic import BaseModel


class PlayerSession(BaseModel):
    '''
    A guild's player state, persisted across a bot restart so playback can resume.

    Written on BOT_SHUTDOWN and consumed once on the next startup.  It holds only
    where the player was: the voice and text channels, and whether it was mid-track.
    The queue itself lives with the broker (the guild queue), which keeps it, and the
    track that was playing, across a gateway restart; the resume re-opens the guild
    and plays on from there.

    Sessions saved before the queue moved to the broker carry a `queue` key.  It is
    ignored on load rather than rejected, so those still resume.

    was_playing distinguishes a player that was mid-track from one that was merely
    parked in a voice channel with an empty queue — only the former is worth
    rejoining for.
    '''
    guild_id: int
    voice_channel_id: int
    text_channel_id: int
    was_playing: bool = False
