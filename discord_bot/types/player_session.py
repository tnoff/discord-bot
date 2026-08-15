from pydantic import BaseModel, Field

from discord_bot.types.playlist_add_request import AnyMediaRequest


class PlayerSession(BaseModel):
    '''
    A guild's player state, persisted across a bot restart so playback can resume.

    Written on BOT_SHUTDOWN and consumed once on the next startup.  The queue holds
    the media requests themselves rather than broker entry uuids: replaying them
    through the cog's normal enqueue path means a resume reuses the cache-hit
    machinery every other request already goes through, instead of reconstructing
    MediaDownload objects out of registry rows.  Requests whose media is still
    cached come back instantly; anything evicted re-downloads exactly as a fresh
    request would.

    was_playing distinguishes a player that was mid-track from one that was merely
    parked in a voice channel with an empty queue — only the former is worth
    rejoining for.
    '''
    guild_id: int
    voice_channel_id: int
    text_channel_id: int
    queue: list[AnyMediaRequest] = Field(default_factory=list)
    was_playing: bool = False
