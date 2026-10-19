'''
What the music commands need from the player, as plain data and one Protocol.

The player (voice client, playback loop, prefetch cache) must share a process with
discord.py. Command logic must not, so it reaches the player only through
``PlayerGateway`` and sees only these dataclasses, never a ``MusicPlayer`` or a discord
object. The cog implements it in-process today; the commands pod (#1030, Phase 2)
would put an HTTP client behind the same Protocol.
'''
from dataclasses import dataclass
from typing import Protocol

from discord_core.types.media_download import MediaDownload


@dataclass(frozen=True)
class PlayerStatus:
    '''A read-only snapshot of one guild's player.'''
    exists: bool
    voice_connected: bool = False
    is_playing: bool = False
    # Downloads queued but not yet playable; a player with these is "busy" even when silent.
    pending_downloads: int = 0
    # The channel the player posts to, which is where its own status messages go.
    text_channel_id: int | None = None


@dataclass(frozen=True)
class EnsureResult:
    '''
    Outcome of ``ensure``. ``error`` is the text to tell the channel, or None when there is
    nothing to add: either it worked, or the player already told the channel why it could not.
    '''
    ok: bool
    error: str | None = None


class PlayerGateway(Protocol):
    '''The calls command logic may make on a guild's player.'''

    async def status(self, guild_id: int) -> PlayerStatus:
        '''Snapshot the player. Read-only: never creates one.'''

    async def has_live_player(self, guild_id: int) -> bool:
        '''Whether the guild has a player that is not shutting down. Cheaper than ``status``.'''

    async def ensure(self, guild_id: int, voice_channel_id: int, text_channel_id: int) -> EnsureResult:
        '''Create the player if needed and join the voice channel.'''

    async def stop(self, guild_id: int, text_channel_id: int) -> None:
        '''Tear the player down at the user's request.'''

    async def move_messages(self, guild_id: int, text_channel_id: int) -> None:
        '''Point the player, and the session a restart would rejoin from, at a new text channel.'''

    async def add_cached(self, guild_id: int, media_download: MediaDownload) -> bool:
        '''
        Queue a track that is already downloaded (a cache hit) on the guild's player. False when
        the guild has no player, or the broker refused the track.
        '''
