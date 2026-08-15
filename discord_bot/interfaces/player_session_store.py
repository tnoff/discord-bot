'''
Player-session persistence, split out of broker_protocols.

Sessions are not media-broker business — they are player state that happens to
live on the broker pod, because in HA that is the only shared store the bot can
reach (it holds no Redis connection of its own).  Keeping the surface in its own
module says so, and keeps three already-wide interfaces from growing a fourth
concern each.

PlayerSessionStore is the engine side (mixed into MediaBrokerBase, so every
broker impl must provide it); PlayerSessionClient is the cog-facing side (mixed
into the BrokerClient Protocol).
'''
from abc import ABC, abstractmethod
from typing import List, Protocol

from discord_bot.types.player_session import PlayerSession


class PlayerSessionStore(ABC):
    '''Storage surface for player sessions, implemented by every broker engine.'''

    @abstractmethod
    async def save_player_session(self, session: PlayerSession) -> None:
        '''Persist a guild's player session, replacing any existing one.'''

    @abstractmethod
    async def list_player_sessions(self) -> List[PlayerSession]:
        '''Return every stored player session — point-in-time snapshot.'''

    @abstractmethod
    async def delete_player_session(self, guild_id: int) -> None:
        '''Drop a guild's player session.  Safe to call when none exists.'''


class PlayerSessionClient(Protocol):
    '''Cog-facing half of the session surface, satisfied by both broker clients.'''

    async def save_player_session(self, session: PlayerSession) -> None:
        '''Persist a guild's player session so the next startup can resume it.'''

    async def list_player_sessions(self) -> List[PlayerSession]:
        '''Return every stored player session — read once per startup.'''

    async def delete_player_session(self, guild_id: int) -> None:
        '''Drop a guild's player session once it has been consumed.'''
