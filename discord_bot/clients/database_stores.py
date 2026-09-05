'''
The persistence handle a gateway process carries.

Replaces the `db_engine` that used to be threaded from the entrypoint through
load_cogs into every cog constructor. The shape is the same on purpose -- one
object, passed positionally, ignored by the cogs that do not need it -- because
the thing that changed is where the rows live, not how the process gets at them.

Bundled rather than passed as three separate arguments so that adding a fourth
store later is a change to this module and the one call site that builds it,
rather than to every cog signature and every test that constructs a cog. That is
the failure the per-image forbidden tuples and the console-script list both hit:
one fact, restated in N places, correct only while somebody keeps them in step.

There is deliberately no video-cache member. The bot has not owned the video
cache since the broker split -- cogs/music.py says so directly ("No in-process
AsyncioBroker and no VideoCacheClient") -- and giving this bundle a slot the bot
never fills would invite somebody to fill it.
'''
from dataclasses import dataclass

from discord_bot.clients.http_guild_analytics_store import HttpGuildAnalyticsStore
from discord_bot.clients.http_markov_store import HttpMarkovStore
from discord_bot.clients.http_playlist_store import HttpPlaylistStore
from discord_bot.interfaces.database_protocols import (GuildAnalyticsStore, MarkovStore,
                                                       PlaylistStore)


@dataclass(frozen=True)
class DatabaseStores:
    '''The persistence Protocols a gateway process holds, or None for each absent one.

    Annotated against the Protocols rather than the HTTP classes so a caller
    cannot come to depend on the transport. Frozen because the bundle is wiring:
    it is built once at startup and read thereafter, and a cog reassigning a
    member would be changing where another cog's rows come from.
    '''
    playlist: PlaylistStore | None = None
    markov: MarkovStore | None = None
    guild_analytics: GuildAnalyticsStore | None = None

    def __bool__(self) -> bool:
        '''True when any store is present.

        Lets `if self.stores:` stand where `if self.db_engine:` used to, which is
        the shape every cog already reads for "is persistence available". An empty
        bundle and None both read as absent, so a caller cannot accidentally treat
        "configured with nothing" as "configured".
        '''
        return any((self.playlist, self.markov, self.guild_analytics))


def build_http_stores(base_url: str) -> DatabaseStores:
    '''
    Build the three HTTP stores a bot process needs, all against one db pod.

    One session per store rather than one shared across them: HttpStoreBase makes
    its session lazily on first use, so three idle stores cost nothing, and the
    entrypoint closes each on shutdown.

    base_url : Root URL of the db pod, e.g. http://discord-db:8085
    '''
    return DatabaseStores(
        playlist=HttpPlaylistStore(base_url),
        markov=HttpMarkovStore(base_url),
        guild_analytics=HttpGuildAnalyticsStore(base_url),
    )
