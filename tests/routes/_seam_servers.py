'''
The six served surfaces of the five seams, and how to build each one.

Extracted so the registry/router drift test and the OpenAPI generator read ONE
table. They ask the same question -- what does this server actually serve -- and
two copies of the answer is the shape this whole project exists to remove.

**Six entries for five seams.** `queue_worker` is a route shape rather than a
pod: it is subclassed at `/downloads` on the downloader and `/search/ytmusic` on
the search pod, with disjoint route sets. A table that walked the seam once would
leave whichever pod it skipped entirely unchecked -- the gap the project spec's
own route inventory had when it counted that class once and reported 4 routes
instead of 8.

The factories take `object()` stubs for their dependencies on purpose. Nothing
here calls a handler; the question is only which routes get registered, and a
server that needed a real store to answer that would be a server whose route set
depends on runtime state.
'''
from discord_bot.routes import broker as broker_routes
from discord_bot.routes import contract
from discord_bot.routes import database as database_routes
from discord_bot.routes import dispatch as dispatch_routes
from discord_bot.routes import media_search as media_search_routes
from discord_bot.routes import queue_worker as queue_worker_routes
from discord_bot.servers.broker_server import BrokerHttpServer
from discord_bot.servers.database_server import DatabaseHttpServer
from discord_bot.servers.dispatch_server import DispatchHttpServer
from discord_bot.servers.download_server import DownloadHttpServer
from discord_bot.servers.media_search_server import MediaSearchHttpServer
from discord_bot.servers.youtube_music_search_server import YoutubeMusicSearchHttpServer
from discord_bot.workers.asyncio_broker import AsyncioBroker

CONTRACT_ENTRY = (contract.CONTRACT_ROUTE.method, contract.CONTRACT_ROUTE.template)


def fully_configured_database():
    '''A db pod with every store, so its route set is the whole registry.

    The groups are conditional, so this is the only configuration whose served set
    equals ALL -- which is why the advertisement is a runtime read and any caller
    of this table has to say which pod shape it is asserting about.
    '''
    return DatabaseHttpServer(guild_analytics_store=object(), markov_store=object(),
                              playlist_store=object(), video_cache_store=object())


# (surface name, server factory, the routes that server should serve, serving image)
SEAMS = [
    ('broker', lambda: BrokerHttpServer(AsyncioBroker()), broker_routes.ALL,
     'discord-broker'),
    ('database', fully_configured_database, database_routes.ALL, 'discord-db'),
    ('dispatch', lambda: DispatchHttpServer(object(), object()), dispatch_routes.ALL,
     'discord-dispatcher'),
    ('media_search', lambda: MediaSearchHttpServer(object()), media_search_routes.ALL,
     'discord-search'),
    ('queue_worker/downloads', lambda: DownloadHttpServer(object()),
     queue_worker_routes.DOWNLOADS.all, 'discord-downloader'),
    ('queue_worker/ytmusic', lambda: YoutubeMusicSearchHttpServer(object()),
     queue_worker_routes.YTMUSIC.all, 'discord-search'),
]


def served(app) -> set:
    '''Seam routes `app` serves: the router's real contents, minus the two things
    that are not seam routes -- HEAD (an add_get affordance) and the contract
    endpoint (served by every application server, owned by no seam).'''
    entries = {entry for entry in contract.served_routes(app) if entry[0] != 'HEAD'}
    return entries - {CONTRACT_ENTRY}
