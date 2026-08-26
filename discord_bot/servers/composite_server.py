'''
One aiohttp listener fronting several servers' route families.

The search pod co-hosts providers -- ytmusic search on /search/ytmusic, media
search on /search/spotify and /search/youtube -- and they share one bind. That
was the point of namespacing the routes per provider instead of letting the first
one own a bare /search: one image, one pin, one netpol tier, and every extra pod
is another pin a revert-then-auto-bump can strand out of step with the bot, which
is what broke this seam twice.

**The trap this class exists to close.** AiohttpServerBase.serve() is what sets a
server's _serving flag, and a child whose routes are merged here never runs its
own serve(). Left alone, every child reports is_serving False forever -- and
QueueWorkerHttpServer builds its heartbeat gauge on exactly that, so
`youtube_music_search_server` would read a flat 0 while the listener was up and
healthy. That series is on the Discord Health dashboard and in the
DiscordHeartbeat alert, so it would not have been a cosmetic regression. The
composite owns the site, so it tells each child when the site is up or down; the
children genuinely share one listener, which makes mirroring accurate rather than
a fudge.

Only routes are taken from the children, not middlewares: draining is a property
of the listener, so the composite's own drain middleware covers every route, and
start_draining() propagates so children report themselves down too.
'''
import logging

from aiohttp import web

from discord_bot.servers.base import AiohttpServerBase

logger = logging.getLogger(__name__)


class CompositeHttpServer(AiohttpServerBase):
    '''Serve several AiohttpServerBase route families from a single TCP site.'''

    def __init__(self, servers: list[AiohttpServerBase], host: str = '0.0.0.0',  # nosec B104
                 port: int = 8084):
        '''
        servers : Servers whose routes to merge; their own serve() is never called
        host : Bind address
        port : Bind port
        '''
        super().__init__()
        if not servers:
            raise ValueError('CompositeHttpServer needs at least one server')
        self._servers = list(servers)
        self._host = host
        self._port = port

    def set_serving(self, value: bool) -> None:
        '''Record the site state here and on every child fronted by it.'''
        super().set_serving(value)
        for server in self._servers:
            server.set_serving(value)

    def start_draining(self) -> None:
        '''Refuse new requests, and let each child report itself down too.'''
        super().start_draining()
        for server in self._servers:
            server.start_draining()

    def build_app(self) -> web.Application:
        '''
        Merge every child's routes into one Application.

        Routes are re-registered by (method, path, handler) rather than by mounting
        sub-applications: the children already declare absolute paths under
        /search, so a subapp mount would prefix them a second time and quietly
        change the cross-pod contract.
        '''
        app = web.Application(middlewares=[self._get_drain_middleware()])
        for server in self._servers:
            for route in server.build_app().router.routes():
                app.router.add_route(route.method, route.resource.canonical, route.handler)
        return app
