'''A stub broker for the tests of the HTTP broker client mixins.'''
import contextlib

import aiohttp
from aiohttp import web
from aiohttp.test_utils import TestServer

from discord_core.clients.http_broker_client import HttpBrokerClient


class RoutedStub:
    '''An aiohttp app with one handler per route; a subclass says which routes and how each one answers.'''
    routes: tuple = ()

    def _handler(self, route):
        raise NotImplementedError

    def build_app(self) -> web.Application:
        '''An app with this stub's handler on every route.'''
        app = web.Application()
        for route in self.routes:
            app.router.add_route(route.method, route.template, self._handler(route))
        return app


@contextlib.asynccontextmanager
async def serve(stub: RoutedStub):
    '''Run the stub's app on a local socket; yields (base_url, client session).'''
    async with TestServer(stub.build_app()) as server:
        async with aiohttp.ClientSession() as session:
            yield str(server.make_url('')), session


class StubBrokerBase(RoutedStub):
    '''A broker that serves the routes in ``routes`` and remembers every call.

    Each route gets a handler that records what the client sent and answers with whatever the test
    set through ``answer``. A subclass sets ``routes`` to the route group under test.
    '''
    def __init__(self):
        self.calls = []
        self.answers = {}

    def answer(self, route, body=None, status=200):
        '''Set what a route answers. body None sends an empty response.'''
        self.answers[route] = (status, body)

    def _handler(self, route):
        async def handle(request: web.Request) -> web.StreamResponse:
            body = await request.json() if request.can_read_body else None
            self.calls.append({'route': route, 'match': dict(request.match_info),
                               'query': dict(request.query), 'body': body,
                               'traceparent': request.headers.get('traceparent')})
            status, answer = self.answers[route]
            if answer is None:
                return web.Response(status=status)
            return web.json_response(answer, status=status)
        return handle

    def last(self, route):
        '''The most recent call to the route.'''
        return [call for call in self.calls if call['route'] == route][-1]


@contextlib.asynccontextmanager
async def connected(stub: StubBrokerBase, **client_kwargs):
    '''An HttpBrokerClient talking to the stub over a real socket.'''
    async with serve(stub) as (url, session):
        yield HttpBrokerClient(url, session=session, **client_kwargs)
