'''A scripted aiohttp server for exercising the HTTP clients end to end.

Where stub_broker.StubBrokerBase answers each route with one fixed reply, a ScriptedServer plays
a sequence: the first call to a route gets the first reply, the next call the next one, and the
final reply repeats. That is what the retry, polling and breaker tests need (a 503 followed by a
200, a 202 pending followed by the result).
'''
import json
from typing import NamedTuple

from aiohttp import web
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter

from discord_core.tests.clients.stub_broker import RoutedStub


class Reply(NamedTuple):
    '''One canned answer: a status, plus a JSON body, a raw text body, or neither.'''
    status: int = 200
    body: object = None
    text: str | None = None


def ok(body=None, status=200) -> Reply:
    '''A JSON reply (an empty one when body is None).'''
    return Reply(status=status, body=body)


def plain(text: str, status: int = 200) -> Reply:
    '''A reply whose body is text/plain rather than JSON.'''
    return Reply(status=status, text=text)


class ScriptedServer(RoutedStub):
    '''Serves the given routes from per-route reply scripts and records every request.'''

    def __init__(self, routes):
        self.routes = tuple(routes)
        self.calls = []
        self._scripts = {}

    def script(self, route, *replies: Reply) -> None:
        '''Set the replies a route plays, in order; the last one repeats.'''
        self._scripts[route] = list(replies)

    def calls_to(self, route) -> list:
        '''Every recorded request to the route.'''
        return [call for call in self.calls if call['route'] == route]

    def _handler(self, route):
        async def handle(request: web.Request) -> web.StreamResponse:
            raw = await request.text()
            self.calls.append({
                'route': route, 'method': request.method, 'path': request.path,
                'query': dict(request.query), 'body': json.loads(raw) if raw else None,
                'traceparent': request.headers.get('traceparent'),
            })
            script = self._scripts[route]
            reply = script.pop(0) if len(script) > 1 else script[0]
            if reply.text is not None:
                return web.Response(status=reply.status, text=reply.text, content_type='text/plain')
            if reply.body is None:
                return web.Response(status=reply.status)
            return web.json_response(reply.body, status=reply.status)
        return handle


def recording_tracer():
    '''A tracer that keeps its finished spans; returns (tracer, exporter).'''
    exporter = InMemorySpanExporter()
    provider = TracerProvider()
    provider.add_span_processor(SimpleSpanProcessor(exporter))
    return provider.get_tracer('stub-http'), exporter
