'''
A server that is nothing but its contract, for testing a client without the pod that serves it.

``ContractStub`` registers every route of a ``{Route: Contract}`` table and answers each from a script the test
sets. It holds the test to the contract in both directions:

  * what the client SENDS must be accepted by the route's request model, or the stub answers 422 and records a
    violation (``serving`` fails the test on exit, so a client that sends a body the real pod would refuse
    cannot pass by never looking at the answer);
  * what the test makes the stub ANSWER must be a status the route declares, with a body that is exactly one
    of that reply's models, so a test cannot make up a reply the pod would never give.

What it cannot show is that the pod does the right thing with a body it accepts: that is the pod's own tests
(the handler is run against the same contract), plus one end-to-end round trip per seam in tests/integration/.
'''
import json
from contextlib import asynccontextmanager

from aiohttp import web
from aiohttp.test_utils import TestClient, TestServer
from pydantic import BaseModel, ValidationError


def _plain(value):
    '''A pydantic model as the JSON it serialises to; anything else as is.'''
    return value.model_dump(mode='json') if isinstance(value, BaseModel) else value


def _is_exactly(model: type[BaseModel], payload) -> bool:
    '''The payload is what ``model`` serialises: nothing missing, nothing extra.'''
    try:
        return model.model_validate(payload).model_dump(mode='json') == payload
    except ValidationError:
        return False


class ContractStub:
    '''Serves the routes of ``contracts`` from per-route reply scripts and records every request.'''

    def __init__(self, contracts):
        self.contracts = dict(contracts)
        self.calls = []
        self.violations = []
        self._scripts = {}

    def script(self, route, body=None, status: int = 200) -> None:
        '''Set the one reply a route plays. It must be a declared status with a declared body.'''
        declared = next((reply for reply in self.contracts[route].replies if reply.status == status), None)
        if declared is None:
            raise AssertionError(f'{route.method} {route.template} declares no {status} reply')
        payload = _plain(body)
        if declared.models:
            if not any(_is_exactly(model, payload) for model in declared.models):
                raise AssertionError(f'{payload!r} is none of {[model.__name__ for model in declared.models]}'
                                     f' for {route.method} {route.template} {status}')
        elif payload is not None:
            raise AssertionError(f'{route.method} {route.template} {status} declares no body, got {payload!r}')
        self._scripts[route] = (status, payload)

    def calls_to(self, route) -> list:
        '''The bodies sent to a route, in order.'''
        return [call['body'] for call in self.calls if call['route'] == route]

    def _handler(self, route):
        contract = self.contracts[route]

        async def handle(request: web.Request) -> web.StreamResponse:
            raw = await request.text()
            try:
                body = json.loads(raw) if raw else None
            except ValueError:
                body = raw
            label = f'{route.method} {route.template}'
            if contract.request is not None:
                try:
                    contract.request.model_validate(body)
                except ValidationError as exc:
                    self.violations.append(f'{label}: sent {body!r}, which {contract.request.__name__} refuses '
                                           f'({exc.error_count()} error(s))')
                    return web.Response(status=422)
            self.calls.append({'route': route, 'body': body})
            if route not in self._scripts:
                self.violations.append(f'{label}: called, but the test scripted no reply')
                return web.Response(status=500)
            status, payload = self._scripts[route]
            return web.Response(status=status) if payload is None else web.json_response(payload, status=status)
        return handle

    def build_app(self) -> web.Application:
        '''The aiohttp application serving every route of the contract table.'''
        app = web.Application()
        for route in self.contracts:
            app.router.add_route(route.method, route.template, self._handler(route))
        return app


@asynccontextmanager
async def serving(stub: ContractStub):
    '''Run the stub on a test server and yield its ``TestClient``; fails if the client broke the contract.'''
    async with TestClient(TestServer(stub.build_app())) as client:
        yield client
    assert not stub.violations, f'the client broke the contract: {stub.violations}'
