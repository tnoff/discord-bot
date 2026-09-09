'''
The broker seam is a checkable contract: what the client calls and what the
server registers come from one definition, and drift is a test failure here
rather than a 404 in production.

Guards the mechanism behind both incidents in
docs/findings/2026-07-31-discord-search-seam-deploy-skew.md — the route string
written twice, once in ``build_app()`` and once as an inline f-string in the
client, with no shared symbol between them. See
docs/projects/http-seam-contract.md.

These tests are about the ROUTE SET only. A route that exists on both sides but
has gained a required field is a different failure class and is deliberately
not covered — see that spec's Open questions.
'''
import ast
import inspect
from pathlib import Path

import pytest
from aiohttp import web

from discord_bot.clients import http_broker_client, http_player_session
from discord_bot.routes import broker as broker_routes
from discord_bot.routes.route import Route, collect
from discord_bot.servers.broker_server import BrokerHttpServer
from discord_bot.workers.asyncio_broker import AsyncioBroker


def _server() -> BrokerHttpServer:
    return BrokerHttpServer(AsyncioBroker())


def _served(app: web.Application) -> set[tuple[str, str]]:
    '''(method, template) for every route the app actually serves.

    Read from the BUILT ROUTER, not from a constant — `resource.canonical` is
    the template aiohttp will match against, so this stays honest for servers
    whose route set depends on runtime construction. It is the same read the
    runtime advertisement will do.
    '''
    # HEAD is excluded: aiohttp's add_get registers it alongside every GET as an
    # affordance, and the registry declares GET. It is not a seam route, and
    # asserting on it would make the registry restate an aiohttp implementation
    # detail.
    return {(route.method, route.resource.canonical) for route in app.router.routes()
            if route.method != 'HEAD'}


def _declared() -> set[tuple[str, str]]:
    return {(route.method, route.template) for route in broker_routes.ALL}


def test_router_and_registry_agree_exactly():
    '''Every route the broker serves is in the registry, and vice versa.

    Both directions matter and they fail differently. A served route missing
    from the registry is a route no client can name; a registry route the
    server never registers is a client call that will 404.
    '''
    served = _served(_server().build_app())
    declared = _declared()
    assert served - declared == set(), 'served but not in the registry'
    assert declared - served == set(), 'in the registry but not served'


def test_every_registry_route_has_a_handler():
    '''The registry and the handler map are the same set.

    build_app() iterates route_handlers(), so a registry entry with no handler
    is silently never registered — the failure this catches would otherwise
    only appear as a 404 at runtime.
    '''
    assert set(_server().route_handlers()) == set(broker_routes.ALL)


def test_a_route_dropped_server_side_is_caught():
    '''The incident, as a fixture: a peer that predates a route now fails a test.

    Rebuilds the real app minus GET /search-results/next — the exact route that
    404'd for ~20 seconds on 2026-07-31 and then ~9.5 hours on 2026-08-03 — and
    asserts the contract check notices. Without this, the assertions above could
    pass vacuously against a comparison that never fails.
    '''
    live = _server().build_app()
    app = web.Application(middlewares=live.middlewares)
    for route in live.router.routes():
        if route.resource.canonical == broker_routes.NEXT_SEARCH_RESULT.template:
            continue
        app.router.add_route(route.method, route.resource.canonical, route.handler)

    missing = _declared() - _served(app)
    assert missing == {(broker_routes.NEXT_SEARCH_RESULT.method,
                        broker_routes.NEXT_SEARCH_RESULT.template)}


@pytest.mark.parametrize('module', [http_broker_client, http_player_session])
def test_clients_build_no_urls_themselves(module):
    '''No client on this seam interpolates `_base_url` into a path.

    This is the defect itself, not a style rule: an f-string path is a second
    definition of a route the server already declares, and the two drifted
    apart twice. Clients name a registry symbol and let `_route_url` build the
    URL; the only permitted mention of `_base_url` is the assignment in
    `__init__`.
    '''
    tree = ast.parse(Path(inspect.getfile(module)).read_text(encoding='utf-8'))
    offenders = [
        node.lineno
        for node in ast.walk(tree)
        if isinstance(node, ast.JoinedStr)
        for inner in ast.walk(node)
        if isinstance(inner, ast.Attribute) and inner.attr == '_base_url'
    ]
    assert offenders == [], (
        f'{module.__name__} builds a route path from _base_url at lines {offenders}; '
        'use a routes/broker.py symbol via _route_url or _call_route')


def test_get_routes_still_answer_head():
    '''Registering from the registry must not narrow what the server answers.

    `add_route` does not register HEAD; `add_get` does. Converting the broker to
    the registry silently dropped HEAD on all five GET routes until this caught
    it — a behaviour change hiding inside a refactor that claimed to move only
    where routes are defined.
    '''
    served = {(route.method, route.resource.canonical)
              for route in _server().build_app().router.routes()}
    for route in broker_routes.ALL:
        if route.method == 'GET':
            assert ('HEAD', route.template) in served


def test_route_path_fills_its_placeholders():
    assert broker_routes.CHECKOUT.path(uuid='abc') == '/requests/abc/checkout'
    assert broker_routes.NEXT_RESULT.path() == '/results/next'


def test_route_path_rejects_a_missing_param():
    '''An unfilled placeholder raises rather than requesting a literal path.'''
    with pytest.raises(KeyError):
        broker_routes.CHECKOUT.path()


def test_collect_finds_only_route_instances():
    '''ALL is derived, so a route that exists is a route these tests see.'''
    namespace = {'A': Route('GET', '/a'), 'NOT_A_ROUTE': 'GET /b', 'Route': Route}
    assert collect(namespace) == (namespace['A'],)
    assert len(broker_routes.ALL) == 22
