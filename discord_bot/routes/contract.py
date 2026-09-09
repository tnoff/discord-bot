'''
The route-contract endpoint: how a pod tells a caller which routes it serves.

Imported by `servers/base.py` (so every server can advertise) and by
`clients/seam_contract.py` (so every client can ask). That is fanout 6, and it
is the same deliberate exception as `route.py`: a cross-cutting mechanism with
no seam knowledge in it, so it does not churn when routes are added.

**Why a runtime advertisement rather than a version.** Pods update at different
rates — over the last 80 commits touching `discord_bot/`, the mean commit
affects 3.08 of 6 images and 37.5% affect exactly one — so pods being on
different builds is the steady state. A SHA or a version integer would differ
almost always and the alert would be muted within a week. A route set is
invariant to how far apart two pods have drifted: it stays quiet until a route
a client actually calls is genuinely absent. See
docs/projects/http-seam-contract.md.
'''
from aiohttp import web

from discord_bot.routes.route import Route

# The endpoint itself. Leading underscore keeps it clear of every seam prefix
# (/requests, /downloads, /database/..., /search/...), and it is served from the
# APPLICATION server rather than the health server on purpose: health servers
# have their own readiness semantics, so a contract endpoint behind readiness is
# unreachable exactly when a client most wants to ask.
CONTRACT_ROUTE = Route('GET', '/_contract/routes')

ROUTES_KEY = 'routes'
METHOD_KEY = 'method'
TEMPLATE_KEY = 'template'


def served_routes(app: web.Application) -> set[tuple[str, str]]:
    '''(method, template) for every route `app` actually serves.

    Read from the BUILT ROUTER, never from a constant. `database_server`
    registers its four store groups conditionally on which stores were
    constructed, so its real route set is not a static property of the source —
    a constant would describe a pod that was never deployed.
    '''
    return {(route.method, route.resource.canonical) for route in app.router.routes()}


def encode(routes: set[tuple[str, str]]) -> dict:
    '''Render a route set as the advertisement body, ordered for stable diffing.'''
    return {ROUTES_KEY: [{METHOD_KEY: method, TEMPLATE_KEY: template}
                         for method, template in sorted(routes)]}


def decode(payload: dict) -> set[tuple[str, str]]:
    '''Parse an advertisement body back into a route set.

    Tolerates a malformed or empty body by returning an empty set rather than
    raising: the caller treats "could not read the peer's routes" as unknown,
    not as a mismatch, and an exception here would turn a garbled response into
    a client-side crash on a path that must never stop a pod from starting.
    '''
    entries = payload.get(ROUTES_KEY) if isinstance(payload, dict) else None
    if not isinstance(entries, list):
        return set()
    return {(entry[METHOD_KEY], entry[TEMPLATE_KEY]) for entry in entries
            if isinstance(entry, dict) and METHOD_KEY in entry and TEMPLATE_KEY in entry}
