'''
The broker seam: every route `BrokerHttpServer` serves and the bot, downloader
and search pods call.

Imported by `servers/broker_server.py`, `clients/http_broker_client.py` and
`clients/http_player_session.py` — fanout 4 (bot, broker, downloader, search),
measured, not estimated.

Two incidents came out of these route strings being written twice, once in
`build_app()` and once as an inline f-string in the client, with no shared
symbol between them: the 2026-07-31 rolling-update skew and its 2026-08-03
steady-state sequel. See docs/findings/2026-07-31-discord-search-seam-deploy-skew.md.
The names below are that shared symbol.

`/sessions*` belongs here rather than with the database stores. It reads like a
persistence route, but `clients/http_player_session.py` is a MIXIN into
`HttpBrokerClient` and the handlers live in `broker_server.py` — it is the
broker seam, and filing it by name rather than by peer is a mistake that has
already been made once while scoping this work.
'''
from discord_bot.routes.route import Route, collect

# Request lifecycle.
REGISTER_REQUEST = Route('POST', '/requests/{uuid}')
UPDATE_STATUS = Route('PUT', '/requests/{uuid}/status')
CHECKOUT = Route('POST', '/requests/{uuid}/checkout')
RELEASE = Route('POST', '/requests/{uuid}/release')
REMOVE = Route('POST', '/requests/{uuid}/remove')
DISCARD = Route('POST', '/requests/{uuid}/discard')

# Download results.
REGISTER_DOWNLOAD = Route('POST', '/downloads')
REGISTER_DOWNLOAD_DIRECT = Route('POST', '/downloads/register')
NEXT_RESULT = Route('GET', '/results/next')

# Search results. The two routes MR2 added, and the ones both incidents were
# about — NEXT_SEARCH_RESULT is what 404'd for ~20 seconds, then ~9.5 hours.
REGISTER_SEARCH_RESULT = Route('POST', '/search-results')
NEXT_SEARCH_RESULT = Route('GET', '/search-results/next')

# Prefetch and the video cache.
PREFETCH = Route('POST', '/prefetch')
CHECK_CACHE = Route('POST', '/cache/check')
CACHE_CLEANUP = Route('POST', '/cache/cleanup')
CACHE_COUNT = Route('GET', '/cache/count')

# Bundles. LIST_BUNDLES is called with a `?guild_id=` query string the server
# reads from the query, not the path — the template stays bare, and the client
# appends. A query string is not part of the route.
LIST_BUNDLES = Route('GET', '/bundles')
CREATE_BUNDLE = Route('POST', '/bundles')
FINALIZE_BUNDLE = Route('POST', '/bundles/{uuid}/finalize')
DELETE_BUNDLE = Route('DELETE', '/bundles/{uuid}')

# Player sessions, served by the broker. See the module docstring.
LIST_SESSIONS = Route('GET', '/sessions')
SAVE_SESSION = Route('PUT', '/sessions/{guild_id}')
DELETE_SESSION = Route('DELETE', '/sessions/{guild_id}')

ALL = collect(globals())
