'''
The guild-queue routes: a guild's player queue, served by the broker pod.

These are broker-seam routes in the sense that matters (the handlers will live in
broker_server.py, the way /sessions* does), and they are declared HERE, not in routes/broker.py,
for a deploy-order reason rather than a design one.

Two things about this repo make adding a route to a served seam a two-step change:
- The tests import `discord_core` from the checkout, and assert that a seam's registry and the
  server that serves it agree exactly (test_broker_seam_contract). A route in routes/broker.py
  with no handler fails them.
- The images install `discord_core` from a pinned release tag. A broker built from the commit
  that adds handlers would import route symbols its pinned core does not have yet.
So the contract lands first, released as a core tag, in a registry nothing asserts against. The
change that bumps the pins then serves these routes and folds this module into routes/broker.py
(its ALL joins the seam's), at which point the agreement tests cover them.

Until then nothing calls these routes: HttpBrokerClient's ROUTES_CALLED, which feeds the peer
route check, deliberately does not include them yet, or every client would report its broker as
missing routes it cannot serve.

Outcomes the caller is expected to handle (queue full, nothing to claim, track already gone)
come back as a 200 with the answer in the body, never as an error status.  They are
deterministic, and async_retry_broker_command retries 5xx, so an error status would only delay
the identical answer.

Everything is keyed by guild, never by request uuid, because a guild has exactly one player and
these routes are about it: its queue, the track it is playing, and what it has played.
'''
from discord_core.routes.route import Route, collect

ENQUEUE_TRACK = Route('POST', '/guilds/{guild_id}/queue')
GET_GUILD_QUEUE = Route('GET', '/guilds/{guild_id}/queue')
REMOVE_QUEUED_TRACK = Route('POST', '/guilds/{guild_id}/queue/remove')
BUMP_QUEUED_TRACK = Route('POST', '/guilds/{guild_id}/queue/bump')
SHUFFLE_QUEUE = Route('POST', '/guilds/{guild_id}/queue/shuffle')
CLEAR_QUEUE = Route('POST', '/guilds/{guild_id}/queue/clear')
# The cheap change check, polled about once a second per active guild. Called with an optional
# `?since=<version>` query string the server reads from the query, not the path, like
# LIST_BUNDLES in routes/broker.py: the template stays bare and the client appends.
POLL_GUILD_QUEUE = Route('GET', '/guilds/{guild_id}/queue/state')
CLAIM_TRACK = Route('POST', '/guilds/{guild_id}/claim')
PLAYING_HEARTBEAT = Route('POST', '/guilds/{guild_id}/playing/heartbeat')
SKIP_TRACK = Route('POST', '/guilds/{guild_id}/skip')
FINISH_TRACK = Route('POST', '/guilds/{guild_id}/finish')
GET_GUILD_HISTORY = Route('GET', '/guilds/{guild_id}/history')
CLOSE_GUILD = Route('POST', '/guilds/{guild_id}/close')
OPEN_GUILD = Route('POST', '/guilds/{guild_id}/open')

ALL = collect(globals())
