'''
The guild-queue routes: a guild's player queue, served by the broker pod.

They are part of the broker seam: routes/broker.py includes this module's ALL in its own, so the
seam registry, the client's ROUTES_CALLED and the agreement tests all cover them.  They are
DEFINED here, as their own module, for a deploy-order reason that still holds: the broker image
pins a released discord_core, and this module is where that release put the symbols the broker's
handlers import.  Moving the definitions into routes/broker.py would break a broker built against
the release that predates the move.

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
