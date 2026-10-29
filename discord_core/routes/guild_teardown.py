'''
The guild-teardown route: how the gateway has a guild's queued music work cleared (#1030).

When a guild's player goes away, whatever it still had queued with the downloader and the search
pod is no use to anyone: the requests should be dropped, new ones refused for a moment, and the
progress messages that track them taken down. That work needs the downloader, search and broker
clients, which the commands pod holds and the gateway is being relieved of, so the gateway asks the
commands pod to do it.

`TEARDOWN_GUILD` answers 202 once the pod has taken the job, not once it is done: it can be a
round trip per queued request, and nothing the gateway does next depends on the outcome. It is
idempotent, so a caller may repeat it.

DEFINED here as its own module and not part of `routes/commands.py`'s ALL yet, for the deploy-order
reason routes/gateway_intents.py records: pods install a released discord_core, so the symbols ship
first, the commands pod serves the route next, and the registry is folded into the commands seam
last. Until then the gateway's peer route check does not demand it of the pod.
'''
from discord_core.routes.route import Route, collect

TEARDOWN_GUILD = Route('POST', '/guilds/{guild_id}/teardown')

ALL = collect(globals())
