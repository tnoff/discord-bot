'''
The gateway-intent routes: what other pods ask a gateway to do, served by the broker pod.

A gateway holds the voice connection, so only a gateway can join a channel, stop a player, move
its status messages or wake an idle one.  Another pod (the commands pod, #1030 Phase 2) cannot call a
gateway: the pod graph points at the broker and nowhere else, and a gateway has no inbound API.
So the asking pod writes an INTENT here and the gateway claims it with a short poll, the same
way it already reads the skip marker (routes/guild_queue.py).

Four routes, in the order a request lives: submit, claim, complete, read the result.

- `SUBMIT_INTENT` is keyed by guild, like the rest of a guild's player state.
- `CLAIM_INTENTS` is keyed by the claiming gateway and answers 204 when there is nothing, polled
  several times a second; a claim leases the intent to that gateway so a slow one (a join) is
  not handed out again to the next poll.
- `COMPLETE_INTENT` and `GET_INTENT_RESULT` are keyed by the intent, because that is all either
  caller holds.  Only `join` is read back; the rest are fire and forget.

Outcomes a caller is expected to handle (a result that is not ready yet, an intent that expired)
come back as a 200 with the answer in the body, never as an error status, for the reason given in
routes/guild_queue.py.

DEFINED here as their own module and part of the broker seam: routes/broker.py's ALL includes this
registry, so HttpBrokerClient.ROUTES_CALLED does and the peer route check covers them.  They shipped
as a separate module first, for the deploy-order reason routes/guild_queue.py records: the broker
image pins a released discord_core, so the symbols ship before the change that serves them.
'''
from discord_core.routes.route import Route, collect

SUBMIT_INTENT = Route('POST', '/guilds/{guild_id}/intents')
CLAIM_INTENTS = Route('POST', '/gateways/{gateway_id}/intents/claim')
COMPLETE_INTENT = Route('POST', '/intents/{intent_id}/result')
GET_INTENT_RESULT = Route('GET', '/intents/{intent_id}/result')

ALL = collect(globals())
