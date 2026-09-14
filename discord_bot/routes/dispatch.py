'''
The dispatch seam: the routes the dispatcher pod serves and the bot and broker
call.

Imported by `servers/dispatch_server.py` and `clients/http_dispatch_client.py` —
fanout 3 (bot, broker, dispatcher).

`GET /dispatch/results/{request_id}` is the odd one and worth knowing about: the
fetch routes are fire-and-forget submits that return a request_id, and the caller
polls this route for the outcome. So a client that can submit but cannot poll is
functional right up to the point it needs an answer — which is exactly the shape
the subset check exists to catch before a user does.
'''
from discord_bot.routes.route import Route, collect

ROUTE_PREFIX = '/dispatch'

SEND = Route('POST', f'{ROUTE_PREFIX}/send')
DELETE = Route('POST', f'{ROUTE_PREFIX}/delete')
UPDATE_MUTABLE = Route('POST', f'{ROUTE_PREFIX}/update_mutable')
REMOVE_MUTABLE = Route('POST', f'{ROUTE_PREFIX}/remove_mutable')
UPDATE_MUTABLE_CHANNEL = Route('POST', f'{ROUTE_PREFIX}/update_mutable_channel')
FETCH_HISTORY = Route('POST', f'{ROUTE_PREFIX}/fetch_history')
FETCH_EMOJIS = Route('POST', f'{ROUTE_PREFIX}/fetch_emojis')
GET_RESULT = Route('GET', f'{ROUTE_PREFIX}/results/{{request_id}}')

ALL = collect(globals())
