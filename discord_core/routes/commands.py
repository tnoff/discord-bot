'''
The commands seam: the one route the commands pod serves and the gateway calls (#1030, Phase 2).

`POST /commands/{command}` carries one parsed command from the gateway to the pod that runs it.
`{command}` is the command's dotted name (`play`, `playlist.item-add`, `markov.list-channels`);
`types/command_args.py` has the list and the argument model each one takes. The body is
`CommandRequestBody` and the answer is a 202 `CommandAcceptedResponse`: the pod has taken the
command, not run it, and replies to the user through the dispatcher.

One route, not one per command, so adding a command changes `COMMAND_ARGS` and not the seam. An
unknown name or arguments that do not fit the command's model are a 422, which is a bug in the
sender and never retried.

This registry is `HttpCommandsClient.ROUTES_CALLED`, so the gateway's peer route check covers it. It
shipped before that, for the deploy-order reason routes/gateway_intents.py records: pods install a
released discord_core, so the symbols went first and the commands pod served them next. The pod
has served the route since #1116.
'''
from discord_core.routes.route import Route, collect

FORWARD_COMMAND = Route('POST', '/commands/{command}')

ALL = collect(globals())
