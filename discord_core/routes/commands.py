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

DEFINED here as its own module, and not yet in any registry or client's `ROUTES_CALLED`, for the
deploy-order reason routes/gateway_intents.py records: pods install a released discord_core, so the
symbols ship first, the commands pod serves them next, and the change after that folds this
registry into the seam check.
'''
from discord_core.routes.route import Route, collect

FORWARD_COMMAND = Route('POST', '/commands/{command}')

ALL = collect(globals())
