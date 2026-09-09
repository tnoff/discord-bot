'''
Per-seam HTTP route registries — the single place a pod-to-pod route string
is written.

**This file is deliberately empty, and that is load-bearing.** Importing the
seam modules here would make `discord_bot.routes` a fanout-6 module that churns
on every route addition — the exact shape of `utils/otel.py`, today the single
largest contributor to rebuild cost. Each seam registry is imported directly by
the pods on that seam and by nothing else, which keeps the fanouts at broker 4,
database 3, dispatch 3, queue_worker 3, media_search 2. See
docs/projects/http-seam-contract.md, acceptance criterion one.
'''
