'''
Per-seam HTTP route registries — the single place a pod-to-pod route string
is written.

**This file is deliberately empty, and that is load-bearing.** The package
itself is already fanout 6 (see docs/image-dependencies.md) — unavoidable, since
every image imports something under it. Empty, that costs nothing: a file that
never changes has no rebuild cost however many images import it.

Importing the seam modules here is what would make it expensive. `discord_bot.routes`
would then churn on every route addition at fanout 6 — the exact shape of
`utils/otel.py`, today the single largest contributor to rebuild cost. It would
also drag every seam's registry into every image, so `routes/broker.py` would
stop being fanout 4 and become 6 along with it.

Each seam registry is therefore imported directly by the pods on that seam and
by nothing else, keeping the fanouts at broker 4, database 3, dispatch 3,
queue_worker 3, media_search 2. See docs/projects/http-seam-contract.md,
acceptance criterion one.
'''
