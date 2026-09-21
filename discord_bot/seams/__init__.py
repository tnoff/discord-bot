'''
One folder per pod-to-pod contract — the single place a seam's route strings,
wire types and client-side protocols are written.

**This file is deliberately empty, and that is load-bearing.** The package itself
is fanout 6 (see docs/image-dependencies.md) — unavoidable, since every image
imports some seam. Empty, that costs nothing: a file that never changes has no
rebuild cost however many images import it.

Importing the seam registries here is what would make it expensive.
`discord_bot.seams` would then churn on every route addition at fanout 6 — the
exact shape `utils/otel.py` had before the per-image-code-split took it apart.
It would also drag every seam into every image, so `seams/broker/` would stop
being fanout 4 and become 6 along with it.

Each seam is therefore imported directly by the pods on it and by nothing else,
which keeps the registries at broker 4, database 3, dispatch 3, queue_worker 3
and media_search 2 — the same numbers they had as `discord_bot/routes/*`, and
the reason that package carried this note before the folders existed.
See docs/projects/http-seam-contract.md, acceptance criterion one.
'''
