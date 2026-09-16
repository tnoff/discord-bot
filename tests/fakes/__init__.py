'''
The single-process engines, retired from production by the HA rollout.

These are the in-memory implementations the tiers used before each became its
own pod: AsyncioBroker behind BrokerHttpServer, AsyncioDownloadWorker and
AsyncioYoutubeMusicSearchWorker behind their queue-worker servers.  Production
runs the Redis-backed siblings (workers/redis_broker.py and friends); nothing
under discord_bot/ imports anything here.

They live under tests/ because that is what they are.  The repo has said so for
a while -- tests/cli/test_import_boundaries.py called them "test doubles, not
deployable code" while they still sat in discord_bot/ and therefore shipped in
all six images.  Moving them makes that claim structural instead of asserted: a
module under tests/ cannot reach an image, so the boundary no longer depends on
anyone remembering to list it.

Keep them honest.  The seam-contract route tests drive real servers with these
engines, so a double that quietly diverges from the Protocol takes the meaning
out of those tests rather than failing them.
'''
