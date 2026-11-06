'''
The route that reports a cached object whose bytes failed their digest, served by the broker pod.

A player that fetches a cache/ key and finds the bytes do not match the digest in the key cannot
evict anything itself: the catalog row and the cache bucket belong to the broker. The broker
re-reads the object, and only evicts it if its own read fails the check too, so one read cut short
in transit does not cost a healthy cached song.

It is part of the broker seam: routes/broker.py includes this module's ALL in its own, so the seam
registry, the client's ROUTES_CALLED and the agreement tests all cover it. It is DEFINED here, as its
own module, for the deploy-order reason routes/guild_queue.py gives: the broker image pins a released
discord_core, and this module is where that release put the symbol the broker's handler imports.

The answer is a 200 body either way (`evicted` says whether the object was removed), because
the retry layer retries 5xx and "the object is fine" is not a failure.
'''
from discord_core.routes.route import Route, collect

REPORT_CORRUPT_CACHE_OBJECT = Route('POST', '/cache/report-corrupt')

ALL = collect(globals())
