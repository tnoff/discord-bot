"""
TCP reachability probes for the pods the gateway depends on.

Shared by the ``/ready`` endpoint (which reports peer reachability) and the music
cog's startup gate (which holds its loops back until the peers are reachable), so
the two can never disagree about what "reachable" means.
"""
import asyncio
from urllib.parse import urlsplit

from discord_core.servers.health_server_base import close_writer

PROBE_TIMEOUT_SECONDS = 1.0

# A gateway that comes up before its peers (a whole-namespace rollout, or a node
# restart) holds its loops for this long, then starts them anyway. Bounded so a peer
# that is down for good costs the old behaviour (loops that error and back off)
# rather than a gateway that never starts; kept well under the loop-health stale
# window so the wait cannot fail the liveness probe.
STARTUP_WAIT_SECONDS = 120.0
STARTUP_POLL_SECONDS = 2.0


async def tcp_probe(url: str | None) -> bool:
    """Return True if a TCP connection to url's host:port succeeds within the timeout."""
    parts = urlsplit(url or '')
    host, port = parts.hostname, parts.port
    if not host or not port:
        return False
    try:
        _, writer = await asyncio.wait_for(
            asyncio.open_connection(host, port),
            timeout=PROBE_TIMEOUT_SECONDS,
        )
    except (OSError, asyncio.TimeoutError):
        return False
    await close_writer(writer)
    return True


async def wait_for_peers(peers: dict[str, str | None], logger,
                         timeout: float = STARTUP_WAIT_SECONDS,
                         interval: float = STARTUP_POLL_SECONDS) -> bool:
    """Block until every peer accepts a TCP connection, or ``timeout`` elapses.

    peers maps a name to its URL; a falsy URL is skipped, matching ``/ready``.
    Returns True when all peers were reachable, False if the wait timed out (the
    caller proceeds either way). Logs once per newly-waiting set, not per poll.
    """
    pending = {name: url for name, url in peers.items() if url}
    deadline = asyncio.get_running_loop().time() + timeout
    announced: set[str] = set()
    while pending:
        results = await asyncio.gather(*(tcp_probe(url) for url in pending.values()))
        pending = {name: url for (name, url), ok in zip(pending.items(), results) if not ok}
        if not pending:
            break
        if asyncio.get_running_loop().time() >= deadline:
            logger.warning('Peer wait timed out after %.0fs, starting anyway: %s',
                           timeout, ', '.join(sorted(pending)))
            return False
        waiting = set(pending)
        if waiting != announced:
            logger.info('Waiting for peers before starting loops: %s', ', '.join(sorted(waiting)))
            announced = waiting
        await asyncio.sleep(interval)
    return True
