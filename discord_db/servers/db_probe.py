'''
The database liveness probe, shared by the health servers that run one.

Two servers ping postgres to decide whether they are healthy: the bot's, where
the database is one input alongside the gateway connection, and the db pod's,
where it is the whole answer. The eight lines were identical and pylint's
duplicate-code check caught the second copy -- correctly, because the part worth
getting right is the bare `except`: a probe that propagates instead of returning
False kills the health server itself, and the kubelet then sees a hanging socket
rather than a 503.

Deliberately NOT in servers/health_server_base.py. That module is imported by the
dispatcher, whose image ships no sqlalchemy at all, and it says so in its own
docstring. A `from sqlalchemy import text` there would be an ImportError at
dispatcher pod start -- the exact failure the per-image extras exist to prevent.
'''
from contextlib import nullcontext

from opentelemetry.instrumentation.utils import suppress_instrumentation
from sqlalchemy import text


async def db_ping(db_engine, suppress_auto_instrumentation: bool = True) -> bool:
    '''
    Return True if the database answers SELECT 1.

    Emits no spans, deliberately. The kubelet runs this on a fixed interval
    whether or not anything has happened, so SQLAlchemy's auto-instrumentation
    turned it into a steady trace stream at a rate set by the probe period
    rather than by real work -- the same problem
    RedisDownloadWorker._peek_next_request and the egress probe already solved
    this way. Two spans per probe, on a 10s period on the db pod and a 30s one on
    the bot: ~23k traces/day between them, and 100% of the db pod's trace volume,
    on a pod nothing calls yet.

    The outcome is not lost, it just stops being a trace: DatabasePingHealthServer
    counts every probe by outcome on `database.ready_check`, which is the series
    an alert should read anyway. Real route traffic keeps its spans -- the
    suppression is scoped to this call and nothing else runs inside it.

    Unlike a hand-rolled span, auto-instrumentation is exactly what
    suppress_instrumentation() gates, so this works here where
    async_untraced_span() was needed for the pollers.

    The suppression is now a toggle rather than a build-time decision, because
    the reason to suppress these spans is volume and the reason to want them
    back is an incident: they are the per-probe record of postgres flapping, and
    docker-apps alerts on exactly that condition. Restoring them used to mean an
    image build and a pod roll. Default True -- unchanged behaviour.

    db_engine : AsyncEngine to probe
    suppress_auto_instrumentation : False re-emits the connect + SELECT spans.
                                    monitoring.tracing.suppress_db_probe_auto_instrumentation.
    '''
    try:
        with (suppress_instrumentation() if suppress_auto_instrumentation else nullcontext()):
            async with db_engine.connect() as conn:
                await conn.execute(text('SELECT 1'))
        return True
    except Exception:
        return False
