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
from sqlalchemy import text


async def db_ping(db_engine) -> bool:
    '''
    Return True if the database answers SELECT 1.

    db_engine : AsyncEngine to probe
    '''
    try:
        async with db_engine.connect() as conn:
            await conn.execute(text('SELECT 1'))
        return True
    except Exception:
        return False
