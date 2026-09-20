'''
HTTP health server for the standalone persistence process.

The db pod is healthy when postgres answers, and that is the whole check --
there is no gateway connection, no Redis and no consumer loop to fold in. It is
the first pod in the fleet whose liveness rests on the database alone, which is
the point of the tier: the bot and broker stop holding an engine, so the thing
that owns it has to be the thing that reports on it.

Kept in its own module rather than reusing servers/health_server.HealthServer,
which takes a Bot and reports the database as a secondary field. Here it is the
primary signal and there is no bot to pass.
'''
from typing import ClassVar

from discord_bot.servers.db_probe import db_ping
from discord_bot.core.servers.health_server_base import HealthServerBase




class DatabasePingHealthServer(HealthServerBase):
    '''
    Lightweight HTTP health endpoint for the persistence process.

    Responds 200 {"status": "ok", "db": "ok"} when postgres answers SELECT 1,
    503 {"status": "unavailable", "db": "unavailable"} otherwise.

    db_engine : AsyncEngine the pod serves its stores from
    port : Bind port
    bind_address : Bind address
    suppress_db_probe_auto_instrumentation : passed to db_ping; False re-emits the
        probe's connect + SELECT spans. This pod is where the toggle matters most --
        the probe was 100% of its trace volume, so suppressing it left the
        docker-apps postgres-reachability alert with nothing to drill into.
    '''

    POD_NAME: ClassVar[str] = 'database'

    # bandit B104: '0.0.0.0' default is intentional -- the kubelet probes this from outside the container; override via MonitoringHealthServerConfig.bind_address
    def __init__(self, db_engine, port=8080, bind_address='0.0.0.0',  # nosec B104
                 suppress_db_probe_auto_instrumentation=True):
        super().__init__(port=port, bind_address=bind_address)
        self._db_engine = db_engine
        self._suppress_db_probe_auto_instrumentation = suppress_db_probe_auto_instrumentation

    async def _check(self):
        db_ok = await db_ping(self._db_engine,
                              self._suppress_db_probe_auto_instrumentation)
        return db_ok, {'db': 'ok' if db_ok else 'unavailable'}
