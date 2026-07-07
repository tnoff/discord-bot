'''
HTTP health server for the standalone broker process.

A thin alias over RedisPingHealthServer: the broker pod is healthy when Redis
(its source of truth) is reachable.  Kept in its own module/name so logs and
telemetry distinguish it from the dispatcher's health server.
'''
from discord_bot.servers.redis_health_server import RedisPingHealthServer
from discord_bot.utils.otel import AttributeNaming, METER_PROVIDER, MetricNaming


# Counts each broker health probe by outcome. The k8s livenessProbe hits /health
# on a fixed interval, so a flapping outcome is an early warning that the broker
# is losing its Redis connection before the pod is killed.
_READY_CHECK_COUNTER = METER_PROVIDER.create_counter(
    name=MetricNaming.BROKER_READY_CHECK.value,
    description='Broker health probe outcomes (Redis reachability)',
    unit='1',
)


class BrokerHealthServer(RedisPingHealthServer):
    '''
    Lightweight HTTP health endpoint for the broker process.

    Responds 200 {"status": "ok"} when Redis is reachable (ping succeeds),
    503 {"status": "unavailable"} otherwise, and counts each probe outcome.
    '''

    async def _check(self):
        ok, extra = await super()._check()
        _READY_CHECK_COUNTER.add(1, {
            AttributeNaming.OUTCOME.value: 'ok' if ok else 'unavailable',
        })
        return ok, extra
