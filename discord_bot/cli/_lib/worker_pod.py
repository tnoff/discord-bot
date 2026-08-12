'''
Shared scaffolding for the queue-consumer worker pods (downloader, search).

Both pods have the same skeleton — validate redis + a broker URL, build an
optional redis-ping health server, start the HTTP server and one or more guarded
consumer loops under a LoopHealth entry, then drain on SIGTERM — and differ only
in which worker they drive and what extra background tasks they run.  With the
skeleton copied per pod, pylint's R0801 rejected the second copy (and a disable
is not the fix); more to the point, the parts most worth getting right are the
ones a copy silently drifts on: the broad-except loop guard, and marking the loop
stopped on a deliberate drain so a draining pod doesn't fail its own probe.

Same call, and the same reason, as MR 4's QueueWorkerHttpServer /
HttpQueueWorkerClient bases: the two tiers are the same shape, so the shape
belongs in one place.
'''
import asyncio
import logging
from typing import Awaitable, Callable, Iterable

from discord_bot.clients.redis_client import RedisManager
from discord_bot.exceptions import DiscordBotException, ExitEarlyException
from discord_bot.servers.redis_health_server import RedisPingHealthServer
from discord_bot.utils.common import GeneralConfig
from discord_bot.utils.loop_health import LOOP_HEALTH, LoopHealth

from discord_bot.cli._lib.common import shutdown_event_signals

logger = logging.getLogger(__name__)


def require_redis_manager(general_config: GeneralConfig, mode_label: str) -> RedisManager:
    '''Build the pod's RedisManager, or fail loudly when redis is unconfigured.

    Both pods are Redis-queue consumers, so there is no in-process fallback worth
    starting: without redis they would idle forever looking healthy.
    '''
    if not (general_config.redis_url or general_config.redis_sentinel):
        raise DiscordBotException(f'Redis required for {mode_label} HA mode')
    return RedisManager.from_general_config(general_config)


def require_broker_url(settings: dict, mode_label: str) -> str:
    '''Read music.broker_client.url, or fail loudly when it is missing.

    A worker pod with no broker cannot report anything back to the bot, so an
    early exception beats a pod that consumes the queue into the void.
    '''
    broker_client_cfg = settings.get('music', {}).get('broker_client') or {}
    broker_url = broker_client_cfg.get('url')
    if not broker_url:
        raise DiscordBotException(f'music.broker_client.url required for {mode_label} HA mode')
    return broker_url


def build_redis_health_server(general_config: GeneralConfig,
                              redis_manager: RedisManager) -> RedisPingHealthServer | None:
    '''Build the /health + /ready server when monitoring.health_server is enabled.

    HealthServerBase folds the LoopHealth registry into the probe for free, so a
    registered-but-stalled consumer loop 503s the pod through this server.
    '''
    if not (general_config.monitoring and general_config.monitoring.health_server
            and general_config.monitoring.health_server.enabled):
        return None
    return RedisPingHealthServer(
        redis_manager,
        port=general_config.monitoring.health_server.port,
        bind_address=general_config.monitoring.health_server.bind_address,
    )


async def drive_loop(run_iteration: Callable[[asyncio.Event], Awaitable],
                     stop_event: asyncio.Event, driver_logger: logging.Logger,
                     health: LoopHealth, error_message: str) -> None:
    '''
    Drive one consumer iteration at a time until shutdown, reporting into health.

    ``run_iteration`` consumes at most one queued item per call and returns; this
    loop re-drives it.  ``health`` is passed in rather than registered here so a
    pool of drivers can share one entry — registering per driver would re-arm the
    same entry N times, and letting each driver mark it stopped would have the
    first one out report the whole pool stopped while its siblings still work.

    The broad ``except Exception`` is load-bearing.  An unguarded loop that dies
    on an unexpected error (a redis or broker blip) wedges the pod forever —
    while the health server keeps reporting green, which is exactly the failure
    LoopHealth exists to make visible: retrying forever is right, but a loop that
    stops making progress must fail its pod's own health check.
    ``broad-except`` is disabled globally in .pylintrc, so no inline disable is
    needed.
    '''
    while not stop_event.is_set():
        try:
            await run_iteration(stop_event)
            health.record_success()
        except ExitEarlyException:
            break
        except Exception:
            health.record_error()
            driver_logger.exception(error_message)
            await asyncio.sleep(1)


async def worker_pod_main_loop(http_server, health_server, redis_manager: RedisManager,
                               loop_name: str, pod_label: str,
                               task_factory: Callable[[asyncio.Event, LoopHealth],
                                                      Iterable[Awaitable]]):
    '''
    Run a worker pod until SIGTERM/SIGINT, then drain the HTTP server and Redis.

    ``task_factory`` is called once with the pod's shutdown event and the
    LoopHealth entry registered for ``loop_name``, and returns the coroutines to
    schedule: the consumer driver(s) plus whatever else that pod runs (a metrics
    poller, an egress probe).  Registering the health entry here — before any
    driver starts — is what lets a pool share one entry.
    '''
    await redis_manager.start()
    with shutdown_event_signals() as stop_event:
        try:
            if health_server:
                asyncio.create_task(health_server.serve())
            asyncio.create_task(http_server.serve())
            loop_health = LOOP_HEALTH.register(loop_name)
            for coro in task_factory(stop_event, loop_health):
                asyncio.create_task(coro)
            logger.info('Main :: %s running', pod_label)
            await stop_event.wait()
        finally:
            # A deliberate drain is not a wedge: mark the loop stopped once, here,
            # so a draining pod doesn't fail its own liveness probe while the
            # drivers wind down (the drivers never mark it themselves).
            LOOP_HEALTH.mark_stopped(loop_name)
            logger.info('Main :: Draining %s server...', pod_label)
            await http_server.drain_and_stop()
            await redis_manager.close()
            logger.info('Main :: Shutdown complete')
