from asyncio import sleep
from logging import getLogger, Formatter, StreamHandler, RootLogger
from logging.handlers import RotatingFileHandler
from pathlib import Path
from sys import stdout
from typing import Callable, Optional, Literal, TYPE_CHECKING

from opentelemetry.trace import get_current_span
from opentelemetry.trace.status import StatusCode
from opentelemetry.instrumentation.logging.handler import LoggingHandler
from pydantic import BaseModel, Field, model_validator


from discord_bot.cogs.schema import StorageConfig
from discord_bot.exceptions import ExitEarlyException
from discord_bot.utils.loop_health import DEFAULT_STALE_AFTER_SECONDS, LoopHealth

if TYPE_CHECKING:  # pragma: no cover - typing only
    from discord.ext.commands import Bot

OTEL_SPAN_PREFIX = 'utils'

# Backoff after an unexpected loop-runner error (e.g. a broker HTTP 500 raised
# when the broker's Redis blips) before re-running the loop body, so a persistent
# failure does not hot-spin the loop while it waits for the dependency to recover.
# Doubles up to _LOOP_ERROR_BACKOFF_MAX_SECONDS and resets on the next success:
# the loop now retries forever, so a multi-minute peer outage must not spin at
# 1 s for its whole duration.
_LOOP_ERROR_BACKOFF_SECONDS = 1.0
_LOOP_ERROR_BACKOFF_MAX_SECONDS = 30.0

# Pydantic models for config validation

class MonitoringOtlpConfig(BaseModel):
    """
    OTLP monitoring configuration -- where spans go, not which spans exist.

    Span filtering by NAME PATTERN used to live here as filter_high_volume_spans
    / high_volume_span_patterns. It moved to the otel-collector
    (filter/drop-ok-high-volume-spans in monitoring/collector/config.yaml in
    docker-apps), so both keys are gone and are not coming back. Extra keys are
    ignored, so a config still carrying them is accepted and has no effect.

    That is still true, and MonitoringTracingConfig is not a reversal of it. The
    collector matches span names it did not author, fleet-wide, after the fact;
    the tracing block below toggles a fixed, enumerated set of suppression
    decisions this codebase makes at specific call sites, by name of the site
    rather than by pattern over the span stream. See that class for why the two
    are different controls.
    """
    enabled: bool

class MonitoringTracingConfig(BaseModel):
    """
    Which spans this process creates at all -- the decisions that were welded in.

    Three call sites wrap themselves in suppress_instrumentation() and one poller
    passes traced=False, each for a measured reason recorded at the site. Every
    one of those was a build-time decision with no off-switch: restoring the
    spans meant editing code, building an image and rolling a pod. That is the
    wrong lifecycle for a control whose whole purpose is to be flipped during an
    incident -- the db probe is the sharp case, since suppressing its spans is
    what left the postgres-reachability alert in docker-apps without a detail
    view.

    Every default here reproduces the behaviour that shipped before this block
    existed. Adding the block changes nothing on its own; that is deliberate.
    Changing a default is a separate change with its own measurement.

    The suppress_* toggles say auto_instrumentation because that is precisely
    what suppress_instrumentation() gates. Hand-rolled spans created under those
    same blocks -- start_as_current_span, otel_span_wrapper -- are unaffected,
    and a reader who expects "suppress the db probe" to silence everything under
    the probe would otherwise be surprised by what still shows up in Tempo.
    """
    # servers/db_probe.py: the kubelet reruns this on a fixed interval, so
    # SQLAlchemy's auto-instrumentation made it a trace stream at the probe
    # period rather than at the rate of real work. Turn off to get the per-probe
    # record back while postgres is misbehaving.
    suppress_db_probe_auto_instrumentation: bool = True
    # utils/integrations/egress_probe.py: one requests-backed CLIENT span per
    # exit per tick, and a relay that cannot connect stamps it ERROR for a
    # failure refresh() already tolerates.
    suppress_egress_probe_auto_instrumentation: bool = True
    # interfaces/download_protocols.py: the readiness peek at the head of the
    # consumer loop, ~98% of the downloader's span volume at a rate set by the
    # poll interval rather than by anything happening.
    suppress_download_readiness_auto_instrumentation: bool = True
    # clients/http_queue_worker_client.py: the status poller's own span, ~99% of
    # the bot's span volume at two clients on a 1Hz tick. Unlike the three
    # above this is a manual span, so it is governed by traced= rather than by
    # suppress_instrumentation() -- hence a trace_* toggle rather than a
    # suppress_* one, and hence the inverted default.
    trace_queue_worker_status_poll: bool = False

class MonitoringMemoryProfilingConfig(BaseModel):
    '''Memory profiling monitoring configuration'''
    enabled: bool = False
    interval_seconds: int = Field(default=60, ge=1)
    top_n_lines: int = Field(default=25, ge=1)

class MonitoringProcessMetricsConfig(BaseModel):
    '''Process metrics monitoring configuration'''
    enabled: bool = False
    interval_seconds: int = Field(default=15, ge=1)

class MonitoringGcCensusConfig(BaseModel):
    '''GC object-census monitoring configuration'''
    enabled: bool = False
    interval_seconds: int = Field(default=300, ge=1)
    top_n: int = Field(default=25, ge=1)

class MonitoringHealthServerConfig(BaseModel):
    '''Health server monitoring configuration'''
    enabled: bool = False
    port: int = Field(default=8080, ge=1, le=65535)
    # bandit B104: '0.0.0.0' default makes the health endpoint reachable from outside the container; users can override to '127.0.0.1' if running behind a sidecar proxy
    bind_address: str = '0.0.0.0'  # nosec B104

class MonitoringLoopHealthConfig(BaseModel):
    '''Background-loop health configuration.

    ``stale_after_seconds`` is how long a loop may go without a successful
    iteration before it is reported unhealthy — which drops its heartbeat gauge
    to 0 *and* fails the health server's probe. Because the k8s livenessProbe
    consumes that, this doubles as the "how long before we restart the pod"
    knob: it must clear a rolling deploy skew, a broker roll and a Redis
    sentinel failover, or a peer outage turns into a crashloop.
    '''
    stale_after_seconds: float = Field(default=DEFAULT_STALE_AFTER_SECONDS, gt=0)

class MonitoringConfig(BaseModel):
    '''Monitoring configuration'''
    otlp: MonitoringOtlpConfig
    tracing: Optional[MonitoringTracingConfig] = None
    memory_profiling: Optional[MonitoringMemoryProfilingConfig] = None
    process_metrics: Optional[MonitoringProcessMetricsConfig] = None
    gc_census: Optional[MonitoringGcCensusConfig] = None
    health_server: Optional[MonitoringHealthServerConfig] = None
    loop_health: Optional[MonitoringLoopHealthConfig] = None

def resolve_tracing_config(general_config) -> MonitoringTracingConfig:
    """
    The tracing block for a process, or an all-defaults one when it is absent.

    monitoring and monitoring.tracing are both optional, so every consumer would
    otherwise repeat the same two-step None-dance -- five times over, which is
    also how R0801 starts failing the build. Returning a real model rather than
    None means the call sites read a plain attribute and cannot accidentally
    treat "no tracing block configured" as "tracing disabled".

    general_config : a GeneralConfig, or None.
    """
    monitoring = getattr(general_config, 'monitoring', None)
    return (monitoring and monitoring.tracing) or MonitoringTracingConfig()

def tracing_config_from_settings(settings: dict) -> MonitoringTracingConfig:
    """
    Same, from the raw settings dict rather than a parsed GeneralConfig.

    The cogs are handed the unparsed settings and validate their own slice of it
    -- see CogHelperBase, which builds LoggingConfig from
    settings['general']['logging'] the same way. The monitoring block sits
    alongside that one, so a cog that needs a tracing toggle reads it here
    instead of growing a new constructor argument threaded from the CLI.

    settings : the raw config dict.
    """
    block = (settings or {}).get('general', {}).get('monitoring', {}) or {}
    return MonitoringTracingConfig.model_validate(block.get('tracing') or {})

class LoggingConfig(BaseModel):
    '''Logging configuration'''
    log_level: Literal[0, 10, 20, 30, 40, 50]
    otlp_only: bool = False
    log_dir: Optional[str] = None
    log_file_count: Optional[int] = None
    log_file_max_bytes: Optional[int] = None
    logging_format: str = '%(asctime)s - %(levelname)s - %(message)s'
    logging_date_format: str = '%Y-%m-%dT%H-%M-%S'
    third_party_log_level: Literal[0, 10, 20, 30, 40, 50] = 30  # Default to WARNING (30)
    # discord.py logs voice/gateway lifecycle (websocket close codes, reconnects,
    # "voice connection is now closed") at INFO, below the third-party WARNING gate.
    # Keep this at INFO so those diagnostics ship without flooding from other libs.
    discord_gateway_log_level: Literal[0, 10, 20, 30, 40, 50] = 20  # Default to INFO (20)

    @model_validator(mode='after')
    def require_file_fields_when_not_otlp_only(self):
        '''Handle logic for no log file settings'''
        if not self.otlp_only:
            missing = [f for f in ('log_dir', 'log_file_count', 'log_file_max_bytes') if getattr(self, f) is None]
            if missing:
                raise ValueError(f'Fields required when otlp_only is false: {", ".join(missing)}')
        return self

class IncludeConfig(BaseModel):
    '''Cog include configuration'''
    default: bool = True
    message_dispatcher: bool = True
    markov: bool = False
    urban: bool = False
    music: bool = False
    delete_messages: bool = False

class RedisSentinelConfig(BaseModel):
    '''Redis Sentinel HA connection configuration.

    ``sentinels`` is a list of "host:port" entries (the redis-sentinel Service),
    ``service_name`` is the monitored primary's name (Sentinel's ``mymaster``).
    '''
    sentinels: list[str]
    service_name: str = 'mymaster'

    def sentinel_addrs(self) -> list[tuple[str, int]]:
        '''Parse "host:port" entries into (host, port) tuples for redis-py.'''
        addrs = []
        for entry in self.sentinels:
            host, _, port = entry.rpartition(':')
            addrs.append((host, int(port)))
        return addrs

class GeneralConfig(BaseModel):
    '''General bot configuration'''
    # Optional in the shared schema: gateway-less processes (broker, downloader)
    # never connect to Discord. The gateway entrypoints (bot/dispatcher/full)
    # require it explicitly via require_discord_token().
    discord_token: Optional[str] = None
    sql_connection_statement: Optional[str] = None
    storage: Optional[StorageConfig] = None
    monitoring: Optional[MonitoringConfig] = None
    logging: Optional[LoggingConfig] = None
    include: IncludeConfig = Field(default_factory=IncludeConfig)
    intents: list[str] = Field(default_factory=list)
    rejectlist_guilds: list[int] = Field(default_factory=list)
    redis_url: Optional[str] = None
    redis_sentinel: Optional[RedisSentinelConfig] = None
    dispatch_cross_process: bool = False
    dispatch_process_id: Optional[str] = None
    dispatch_shard_id: int = 0
    dispatch_gateway: bool = True

def get_logger(logger_name, logging_config: Optional[LoggingConfig], otlp_logger=None):
    '''
    Generic logger
    '''
    logger = getLogger(logger_name)
    logging_format = logging_config.logging_format if logging_config else '%(asctime)s - %(levelname)s - %(message)s'
    logging_date_format = logging_config.logging_date_format if logging_config else '%Y-%m-%dT%H-%M-%S'
    formatter = Formatter(logging_format, datefmt=logging_date_format)
    # If no logging section given, return generic logger
    # That logs to stdout
    if not logging_config:
        ch = StreamHandler(stdout)
        ch.setFormatter(formatter)
        logger.addHandler(ch)
        logger.setLevel(10)
        return logger
    logger.setLevel(logging_config.log_level)
    if not logging_config.otlp_only:
        log_file = Path(logging_config.log_dir) / f'{logger_name}.log'
        fh = RotatingFileHandler(str(log_file),
                                 backupCount=logging_config.log_file_count,
                                 maxBytes=logging_config.log_file_max_bytes)
        fh.setFormatter(formatter)
        logger.addHandler(fh)
    if otlp_logger:
        handler = LoggingHandler(level=logging_config.log_level, logger_provider=otlp_logger)
        logger.addHandler(handler)

    return logger

def rm_tree(pth: Path) -> bool:
    '''
    Remove all files in a tree
    '''
    # https://stackoverflow.com/questions/50186904/pathlib-recursively-remove-directory
    for child in pth.glob('*'):
        if child.is_file():
            child.unlink()
        else:
            rm_tree(child)
    pth.rmdir()
    return True

def return_loop_runner(function: Callable, bot: 'Bot', logger: RootLogger, continue_exceptions=None, exit_exceptions=ExitEarlyException,
                       health: Optional[LoopHealth] = None):
    '''
    Return a basic standard bot loop

    function : Function to run, must by async
    bot : Bot object
    logger : Logger for exceptions
    checkfile: Writes 1 to file when loop active, writes 0 when its not
    continue_exceptions: Do not exit on these exceptions
    exit_exceptions : Exit on these exceptions
    health : LoopHealth to report iteration outcomes to. This — not task
             liveness — is what drives the loop's heartbeat gauge and the health
             server's probe result.
    '''
    continue_exceptions = continue_exceptions or ()
    async def loop_runner(): #pylint:disable=duplicate-code
        await bot.wait_until_ready()

        backoff = _LOOP_ERROR_BACKOFF_SECONDS
        while not bot.is_closed():
            try:
                await function()
                if health:
                    health.record_success()
                backoff = _LOOP_ERROR_BACKOFF_SECONDS
            except continue_exceptions as e:
                logger.exception('Continue exception in loop runner: %s', type(e).__name__, exc_info=True)
                if health:
                    health.record_error()
                continue
            except exit_exceptions:
                # Set status code because we know these ones are fine
                span = get_current_span()
                if span.is_recording():
                    span.set_status(StatusCode.OK)
                # A deliberate exit is not a wedge: mark stopped so a draining
                # pod doesn't 503 its own liveness probe on the way out.
                if health:
                    health.mark_stopped()
                return False
            except Exception as e:
                # An unexpected/transient error (e.g. a broker HTTP 500 raised by
                # next_result when the broker's Redis blips, or a 404 from a
                # not-yet-upgraded peer mid-rolling-deploy) must NOT kill the loop
                # task: retry forever with capped backoff so the loop self-heals
                # the moment its dependency recovers.
                #
                # This deliberately reverses the old give-up-after-5-errors rule.
                # That rule existed only because "healthy" meant "the task object
                # is alive", so killing the task was the sole way to raise an
                # alarm — which also killed any chance of recovery, and turned a
                # ~20 s deploy skew into a dead consumer for the life of the pod
                # (docs findings/2026-07-31 search-seam deploy skew). Health now
                # comes from LoopHealth (successful iterations, not liveness), so
                # a persistent failure still alerts and still fails the probe
                # while the task stays alive and able to recover.
                #
                # asyncio.CancelledError is a BaseException, so a real
                # shutdown/drain still bypasses this handler and cancels cleanly.
                if health:
                    health.record_error()
                    logger.exception('Exception in loop runner (%s consecutive, %.0fs since last success): %s',
                                     health.consecutive_errors, health.seconds_since_success,
                                     type(e).__name__, exc_info=True)
                else:
                    logger.exception('Exception in loop runner: %s', type(e).__name__, exc_info=True)
                await sleep(backoff)
                backoff = min(backoff * 2, _LOOP_ERROR_BACKOFF_MAX_SECONDS)
                continue
        # Bot closed: an orderly shutdown, same as an exit exception.
        if health:
            health.mark_stopped()
        return None
    return loop_runner
