from logging import getLogger, Formatter, StreamHandler, RootLogger
from logging.handlers import RotatingFileHandler
from pathlib import Path
from sys import stdout
from typing import Callable, Optional, Literal

from discord.ext.commands import Bot
from opentelemetry.trace import get_current_span
from opentelemetry.trace.status import StatusCode
from pydantic import BaseModel, Field, model_validator


from discord_bot.cogs.schema import StorageConfig
from discord_bot.exceptions import ExitEarlyException

OTEL_SPAN_PREFIX = 'utils'

# Pydantic models for config validation
# Default patterns for high volume spans that are filtered when in OK state
DEFAULT_HIGH_VOLUME_SPAN_PATTERNS = [
    r'^sql_retry\.retry_db_command$',
    r'^utils\.retry_command_async$',
    r'^utils\.message_send_async$',
]

class MonitoringOtlpConfig(BaseModel):
    '''OTLP monitoring configuration'''
    enabled: bool
    # Filter high volume spans, only filters those in OK state
    filter_high_volume_spans: bool = True
    # List of regex patterns to filter (when in OK state)
    high_volume_span_patterns: list[str] = Field(default_factory=DEFAULT_HIGH_VOLUME_SPAN_PATTERNS.copy)

class MonitoringMemoryProfilingConfig(BaseModel):
    '''Memory profiling monitoring configuration'''
    enabled: bool = False
    interval_seconds: int = Field(default=60, ge=1)
    top_n_lines: int = Field(default=25, ge=1)

class MonitoringProcessMetricsConfig(BaseModel):
    '''Process metrics monitoring configuration'''
    enabled: bool = False
    interval_seconds: int = Field(default=15, ge=1)

class MonitoringHealthServerConfig(BaseModel):
    '''Health server monitoring configuration'''
    enabled: bool = False
    port: int = Field(default=8080, ge=1, le=65535)

class MonitoringConfig(BaseModel):
    '''Monitoring configuration'''
    otlp: MonitoringOtlpConfig
    memory_profiling: Optional[MonitoringMemoryProfilingConfig] = None
    process_metrics: Optional[MonitoringProcessMetricsConfig] = None
    health_server: Optional[MonitoringHealthServerConfig] = None

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

class GeneralConfig(BaseModel):
    '''General bot configuration'''
    discord_token: str
    sql_connection_statement: Optional[str] = None
    storage: Optional[StorageConfig] = None
    monitoring: Optional[MonitoringConfig] = None
    logging: Optional[LoggingConfig] = None
    include: IncludeConfig = Field(default_factory=IncludeConfig)
    intents: list[str] = Field(default_factory=list)
    rejectlist_guilds: list[int] = Field(default_factory=list)
    redis_url: Optional[str] = None
    dispatch_cross_process: bool = False
    dispatch_process_id: Optional[str] = None
    dispatch_shard_id: int = 0

def get_logger(logger_name, logging_config: Optional[LoggingConfig]):
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

def return_loop_runner(function: Callable, bot: Bot, logger: RootLogger, continue_exceptions=None, exit_exceptions=ExitEarlyException):
    '''
    Return a basic standard bot loop

    function : Function to run, must by async
    bot : Bot object
    logger : Logger for exceptions
    checkfile: Writes 1 to file when loop active, writes 0 when its not
    continue_exceptions: Do not exit on these exceptions
    exit_exceptions : Exit on these exceptions
    '''
    continue_exceptions = continue_exceptions or ()
    async def loop_runner(): #pylint:disable=duplicate-code
        await bot.wait_until_ready()

        while not bot.is_closed():
            try:
                await function()
            except continue_exceptions as e:
                logger.exception('Continue exception in loop runner: %s', type(e).__name__, exc_info=True)
                continue
            except exit_exceptions:
                # Set status code because we know these ones are fine
                span = get_current_span()
                if span.is_recording():
                    span.set_status(StatusCode.OK)
                return False
            except Exception as e:
                logger.exception('Exception in loop runner: %s', type(e).__name__, exc_info=True)
                return False
    return loop_runner
