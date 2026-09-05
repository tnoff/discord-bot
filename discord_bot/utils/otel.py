from enum import Enum
from contextlib import asynccontextmanager, contextmanager
from typing import TYPE_CHECKING

from opentelemetry import trace
from opentelemetry.trace.status import StatusCode
from opentelemetry.metrics import get_meter_provider, Observation

from discord_bot.utils.loop_health import heartbeat_observation_value

if TYPE_CHECKING:  # pragma: no cover - typing only
    from discord.ext.commands import Context

TRACER = trace.get_tracer(__name__)
METER_PROVIDER = get_meter_provider().get_meter(__name__, '0.0.1')

class MetricNaming(Enum):
    '''
    Metric naming
    '''
    HEARTBEAT = 'heartbeat'
    ACTIVE_PLAYERS = 'active_players'
    VOICE_CLIENTS_CONNECTED = 'voice_clients_connected'
    CACHE_FILESYSTEM_MAX = 'cache_filesystem_max'
    CACHE_FILESYSTEM_USED = 'cache_filesystem_used'
    DISPATCHER_QUEUE_DEPTH = 'message_dispatcher_queue_depth'
    DISPATCHER_READY_CHECK = 'dispatcher_ready_check'
    # The bot's TCP probe of the db POD, added with MR 4b. Deliberately not
    # 'database_ready_check': that name belongs to the db pod's own /health
    # outcome (servers/database_health_server) and is what the
    # discord-db-postgres-unreachable alert watches. Reusing it would make a
    # bot-side network failure fire an alert that means 'postgres is
    # unreachable from the db pod' -- two different faults, one page.
    DATABASE_PEER_READY_CHECK = 'database_peer_ready_check'
    DISPATCH_RESULT_QUEUE_DEPTH = 'dispatch_result_queue_depth'
    DOWNLOAD_RESULT_QUEUE_DEPTH = 'music.download_result_queue_depth'
    SEARCH_RESULT_QUEUE_DEPTH = 'music.search_result_queue_depth'
    DOWNLOAD_QUEUE_DEPTH = 'download_queue_depth'
    DOWNLOAD_YOUTUBE_BACKOFF = 'download_youtube_backoff_seconds'
    DOWNLOAD_FAILURE_COUNT = 'download_failure_count'
    SEARCH_QUEUE_DEPTH = 'search_queue_depth'
    SEARCH_YOUTUBE_BACKOFF = 'search_youtube_backoff_seconds'
    SEARCH_FAILURE_COUNT = 'search_failure_count'
    BROKER_ENTRIES = 'broker.entries'
    BROKER_BUNDLES = 'broker.bundles'
    BROKER_RESULT_FETCH = 'broker.result_fetch'
    BROKER_SEARCH_RESULT_FETCH = 'broker.search_result_fetch'
    BROKER_READY_CHECK = 'broker.ready_check'
    DATABASE_READY_CHECK = 'database.ready_check'

class AttributeNaming(Enum):
    '''
    More generic span attribute constants
    '''
    RETRY_COUNT = 'retry_count'
    BACKGROUND_JOB = 'background_job'
    OUTCOME = 'outcome'
    ZONE = 'zone'
    # Provider-agnostic egress exit the download traffic left from (see
    # utils/integrations/egress_probe.py).  High-cardinality attribution lives on
    # spans/logs, never a metric label.
    EGRESS_HOSTNAME = 'egress.hostname'
    EGRESS_IP = 'egress.ip'
    # Why a queue submit was refused (PutsBlocked / QueueFull). Set on the
    # seam spans, which stay OK: a refusal is a decision, not a fault, and
    # marking it ERROR is what inflates the seam's error rate.
    SUBMIT_REJECTION = 'queue.submit_rejection'
    # Set on the retryable fallback only, so absence means a classification
    # branch matched. True there means the message is an understood transient
    # failure (socks timeout, yt-dlp EOFError); FALSE means nothing recognised
    # it at all, which is the signature of a matcher gone stale against reworded
    # upstream text -- the failure mode that hid the dead age-gate matcher.
    # Querying for false is what surfaces the next one.
    DOWNLOAD_ERROR_CLASSIFIED = 'download.error_classified'

class DiscordContextNaming(Enum):
    '''
    Context attribute constants
    '''
    AUTHOR = 'discord.author'
    CHANNEL = 'discord.channel'
    GUILD = 'discord.guild'
    COMMAND = 'discord.context.command'
    MESSAGE = 'discord.context.message'

class ThirdPartyNaming(Enum):
    '''
    Third party client naming
    '''
    SPOTIFY_PLAYLIST = 'spotify.playlist.id'
    SPOTIFY_ALBUM = 'spotify.album.id'
    SPOTIFY_TRACK = 'spotify.track.id'
    YOUTUBE_PLAYLIST = 'youtube.playlist.id'
    YOUTUBE_MUSIC_SEARCH = 'youtube_music.search_string'

class MediaRequestNaming(Enum):
    '''
    Media request naming
    '''
    SEARCH_STRING = 'music.media_request.search_string'
    REQUESTER = 'music.media_request.requester'
    GUILD = 'music.media_request.guild'
    SEARCH_TYPE = 'music.media_request.search_type'
    UUID = 'music.media_request.uuid'

class MusicMediaDownloadNaming(Enum):
    '''
    Music media download naming
    '''
    VIDEO_URL = 'music.media_download.video_url'
    VIDEO_ID = 'music.media_download.video_id'
    EXTRACTOR = 'music.media_download.extractor'

def capture_span_context() -> dict | None:
    '''
    Capture the currently-active span context as a JSON-serialisable dict.
    Returns None when no valid span is active (e.g. during background tasks or
    when the no-op tracer is in use).
    '''
    ctx = trace.get_current_span().get_span_context()
    if not ctx.is_valid:
        return None
    return {
        'trace_id': ctx.trace_id,
        'span_id': ctx.span_id,
        'trace_flags': int(ctx.trace_flags),
    }


def span_links_from_context(span_context: dict | None) -> list:
    '''
    Reconstruct a list of trace.Link objects from a dict produced by
    capture_span_context().  Returns an empty list when the context is None
    or cannot be reconstructed into a valid SpanContext.
    '''
    if not span_context:
        return []
    ctx = trace.SpanContext(
        trace_id=span_context['trace_id'],
        span_id=span_context['span_id'],
        is_remote=True,
        trace_flags=trace.TraceFlags(span_context['trace_flags']),
    )
    if not ctx.is_valid:
        return []
    return [trace.Link(ctx)]


class DispatchNaming(Enum):
    '''
    Dispatch system attribute constants
    '''
    REQUEST_ID = 'dispatch.request_id'
    PROCESS_ID = 'dispatch.process_id'

def _set_ok_unless_already_set(span) -> None:
    '''
    Stamp OK on a span only when its body left the status UNSET.

    Callers that handle an error and *return* rather than raise — the
    "return an error result" pattern used all over the download, retry and
    dispatch paths — set StatusCode.ERROR themselves and then exit the
    context manager normally.  OTel treats OK as final and lets it override
    ERROR, so an unconditional set_status(OK) on the normal-exit path silently
    turns every handled failure green in Tempo.  Only fill in OK when nobody
    else has spoken.

    A non-recording span (sampled out, or no SDK configured) exposes no
    ``status``; set_status is a no-op there, so stamp it and move on.
    '''
    status = getattr(span, 'status', None)
    if status is None or status.status_code is StatusCode.UNSET:
        span.set_status(StatusCode.OK)


@contextmanager
def otel_span_wrapper(span_name: str, ctx: 'Context' = None,
                      kind: trace.SpanKind = trace.SpanKind.INTERNAL,
                      attributes: dict = None,
                      context=None,
                      links: list | None = None):
    '''
    Wrap a generic span
    '''
    with TRACER.start_as_current_span(span_name, kind=kind, context=context, links=links or []) as span:
        if ctx:
            span.set_attributes({
                DiscordContextNaming.AUTHOR.value: ctx.author.id,
                DiscordContextNaming.CHANNEL.value: ctx.channel.id,
                DiscordContextNaming.GUILD.value: ctx.guild.id,
                DiscordContextNaming.COMMAND.value: ctx.command.name,
                DiscordContextNaming.MESSAGE.value: ' '.join(i for i in ctx.message.content.split(' ')[1:]),
            })
        if attributes:
            span.set_attributes(attributes)
        try:
            yield span
            _set_ok_unless_already_set(span)
        except Exception as e:
            span.set_status(StatusCode.ERROR)
            span.record_exception(e)
            raise e
        finally:
            pass


@asynccontextmanager
async def async_untraced_span():
    '''
    Stand-in for async_otel_span_wrapper that starts no span at all.

    Background pollers re-run on a fixed interval whether or not anything has
    happened, so every tick they emit is noise at a rate set by the poll
    interval rather than by real work — the same problem
    RedisDownloadWorker._peek_next_request solved with suppress_instrumentation().
    That helper only silences *auto*-instrumented spans, so it cannot suppress a
    manual TRACER.start_as_current_span; a poller on a hand-rolled span has to
    skip creating it instead.

    Yields the non-recording INVALID_SPAN so a shared body can keep calling
    set_attributes / set_status / record_exception unconditionally — they are
    all no-ops on it — and needs no branch for the untraced case.

    A tolerated failure still reaches the operator: pollers log it at WARNING.
    '''
    yield trace.INVALID_SPAN


@asynccontextmanager
async def async_otel_span_wrapper(span_name: str, ctx: 'Context' = None,
                                   kind: trace.SpanKind = trace.SpanKind.INTERNAL,
                                   attributes: dict = None,
                                   context=None,
                                   links: list | None = None):
    '''
    Wrap a generic span in an async context manager
    '''
    with TRACER.start_as_current_span(span_name, kind=kind, context=context, links=links or []) as span:
        if ctx:
            span.set_attributes({
                DiscordContextNaming.AUTHOR.value: ctx.author.id,
                DiscordContextNaming.CHANNEL.value: ctx.channel.id,
                DiscordContextNaming.GUILD.value: ctx.guild.id,
                DiscordContextNaming.COMMAND.value: ctx.command.name,
                DiscordContextNaming.MESSAGE.value: ' '.join(i for i in ctx.message.content.split(' ')[1:]),
            })
        if attributes:
            span.set_attributes(attributes)
        try:
            yield span
            _set_ok_unless_already_set(span)
        except Exception as e:
            span.set_status(StatusCode.ERROR)
            span.record_exception(e)
            raise


def create_observable_gauge(meter_provider, name: str, function, description: str, unit: str = '1'):
    '''
    Yield a loop callback method for heartbeat
    '''
    meter_provider.create_observable_gauge(
        name=name,
        callbacks=[function],
        unit=unit,
        description=description,
    )


def loop_heartbeat_observations(job_name: str, _options=None):
    '''
    Heartbeat gauge callback for a background loop, driven by LoopHealth.

    1 while the loop is completing iterations, 0 once it has gone its staleness
    window without one — the same bit the health server's probe reads, so the
    alert and the probe can never disagree. Emits nothing at all when the loop
    isn't registered in this process, so a loop that legitimately doesn't run
    here (e.g. the bot-side download loop under HA) leaves no permanently-0
    series to trip the stalled-loop alert.

    Bind with functools.partial(loop_heartbeat_observations, 'job_name') when
    registering the gauge.
    '''
    value = heartbeat_observation_value(job_name)
    if value is None:
        return []
    return [
        Observation(value, attributes={
            AttributeNaming.BACKGROUND_JOB.value: job_name,
        })
    ]
