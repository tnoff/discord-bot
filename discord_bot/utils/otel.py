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
    Every metric this project emits, under one naming scheme.

    The scheme, applied 2026-09-17 after the names had grown one PR at a time:

    1. **Underscores, never dots.** OTLP normalises `broker.entries` to
       `broker_entries` on the way into Mimir, so a dotted source name was a
       second spelling of the same series that only the emitting code ever saw.
    2. **One name per concept; dimensions are labels.** Two names for the same
       measurement taken by different pods is redundant -- `job` already
       separates them. Two names for the same measurement taken by ONE pod needs
       a label, because `job` does not.
    3. **Units in the name when they are not obvious** -- `_seconds`, `_bytes`.
       A gauge called `cache_filesystem_max` does not say what it counts.
    4. **No `_count` on a gauge.** Prometheus reserves `_count` for the count
       half of a histogram or summary, so `download_failure_count` read as a
       counter and was a gauge.

    This rename is deliberately breaking. Two docker-apps alerts and several
    dashboard panels query the old names and are fixed in a fast follow; there
    is no dual-emission window, so between the two merges those panels are blank
    and `discord-db-postgres-unreachable` cannot fire. That is a conscious
    trade, not an oversight -- see the PR that introduced this.
    '''
    # Shared by every image.
    HEARTBEAT = 'heartbeat'

    # Readiness. Two metrics, not one, because a pod reporting its OWN health and
    # the bot PROBING a peer answer different questions -- and folding them into
    # one name would make every query depend on remembering a filter, where a
    # forgotten filter reads as a plausible number rather than an error.
    #
    # Replaces broker.ready_check and database.ready_check, and every pod emits it
    # -- which is what keeps it here. Its sibling, the bot's probe OF a peer, is
    # bot-only and lives in utils/bot_metrics.py.
    POD_READY_CHECK = 'pod_ready_check'

    # Queue workers. One set of names for the downloader and the search pod,
    # which emit the same three measurements from different processes -- already
    # separated by `job`, and by the `background_job` attribute the base class
    # has always set. Two name prefixes for it bought nothing.
    # Results queued awaiting a consumer, wherever they queue. The broker holds
    # the bot-ready download and search queues; the bot holds its dispatch result
    # queues. Same measurement, different pods, so `job` separates them and
    # `result_type` separates the broker's two -- there is no reason for a pod
    # name in the metric itself when `job` already carries one.
    #
    # It was broker_result_queue_depth and dispatch_result_queue_depth, which
    # measured the same thing under two names and could not be summed or compared.
    RESULT_QUEUE_DEPTH = 'result_queue_depth'
    QUEUE_WORKER_DEPTH = 'queue_worker_depth'
    QUEUE_WORKER_BACKOFF_SECONDS = 'queue_worker_backoff_seconds'
    # A GAUGE of the current failure queue, not a counter of failures. The old
    # name said _count and it has never been one.
    QUEUE_WORKER_FAILURES = 'queue_worker_failures'

    # Seams.
    # 1 when a peer has been missing a route this client calls for longer than
    # the grace window. Deliberately NOT named for the mismatch itself: a
    # mismatch inside the window is the normal middle of a rolling update, and a
    # metric that fired on it would be muted within a week. The breach is the
    # part worth alerting on.
    SEAM_CONTRACT_BREACH = 'seam_contract_breach'
    # Incremented when a peer's response body fails validation. A COUNTER, not a
    # gauge: unlike the breach above there is no steady state to observe, only
    # events, and a rate() over this is what distinguishes one malformed row from
    # a peer that has drifted wholesale.
    SEAM_RESPONSE_INVALID = 'seam_response_invalid'

class AttributeNaming(Enum):
    '''
    More generic span attribute constants
    '''
    RETRY_COUNT = 'retry_count'
    BACKGROUND_JOB = 'background_job'
    OUTCOME = 'outcome'
    # Readiness dimensions. POD names the pod reporting its own health;
    # SOURCE/TARGET name the two ends of a peer probe.
    POD = 'pod'
    SOURCE = 'source'
    TARGET = 'target'
    # Separates the broker's download and search result streams, which share
    # a job and so cannot be told apart by `job` alone.
    RESULT_TYPE = 'result_type'
    # Which side of the voice stack counted a session: the cog's player
    # objects, or discord.py's sockets.
    TRACKED_BY = 'tracked_by'
    ZONE = 'zone'
    # Provider-agnostic egress exit the download traffic left from (see
    # utils/integrations/egress_probe.py).  High-cardinality attribution lives on
    # spans/logs, never a metric label.
    # Which pod-to-pod seam a contract check is about ('broker', 'database', ...).
    # Low cardinality by construction: there are five seams and there will not be
    # many more. The MISSING ROUTES are not a label -- that would put up to 33
    # values on one series -- they go in the log line and the span.
    SEAM = 'seam'
    # Why a seam check is unhappy: routes_missing, or no_contract_endpoint for a
    # peer predating the advertisement. Separate values because an operator
    # rolling the advertisement out wants to alert on one and not yet the other.
    SEAM_CONTRACT_REASON = 'seam_contract_reason'
    # Which client on the seam, for pods that run several. The bot speaks the
    # database seam through three store clients at three prefixes; without this
    # they share one label set and a breach cannot be attributed to one of them.
    # The client's ROUTE_PREFIX, or '' for a seam that has none.
    SEAM_PREFIX = 'seam_prefix'
    # The model that rejected the body. Low cardinality by construction -- it is
    # a class name from this build, not anything the peer controls.
    SEAM_RESPONSE_MODEL = 'seam_response_model'
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
