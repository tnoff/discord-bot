'''
Consumer loop body for the YouTube-Music search queue.

One iteration is: wait out a slice of any active 429 window, pop the next queued
MediaRequest, resolve it to a YouTube videoId, then hand the resolution back to
the bot through the broker's search-result queue.  The per-request retry policy
(retry_count, re-enqueue, the RETRY_SEARCH / FAILED lifecycle pushes) lives here
too — it is request policy rather than queue mechanics, so it belongs with the
loop that applies it, not with the queue implementation underneath.

This lives in its own module because it had two drivers, in two different
processes: the music cog's ``search_youtube_music`` loop against an
InMemoryYoutubeMusicSearchClient, and ``cli/search.py`` inside the standalone
search pod against a RedisYoutubeMusicSearchWorker and an HttpBrokerClient.  The
cog's half is gone (projects/discord-bot-ha-only) — the pod is the only driver
in production now, and the in-memory collaborators survive only as test doubles
(tests.helpers.attach_in_process_search).

The seam stays because the collaborator-agnostic shape is what let the loop move
processes without a rewrite, and because it is what keeps the pod's tests able to
drive the real loop body without an aiohttp server.

Nothing here imports the cog, so the search pod's import chain never reaches
yt-dlp, sqlalchemy or boto3 (the same discipline that keeps sqlalchemy off the
dispatcher).  The collaborators are taken as constructor arguments rather than
imported for the same reason.
'''
import asyncio
import logging
from asyncio import QueueEmpty

from opentelemetry.trace import SpanKind
from opentelemetry.trace.status import StatusCode

from discord_bot.types.download import LifecycleEvent, LifecycleStatusUpdate
from discord_bot.types.media_request import MediaRequest, media_request_attributes
from discord_bot.types.search_resolution import SearchResolution
from discord_bot.utils.integrations.common import YOUTUBE_VIDEO_PREFIX
from discord_bot.exceptions import YoutubeMusicRetryException
from discord_bot.utils.otel import (
    async_otel_span_wrapper, capture_span_context, span_links_from_context,
)

# Span name is deliberately the cog's old one ('music' + the loop name) rather
# than a pod-flavoured rename: the same logical operation moved processes at the
# cutover, and a renamed span would have broken trace comparison across exactly
# the window where a regression was most likely. Keep it.
SEARCH_SPAN_NAME = 'music.search_youtube_music'

# Longest a single iteration sleeps out a 429 backoff window.  A search backoff
# runs wait_period_minimum * 2**failures (30 s doubling), which outgrows the
# loop-health staleness window (300 s default) after four failures, so the wait
# is taken in slices — each returning iteration re-arms health, and a genuinely
# wedged loop still goes stale on schedule.  Carried over from the cog (!192);
# it matters MORE in the pod, where a stale loop fails the livenessProbe and
# restarts a pod over a rate limit a restart cannot fix.
SEARCH_BACKOFF_SLICE_SECONDS = 30.0

# Idle backoff when the queue is empty, so an idle pod doesn't busy-spin the
# pop (which is a Redis round-trip per call in the HA shape).
SEARCH_IDLE_POLL_BACKOFF_SECONDS = 0.25


class YoutubeMusicSearchDriver:
    '''
    Drives one YouTube-Music search per ``run_once`` call.

    search_client : anything with the pop/resolve half of the search surface —
        ``backoff_wait`` / ``backoff_seconds_remaining`` / ``get_input_nowait`` /
        ``resolve`` / ``submit``.  In-process that is an
        InMemoryYoutubeMusicSearchClient; in the pod it is the
        RedisYoutubeMusicSearchWorker itself (HttpYoutubeMusicSearchClient
        deliberately does NOT implement this half — under HA the loop runs where
        the queue and the ytmusicapi client are).
    broker_client : InMemoryBrokerClient or HttpBrokerClient.  Receives the
        lifecycle transitions and the finished SearchResolution.
    max_retries : re-enqueue budget per request before it is failed.
    queue_priority : {guild_id: priority} used when re-enqueueing a retry, so a
        retried request keeps its guild's queue priority instead of silently
        dropping to the default bucket.
    '''
    def __init__(self, search_client, broker_client, logger: logging.Logger,
                 max_retries: int = 3, queue_priority: dict[int, int] | None = None,
                 backoff_slice_seconds: float = SEARCH_BACKOFF_SLICE_SECONDS,
                 idle_sleep_seconds: float = SEARCH_IDLE_POLL_BACKOFF_SECONDS):
        '''Wire the driver to its queue, broker and retry policy.'''
        self.search_client = search_client
        self.broker_client = broker_client
        self.logger = logger
        self.max_retries = max_retries
        self.queue_priority = queue_priority or {}
        self.backoff_slice_seconds = backoff_slice_seconds
        self.idle_sleep_seconds = idle_sleep_seconds

    async def _push_lifecycle(self, media_request: MediaRequest, event: LifecycleEvent,
                              **details) -> None:
        '''Send a lifecycle transition to the broker (which re-renders the bundle).'''
        update = LifecycleStatusUpdate(event=event, **details)
        await self.broker_client.update_request_status(str(media_request.uuid), update)

    async def _handle_retry(self, media_request: MediaRequest,
                            error: YoutubeMusicRetryException) -> None:
        '''
        Apply the 429 retry policy to one request.

        ``resolve`` has already recorded the failure and armed the backoff window
        (shared across pods when the worker is Redis-backed); what is left is the
        per-request half — spend one retry and re-enqueue, or fail the request
        once the budget is gone.
        '''
        backoff_seconds = self.search_client.backoff_seconds_remaining
        if backoff_seconds is not None:
            self.logger.info(f'Youtube music search rate limited, waiting {backoff_seconds} seconds')
        media_request.youtube_music_retry_information.retry_count += 1
        if media_request.youtube_music_retry_information.retry_count >= self.max_retries:
            self.logger.warning(f'Youtube music search retry limit exceeded for "{media_request.search_result.raw_search_string}"')
            await self._push_lifecycle(
                media_request, LifecycleEvent.FAILED,
                failure_reason='Youtube music search rate limit exceeded after max retries')
            return
        await self.search_client.submit(
            media_request.guild_id, media_request,
            priority=self.queue_priority.get(media_request.guild_id, None))
        await self._push_lifecycle(
            media_request, LifecycleEvent.RETRY_SEARCH, error_detail=str(error),
            backoff_seconds=backoff_seconds,
            retry_count=media_request.youtube_music_retry_information.retry_count,
            max_retries=self.max_retries)

    async def run_once(self, shutdown_event: asyncio.Event) -> bool:
        '''
        Run a single search iteration.

        Returns True for a completed iteration (including the idle and
        backoff-slice paths, which are progress as far as loop health is
        concerned) and False when a request hit a 429 and was retried or failed.
        Raises ExitEarlyException, via backoff_wait, when shutdown fires mid-wait.
        '''
        # Wait out any active 429 backoff BEFORE popping, one slice per iteration.
        # Popping first would hold a request in this process's memory for the whole
        # window — and the Redis-backed queue DELetes on pop, so a restart during
        # the wait loses it outright.
        await self.search_client.backoff_wait(
            shutdown_event, max_wait_seconds=self.backoff_slice_seconds)
        if self.search_client.backoff_seconds_remaining:
            # Window still open after this slice. Return so the caller records a
            # completed iteration, then wait the next slice.
            return True

        try:
            media_request = await self.search_client.get_input_nowait()
        except QueueEmpty:
            # Idle: no search queued — back off before the caller re-runs rather
            # than busy-spinning.
            await asyncio.sleep(self.idle_sleep_seconds)
            return True

        # Default lifecycle_stage is already SEARCHING — register_request rendered
        # the bundle when the request entered the pipeline.
        async with async_otel_span_wrapper(SEARCH_SPAN_NAME, kind=SpanKind.CLIENT,
                                           attributes=media_request_attributes(media_request),
                                           links=span_links_from_context(media_request.span_context)) as span:
            self.logger.debug(f'Running youtube music search for input "{media_request.search_result.raw_search_string}"')
            try:
                youtube_music_result = await self.search_client.resolve(media_request)
            except YoutubeMusicRetryException as e:
                await self._handle_retry(media_request, e)
                span.set_status(StatusCode.ERROR)
                return False
            if youtube_music_result:
                # This returns the raw id, make sure we add the proper prefix for caching bits
                media_request.search_result.add_youtube_music_result(f'{YOUTUBE_VIDEO_PREFIX}{youtube_music_result}')

            # Hand the resolved request back through the broker's search-result
            # queue; the bot's process_search_results loop runs the bot-side tail
            # (cache-check then download submit), which can only run where the
            # download client and the cache live.  This is the seam that lets the
            # search pod return resolutions to a bot it shares no memory with.
            await self.broker_client.register_search_result(
                SearchResolution(media_request=media_request, span_context=capture_span_context()))
        return True
