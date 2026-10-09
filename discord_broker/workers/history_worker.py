'''
Records tracks that played out: the broker's port of MusicPlayer's post-play loop.

The gateway used to do this itself, from an in-process queue its player filled: count the play in
the guild's analytics, then add the track to the guild's history playlist.  Both are db-pod calls,
so the work moved here with the queue.  `finish` pushes a play record onto a Redis list when a
track plays out (skipped tracks are never recorded, as before) and this drains it.

Playback never waits on this.  A db that is slow or down delays the history, not the music:
records wait in Redis and are retried.  That is better than what it replaces, where a failure lost
the record outright.  Delivery is still at-most-once across a crash between taking a record and
finishing it, which costs one play count at worst; making that exact would need an ack list for
the sake of a counter.

A record that can never succeed (no URL to store, say) would block everything behind it if it were
retried forever, so errors are split: ones that say the db could not answer are retried, up to
MAX_ATTEMPTS, and anything else is logged and dropped.
'''
import asyncio
import logging
from enum import Enum
from functools import partial

import aiohttp
from opentelemetry.metrics import Observation

from discord_core.exceptions import DatabaseUnavailable
from discord_core.types.playlist import PlaylistItemWrite
from discord_core.utils.loop_health import LOOP_HEALTH
from discord_core.utils.otel import (AttributeNaming, METER_PROVIDER, MetricNaming,
                                     create_observable_gauge, loop_heartbeat_observations)

from discord_broker.workers.broker_metrics import BrokerMetricNaming
from discord_broker.workers.guild_queue_registry import GuildQueueRegistry

logger = logging.getLogger(__name__)

LOOP_HISTORY_WORKER = 'history_worker'
DEFAULT_IDLE_INTERVAL_SECONDS = 1.0
DEFAULT_ERROR_BACKOFF_SECONDS = 5.0
MAX_ATTEMPTS = 5

# The db could not answer, or the call to it failed in transit: worth trying again.
TRANSIENT_ERRORS = (DatabaseUnavailable, aiohttp.ClientError, asyncio.TimeoutError)


class Outcome(Enum):
    '''What one pass over the record list did.'''
    IDLE = 'idle'    # nothing waiting
    DONE = 'done'    # a record was recorded, or dropped as unrecordable
    RETRY = 'retry'  # a record could not be written yet and was put back


class HistoryWorker:
    '''Drains the play-record list into the db pod.'''

    def __init__(self, queues: GuildQueueRegistry, playlists, analytics, max_size: int,
                 idle_interval: float = DEFAULT_IDLE_INTERVAL_SECONDS,
                 error_backoff: float = DEFAULT_ERROR_BACKOFF_SECONDS,
                 max_attempts: int = MAX_ATTEMPTS):
        '''
        playlists : something with ensure_history_playlist / record_history_item
        analytics : something with record_play
        max_size : ceiling on a history playlist's item count (music.playlist.server_playlist_max_size)
        '''
        self._queues = queues
        self._playlists = playlists
        self._analytics = analytics
        self._max_size = max_size
        self._idle_interval = idle_interval
        self._error_backoff = error_backoff
        self._max_attempts = max_attempts
        self._history_playlist_ids: dict[int, int] = {}
        self._depth = 0
        create_observable_gauge(METER_PROVIDER, MetricNaming.HEARTBEAT.value,
                                partial(loop_heartbeat_observations, LOOP_HISTORY_WORKER),
                                'Broker history worker heartbeat')
        create_observable_gauge(METER_PROVIDER, BrokerMetricNaming.HISTORY_EVENT_QUEUE_DEPTH.value,
                                self.depth_observations,
                                'Play records waiting for the broker history worker')

    def depth_observations(self, _options=None):
        '''Records waiting to be written; climbs if the db is down or the worker has stalled.'''
        return [Observation(self._depth, attributes={
            AttributeNaming.BACKGROUND_JOB.value: LOOP_HISTORY_WORKER})]

    async def _history_playlist_id(self, guild_id: int) -> int:
        '''The guild's history playlist, asked for once per guild per process.'''
        if guild_id not in self._history_playlist_ids:
            self._history_playlist_ids[guild_id] = await self._playlists.ensure_history_playlist(guild_id)
        return self._history_playlist_ids[guild_id]

    async def _record(self, event: dict) -> None:
        '''
        Write one play record.  Mutates `event` to remember what already succeeded, so a retry does
        not count the same play twice when only the playlist write failed.
        '''
        guild_id = int(event['guild_id'])
        if not event.get('analytics_done'):
            await self._analytics.record_play(guild_id, int(event.get('duration') or 0),
                                              bool(event.get('cache_hit')))
            event['analytics_done'] = True
        # A track replayed from history is not added to history again.
        if event.get('added_from_history'):
            return
        # A play with no URL cannot be put in a playlist; say so rather than store an empty row.
        if not event.get('webpage_url'):
            raise ValueError('play record has no webpage_url')
        item = PlaylistItemWrite(video_url=event['webpage_url'], title=event.get('title'),
                                 uploader=event.get('uploader'))
        playlist_id = await self._history_playlist_id(guild_id)
        if not await self._playlists.record_history_item(playlist_id, item, self._max_size):
            # The playlist was deleted since we looked it up. The next record asks again.
            del self._history_playlist_ids[guild_id]
            logger.warning('History playlist %s for guild %s no longer exists, dropping %s',
                           playlist_id, guild_id, event.get('webpage_url'))

    async def process_one(self) -> Outcome:
        '''Take one record and write it; see Outcome.'''
        event = await self._queues.pop_history_event()
        if event is None:
            return Outcome.IDLE
        try:
            await self._record(event)
        except TRANSIENT_ERRORS as exc:
            event['attempts'] = int(event.get('attempts', 0)) + 1
            if event['attempts'] >= self._max_attempts:
                logger.error('Giving up recording play of %s in guild %s after %s attempts: %s',
                             event.get('webpage_url'), event.get('guild_id'), event['attempts'], exc)
                return Outcome.DONE
            logger.warning('Could not record play of %s in guild %s (attempt %s of %s), will retry: %s',
                           event.get('webpage_url'), event.get('guild_id'), event['attempts'],
                           self._max_attempts, exc)
            await self._queues.requeue_history_event(event)
            return Outcome.RETRY
        except Exception:
            logger.exception('Dropping unrecordable play record %s', event)
            return Outcome.DONE
        return Outcome.DONE

    async def run(self, stop_event: asyncio.Event) -> None:
        '''Drain until stop_event is set; busy while there are records, polling when idle.'''
        health = LOOP_HEALTH.register(LOOP_HISTORY_WORKER)
        try:
            while not stop_event.is_set():
                try:
                    outcome = await self.process_one()
                except Exception:
                    logger.exception('HistoryWorker :: pass failed')
                    health.record_error()
                    outcome = Outcome.RETRY
                if outcome is Outcome.DONE:
                    health.record_success()
                    continue
                if outcome is Outcome.IDLE:
                    health.record_success()
                else:
                    health.record_error()
                await self._refresh_depth()
                delay = self._idle_interval if outcome is Outcome.IDLE else self._error_backoff
                try:
                    await asyncio.wait_for(stop_event.wait(), timeout=delay)
                except asyncio.TimeoutError:
                    pass
        finally:
            health.mark_stopped()

    async def close(self) -> None:
        '''Close the db clients' HTTP sessions.'''
        await self._playlists.close()
        await self._analytics.close()

    async def _refresh_depth(self) -> None:
        try:
            self._depth = await self._queues.history_event_depth()
        except Exception:
            logger.exception('HistoryWorker :: could not read the record backlog')
