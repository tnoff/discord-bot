import asyncio
from asyncio import Event, TimeoutError as async_timeout, Task
from io import BytesIO
from pathlib import Path
from time import time, monotonic

from discord import PCMAudio
from discord.errors import ClientException
from opentelemetry.trace import SpanKind

from discord_core.exceptions import ExitEarlyException
from discord_core.types.media_download import MediaDownload, media_download_attributes
from discord_core.utils.common import get_logger, LoggingConfig
from discord_core.utils.common import return_loop_runner
from discord_core.utils.discord_context import DiscordContextNaming
from discord_core.utils.otel import async_otel_span_wrapper, span_links_from_context
from discord_core.interfaces.broker_client_protocol import BrokerClient
from discord_core.types.guild_queue import ClaimedDownload
from discord_core.utils.integrations.s3 import ObjectStorageException, get_file

from discord_gateway.types.cleanup_reason import CleanupReason


# Staging a track for playback (broker checkout + S3 fetch) happens between a
# track leaving the play queue and audio starting, so slow staging is dead air.
# Past this many seconds the per-track timing log is escalated from DEBUG to
# WARNING so a stall is visible in prod without DEBUG logging enabled.
PLAY_STAGING_SLOW_SECONDS = 5.0

# How long the player loop will wait for a voice client to appear before giving up
# on a track. Sized against the real handshake, not a guess: the incident this was
# written for measured ~45s between a resumed session filling the queue and the
# voice connection completing, during a rollout.
VOICE_CLIENT_WAIT_SECONDS = 60.0
VOICE_CLIENT_POLL_SECONDS = 0.5

# How often the broker is told this process is still playing the track it claimed. The broker's
# now-playing record expires after 15s, so this survives two missed beats; a gateway that dies
# stops beating and the record lapses, which is what lets a replacement take the track over.
HEARTBEAT_INTERVAL_SECONDS = 5.0
# How often an idle player asks the broker for its next track. The gateway's own enqueue wakes it
# at once (see notify_enqueued), so this only bounds how long a track added by anything else waits.
CLAIM_POLL_SECONDS = 1.0



def cleanup_source(audio_source: PCMAudio):
    '''
    Cleanup audio source
    '''
    if audio_source:
        try:
            audio_source.cleanup()
        except ValueError:
            # Check if file is closed
            pass

class MusicPlayer:
    '''
    A class which is assigned to each guild using the bot for Music.

    This class implements a queue and loop, which allows for different guilds
    to listen to different playlists simultaneously.

    When the bot disconnects from the Voice it's instance will be destroyed.
    '''

    def __init__(self, bot, guild, text_channel,
                 logging_config: LoggingConfig,
                 queue_max_size: int, disconnect_timeout: int, file_dir: Path,
                 broker: BrokerClient,
                 gateway_id: str,
                 prefetch_limit: int = 5,
                 bucket_name: str | None = None):
        '''
        Music Player to sit in voice chat

        Takes bot / guild / text_channel rather than a Context: these three were
        all a Context was ever read for, and a player resumed at startup is built
        from a stored session with no command behind it.

        The player does not own the queue. The broker does (queue, history and the
        now-playing record), so a restart of this process loses none of it. What stays
        here is what has to share a process with discord.py: the voice connection,
        the audio, and the staged copies of the files about to be played.

        broker : Where the guild's queue lives, and how this player claims tracks.
        gateway_id : Names this process in the broker's now-playing record, so a
            replacement can tell another gateway's heartbeat from its own.
        queue_max_size : How many played tracks the broker keeps as history.
        '''
        self.logger = get_logger(__name__, logging_config)
        self.bot = bot
        self.guild = guild
        self.text_channel = text_channel

        self.disconnect_timeout: int = disconnect_timeout
        self.file_dir: Path = file_dir
        self.queue_max_size: int = queue_max_size

        self.next: Event = Event()
        # Set when something is queued, so an idle player claims at once rather than on its poll.
        self._wake: Event = Event()

        # Tasks
        self._player_task: Task | None = None
        self._prefetch_task: Task | None = None

        # Random things to store
        self.current_media_download: MediaDownload | None = None
        self.current_audio_source: PCMAudio | None = None
        self.video_skipped: bool = False
        # Shutdown called externally
        self.shutdown_called: bool = False
        self.shutdown_reason: CleanupReason | None = None
        # Inactive timestamp for bot timeout
        self.inactive_timestamp: int | None = None
        self.broker: BrokerClient = broker
        self.gateway_id: str = gateway_id
        self.prefetch_limit: int = prefetch_limit
        # Bucket the queued items' file paths are object keys in. None means the
        # downloads are local files already, so there is nothing to stage.
        self.bucket_name: str | None = bucket_name
        # Staging downloads in flight, by request uuid, so playback and the
        # prefetch window never fetch the same object twice.
        self._staging: dict[str, asyncio.Task] = {}

    async def start_tasks(self):
        '''
        Start background methods
        '''
        if not self._player_task:
            self._player_task = self.bot.loop.create_task(return_loop_runner(self.player_loop, self.bot, self.logger, None)())

    async def _wait_for_voice_client(self):
        '''
        Return the guild's voice client, waiting for it to appear if it has not yet.

        Music.get_player starts this loop (start_tasks) BEFORE it awaits
        join_voice, so there is a real window where the loop is live and the guild
        has no voice client. A resumed session walks straight into it: the resume
        re-queues its whole backlog the moment the player exists, so the loop can
        reach play() while the voice handshake is still in flight.

        That used to be treated as fatal. play() raised AttributeError on None, and
        the handler destroyed the player and dropped every queued track — 15 of
        them in the incident this was written for, with the voice client arriving
        29 seconds later and nothing left to play. Waiting costs nothing when the
        client is already connected, which is the overwhelmingly common case, and
        preserves the queue when it is not.

        Returns None if the wait times out or the player is shut down while
        waiting; the caller's existing AttributeError path then handles it, so a
        genuinely voice-less player still gets torn down rather than looping.
        '''
        if self.guild.voice_client:
            return self.guild.voice_client
        self.logger.info(
            f'No voice client yet for guild {self.guild.id}, waiting up to '
            f'{VOICE_CLIENT_WAIT_SECONDS:.0f}s for the connection to complete'
        )
        deadline = monotonic() + VOICE_CLIENT_WAIT_SECONDS
        while monotonic() < deadline:
            await asyncio.sleep(VOICE_CLIENT_POLL_SECONDS)
            if self.shutdown_called:
                return None
            if self.guild.voice_client:
                self.logger.info(f'Voice client became available for guild {self.guild.id}')
                return self.guild.voice_client
        self.logger.warning(
            f'Voice client never appeared for guild {self.guild.id} after '
            f'{VOICE_CLIENT_WAIT_SECONDS:.0f}s'
        )
        return None

    def notify_enqueued(self):
        '''
        Tell an idle player something was queued, so it claims now instead of on its next poll.
        '''
        self._wake.set()

    async def _claim_next(self) -> ClaimedDownload:
        '''
        Wait for the broker to hand over the next track.

        If nothing arrives within disconnect_timeout the player shuts itself down, exactly as it
        did when it waited on its own queue.
        '''
        deadline = monotonic() + self.disconnect_timeout
        while True:
            # Clear BEFORE asking: a wake that lands while the claim is in flight must still
            # end the wait below, or it would sit unnoticed until the poll.
            self._wake.clear()
            claimed = await self.broker.claim_next_track(self.guild.id, self.gateway_id)
            if claimed is not None:
                return claimed
            remaining = deadline - monotonic()
            if remaining <= 0:
                self.logger.info(f'Bot reached timeout on queue in guild "{self.guild.id}"')
                self.destroy()
                raise ExitEarlyException('MusicPlayer hit timeout waiting for the next track')
            try:
                await asyncio.wait_for(self._wake.wait(), timeout=min(CLAIM_POLL_SECONDS, remaining))
            except async_timeout:
                pass

    async def _heartbeat(self, media_uuid: str, stop: Event):
        '''
        Keep the broker's now-playing record alive until `stop` is set.

        A failed beat is logged and retried rather than raised: the broker blipping must not
        stop the music, and the record only lapses after three missed beats.
        '''
        while True:
            try:
                await asyncio.wait_for(stop.wait(), timeout=HEARTBEAT_INTERVAL_SECONDS)
                return
            except async_timeout:
                pass
            try:
                if not await self.broker.playing_heartbeat(self.guild.id, media_uuid):
                    self.logger.warning(
                        f'Broker no longer lists "{media_uuid}" as playing in guild {self.guild.id}')
            except Exception as exc:  #pylint:disable=broad-except
                self.logger.warning(f'Heartbeat for guild {self.guild.id} failed: {exc}')

    async def _finish_track(self, media_download: MediaDownload, skipped: bool):
        '''
        Tell the broker this track is done: it clears the now-playing record, records the play
        (unless skipped) and releases the entry.
        '''
        await self.broker.finish_track(self.guild.id, str(media_download.media_request.uuid),
                                       skipped, self.queue_max_size)

    async def player_loop(self):
        '''
        Player loop logic
        '''
        self.next.clear()

        claimed = await self._claim_next()
        media_download = claimed.download
        media_uuid = str(media_download.media_request.uuid)
        self.current_media_download = media_download
        # Started before staging, not after: fetching a large file from S3 can outlast the
        # broker's 15s now-playing window, and a lapsed record is how a replacement decides the
        # track is orphaned.
        stop_heartbeat = Event()
        heartbeat = asyncio.create_task(self._heartbeat(media_uuid, stop_heartbeat))
        try:
            audio_source = await self._start_track(claimed)
            if audio_source is None:
                return
            await self.next.wait()
            cleanup_source(audio_source)
            # skipped tracks are released but not recorded in the guild's history
            await self._finish_track(media_download, self.video_skipped)
            self._discard_staged(media_download)
        finally:
            stop_heartbeat.set()
            heartbeat.cancel()
            self.current_media_download = None

    async def _start_track(self, claimed: ClaimedDownload) -> PCMAudio | None:
        '''
        Stage the claimed track's file and start it playing.

        Returns the audio source, or None when the track could not be started (it has been
        released). Raises ExitEarlyException when there is no voice connection to play into.
        '''
        media_download = claimed.download
        # Span the active play-start path — file staging and the voice_client.play() call —
        # so a failed/absent voice client (the "Voice client unavailable" case) is recorded as
        # an ERROR span instead of only a log line. Scoped to track start, not the preceding
        # queue wait or the song's full playback, to keep the span bounded. Linked back to the
        # request that queued the track so the play correlates with its download.
        span_attributes = media_download_attributes(media_download)
        span_attributes[DiscordContextNaming.GUILD.value] = self.guild.id
        async with async_otel_span_wrapper(
                'music.play_track', kind=SpanKind.INTERNAL, attributes=span_attributes,
                links=span_links_from_context(media_download.media_request.span_context)):
            # The claim already checked the entry out; its checkout says where the file is.
            # s3_key means the broker pod holds it in S3 and the bot must fetch it first.
            checkout_result = claimed.checkout
            file_path = media_download.file_path
            s3_fetch_seconds = 0.0
            media_uuid = str(media_download.media_request.uuid)
            if checkout_result and checkout_result.s3_key and checkout_result.bucket_name:
                s3_fetch_started = monotonic()
                # Already on disk when the prefetch window got here first.
                file_path = await self._ensure_staged(media_uuid, checkout_result.bucket_name,
                                                      checkout_result.s3_key)
                s3_fetch_seconds = monotonic() - s3_fetch_started
            elif self.bucket_name and self._staged_path(media_uuid, media_download.file_path).exists():
                # The prefetch window already staged the file.
                file_path = self._staged_path(media_uuid, media_download.file_path)
            # Surface how long staging this track took: the S3 fetch sits between the track
            # leaving the queue and audio starting, so a slow GET reads as dead air. DEBUG
            # normally; escalated to WARNING past PLAY_STAGING_SLOW_SECONDS so a prod stall is
            # visible without DEBUG logging.
            staging_log = (self.logger.warning if s3_fetch_seconds >= PLAY_STAGING_SLOW_SECONDS
                           else self.logger.debug)
            staging_log(
                'Play staging for "%s" in guild %s took %.2fs (S3 fetch)',
                media_download.webpage_url, self.guild.id, s3_fetch_seconds,
            )
            # A missing file leaves file_path pointing at the download's own path, which in S3
            # mode is the object key ("cache/…"), not a local file. Skip the track rather than
            # letting open() raise and take the whole player loop down for this guild.
            if file_path is None or not Path(file_path).exists():
                self.logger.warning(
                    f'No playable file for "{media_download.webpage_url}" in guild {self.guild.id} '
                    f'(resolved path {str(file_path)!r} does not exist); skipping track'
                )
                await self._finish_track(media_download, True)
                return None
            self.logger.debug(f'Gathered new file to play {str(file_path)}')
            with open(file_path, 'rb') as f:
                audio_data = BytesIO(f.read())
            audio_source = PCMAudio(audio_data)
            self.current_audio_source = audio_source
            self.video_skipped = False
            voice_client = await self._wait_for_voice_client()
            try:
                voice_client.play(audio_source, after=self.set_next)
            except (AttributeError, ClientException) as e:
                self.logger.warning(
                    f'Voice client unavailable for guild {self.guild.id} ({type(e).__name__}: {e}), '
                    f'shutting down player'
                )
                cleanup_source(audio_source)
                await self._finish_track(media_download, True)
                if not self.shutdown_called:
                    self.destroy(reason=CleanupReason.VOICE_DISCONNECT)
                raise ExitEarlyException('No voice client in guild, ending loop') from e
            self.trigger_prefetch()
            self.logger.info(f'Now playing "{media_download.webpage_url}" requested '
                             f'by "{media_download.media_request.requester_id}" in guild {self.guild.id}, url '
                             f'"{media_download.webpage_url}"')
            return audio_source

    def set_next(self, *_args, **_kwargs):
        '''
        Used for loop to call once voice channel done
        '''
        self.logger.info(f'Set next called on player in guild "{self.guild.id}"')
        self.next.set()

    async def join_voice(self, channel):
        '''
        Join voice channel

        channel : Voice channel to join
        '''
        attributes = {
            DiscordContextNaming.GUILD.value: self.guild.id,
            DiscordContextNaming.CHANNEL.value: channel.id,
        }
        # Span + logs so a failed/timed-out voice join is visible in telemetry — the
        # wrapper records any exception that propagates and marks the span ERROR.
        async with async_otel_span_wrapper('music.join_voice', kind=SpanKind.CLIENT, attributes=attributes):
            if not self.guild.voice_client:
                # Turn off reconnect
                # If bot is having issues this just ends up connecting and reconnecting over and over
                # Tends to be more annoying that anything
                self.logger.info(f'Connecting to voice channel {channel.id} in guild {self.guild.id}')
                try:
                    await channel.connect()
                except async_timeout as error:
                    self.logger.warning(
                        f'Timed out connecting to voice channel {channel.id} in guild {self.guild.id}'
                    )
                    raise ClientException('Timed out connecting to voice channel, please try again') from error
                except Exception as error:
                    self.logger.warning(
                        f'Failed to connect to voice channel {channel.id} in guild {self.guild.id} '
                        f'({type(error).__name__}: {error})'
                    )
                    raise
                self.logger.info(f'Connected to voice channel {channel.id} in guild {self.guild.id}')
                return True
            if self.guild.voice_client.channel and self.guild.voice_client.channel.id == channel.id:
                return True
            self.logger.info(f'Moving to voice channel {channel.id} in guild {self.guild.id}')
            try:
                await self.guild.voice_client.move_to(channel)
            except Exception as error:
                self.logger.warning(
                    f'Failed to move to voice channel {channel.id} in guild {self.guild.id} '
                    f'({type(error).__name__}: {error})'
                )
                raise
            return True

    def voice_channel_inactive_timeout(self, timeout_seconds: int = 60) -> bool:
        '''
        If voice channel inactive for timeout length, return True
        '''
        result = self.voice_channel_active()
        if result:
            self.inactive_timestamp = None
            return False
        # If value exists already, check timeout and return
        if self.inactive_timestamp:
            if int(time()) - self.inactive_timestamp > timeout_seconds:
                return True
            return False
        self.inactive_timestamp = int(time())
        return False

    def voice_channel_active(self):
        '''
        Check if voice channel has active users
        '''
        if not self.guild.voice_client:
            return True
        if not self.guild.voice_client.channel:
            return True
        for member in self.guild.voice_client.channel.members:
            if member.id != self.bot.user.id:
                return True
        return False

    def _on_prefetch_done(self, task: asyncio.Task):
        if not task.cancelled() and (exc := task.exception()):
            self.logger.warning(f'Prefetch failed in guild {self.guild.id}: {exc}')

    def trigger_prefetch(self):
        '''
        Fire a non-blocking task that downloads the next queued items from S3 to
        the guild's player directory, so they are on disk when the player reaches
        them.  Replaces any previous prefetch task reference so cleanup can cancel
        it.  No-op when there is no bucket or prefetch_limit is 0.
        '''
        if self.bucket_name and self.prefetch_limit > 0:
            if self._prefetch_task and not self._prefetch_task.done():
                self._prefetch_task.cancel()
            self._prefetch_task = asyncio.create_task(self._prefetch())
            self._prefetch_task.add_done_callback(self._on_prefetch_done)

    async def _prefetch(self):
        '''Stage the first prefetch_limit queued items, in queue order, as the broker has them now.'''
        queue = await self.broker.get_guild_queue(self.guild.id)
        for item in queue.items[:self.prefetch_limit]:
            try:
                await self._ensure_staged(str(item.media_request.uuid), self.bucket_name, str(item.file_path))
            except ObjectStorageException as exc:
                # Playback retries the fetch at checkout, so a failed prefetch
                # costs only the head start.
                self.logger.warning(f'Prefetch of "{item.webpage_url}" failed in guild {self.guild.id}: {exc}')

    def _staged_path(self, media_uuid: str, s3_key) -> Path:
        '''Where the object for this request is staged under the player directory.'''
        return self.file_dir / f'{media_uuid}{"".join(Path(str(s3_key)).suffixes)}'

    async def _ensure_staged(self, media_uuid: str, bucket_name: str, s3_key) -> Path:
        '''
        Return the local path of the object, downloading it unless it is already
        there.  A download already in flight for the same request is awaited, not
        repeated.  The file lands under a .part name and is renamed when whole, so
        nothing ever sees half an object.
        '''
        local_path = self._staged_path(media_uuid, s3_key)
        if local_path.exists():
            return local_path
        task = self._staging.get(media_uuid)
        if task is None:
            task = asyncio.create_task(self._download_staged(bucket_name, str(s3_key), local_path))
            self._staging[media_uuid] = task
            task.add_done_callback(lambda done, uuid=media_uuid: self._staging_done(uuid, done))
        # Shielded so cancelling one waiter (a superseded prefetch) leaves the
        # download running for the other.
        await asyncio.shield(task)
        return local_path

    def _staging_done(self, media_uuid: str, task: asyncio.Task):
        self._staging.pop(media_uuid, None)
        if not task.cancelled():
            task.exception()  # retrieved here so an unawaited failure is not logged as lost

    async def _download_staged(self, bucket_name: str, s3_key: str, local_path: Path):
        self.file_dir.mkdir(exist_ok=True, parents=True)
        part_path = local_path.with_name(f'{local_path.name}.part')
        try:
            await asyncio.to_thread(get_file, bucket_name, s3_key, part_path)
            part_path.replace(local_path)
        finally:
            part_path.unlink(missing_ok=True)

    def _discard_staged(self, media_download: MediaDownload):
        '''Delete the staged copy of a track that has played or left the queue.'''
        if not self.bucket_name:
            return
        media_uuid = str(media_download.media_request.uuid)
        task = self._staging.get(media_uuid)
        if task:
            task.cancel()
        self._staged_path(media_uuid, media_download.file_path).unlink(missing_ok=True)

    def discard_staged(self, media_download: MediaDownload):
        '''Delete the staged copy of a track that left the queue without playing.'''
        self._discard_staged(media_download)

    async def cleanup(self):
        '''
        Release this process's resources for the guild: the audio, the staged files, and the
        tasks.

        Deliberately leaves the broker alone. Whether the guild's queue goes with the player
        is the caller's decision: a stop or a timeout closes the guild, a restart does not (the
        queue is what the next gateway resumes). See Music.cleanup.
        '''
        self.logger.info(f'Clearing out resources for player in {self.guild.id}')
        cleanup_source(self.current_audio_source)
        if self._prefetch_task and not self._prefetch_task.done():
            self._prefetch_task.cancel()
            self._prefetch_task = None
        for staging in list(self._staging.values()):
            staging.cancel()
        if self._player_task:
            self._player_task.cancel()
            self._player_task = None
        return True

    def destroy(self, reason: CleanupReason = CleanupReason.QUEUE_TIMEOUT):
        '''
        Disconnect and cleanup the player.

        reason : CleanupReason describing why playback is ending
        '''
        self.logger.info(f'Calling shutdown on music player for guild {self.guild.id}, reason: {reason.value}')
        self.shutdown_called = True
        self.shutdown_reason = reason
