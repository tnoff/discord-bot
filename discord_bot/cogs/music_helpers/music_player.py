import asyncio
from asyncio import Event, QueueEmpty, QueueFull, TimeoutError as async_timeout, Task
from datetime import timedelta
import logging
from pathlib import Path
from re import sub
from time import time
from typing import List

from async_timeout import timeout
from dappertable import DapperTable, Column, Columns, PaginationLength
from discord import FFmpegPCMAudio
from discord.ext.commands import Context
from discord.errors import ClientException

from discord_bot.common import DISCORD_MAX_MESSAGE_LENGTH
from discord_bot.cogs.music_helpers.common import MultipleMutableType
from discord_bot.exceptions import ExitEarlyException
from discord_bot.types.cleanup_reason import CleanupReason
from discord_bot.types.history_playlist_item import HistoryPlaylistItem
from discord_bot.types.media_download import MediaDownload
from discord_bot.cogs.music_helpers.media_broker import MediaBroker
from discord_bot.utils.queue import Queue
from discord_bot.utils.common import return_loop_runner

def cleanup_source(audio_source: FFmpegPCMAudio):
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

    def __init__(self, ctx: Context,
                 queue_max_size: int, disconnect_timeout: int, file_dir: Path,
                 dispatcher,
                 history_playlist_id: int,
                 history_playlist_queue: Queue,
                 broker: MediaBroker | None = None,
                 prefetch_limit: int = 5):
        '''
        file_dir : Files for guild stored here
        '''
        self.bot = ctx.bot
        self.guild = ctx.guild
        self.text_channel = ctx.channel
        self.logger = logging.getLogger('music')

        self.disconnect_timeout: int = disconnect_timeout
        self.file_dir: Path = file_dir

        # Queues
        self._play_queue: Queue[MediaDownload] = Queue(maxsize=queue_max_size)
        self._history: Queue[MediaDownload] = Queue(maxsize=queue_max_size)
        self.next: Event = Event()
        self.dispatcher = dispatcher

        # History playlist
        self.history_playlist_id: int = history_playlist_id
        self.history_playlist_queue: Queue[HistoryPlaylistItem] = history_playlist_queue

        # Tasks
        self._player_task: Task | None = None
        self._prefetch_task: Task | None = None

        # Random things to store
        self.current_media_download: MediaDownload | None = None
        self.current_audio_source: FFmpegPCMAudio | None = None
        self.np_message: str = ''
        self.video_skipped: bool = False
        self.queue_messages: list[str] = [] # Show current queue
        # Shutdown called externally
        self.shutdown_called: bool = False
        self.shutdown_reason: CleanupReason | None = None
        # Inactive timestamp for bot timeout
        self.inactive_timestamp: int | None = None
        self.broker: MediaBroker | None = broker
        self.prefetch_limit: int = prefetch_limit

    async def start_tasks(self):
        '''
        Start background methods
        '''
        if not self._player_task:
            self._player_task = self.bot.loop.create_task(return_loop_runner(self.player_loop, self.bot, self.logger, None)())

    async def player_loop(self):
        '''
        Player loop logic
        '''
        self.next.clear()

        try:
            # Wait for the next video. If we timeout cancel the player and disconnect...
            async with timeout(self.disconnect_timeout):
                media_download = await self._play_queue.get()
        except async_timeout as e:
            self.logger.info(f'Bot reached timeout on queue in guild "{self.guild.id}"')
            self.destroy()
            raise ExitEarlyException('MusicPlayer hit async timeout on player wait') from e
        self.current_media_download = media_download
        guild_file_path = None
        if self.broker:
            guild_file_path = await asyncio.to_thread(
                self.broker.checkout, str(media_download.media_request.uuid), self.guild.id, self.file_dir
            )

        audio_source = FFmpegPCMAudio(str(guild_file_path or media_download.file_path))
        self.current_audio_source = audio_source
        self.video_skipped = False
        audio_source.volume = 1
        try:
            self.guild.voice_client.play(audio_source, after=self.set_next)
        except (AttributeError, ClientException) as e:
            self.logger.info(f'No voice found, disconnecting from guild {self.guild.id}')
            self.np_message = ''
            cleanup_source(audio_source)
            if self.broker:
                self.broker.release(str(media_download.media_request.uuid))
            if not self.shutdown_called:
                self.destroy()
            raise ExitEarlyException('No voice client in guild, ending loop') from e
        self.trigger_prefetch()
        self.logger.info(f'Now playing "{media_download.webpage_url}" requested '
                            f'by "{media_download.media_request.requester_id}" in guild {self.guild.id}, url '
                            f'"{media_download.webpage_url}"')
        self.np_message = f'Now playing {media_download.webpage_url} requested by {media_download.media_request.requester_name}'
        key = f'{MultipleMutableType.PLAY_ORDER.value}-{self.guild.id}'
        self.dispatcher.update_mutable(key, self.guild.id,
                                       self.get_queue_order_messages(), self.text_channel.id)

        await self.next.wait()
        self.np_message = ''
        cleanup_source(audio_source)
        if self.broker:
            self.broker.release(str(media_download.media_request.uuid))

        # Add video to history if possible
        # Add here to history playlist queue to save items for metrics as well
        # Check on the other side if this was added from history
        if not self.video_skipped:
            if self.history_playlist_id:
                self.history_playlist_queue.put_nowait(HistoryPlaylistItem(self.history_playlist_id, media_download))

            try:
                self._history.put_nowait(media_download)
            except QueueFull:
                await self._history.get()
                self._history.put_nowait(media_download)

        # Make sure we delete queue messages if nothing left
        if self._play_queue.empty():
            key = f'{MultipleMutableType.PLAY_ORDER.value}-{self.guild.id}'
            self.dispatcher.update_mutable(key, self.guild.id,
                                           self.get_queue_order_messages(), self.text_channel.id)

    def get_queue_order_messages(self):
        '''
        Get full queue message
        '''
        queue_items = self._play_queue.items()
        # Always include the now playing message if it exists, even when queue is empty
        items = [self.np_message] if self.np_message else []

        if not queue_items:
            return items
        headers = [
            Column('Pos', 3, zero_pad=True),
            Column('Wait Time', 9),
            Column('Title', 48),
            Column('Uploader', 48)
        ]
        table = DapperTable(columns=Columns(headers), pagination_options=PaginationLength(DISCORD_MAX_MESSAGE_LENGTH),
                            enclosure_start='```', enclosure_end='```')
        duration = 0
        # The now playing message should show as a distinct message since you want the embed of the video played right under that message
        # and then before the rest of the queue is shown
        if self.current_media_download:
            duration = int(self.current_media_download.duration) if self.current_media_download.duration else 0
        for (count, item) in enumerate(queue_items):
            uploader = item.uploader or ''
            delta = timedelta(seconds=duration)
            delta_string = sub(r'^0:(?=\d{2}:\d{2})', '', str(delta))
            duration += int(item.duration) if item.duration else 0
            table.add_row([
                f'{count + 1}',
                f'{delta_string}',
                f'{item.title}',
                f'{uploader}',
            ])
        # Manually add code block formatting to table output
        table_output = table.render()
        if not isinstance(table_output, list):
            table_output = [table_output]
        return items + table_output

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
        if not self.guild.voice_client:
            # Turn off reconnect
            # If bot is having issues this just ends up connecting and reconnecting over and over
            # Tends to be more annoying that anything
            await channel.connect()
            return True
        if self.guild.voice_client.channel and self.guild.voice_client.channel.id == channel.id:
            return True
        await self.guild.voice_client.move_to(channel)
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

    def trigger_prefetch(self):
        '''
        Fire a non-blocking prefetch task to pre-stage the next items in the
        queue from S3.  Replaces any previous prefetch task reference so cleanup
        can cancel it.  No-op in local mode or when prefetch_limit is 0.
        '''
        if self.broker and self.prefetch_limit > 0:
            self._prefetch_task = asyncio.create_task(asyncio.to_thread(
                self.broker.prefetch,
                self.get_queue_items(), self.guild.id, self.file_dir, self.prefetch_limit,
            ))

    def add_to_play_queue(self, source_download: MediaDownload) -> bool:
        '''
        Add source download to this play queue
        '''
        self._play_queue.put_nowait(source_download)
        return True

    def check_queue_empty(self) -> bool:
        '''
        Check if queue is empty
        '''
        return self._play_queue.empty()

    def clear_queue(self) -> List[MediaDownload]:
        '''
        Clear queue and return items
        '''
        items = self._play_queue.clear()
        for item in items:
            if self.broker:
                self.broker.remove(str(item.media_request.uuid))
        return items

    def shuffle_queue(self) -> bool:
        '''
        Shuffle play queue
        '''
        self._play_queue.shuffle()
        return True

    def remove_queue_item(self, queue_index: int) -> MediaDownload:
        '''
        Remove item from queue
        '''
        return self._play_queue.remove_item(queue_index)

    def bump_queue_item(self, queue_index: int) -> MediaDownload:
        '''
        Bump queue item
        '''
        return self._play_queue.bump_item(queue_index)

    def get_queue_items(self) -> List[MediaDownload]:
        '''
        Get a copy of the queue items
        '''
        return self._play_queue.items()

    def get_history_items(self) -> List[MediaDownload]:
        '''
        Get a copy of the history items
        '''
        return self._history.items()

    def check_history_empty(self) -> bool:
        '''
        Check if history is empty
        '''
        return self._history.empty()

    def get_file_paths(self) -> List[Path]:
        '''
        Get base paths of for player
        '''
        items = []
        if self.current_media_download:
            items.append(self.current_media_download.file_path)
        for item in self._play_queue.items():
            items.append(item.file_path)
        return items

    async def cleanup(self):
        '''
        Cleanup all resources for player
        '''
        self.logger.info(f'Clearing out resources for player in {self.guild.id}')
        self._play_queue.block()
        cleanup_source(self.current_audio_source)
        if self.broker and self.current_media_download:
            self.broker.release(str(self.current_media_download.media_request.uuid))
        # Delete any messages from download queue
        # Delete any files in play queue that are already added
        while True:
            try:
                media_download = self._play_queue.get_nowait()
                self.logger.debug(f'Removing item {media_download} from play queue')
                if self.broker:
                    self.broker.remove(str(media_download.media_request.uuid))
            except QueueEmpty:
                break

        # Clear out all the queues
        self.logger.debug('Calling clear on queues and queue messages')
        self._history.clear()
        self._play_queue.clear()
        # Clear any messages in the current queue
        self.np_message = ''

        if self._prefetch_task and not self._prefetch_task.done():
            self._prefetch_task.cancel()
            self._prefetch_task = None
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
