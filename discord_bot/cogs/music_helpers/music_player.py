from asyncio import Event, QueueEmpty, QueueFull, TimeoutError as async_timeout
from datetime import timedelta
from logging import RootLogger
from pathlib import Path
from re import sub
from time import time
from typing import List

from async_timeout import timeout
from dappertable import DapperTable, DapperTableHeader, DapperTableHeaderOptions, PaginationLength
from discord import FFmpegPCMAudio
from discord.ext.commands import Context
from discord.errors import ClientException

from discord_bot.common import DISCORD_MAX_MESSAGE_LENGTH
from discord_bot.cogs.music_helpers.common import MultipleMutableType
from discord_bot.exceptions import ExitEarlyException
from discord_bot.cogs.music_helpers.history_playlist_item import HistoryPlaylistItem
from discord_bot.cogs.music_helpers.media_download import MediaDownload
from discord_bot.cogs.music_helpers.message_queue import MessageQueue
from discord_bot.utils.queue import Queue
from discord_bot.utils.common import return_loop_runner

class MusicPlayer:
    '''
    A class which is assigned to each guild using the bot for Music.

    This class implements a queue and loop, which allows for different guilds
    to listen to different playlists simultaneously.

    When the bot disconnects from the Voice it's instance will be destroyed.
    '''

    def __init__(self, logger: RootLogger, ctx: Context,
                 queue_max_size: int, disconnect_timeout: int, file_dir: Path,
                 message_queue: MessageQueue,
                 history_playlist_id: int,
                 history_playlist_queue: Queue):
        '''
        file_dir : Files for guild stored here
        '''
        self.bot = ctx.bot
        self.guild = ctx.guild
        self.text_channel = ctx.channel
        self.logger = logger

        self.disconnect_timeout = disconnect_timeout
        self.file_dir = file_dir

        # Queues
        self._play_queue = Queue(maxsize=queue_max_size)
        self._history = Queue(maxsize=queue_max_size)
        self.next = Event()
        self.message_queue = message_queue

        # History playlist
        self.history_playlist_id = history_playlist_id
        self.history_playlist_queue = history_playlist_queue

        # Tasks
        self._player_task = None

        # Random things to store
        self.current_source = None
        self.np_message = ''
        self.video_skipped = False
        self.queue_messages = [] # Show current queue
        self.volume = 0.5
        # Shutdown called externally
        self.shutdown_called = False
        # Inactive timestamp for bot timeout
        self.inactive_timestamp = None

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
                source = await self._play_queue.get()
        except async_timeout as e:
            self.logger.info(f'Bot reached timeout on queue in guild "{self.guild.id}"')
            self.destroy()
            raise ExitEarlyException('MusicPlayer hit async timeout on player wait') from e
        self.current_source = source

        audio_source = FFmpegPCMAudio(str(source.file_path))
        self.video_skipped = False
        audio_source.volume = self.volume
        try:
            self.guild.voice_client.play(audio_source, after=self.set_next)
        except (AttributeError, ClientException) as e:
            self.logger.info(f'No voice found, disconnecting from guild {self.guild.id}')
            self.np_message = ''
            if not self.shutdown_called:
                self.destroy()
            raise ExitEarlyException('No voice client in guild, ending loop') from e
        self.logger.info(f'Now playing "{source.webpage_url}" requested '
                            f'by "{source.media_request.requester_id}" in guild {self.guild.id}, url '
                            f'"{source.webpage_url}"')
        self.np_message = f'Now playing {source.webpage_url} requested by {source.media_request.requester_name}'
        self.message_queue.update_multiple_mutable(
            f'{MultipleMutableType.PLAY_ORDER.value}-{self.guild.id}',
            self.text_channel
        )

        await self.next.wait()
        self.np_message = ''
        # Make sure the FFmpeg process is cleaned up.
        try:
            audio_source.cleanup()
        except ValueError:
            # Check if file is closed
            pass
        # Cleanup source files, if cache not enabled delete base/original as well
        source.delete()

        # Add video to history if possible
        # Add here to history playlist queue to save items for metrics as well
        # Check on the other side if this was added from history
        if not self.video_skipped:
            if self.history_playlist_id:
                self.history_playlist_queue.put_nowait(HistoryPlaylistItem(self.history_playlist_id, source))

            try:
                self._history.put_nowait(source)
            except QueueFull:
                await self._history.get()
                self._history.put_nowait(source)

        # Make sure we delete queue messages if nothing left
        if self._play_queue.empty():
            self.message_queue.update_multiple_mutable(
                f'{MultipleMutableType.PLAY_ORDER.value}-{self.guild.id}',
                self.text_channel
            )

    def get_queue_order_messages(self):
        '''
        Get full queue message
        '''
        items = []
        if self.np_message:
            items.append(self.np_message)
        queue_items = self._play_queue.items()
        if not queue_items:
            return items
        headers = [
            DapperTableHeader('Pos', 3, zero_pad_index=True),
            DapperTableHeader('Wait Time', 9),
            DapperTableHeader('Title', 48),
            DapperTableHeader('Uploader', 48)
        ]
        table = DapperTable(header_options=DapperTableHeaderOptions(headers), pagination_options=PaginationLength(DISCORD_MAX_MESSAGE_LENGTH))
        duration = 0
        if self.current_source:
            duration = int(self.current_source.duration) if self.current_source.duration else 0
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
        for t in table.print():
            items.append(f'```{t}```')
        return items

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
            item.delete()
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
        if self.current_source:
            items.append(self.current_source.base_path)
        for item in self._play_queue.items():
            items.append(item.base_path)
        return items

    async def cleanup(self):
        '''
        Cleanup all resources for player
        '''
        self.logger.info(f'Clearing out resources for player in {self.guild.id}')
        self._play_queue.block()
        # Delete any messages from download queue
        # Delete any files in play queue that are already added
        while True:
            try:
                source = self._play_queue.get_nowait()
                self.logger.debug(f'Removing item {source} from play queue')
                source.delete()
            except QueueEmpty:
                break

        # Clear out all the queues
        self.logger.debug('Calling clear on queues and queue messages')
        self._history.clear()
        self._play_queue.clear()
        # Clear any messages in the current queue
        self.np_message = ''

        if self._player_task:
            self._player_task.cancel()
            self._player_task = None
        return True

    def destroy(self):
        '''
        Disconnect and cleanup the player.
        '''
        self.logger.info(f'Removing music bot from guild id {self.guild.id}')
        self.shutdown_called = True
