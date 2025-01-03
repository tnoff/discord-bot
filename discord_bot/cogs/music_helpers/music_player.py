from asyncio import Event, QueueEmpty, QueueFull, TimeoutError as asyncio_timeout
from copy import deepcopy
from datetime import timedelta
from logging import RootLogger
from pathlib import Path
from traceback import format_exc
from typing import Callable, List

from async_timeout import timeout
from dappertable import DapperTable
from discord import FFmpegPCMAudio
from discord.ext.commands import Context
from discord.errors import ClientException


from discord_bot.exceptions import ExitEarlyException
from discord_bot.cogs.music_helpers.source_download import SourceDownload
from discord_bot.utils.queue import Queue

class MusicPlayer:
    '''
    A class which is assigned to each guild using the bot for Music.

    This class implements a queue and loop, which allows for different guilds
    to listen to different playlists simultaneously.

    When the bot disconnects from the Voice it's instance will be destroyed.
    '''

    def __init__(self, logger: RootLogger, ctx: Context, cog_cleanup: Callable,
                 queue_max_size: int, disconnect_timeout: int, file_dir: Path):
        '''
        file_dir : Files for guild stored here
        '''
        self.bot = ctx.Bot
        self.guild = ctx.guild
        self.text_channel = ctx.channel
        self.logger = logger
        self.voice_client = ctx.voice_client
        self.voice_channel = None

        self.cog_cleanup = cog_cleanup
        self.disconnect_timeout = disconnect_timeout
        self.file_dir = file_dir

        # Queues
        self._play_queue = Queue(maxsize=queue_max_size)
        self._history = Queue(maxsize=queue_max_size)
        self.next = Event()

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

    async def start_tasks(self):
        '''
        Start background methods
        '''
        if not self._player_task:
            self._player_task = self.bot.loop.create_task(self.player_loop())

    async def player_loop(self): #pylint:disable=duplicate-code
        '''
        Our main player loop.
        '''
        await self.bot.wait_until_ready()

        while not self.bot.is_closed():
            try:
                await self.__player_loop()
            except ExitEarlyException:
                return
            except Exception as e:
                self.logger.exception(e)
                self.logger.error(format_exc())
                self.logger.error(str(e))
                print(f'Player loop exception {str(e)}')
                print('Formatted exception:', format_exc())

    async def __player_loop(self):
        '''
        Player loop logic
        '''
        self.next.clear()

        try:
            # Wait for the next video. If we timeout cancel the player and disconnect...
            async with timeout(self.disconnect_timeout):
                source = await self._play_queue.get()
        except asyncio_timeout:
            self.logger.info(f'Music :: bot reached timeout on queue in guild "{self.guild.id}"')
            await self.destroy()
            raise ExitEarlyException('Bot timeout, exiting') #pylint:disable=raise-missing-from

        self.current_source = source

        audio_source = FFmpegPCMAudio(str(source.file_path))
        self.video_skipped = False
        audio_source.volume = self.volume
        try:
            self.guild.voice_client.play(audio_source, after=self.set_next) #pylint:disable=line-too-long
        except (AttributeError, ClientException):
            self.logger.info(f'Music :: No voice found, disconnecting from guild {self.guild.id}')
            if not self.shutdown_called:
                await self.destroy()
            raise ExitEarlyException('No voice client in guild, ending loop') #pylint:disable=raise-missing-from
        self.logger.info(f'Music :: Now playing "{source.webpage_url}" requested '
                            f'by "{source.source_dict.requester_id}" in guild {self.guild.id}, url '
                            f'"{source.webpage_url}"')
        self.np_message = f'Now playing {source.webpage_url} requested by {source.source_dict.requester_name}'
        for func in source.post_play_callback_functions:
            func()

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
        if not self.video_skipped and not source.source_dict.added_from_history:
            try:
                self._history.put_nowait(source)
            except QueueFull:
                await self._history.get()
                self._history.put_nowait(source)


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
            {
                'name': 'Pos',
                'length': 3,
            },
            {
                'name': 'Wait Time',
                'length': 9,
            },
            {
                'name': 'Title /// Uploader',
                'length': 80,
            },
        ]
        table = DapperTable(headers, rows_per_message=15)
        duration = 0
        if self.current_source:
            duration = self.current_source.duration
        for (count, item) in enumerate(queue_items):
            uploader = item.uploader or ''
            delta = timedelta(seconds=duration)
            duration += item.duration
            table.add_row([
                f'{count + 1}',
                f'{str(delta)}',
                f'{item.title} /// {uploader}'
            ])
        for t in table.print():
            items.append(f'```{t}```')
        return items

    def set_next(self, *_args, **_kwargs):
        '''
        Used for loop to call once voice channel done
        '''
        self.logger.info(f'Music :: Set next called on player in guild "{self.guild.id}"')
        self.next.set()

    def voice_channel_active(self):
        '''
        Check if voice channel has active users
        '''
        if not self.voice_channel:
            return True
        for member in self.voice_channel.members:
            if member.id != self.bot.user.id:
                return True
        return False

    def add_to_play_queue(self, source_download: SourceDownload) -> bool:
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

    def clear_queue(self) -> List[SourceDownload]:
        '''
        Clear queue and return items
        '''
        return self._play_queue.clear()

    def shuffle_queue(self) -> bool:
        '''
        Shuffle play queue
        '''
        self._play_queue.shuffle()
        return True

    def remove_queue_item(self, queue_index: int) -> SourceDownload:
        '''
        Remove item from queue
        '''
        return self._play_queue.remove_item(queue_index)

    def bump_queue_item(self, queue_index: int) -> SourceDownload:
        '''
        Bump queue item
        '''
        return self._play_queue.bump_item(queue_index)

    def get_queue_items(self) -> List[SourceDownload]:
        '''
        Get a copy of the queue items
        '''
        return self._play_queue.items()

    def get_history_items(self) -> List[SourceDownload]:
        '''
        Get a copy of the history items
        '''
        return self._history.items()

    def check_history_empty(self) -> bool:
        '''
        Check if history is empty
        '''
        return self._history.empty()

    def get_symlinks(self) -> List[Path]:
        '''
        Get base paths of symlinks for player
        '''
        items = []
        if self.current_source:
            items.append(self.current_source.base_path)
        for item in self._play_queue.items():
            items.append(item.base_path)
        return items

    def clear_queue_order_messages(self):
        '''
        Remove items from queue order and return for deletion later
        '''
        items = deepcopy(self.queue_messages)
        self.queue_messages = []
        return items

    async def cleanup(self):
        '''
        Cleanup all resources for player
        '''
        self.logger.info(f'Music :: Clearing out resources for player in {self.guild.id}')
        self._play_queue.block()
        # Delete any messages from download queue
        # Delete any files in play queue that are already added
        while True:
            try:
                source = self._play_queue.get_nowait()
                self.logger.debug(f'Music :: Removing item {source} from play queue')
                source.delete()
            except QueueEmpty:
                break

        # Grab history items
        history_items = []
        while True:
            try:
                item = self._history.get_nowait()
                self.logger.debug(f'Music :: Gathering history item {item} from history queue')
                # If item wasn't history originally, track it for the history playlist
                if not item.source_dict.added_from_history:
                    history_items.append(item)
            except QueueEmpty:
                break
        # Clear out all the queues
        self.logger.debug('Music :: Calling clear on queues and queue messages')
        self._history.clear()
        self._play_queue.clear()
        # Clear any messages in the current queue
        queue_messages = self.clear_queue_order_messages()
        self.np_message = ''

        if self._player_task:
            self._player_task.cancel()
            self._player_task = None
        return history_items, queue_messages

    async def destroy(self):
        '''
        Disconnect and cleanup the player.
        '''
        self.logger.info(f'Music :: Removing music bot from guild id {self.guild.id}')
        await self.cog_cleanup()
