from asyncio import Event, QueueEmpty, QueueFull, TimeoutError as asyncio_timeout
from datetime import timedelta
from pathlib import Path
from traceback import format_exc
from typing import List

from async_timeout import timeout
from dappertable import DapperTable
from discord import FFmpegPCMAudio
from discord.errors import ClientException, NotFound


from discord_bot.exceptions import ExitEarlyException
from discord_bot.cogs.music_helpers.source_download import SourceDownload
from discord_bot.utils.queue import Queue
from discord_bot.utils.common import retry_discord_message_command

class MusicPlayer:
    '''
    A class which is assigned to each guild using the bot for Music.

    This class implements a queue and loop, which allows for different guilds
    to listen to different playlists simultaneously.

    When the bot disconnects from the Voice it's instance will be destroyed.
    '''

    def __init__(self, bot, guild, cog_cleanup, text_channel, voice_channel, logger,
                 queue_max_size, disconnect_timeout, file_dir: Path):
        '''
        file_dir : Files for guild stored here
        '''
        self.bot = bot
        self.logger = logger
        self.guild = guild
        self.text_channel = text_channel
        self.voice_channel = voice_channel
        self.cog_cleanup = cog_cleanup
        self.disconnect_timeout = disconnect_timeout
        self.file_dir = file_dir

        # Queues
        self.play_queue = Queue(maxsize=queue_max_size)
        self.history = Queue(maxsize=queue_max_size)
        self.next = Event()

        # Keep these for later
        self._player_task = None
        self.current_track_duration = 0
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

    async def should_delete_messages(self):
        '''
        Check if known queue messages match whats in channel history
        '''
        num_messages = len(self.queue_messages)
        history = [message async for message in self.text_channel.history(limit=num_messages)]
        for (count, hist_item) in enumerate(history):
            mess = self.queue_messages[num_messages - 1 - count]
            if mess.id != hist_item.id:
                return True
        return False

    async def clear_queue_messages(self):
        '''
        Delete queue messages
        '''
        for queue_message in self.queue_messages:
            await retry_discord_message_command(queue_message.delete)
        self.queue_messages = []

    def get_queue_message(self):
        '''
        Get full queue message
        '''
        items = []
        if self.np_message:
            items.append(self.np_message)
        queue_items = self.play_queue.items()
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
        duration = self.current_track_duration
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

    async def move_queue_message_channel(self, new_channel):
        '''
        Move queue messages to new text channel
        '''
        self.logger.debug(f'Music :: Moving queue messages in guild {self.guild.id} from channel {self.text_channel.id} to channel {new_channel.id}')
        new_messages = []
        for message in self.queue_messages:
            new_messages.append(await retry_discord_message_command(new_channel.send, message.content))
        for queue_message in self.queue_messages:
            try:
                await retry_discord_message_command(queue_message.delete)
            except NotFound:
                pass
        self.queue_messages = new_messages
        self.text_channel = new_channel

    async def update_queue_strings(self):
        '''
        Update queue message in channel
        '''
        delete_messages = await self.should_delete_messages()
        self.logger.debug(f'Music :: Updating queue messages in channel {self.text_channel.id} in guild {self.guild.id}')
        new_queue_strings = self.get_queue_message() or []
        if delete_messages:
            for queue_message in self.queue_messages:
                try:
                    await retry_discord_message_command(queue_message.delete)
                except NotFound:
                    pass
            self.queue_messages = []
        elif len(self.queue_messages) > len(new_queue_strings):
            for _ in range(len(self.queue_messages) - len(new_queue_strings)):
                queue_message = self.queue_messages.pop(-1)
                await retry_discord_message_command(queue_message.delete)
        for (count, queue_message) in enumerate(self.queue_messages):
            # Check if queue message is the same before updating
            if queue_message.content == new_queue_strings[count]:
                continue
            await retry_discord_message_command(queue_message.edit, content=new_queue_strings[count])
        if len(self.queue_messages) < len(new_queue_strings):
            for table in new_queue_strings[-(len(new_queue_strings) - len(self.queue_messages)):]:
                self.queue_messages.append(await retry_discord_message_command(self.text_channel.send, table))

    def set_next(self, *_args, **_kwargs):
        '''
        Used for loop to call once voice channel done
        '''
        self.logger.info(f'Music :: Set next called on player in guild "{self.guild.id}"')
        self.next.set()

    async def player_loop(self):
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

    def add_to_play_queue(self, source_download: SourceDownload) -> bool:
        '''
        Add source download to this play queue
        '''
        self.play_queue.put_nowait(source_download)
        return True

    def check_queue_empty(self) -> bool:
        '''
        Check if queue is empty
        '''
        return self.play_queue.empty()

    def clear_queue(self) -> List[SourceDownload]:
        '''
        Clear queue and return items
        '''
        return self.play_queue.clear()

    def get_symlinks(self) -> List[Path]:
        '''
        Get base paths of symlinks for player
        '''
        items = []
        for item in self.play_queue.items():
            items.append(item.base_path)
        return items

    async def __player_loop(self):
        '''
        Player loop logic
        '''
        self.next.clear()

        try:
            # Wait for the next video. If we timeout cancel the player and disconnect...
            async with timeout(self.disconnect_timeout):
                source = await self.play_queue.get()
        except asyncio_timeout:
            self.logger.info(f'Music :: bot reached timeout on queue in guild "{self.guild.id}"')
            await self.destroy()
            raise ExitEarlyException('Bot timeout, exiting') #pylint:disable=raise-missing-from

        # Double check file didnt go away
        if not source.file_path.exists():
            await retry_discord_message_command(self.text_channel.send, f'Unable to play "{source.title}", local file dissapeared')
            return

        self.current_track_duration = source.duration

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
        await self.update_queue_strings()

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
        if not self.video_skipped:
            try:
                self.history.put_nowait(source)
            except QueueFull:
                await self.history.get()
                self.history.put_nowait(source)

        # If play queue empty, set np message to nill
        if self.play_queue.empty():
            await self.update_queue_strings()

    async def cleanup(self):
        '''
        Cleanup all resources for player
        '''
        self.logger.info(f'Music :: Clearing out resources for player in {self.guild.id}')
        self.play_queue.block()
        # Delete any messages from download queue
        # Delete any files in play queue that are already added
        while True:
            try:
                source = self.play_queue.get_nowait()
                self.logger.debug(f'Music :: Removing item {source} from play queue')
                source.delete()
            except QueueEmpty:
                break

        # Grab history items
        history_items = []
        while True:
            try:
                item = self.history.get_nowait()
                self.logger.debug(f'Music :: Gathering history item {item} from history queue')
                # If item wasn't history originally, track it for the history playlist
                if not item.source_dict.added_from_history:
                    history_items.append(item)
            except QueueEmpty:
                break
        # Clear out all the queues
        self.logger.debug('Music :: Calling clear on queues and queue messages')
        self.history.clear()
        self.play_queue.clear()
        # Clear any messages in the current queue
        self.np_message = ''
        for queue_message in self.queue_messages:
            # Ignore delete if message not found
            try:
                await retry_discord_message_command(queue_message.delete)
            except NotFound:
                pass
        self.queue_messages = []

        if self._player_task:
            self._player_task.cancel()
            self._player_task = None
        return history_items

    async def destroy(self):
        '''
        Disconnect and cleanup the player.
        '''
        self.logger.info(f'Music :: Removing music bot from guild id {self.guild.id}')
        await self.cog_cleanup()
