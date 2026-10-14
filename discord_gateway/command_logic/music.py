from logging import Logger

from dappertable import DapperTable, Columns, Column, PaginationLength

from discord_core.clients.dispatch_client_base import DispatchClientBase
from discord_core.common import DISCORD_MAX_MESSAGE_LENGTH
from discord_core.interfaces.broker_client_protocol import BrokerClient
from discord_core.interfaces.database_protocols import GuildAnalyticsStore

from discord_gateway.command_logic.envelope import CommandEnvelope
from discord_gateway.command_logic.player_gateway import PlayerGateway, PlayerStatus


class MusicCommands:
    '''
    The music commands that reach the player only through ``PlayerGateway``. Discord-free: see
    ``command_logic.envelope``.

    ``check_author_voice_chat`` and ``check_player_active`` are public because the music commands
    that still live on the cog call them too.
    '''

    def __init__(self, dispatcher: DispatchClientBase, broker_client: BrokerClient,
                 guild_analytics_store: GuildAnalyticsStore | None, player_gateway: PlayerGateway,
                 logger: Logger, message_delete_after: int):
        self.dispatcher = dispatcher
        self.broker_client = broker_client
        self.guild_analytics_store = guild_analytics_store
        self.player_gateway = player_gateway
        self.logger = logger
        self.message_delete_after = message_delete_after

    def _send(self, guild_id: int, channel_id: int, content: str) -> None:
        self.dispatcher.send_message(guild_id, channel_id, content,
                                     delete_after=self.message_delete_after)

    async def check_author_voice_chat(self, env: CommandEnvelope, check_voice_chats: bool = True) -> int | None:
        '''
        Check the command author is in a voice channel, returning its id.

        With ``check_voice_chats`` it must also be in this guild. Tells the channel and returns None
        when the check fails.
        '''
        if env.author_voice_channel_id is None:
            self._send(env.guild_id, env.channel_id,
                       f'{env.author_display_name} not in voice chat channel. Please join one and try again')
            return None

        if not check_voice_chats:
            return env.author_voice_channel_id

        if env.author_voice_guild_id != env.guild_id:
            self._send(env.guild_id, env.channel_id, 'User not joined to channel bot is in, ignoring command')
            return None
        return env.author_voice_channel_id

    async def check_player_active(self, env: CommandEnvelope) -> PlayerStatus | None:
        '''
        Check the guild has a player that is connected and either playing or about to, returning
        its status. Tells the channel and returns None when it does not.
        '''
        status = await self.player_gateway.status(env.guild_id)
        if not status.exists:
            self._send(env.guild_id, env.channel_id, 'I am not currently playing anything')
            return None
        if not status.voice_connected or (not status.is_playing and not status.pending_downloads):
            # The player's own channel, not the one the command came from: that is where this
            # message has always gone once a player exists.
            self._send(env.guild_id, status.text_channel_id, 'I am not currently playing anything')
            return None
        return status

    async def _queued_items(self, env: CommandEnvelope):
        '''
        The gate the queue-editing commands share: author in voice, player active, something
        queued. Returns the queued tracks, or None after telling the channel why not.
        '''
        if not await self.check_author_voice_chat(env):
            return None
        if not await self.check_player_active(env):
            return None
        queue = await self.broker_client.get_guild_queue(env.guild_id)
        if not queue.items:
            self._send(env.guild_id, env.channel_id, 'There are currently no more queued videos.')
            return None
        return queue.items

    def _queue_index(self, env: CommandEnvelope, queue_index: str) -> int | None:
        '''The index the user typed, or None after telling the channel it is not a number.'''
        try:
            return int(queue_index)
        except ValueError:
            self._send(env.guild_id, env.channel_id, f'Invalid queue index {queue_index}')
            return None

    async def music_stats(self, env: CommandEnvelope) -> None:
        '''Show the guild's play totals.'''
        if not self.guild_analytics_store:
            self.dispatcher.send_message(env.guild_id, env.channel_id,
                                         'Functionality not available, database is not enabled')
            return

        # An entry, not a row: every read below happened inside the session
        # block that loaded it, which is exactly what could not survive the seam.
        guild_analytics = await self.guild_analytics_store.get_analytics(env.guild_id)
        hours = guild_analytics.total_duration_seconds // 3600
        minutes = (guild_analytics.total_duration_seconds % 3600) // 60
        seconds = guild_analytics.total_duration_seconds % 60
        message = f'```Music Stats for Server\nTotal Plays: {guild_analytics.total_plays}\nCached Plays: {guild_analytics.cached_plays}\n' \
                f'Total Time Played: {guild_analytics.total_duration_days} days, {hours} hours, {minutes} minutes, and {seconds} seconds\n' \
                f'Tracked Since: {guild_analytics.created_at.strftime("%Y-%m-%d %H:%M:%S")} UTC\n```'
        self._send(env.guild_id, env.channel_id, message)

    async def history(self, env: CommandEnvelope) -> None:
        '''Show recently played videos.'''
        if not await self.check_author_voice_chat(env):
            return
        history = await self.broker_client.get_guild_history(env.guild_id)

        if not history:
            self._send(env.guild_id, env.channel_id, 'There have been no videos played.')
            return

        headers = [
            Column('Pos', 3, zero_pad=True),
            Column('Title', 40),
            Column('Uploader', 40)
        ]
        table = DapperTable(columns=Columns(headers), pagination_options=PaginationLength(DISCORD_MAX_MESSAGE_LENGTH),
                            enclosure_start='```', enclosure_end='```', prefix='History\n')
        for (count, item) in enumerate(history):
            uploader = item.get('uploader') or ''
            table.add_row([
                f'{count + 1}',
                f"{item.get('title')}",
                f'{uploader}',
            ])
        for mess in table.render():
            self._send(env.guild_id, env.channel_id, mess)

    async def join(self, env: CommandEnvelope) -> None:
        '''Join the author's voice channel.'''
        voice_channel_id = await self.check_author_voice_chat(env, check_voice_chats=False)
        if not voice_channel_id:
            return

        result = await self.player_gateway.ensure(env.guild_id, voice_channel_id, env.channel_id)
        if result.error:
            self._send(env.guild_id, env.channel_id, result.error)

        # Said even when the join failed, as it always has been.
        self._send(env.guild_id, env.channel_id, f'Connected to: {env.author_voice_channel_name}')

    async def skip(self, env: CommandEnvelope) -> None:
        '''Skip the current video.'''
        if not await self.check_author_voice_chat(env):
            return
        if not await self.check_player_active(env):
            return

        current_title = await self.player_gateway.skip(env.guild_id)
        if current_title is None:
            return
        self._send(env.guild_id, env.channel_id, f'Skipping video "{current_title}"')

    async def stop(self, env: CommandEnvelope) -> None:
        '''Stop playback and leave voice.'''
        if not await self.check_author_voice_chat(env):
            return
        self.logger.info(f'Stop command called for guild {env.guild_id}')
        await self.player_gateway.stop(env.guild_id, env.channel_id)

    async def move_messages(self, env: CommandEnvelope) -> None:
        '''Move the queue messages to the channel the command came from.'''
        if not await self.check_author_voice_chat(env):
            return
        status = await self.check_player_active(env)
        if not status:
            return

        if env.channel_id == status.text_channel_id:
            self._send(env.guild_id, env.channel_id,
                       f'I am already sending messages to channel {env.channel_name}')
            return
        # The broker owns the play-order message, so telling it the new channel moves it
        # (deletes the old message, sends it again in the new channel).
        await self.broker_client.open_guild(env.guild_id, env.channel_id)

        # Update the player's text channel reference, and the session a restart would rejoin from
        await self.player_gateway.move_messages(env.guild_id, env.channel_id)

    async def clear(self, env: CommandEnvelope) -> None:
        '''Clear every queued item.'''
        items = await self._queued_items(env)
        if items is None:
            return
        self.logger.info(f'Player clear called in guild {env.guild_id}')
        await self.broker_client.clear_queue(env.guild_id)
        # Nothing is left to prefetch, so only the staged copies need to go.
        await self.player_gateway.queue_changed(env.guild_id, items, prefetch=False)
        self._send(env.guild_id, env.channel_id, 'Cleared player queue')

    async def shuffle(self, env: CommandEnvelope) -> None:
        '''Shuffle the queue.'''
        if await self._queued_items(env) is None:
            return
        await self.broker_client.shuffle_queue(env.guild_id)
        # The head of the queue changed, so the files worth having on disk did too.
        await self.player_gateway.queue_changed(env.guild_id, [])

    async def remove(self, env: CommandEnvelope, queue_index: str) -> None:
        '''Remove the item at ``queue_index``.'''
        items = await self._queued_items(env)
        if items is None:
            return
        index = self._queue_index(env, queue_index)
        if index is None:
            return
        # The broker works by track, not position: the index names the track the user saw, and
        # if it has moved or started playing since, nothing is changed.
        item = None
        if 1 <= index <= len(items):
            item = await self.broker_client.remove_queued_track(
                env.guild_id, str(items[index - 1].media_request.uuid))
        if item is None:
            self._send(env.guild_id, env.channel_id, f'Unable to remove queue index {index}')
            return
        self._send(env.guild_id, env.channel_id, f'Removed item {item.title} from queue')
        await self.player_gateway.queue_changed(env.guild_id, [item])

    async def bump(self, env: CommandEnvelope, queue_index: str) -> None:
        '''Bump the item at ``queue_index`` to the top of the queue.'''
        items = await self._queued_items(env)
        if items is None:
            return
        index = self._queue_index(env, queue_index)
        if index is None:
            return
        # The broker works by track, not position: the index names the track the user saw, and
        # if it has moved or started playing since, nothing is changed.
        item = None
        if 1 <= index <= len(items):
            item = await self.broker_client.bump_queued_track(
                env.guild_id, str(items[index - 1].media_request.uuid))
        if item is None:
            self._send(env.guild_id, env.channel_id, f'Unable to bump queue index {index}')
            return
        self._send(env.guild_id, env.channel_id, f'Bumped item "{item.title}" to top of queue')
        await self.player_gateway.queue_changed(env.guild_id, [])
