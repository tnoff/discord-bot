import random
from logging import Logger
from time import time

from dappertable import shorten_string, DapperTable, Columns, Column, PaginationLength

from discord_core.clients.http_playlist_store import HttpPlaylistStore
from discord_core.clients.search_client import check_youtube_video
from discord_core.cogs.music_helpers.common import PLAYHISTORY_PREFIX, SearchType
from discord_core.common import DISCORD_MAX_MESSAGE_LENGTH
from discord_core.interfaces.broker_client_protocol import BrokerClient
from discord_core.interfaces.player_gateway import PlayerGateway
from discord_core.types.command_envelope import CommandEnvelope
from discord_core.types.media_request import MediaRequest
from discord_core.types.playlist import PlaylistItemAddStatus, PlaylistItemWrite
from discord_core.types.search import SearchResult
from discord_core.utils.media_enqueuer import MediaEnqueuer
from discord_core.utils.playlist_view import playlist_public_view

from discord_commands.command_logic.reply import Reply

HISTORY_PLAYLIST_NAME = 'Channel History'


class PlaylistCommands:
    '''
    The ``!playlist`` subcommands. Discord-free: see ``discord_core.types.command_envelope``.

    ``create``, ``list``, ``show``, ``delete``, ``rename``, ``item-remove`` and ``merge`` only touch
    the playlist store. ``save-queue`` and ``save-history`` read the broker's queue and history, and
    ask the player only whether one exists. ``item-add`` and ``queue_items`` hand their requests to
    the ``MediaEnqueuer``. ``find`` and ``queue_items`` are public because ``MusicCommands``
    (``playlist queue``, ``random-play``) calls them.

    Annotated against the concrete HTTP store, not the ``PlaylistStore`` Protocol: the Protocol's
    module imports types that need discord.py, which this pod does not install.
    '''

    def __init__(self, reply: Reply, logger: Logger, playlist_store: HttpPlaylistStore,
                 message_delete_after: int, server_playlist_max_size: int,
                 broker_client: BrokerClient, player_gateway: PlayerGateway, enqueuer: MediaEnqueuer):
        self.reply = reply
        self.logger = logger
        self.playlist_store = playlist_store
        self.broker_client = broker_client
        self.player_gateway = player_gateway
        self.enqueuer = enqueuer
        self.message_delete_after = message_delete_after
        self.server_playlist_max_size = server_playlist_max_size

    async def _say(self, env: CommandEnvelope, content: str) -> None:
        '''Reply, and let Discord remove the message after ``message_delete_after`` seconds.'''
        await self.reply(env.guild_id, env.channel_id, content, delete_after=self.message_delete_after)

    async def _say_kept(self, env: CommandEnvelope, content: str) -> None:
        '''Reply without the delete timer, for the messages that never had one.'''
        await self.reply(env.guild_id, env.channel_id, content)

    async def find(self, env: CommandEnvelope, playlist_index: int | str) -> tuple[int | None, bool]:
        '''
        Resolve the index users see (0 is the history playlist, 1 the first of the guild's own
        playlists) to ``(playlist id, is_history)``. Gives ``(None, False)`` after telling the
        channel why there is no such playlist.
        '''
        try:
            index = int(playlist_index)
        except ValueError:
            await self._say(env, f'Invalid playlist index {playlist_index}')
            return None, False

        playlist = None
        if index == 0:
            playlist = await self.playlist_store.get_history_playlist(env.guild_id)
        elif index > 0:
            guild_playlists = await self.playlist_store.list_playlists(env.guild_id)
            if not guild_playlists:
                await self._say(env, 'No playlists in database')
                return None, False
            playlist = guild_playlists[index - 1] if index <= len(guild_playlists) else None
        if not playlist:
            await self._say(env, f'Invalid playlist index {playlist_index}')
            return None, False
        return playlist.id, index == 0

    async def create(self, env: CommandEnvelope, name: str) -> int | None:
        '''
        Create a playlist called ``name``, returning its id, or None after telling the channel why
        not: the name is reserved or taken.
        '''
        playlist_name = shorten_string(name, 256)
        if PLAYHISTORY_PREFIX in playlist_name.lower():
            await self._say_kept(env, f'Unable to create playlist "{name}", name cannot contain {PLAYHISTORY_PREFIX}')
            return None
        if await self.playlist_store.get_playlist_by_name(env.guild_id, playlist_name):
            await self._say_kept(env, f'Unable to create playlist "{name}", a playlist with that name already exists')
            return None

        playlist = await self.playlist_store.create_playlist(env.guild_id, name)
        self.logger.info(f'Playlist created "{playlist_name}" with id {playlist.id} in guild {env.guild_id}')
        public_id = await playlist_public_view(self.playlist_store, playlist.id, env.guild_id)
        await self._say(env, f'Created playlist "{playlist_name}" with ID {public_id}')
        return playlist.id

    async def list_playlists(self, env: CommandEnvelope) -> None:
        '''List the guild's playlists, the history playlist first.'''
        history_playlist = await self.playlist_store.get_history_playlist(env.guild_id)
        playlists = list(await self.playlist_store.list_playlists(env.guild_id))
        if history_playlist:
            playlists.insert(0, history_playlist)
        if not playlists:
            await self._say(env, 'No playlists in database')
            return

        table = DapperTable(
            columns=Columns([Column('ID', 3), Column('Playlist Name', 64), Column('Last Queued', 20)]),
            pagination_options=PaginationLength(DISCORD_MAX_MESSAGE_LENGTH),
            enclosure_start='```', enclosure_end='```', prefix='Playlist List\n')
        for position, playlist in enumerate(playlists):
            last_queued = playlist.last_queued.strftime('%Y-%m-%d %H:%M:%S') if playlist.last_queued else 'N/A'
            table.add_row([f'{position}', HISTORY_PLAYLIST_NAME if playlist.is_history else playlist.name, last_queued])
        for page in table.render():
            await self._say(env, page)

    async def item_remove(self, env: CommandEnvelope, playlist_index: int, video_index: int) -> None:
        '''Remove the item at ``video_index`` (1-based) from a playlist.'''
        playlist_id, _is_history = await self.find(env, playlist_index)
        if not playlist_id:
            return
        if video_index < 1:
            await self._say(env, f'Invalid item index {video_index}')
            return

        # The entry comes back built, because the title is wanted for the message and the row is
        # gone by then.
        item = await self.playlist_store.delete_item_by_index(playlist_id, video_index - 1)
        public_id = await playlist_public_view(self.playlist_store, playlist_id, env.guild_id)
        if item:
            await self._say(env, f'Removed item "{item.title}" from playlist {public_id}')
            return
        await self._say(env, f'Unable to find item {video_index}')

    async def show(self, env: CommandEnvelope, playlist_index: int) -> None:
        '''Show the items in a playlist.'''
        playlist_id, _is_history = await self.find(env, playlist_index)
        if not playlist_id:
            return

        items = await self.playlist_store.list_items(playlist_id)
        if not items:
            await self._say(env, f'No items in playlist {playlist_id}')
            return
        table = DapperTable(
            columns=Columns([Column('Pos', 3, zero_pad=True), Column('Title', 32), Column('Uploader', 32)]),
            pagination_options=PaginationLength(DISCORD_MAX_MESSAGE_LENGTH),
            enclosure_start='```', enclosure_end='```', prefix=f'Playlist {playlist_index} Items\n')
        # The position shown is the index `!playlist item-remove` takes, so it has to be the same
        # order the store deletes by.
        for position, item in enumerate(items, start=1):
            table.add_row([f'{position}', f'{item.title}', f'{item.uploader or ""}'])
        for page in table.render():
            await self._say(env, page)

    async def delete(self, env: CommandEnvelope, playlist_index: int) -> None:
        '''Delete a playlist. The history playlist is reserved.'''
        playlist_id, is_history = await self.find(env, playlist_index)
        if not playlist_id:
            await self._say(env, f'Cannot delete playlist, unable to find id {playlist_index}')
            return
        if is_history:
            await self._say(env, 'Cannot delete history playlist, is reserved')
            return
        await self._delete_playlist(playlist_id)
        await self._say(env, f'Deleted playlist {playlist_index}')

    async def _delete_playlist(self, playlist_id: int) -> None:
        self.logger.info(f'Deleting playlist items "{playlist_id}"')
        await self.playlist_store.delete_playlist(playlist_id)

    async def rename(self, env: CommandEnvelope, playlist_index: int, playlist_name: str) -> None:
        '''Rename a playlist. The history playlist is reserved.'''
        playlist_id, is_history = await self.find(env, playlist_index)
        if is_history:
            await self._say(env, 'Cannot rename history playlist, is reserved')
            return
        if not playlist_id:
            return

        playlist_name = shorten_string(playlist_name, 256)
        if PLAYHISTORY_PREFIX in playlist_name.lower():
            await self._say_kept(env, f'Unable to create playlist "{playlist_name}", name cannot contain {PLAYHISTORY_PREFIX}')
            return

        self.logger.info(f'Renaming playlist {playlist_id} to name "{playlist_name}"')
        await self.playlist_store.rename_playlist(playlist_id, playlist_name)
        await self._say(env, f'Renamed playlist {playlist_index} to name "{playlist_name}"')

    async def merge(self, env: CommandEnvelope, playlist_index_one: str, playlist_index_two: str) -> None:
        '''Merge the second playlist into the first, then delete the second.'''
        self.logger.info(f'Calling playlist merge of "{playlist_index_one}" and "{playlist_index_two}" in server "{env.guild_id}"')
        target_id, target_is_history = await self.find(env, playlist_index_one)
        source_id, source_is_history = await self.find(env, playlist_index_two)
        if target_is_history or source_is_history:
            await self._say(env, 'Cannot merge history playlist, is reserved')
            return
        if not target_id:
            await self._say(env, f'Cannot find playlist {playlist_index_one}')
            return
        if not source_id:
            await self._say(env, f'Cannot find playlist {playlist_index_two}')
            return

        source_items = await self.playlist_store.list_items(source_id)
        outcomes = await self.playlist_store.add_items(
            target_id,
            [PlaylistItemWrite(video_url=item.video_url, title=item.title, uploader=item.uploader)
             for item in source_items],
            self.server_playlist_max_size)
        for outcome in outcomes:
            if outcome.status == PlaylistItemAddStatus.PLAYLIST_FULL:
                await self._say(env, f'Cannot add more items to playlist "{target_id}", already max size')
                # Returning rather than breaking: a merge that could not take every item must not
                # then delete the source.
                return
            if outcome.status == PlaylistItemAddStatus.ADDED:
                await self._say(env, f'Added item "{outcome.title}" to playlist {playlist_index_one}')
                continue
            await self._say(env, f'Unable to add playlist item "{outcome.title}", likely already exists')
        await self._delete_playlist(source_id)

    async def save(self, env: CommandEnvelope, name: str, is_history: bool = False) -> None:
        '''Save the guild's queue, or its play history, to a new playlist called ``name``.'''
        playlist_id = await self.create(env, name)
        if not playlist_id:
            return

        if not (await self.player_gateway.status(env.guild_id)).exists:
            await self._say(env, 'No player connected, no queue to save')
            return

        # Snapshot first, so the queue changing under us cannot change what is saved.
        if is_history:
            queue_copy = [PlaylistItemWrite(video_url=item['webpage_url'], title=item.get('title'),
                                            uploader=item.get('uploader'))
                          for item in await self.broker_client.get_guild_history(env.guild_id)]
        else:
            queue_copy = [PlaylistItemWrite(video_url=item.webpage_url, title=item.title, uploader=item.uploader)
                          for item in (await self.broker_client.get_guild_queue(env.guild_id)).items]

        self.logger.info(f'Saving queue contents to playlist "{name}", is_history? {is_history}')

        if not queue_copy:
            await self._say(env, 'There are no videos to add to playlist')
            return

        outcomes = await self.playlist_store.add_items(playlist_id, queue_copy, self.server_playlist_max_size)
        for outcome in outcomes:
            if outcome.status == PlaylistItemAddStatus.PLAYLIST_FULL:
                await self._say(env, 'Cannot add more items to playlist, already max size')
                break
            if outcome.status == PlaylistItemAddStatus.ADDED:
                await self._say(env, f'Added item "{outcome.title}" to playlist')
                continue
            await self._say(env, f'Unable to add playlist item "{outcome.title}", likely already exists')
        await self._say(env, f'Finished adding items to playlist "{name}"')

    async def item_add(self, env: CommandEnvelope, playlist_index: int | str, search: str) -> None:
        '''Search for ``search`` and add what it finds to a playlist.'''
        playlist_id, is_history = await self.find(env, playlist_index)
        if not playlist_id:
            return

        if is_history:
            await self._say(env, f'Unable to add "{search}" to history playlist, is reserved and cannot be added to manually')
            return

        await self.enqueuer.generate_media_requests_from_search(env, search, add_to_playlist=playlist_id)

    async def queue_items(self, env: CommandEnvelope, playlist_id: int, shuffle: bool, max_num: int | None,
                          is_history: bool = False) -> None:
        '''Put the playlist's items on the guild's queue, optionally shuffled and capped at ``max_num``.'''
        self.logger.info(f'Playlist queue called for playlist {playlist_id} in server "{env.guild_id}"')

        playlist = await self.playlist_store.get_playlist(playlist_id)
        playlist_name = playlist.name if playlist else None
        items = await self.playlist_store.list_items(playlist_id)
        if is_history:
            playlist_name = HISTORY_PLAYLIST_NAME
        playlist_items = []
        for item in items:
            search_result = SearchResult(search_type=SearchType.YOUTUBE if check_youtube_video(item.video_url) else SearchType.DIRECT,
                                         raw_search_string=item.video_url, proper_name=item.title)
            media_request = MediaRequest(guild_id=env.guild_id,
                                         channel_id=env.channel_id,
                                         requester_name=env.author_display_name,
                                         requester_id=env.author_id,
                                         search_result=search_result,
                                         added_from_history=is_history,
                                         history_playlist_item_id=item.id)
            playlist_items.append(media_request)

        # Check if playlist is empty and provide user feedback
        if not playlist_items:
            await self._say(env, f'Playlist "{playlist_name}" contains no items to queue')
            return

        if shuffle:
            # https://stackoverflow.com/a/51295230
            random.seed(time())
            random.shuffle(playlist_items)

        if max_num:
            if max_num < 0:
                await self._say(env, f'Invalid number of videos {max_num}')
                return
            if max_num < len(playlist_items):
                playlist_items = playlist_items[:max_num]

        bundle_uuid = await self.broker_client.create_bundle(
            env.guild_id, env.channel_id, input_string=playlist_name, has_search_banner=True)
        finished_all = await self.enqueuer.enqueue_media_requests(env, playlist_items, bundle_uuid)

        if not finished_all:
            await self._say(env, f'Added as many videos in playlist "{playlist_name}" to queue as possible, but hit limit')

        await self.playlist_store.mark_queued(playlist_id)
