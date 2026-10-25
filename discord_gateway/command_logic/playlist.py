import random
from logging import Logger
from time import time

from dappertable import shorten_string, DapperTable, Columns, Column, PaginationLength

from discord_core.cogs.music_helpers.common import PLAYHISTORY_PREFIX, SearchType
from discord_core.clients.dispatch_client_base import DispatchClientBase
from discord_core.common import DISCORD_MAX_MESSAGE_LENGTH
from discord_core.interfaces.broker_client_protocol import BrokerClient
from discord_core.interfaces.database_protocols import PlaylistStore
from discord_core.types.media_request import MediaRequest
from discord_core.types.playlist import PlaylistItemAddStatus, PlaylistItemWrite
from discord_core.types.search import SearchResult
from discord_core.types.command_envelope import CommandEnvelope
from discord_core.utils.playlist_view import playlist_public_view

from discord_gateway.clients.search_client import check_youtube_video
from discord_gateway.command_logic.enqueue import MediaEnqueuer
from discord_gateway.command_logic.player_gateway import PlayerGateway

PLAYHISTORY_NAME = 'Channel History'


class PlaylistCommands:
    '''
    Playlist commands that only touch the playlist store. Discord-free: see
    ``command_logic.envelope``.

    The resolve/create helpers are public because ``MusicCommands`` (``playlist queue``,
    ``random-play``) calls them. ``save-queue`` and ``save-history`` read the broker's queue
    and history, and ask the player only whether one exists. ``item-add`` and ``queue_items``
    hand their requests to the ``MediaEnqueuer``.

    playlist_store is None when the database is not enabled, which every command
    reports rather than failing on.
    '''

    def __init__(self, dispatcher: DispatchClientBase, playlist_store: PlaylistStore | None,
                 logger: Logger, message_delete_after: int, server_playlist_max_size: int,
                 broker_client: BrokerClient, player_gateway: PlayerGateway, enqueuer: MediaEnqueuer):
        self.dispatcher = dispatcher
        self.playlist_store = playlist_store
        self.broker_client = broker_client
        self.player_gateway = player_gateway
        self.enqueuer = enqueuer
        self.logger = logger
        self.message_delete_after = message_delete_after
        self.server_playlist_max_size = server_playlist_max_size

    def _send(self, env: CommandEnvelope, content: str) -> None:
        self.dispatcher.send_message(env.guild_id, env.channel_id, content,
                                     delete_after=self.message_delete_after)

    def _send_kept(self, env: CommandEnvelope, content: str) -> None:
        '''Send without the delete timer, for the messages that never had one.'''
        self.dispatcher.send_message(env.guild_id, env.channel_id, content)

    async def check_database_session(self, env: CommandEnvelope) -> bool:
        '''
        Check the database is enabled, telling the channel if it is not.
        '''
        if not self.playlist_store:
            self._send_kept(env, 'Functionality not available, database is not enabled')
            return False
        return True

    async def get_playlist_public_view(self, playlist_id: int, guild_id: int):
        '''
        Get playlist by db id, and view which public index servers see it as
        '''
        return await playlist_public_view(self.playlist_store, playlist_id, guild_id)

    async def get_playlist(self, playlist_index: int, env: CommandEnvelope):
        '''
        Get playlist by 'public' index
        public index meaning what the users in the servers see

        Returns (playlist id, is_history); (None, False) after telling the channel why.
        '''
        try:
            index = int(playlist_index)
        except ValueError:
            self._send(env, f'Invalid playlist index {playlist_index}')
            return None, False

        if index > 0:
            if not await self.playlist_store.count_playlists(env.guild_id):
                self._send(env, 'No playlists in database')
                return None, False

        is_history = False
        if index == 0:
            playlist = await self.playlist_store.get_history_playlist(env.guild_id)
            is_history = True
        else:
            # The offset-and-take-first was an IndexError waiting on an index
            # past the end; the count check above only rules out zero playlists,
            # not `!playlist show 9` against three of them.
            playlists = await self.playlist_store.list_playlists(env.guild_id)
            playlist = playlists[index - 1] if index <= len(playlists) else None
        if not playlist:
            self._send(env, f'Invalid playlist index {playlist_index}')
            return None, False
        return playlist.id, is_history

    async def create(self, env: CommandEnvelope, name: str):
        '''
        Create a playlist, returning its id, or None after telling the channel why not.
        '''
        if not await self.check_database_session(env):
            return None
        # Check name doesn't conflict with history
        playlist_name = shorten_string(name, 256)
        if PLAYHISTORY_PREFIX in playlist_name.lower():
            self._send_kept(env, f'Unable to create playlist "{name}", name cannot contain {PLAYHISTORY_PREFIX}')
            return None
        existing_playlist = await self.playlist_store.get_playlist_by_name(env.guild_id, playlist_name)
        if existing_playlist:
            self._send_kept(env, f'Unable to create playlist "{name}", a playlist with that name already exists')
            return None

        playlist = await self.playlist_store.create_playlist(env.guild_id, name)
        self.logger.info(f'Playlist created "{playlist_name}" with id {playlist.id} in guild {env.guild_id}')
        public_playlist_id = await self.get_playlist_public_view(playlist.id, env.guild_id)
        self._send(env, f'Created playlist "{playlist_name}" with ID {public_playlist_id}')
        return playlist.id

    async def list_playlists(self, env: CommandEnvelope) -> None:
        '''List the guild's playlists, the history playlist first.'''
        if not await self.check_database_session(env):
            return
        history_playlist = await self.playlist_store.get_history_playlist(env.guild_id)
        playlist_items = await self.playlist_store.list_playlists(env.guild_id)

        if not playlist_items and not history_playlist:
            self._send(env, 'No playlists in database')
            return

        if history_playlist:
            playlist_items = [history_playlist] + list(playlist_items)

        headers = [
            Column('ID', 3),
            Column('Playlist Name', 64),
            Column('Last Queued', 20),
        ]
        table = DapperTable(columns=Columns(headers), pagination_options=PaginationLength(DISCORD_MAX_MESSAGE_LENGTH),
                            enclosure_start='```', enclosure_end='```', prefix='Playlist List\n')
        for (count, item) in enumerate(playlist_items):
            last_queued = 'N/A'
            if item.last_queued:
                last_queued = item.last_queued.strftime('%Y-%m-%d %H:%M:%S')
            name = item.name
            if item.is_history:
                name = PLAYHISTORY_NAME
            table.add_row([
                f'{count}',
                name,
                last_queued,
            ])
        for mess in table.render():
            self._send(env, mess)

    async def item_remove(self, env: CommandEnvelope, playlist_index: int, video_index: int) -> None:
        '''Remove the item at ``video_index`` (1-based) from a playlist.'''
        if not await self.check_database_session(env):
            return

        playlist_id, _is_history = await self.get_playlist(playlist_index, env)
        if not playlist_id:
            return
        try:
            video_index = int(video_index)
        except ValueError:
            self._send(env, f'Invalid item index {video_index}')
            return
        if video_index < 1:
            self._send(env, f'Invalid item index {video_index}')
            return

        # The entry comes back built, because the caller wants the deleted
        # item's title for its message and the row is gone by then.
        item = await self.playlist_store.delete_item_by_index(playlist_id, video_index - 1)
        public_playlist_id = await self.get_playlist_public_view(playlist_id, env.guild_id)
        if item:
            self._send(env, f'Removed item "{item.title}" from playlist {public_playlist_id}')
            return
        self._send(env, f'Unable to find item {video_index}')

    async def show(self, env: CommandEnvelope, playlist_index: int) -> None:
        '''Show the items in a playlist.'''
        if not await self.check_database_session(env):
            return

        playlist_id, _is_history = await self.get_playlist(playlist_index, env)
        if not playlist_id:
            return

        headers = [
            Column('Pos', 3, zero_pad=True),
            Column('Title', 32),
            Column('Uploader', 32),
        ]
        table = DapperTable(columns=Columns(headers), pagination_options=PaginationLength(DISCORD_MAX_MESSAGE_LENGTH),
                            enclosure_start='```', enclosure_end='```', prefix=f'Playlist {playlist_index} Items\n')
        total = 0
        # The position shown here is the index `!playlist item-remove` takes, so
        # it has to be the same order the store deletes by.
        for (count, item) in enumerate(await self.playlist_store.list_items(playlist_id)):
            uploader = item.uploader or ''
            table.add_row([
                f'{count + 1}',
                f'{item.title}',
                f'{uploader}',
            ])
            total += 1
        if not total:
            self._send(env, f'No items in playlist {playlist_id}')
            return
        for mess in table.render():
            self._send(env, mess)

    async def delete(self, env: CommandEnvelope, playlist_index: int) -> None:
        '''Delete a playlist. The history playlist is reserved.'''
        if not await self.check_database_session(env):
            return

        playlist_id, is_history = await self.get_playlist(playlist_index, env)
        if not playlist_id:
            self._send(env, f'Cannot delete playlist, unable to find id {playlist_index}')
            return
        if is_history:
            self._send(env, 'Cannot delete history playlist, is reserved')
            return
        await self.delete_playlist(playlist_id)
        self._send(env, f'Deleted playlist {playlist_index}')

    async def delete_playlist(self, playlist_id: int) -> None:
        '''Delete a playlist and its items by db id.'''
        self.logger.info(f'Deleting playlist items "{playlist_id}"')
        await self.playlist_store.delete_playlist(playlist_id)

    async def rename(self, env: CommandEnvelope, playlist_index: int, playlist_name: str) -> None:
        '''Rename a playlist. The history playlist is reserved.'''
        if not await self.check_database_session(env):
            return

        playlist_id, is_history = await self.get_playlist(playlist_index, env)
        if is_history:
            self._send(env, 'Cannot rename history playlist, is reserved')
            return
        if not playlist_id:
            return

        playlist_name = shorten_string(playlist_name, 256)
        if PLAYHISTORY_PREFIX in playlist_name.lower():
            self._send_kept(env, f'Unable to create playlist "{playlist_name}", name cannot contain {PLAYHISTORY_PREFIX}')
            return

        self.logger.info(f'Renaming playlist {playlist_id} to name "{playlist_name}"')
        await self.playlist_store.rename_playlist(playlist_id, playlist_name)
        self._send(env, f'Renamed playlist {playlist_index} to name "{playlist_name}"')

    async def merge(self, env: CommandEnvelope, playlist_index_one: str, playlist_index_two: str) -> None:
        '''Merge the second playlist into the first, then delete the second.'''
        if not await self.check_database_session(env):
            return

        self.logger.info(f'Calling playlist merge of "{playlist_index_one}" and "{playlist_index_two}" in server "{env.guild_id}"')
        playlist_one_id, is_history1 = await self.get_playlist(playlist_index_one, env)
        playlist_two_id, is_history2 = await self.get_playlist(playlist_index_two, env)
        if is_history1 or is_history2:
            self._send(env, 'Cannot merge history playlist, is reserved')
            return
        if not playlist_one_id:
            self._send(env, f'Cannot find playlist {playlist_index_one}')
            return
        if not playlist_two_id:
            self._send(env, f'Cannot find playlist {playlist_index_two}')
            return
        source_items = await self.playlist_store.list_items(playlist_two_id)
        outcomes = await self.playlist_store.add_items(
            playlist_one_id,
            [PlaylistItemWrite(video_url=item.video_url, title=item.title, uploader=item.uploader)
             for item in source_items],
            self.server_playlist_max_size)
        for outcome in outcomes:
            if outcome.status == PlaylistItemAddStatus.PLAYLIST_FULL:
                self._send(env, f'Cannot add more items to playlist "{playlist_one_id}", already max size')
                # Returning rather than breaking, same as before: a merge that
                # could not take every item must not then delete the source.
                return
            if outcome.status == PlaylistItemAddStatus.ADDED:
                self._send(env, f'Added item "{outcome.title}" to playlist {playlist_index_one}')
                continue
            self._send(env, f'Unable to add playlist item "{outcome.title}", likely already exists')
        await self.delete_playlist(playlist_two_id)

    async def save(self, env: CommandEnvelope, name: str, is_history: bool = False) -> None:
        '''Save the guild's queue, or its play history, to a new playlist called ``name``.'''
        playlist_id = await self.create(env, name)
        if not playlist_id:
            return

        if not (await self.player_gateway.status(env.guild_id)).exists:
            self._send(env, 'No player connected, no queue to save')
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
            self._send(env, 'There are no videos to add to playlist')
            return

        # One call for the batch, then a message per outcome. The loop used to
        # hold a session open while awaiting a Discord send per item.
        outcomes = await self.playlist_store.add_items(playlist_id, queue_copy, self.server_playlist_max_size)
        for outcome in outcomes:
            if outcome.status == PlaylistItemAddStatus.PLAYLIST_FULL:
                self._send(env, 'Cannot add more items to playlist, already max size')
                break
            if outcome.status == PlaylistItemAddStatus.ADDED:
                self._send(env, f'Added item "{outcome.title}" to playlist')
                continue
            self._send(env, f'Unable to add playlist item "{outcome.title}", likely already exists')
        self._send(env, f'Finished adding items to playlist "{name}"')

    async def item_add(self, env: CommandEnvelope, playlist_index, search: str) -> None:
        '''Search for ``search`` and add what it finds to a playlist.'''
        if not await self.check_database_session(env):
            return

        playlist_id, is_history = await self.get_playlist(playlist_index, env)
        if not playlist_id:
            return

        if is_history:
            self._send(env, f'Unable to add "{search}" to history playlist, is reserved and cannot be added to manually')
            return

        await self.enqueuer.generate_media_requests_from_search(env, search, add_to_playlist=playlist_id)

    async def queue_items(self, env: CommandEnvelope, playlist_id: int, shuffle: bool, max_num: int | None,
                          is_history: bool = False) -> None:
        '''Put the playlist's items on the guild's queue, optionally shuffled and capped at ``max_num``.'''
        self.logger.info(f'Playlist queue called for playlist {playlist_id} in server "{env.guild_id}"')

        # Both reads happen up front and the connection is released before the
        # enqueue below, which dispatches searches and downloads. The session
        # used to stay open for that entire stretch because these rows were live.
        playlist = await self.playlist_store.get_playlist(playlist_id)
        playlist_name = playlist.name if playlist else None
        items = await self.playlist_store.list_items(playlist_id)
        if is_history:
            playlist_name = PLAYHISTORY_NAME
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
            self._send(env, f'Playlist "{playlist_name}" contains no items to queue')
            return

        if shuffle:
            # https://stackoverflow.com/a/51295230
            random.seed(time())
            random.shuffle(playlist_items)

        if max_num:
            if max_num < 0:
                self._send(env, f'Invalid number of videos {max_num}')
                return
            if max_num < len(playlist_items):
                playlist_items = playlist_items[:max_num]

        bundle_uuid = await self.broker_client.create_bundle(
            env.guild_id, env.channel_id, input_string=playlist_name, has_search_banner=True)
        finished_all = await self.enqueuer.enqueue_media_requests(env, playlist_items, bundle_uuid)

        if not finished_all:
            self._send(env, f'Added as many videos in playlist "{playlist_name}" to queue as possible, but hit limit')

        await self.playlist_store.mark_queued(playlist_id)
