'''
The commands pod's result loops over in-process doubles, for tests of the music pipeline.

The result loops used to run in the gateway cog, and the tests that drove them used the cog as a
bag of doubles. ``ResultsPod`` is that bag for the pod: it has the attributes
``attach_in_process_broker`` / ``_download`` / ``_search`` read from a cog, so the same doubles are
wired the same way, and it builds the pod's ``MediaEnqueuer``, ``BrokerPlayerGateway`` and
``ResultLoops`` over them.

The player is the broker's open play queue (``open_player``), which is all the pod can see of one.
A wake intent is recorded in ``woken`` instead of going through an intent registry.
'''
from asyncio import Event
from logging import getLogger
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import MagicMock

from discord_core.clients.search_client import SearchClient
from discord_core.utils.media_enqueuer import MediaEnqueuer, TrackQueuer

from discord_commands.command_logic.broker_player_gateway import BrokerPlayerGateway
from discord_commands.loop_logic.music_results import MusicResultsLoop
from discord_commands.workers.result_loops import ResultLoops
from discord_gateway.cogs.music import MusicConfig

from tests.helpers import attach_in_process_broker, attach_in_process_download, attach_in_process_search

BASE_MUSIC = {
    'broker_client': {'url': 'http://broker-host:8081'},
    'download_client': {'url': 'http://downloader-host:8083'},
    'youtube_music_search_client': {'url': 'http://search-host:8084'},
    'media_search_client': {'url': 'http://search-host:8084'},
}

# Marks a dispatcher not passed, since None is a dispatcher a test can ask for.
_FROM_CONTEXT = object()


def music_settings(*overrides: dict) -> dict:
    '''The ``music`` block, with each override merged one level deep (``download``, ``player``...).'''
    merged = {key: dict(value) for key, value in BASE_MUSIC.items()}
    for override in overrides:
        for key, value in override.items():
            if isinstance(value, dict) and isinstance(merged.get(key), dict):
                merged[key] = merged[key] | value
            else:
                merged[key] = value
    return merged


class ResultsPod:  # pylint:disable=too-many-instance-attributes
    '''A commands pod's result-loop wiring over the in-process broker, download and search doubles.'''

    def __init__(self, fake_context, fake_stores, music: dict | None = None, download_worker_cls=None,
                 source_search_client=None, ytm_client=None, with_search: bool = False,
                 dispatcher=_FROM_CONTEXT):
        self.config = MusicConfig.model_validate(music or music_settings())
        self.logger = getLogger('results_pod')
        self.logging_config = None
        self.download_dir = Path(TemporaryDirectory().name)  #pylint:disable=consider-using-with
        self.download_dir.mkdir(exist_ok=True, parents=True)
        self.dispatcher = fake_context['dispatcher'] if dispatcher is _FROM_CONTEXT else dispatcher
        self.server_queue_priority = {int(item.server_id): item.priority
                                      for item in self.config.download.server_queue_priority}
        self.stop_event = Event()
        self.woken: list[int] = []
        self.playlist_store = fake_stores.playlist

        self.broker_client = None
        self.download_client = None
        self.youtube_music_search_client = None
        attach_in_process_broker(self)
        attach_in_process_download(self, worker_cls=download_worker_cls)
        self.search_driver = attach_in_process_search(self, client=ytm_client) if with_search else None

        queue_max_size = self.config.player.queue_max_size
        self.player_gateway = BrokerPlayerGateway(
            self.broker_client, self.download_client,
            TrackQueuer(self.broker_client, self.logger, queue_max_size), poll_interval=0)

        async def wake(guild_id: int) -> None:
            self.woken.append(guild_id)
        self.player_gateway.wake = wake
        # Source expansion is a round trip to the search pod; tests pass what answers it.
        self.search_client = source_search_client or SearchClient(MagicMock())
        self.enqueuer = MediaEnqueuer(
            self.broker_client, self.search_client, self.youtube_music_search_client,
            self.download_client, self.playlist_store, self.player_gateway, self.dispatcher, self.logger,
            queue_max_size, self.config.playlist.server_playlist_max_size,
            self.config.general.message_delete_after, self.server_queue_priority)
        self.results = MusicResultsLoop(self.logger, self.broker_client, self.download_client, self.enqueuer,
                                        self.player_gateway, self.playlist_store, self.server_queue_priority)
        self.loops = ResultLoops(self.broker_client, self.results, self.logger, poll_interval=0)
        self.loops.stop_event = self.stop_event

    async def open_player(self, guild_id: int, text_channel_id: int) -> None:
        '''Open the guild's play queue, which is what makes the pod see a player.'''
        await self.broker_client.open_guild(guild_id, text_channel_id)
