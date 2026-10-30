'''
``PlayerGateway`` for a pod that has no player: everything goes through the broker.

The player (voice client, playback loop) lives in the gateway, which is the only process holding
the Discord connection. This pod reads what it needs from the broker's view of the guild and asks
the gateway to act with gateway intents (``discord_core.types.gateway_intents``).
'''
from asyncio import sleep
from time import monotonic

from discord_core.interfaces.broker_client_protocol import BrokerClient
from discord_core.interfaces.download_client_protocol import DownloadClient
from discord_core.interfaces.player_gateway import EnsureResult, PlayerStatus
from discord_core.types.gateway_intents import IntentKind
from discord_core.types.media_download import MediaDownload
from discord_commands.utils.media_enqueuer import TrackQueuer

# An intent expires unclaimed after 30 seconds (discord_broker.workers.gateway_intents), so a join
# not answered by then never will be. Waiting a little past that tells "slow" from "never claimed".
JOIN_TIMEOUT_SECONDS = 35.0
JOIN_POLL_SECONDS = 0.25
JOIN_TIMEOUT_MESSAGE = 'The bot did not join in time, please try again'


class BrokerPlayerGateway:
    '''
    ``PlayerGateway`` over the broker.

    A player exists when the broker has an open play queue for the guild: the gateway opens it
    when the player joins and closes it when the player goes. ``join`` is the one intent waited
    for, because its outcome is what the command says next. The others are asked and left, since
    each is idempotent and the gateway claims them within a poll.
    '''

    def __init__(self, broker_client: BrokerClient, download_client: DownloadClient,
                 queuer: TrackQueuer, join_timeout: float = JOIN_TIMEOUT_SECONDS,
                 poll_interval: float = JOIN_POLL_SECONDS):
        self.broker_client = broker_client
        self.download_client = download_client
        self.queuer = queuer
        self.join_timeout = join_timeout
        self.poll_interval = poll_interval

    async def status(self, guild_id: int) -> PlayerStatus:
        '''Snapshot the guild's player, as far as the broker can tell. Never creates one.'''
        queue = await self.broker_client.get_guild_queue(guild_id)
        if queue.text_channel_id is None or queue.closed:
            return PlayerStatus(exists=False)
        return PlayerStatus(
            exists=True,
            # The broker cannot see the voice client. A queue is only open while the player holds
            # one, and a player that lost it closes the queue as it tears down.
            voice_connected=True,
            is_playing=queue.playing is not None,
            pending_downloads=await self.download_client.queue_size(guild_id),
            text_channel_id=queue.text_channel_id,
        )

    async def has_live_player(self, guild_id: int) -> bool:
        '''Whether the guild has an open play queue.'''
        queue = await self.broker_client.get_guild_queue(guild_id)
        return queue.text_channel_id is not None and not queue.closed

    async def ensure(self, guild_id: int, voice_channel_id: int, text_channel_id: int) -> EnsureResult:
        '''Ask the gateway to create the player and join, and wait for how that went.'''
        intent_id = await self.broker_client.submit_intent(
            guild_id, IntentKind.JOIN,
            {'voice_channel_id': voice_channel_id, 'text_channel_id': text_channel_id})
        deadline = monotonic() + self.join_timeout
        while True:
            result = await self.broker_client.get_intent_result(intent_id)
            if result.done:
                return EnsureResult(ok=bool(result.ok), error=result.error)
            if monotonic() >= deadline:
                return EnsureResult(ok=False, error=JOIN_TIMEOUT_MESSAGE)
            await sleep(self.poll_interval)

    async def stop(self, guild_id: int, text_channel_id: int) -> None:
        '''Ask the gateway to tear the player down.'''
        await self.broker_client.submit_intent(guild_id, IntentKind.STOP, {'text_channel_id': text_channel_id})

    async def move_messages(self, guild_id: int, text_channel_id: int) -> None:
        '''Ask the gateway to point the player at a new text channel.'''
        await self.broker_client.submit_intent(
            guild_id, IntentKind.MOVE_MESSAGES, {'text_channel_id': text_channel_id})

    async def wake(self, guild_id: int) -> None:
        '''Ask the gateway to have the guild's player claim a freshly queued track now.'''
        await self.broker_client.submit_intent(guild_id, IntentKind.WAKE)

    async def add_cached(self, guild_id: int, media_download: MediaDownload) -> bool:
        '''Queue a cache hit for the guild's player, if it has one, and wake the player.'''
        if not await self.has_live_player(guild_id):
            return False

        async def wake() -> None:
            await self.wake(guild_id)
        return await self.queuer.enqueue_download(media_download, wake)
