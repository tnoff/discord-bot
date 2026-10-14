from asyncio import TimeoutError as async_timeout

from discord_gateway.command_logic.player_gateway import EnsureResult, PlayerStatus
from discord_gateway.types.cleanup_reason import CleanupReason


class CogPlayerGateway:
    '''
    ``PlayerGateway`` implemented in-process, over the music cog's players.

    Every method resolves ids to discord objects through the bot's cache and then does what the
    command bodies used to do inline, so the behaviour is the same code moved, not rewritten.
    Stateless: it reads the cog's current players and clients on each call.
    '''

    def __init__(self, cog):
        self.cog = cog

    async def _channel(self, channel_id: int):
        '''A channel from the bot's cache, fetched when it is not there.'''
        return self.cog.bot.get_channel(channel_id) or await self.cog.bot.fetch_channel(channel_id)

    async def status(self, guild_id: int) -> PlayerStatus:
        '''Snapshot the guild's player without creating one.'''
        player = self.cog.players.get(guild_id)
        if player is None:
            return PlayerStatus(exists=False)
        voice_client = player.guild.voice_client
        pending_downloads = await self.cog.download_client.queue_size(guild_id)
        return PlayerStatus(
            exists=True,
            voice_connected=bool(voice_client),
            is_playing=bool(voice_client and voice_client.is_playing()),
            pending_downloads=pending_downloads,
            text_channel_id=player.text_channel.id,
        )

    async def ensure(self, guild_id: int, voice_channel_id: int, text_channel_id: int) -> EnsureResult:
        '''Create the guild's player if needed and join the voice channel.'''
        voice_channel = await self._channel(voice_channel_id)
        try:
            player = await self.cog.get_player(
                guild_id, join_channel=voice_channel,
                guild=self.cog.bot.get_guild(guild_id),
                text_channel=await self._channel(text_channel_id))
        except async_timeout as e:
            self.cog.logger.warning(f'Reached async timeout error on bot joining channel, {str(e)}')
            return EnsureResult(ok=False, error=f'Bot cannot join channel {voice_channel}')
        # get_player has already told the channel why when it returns nothing.
        return EnsureResult(ok=player is not None)

    async def skip(self, guild_id: int) -> str | None:
        '''Skip the current track, returning its title, or None if nothing is playing.'''
        player = self.cog.players.get(guild_id)
        if player is None or not player.guild.voice_client or not player.guild.voice_client.is_playing():
            return None
        # No await from the check to the stop: nothing can change what is playing in between.
        current_title = player.current_media_download.title
        player.video_skipped = True
        player.guild.voice_client.stop()
        return current_title

    async def stop(self, guild_id: int, text_channel_id: int) -> None:
        '''Destroy the guild's player at the user's request.'''
        player = await self.cog.get_player(
            guild_id, guild=self.cog.bot.get_guild(guild_id),
            text_channel=await self._channel(text_channel_id))
        if not player:
            return
        player.destroy(reason=CleanupReason.USER_STOP)

    async def move_messages(self, guild_id: int, text_channel_id: int) -> None:
        '''Point the player, and its saved session, at a new text channel.'''
        player = self.cog.players[guild_id]
        player.text_channel = await self._channel(text_channel_id)
        await self.cog._save_player_session(player.guild, player)  # pylint: disable=protected-access
