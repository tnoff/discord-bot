from logging import Logger
from typing import Optional

from dappertable import DapperTable, Columns, Column, PaginationLength

from discord_core.common import DISCORD_MAX_MESSAGE_LENGTH
from discord_core.clients.http_markov_store import HttpMarkovStore
from discord_core.types.command_envelope import CommandEnvelope

from discord_commands.command_logic.reply import Reply


class MarkovCommands:
    '''
    The ``!markov`` subcommands. Discord-free: see ``discord_core.types.command_envelope``.

    Annotated against the concrete HTTP store, not the ``MarkovStore`` Protocol: the Protocol's
    module imports types that need discord.py, which this pod does not install.
    '''

    def __init__(self, reply: Reply, logger: Logger, markov_store: HttpMarkovStore,
                 server_reject_list: list):
        self.reply = reply
        self.logger = logger
        self.markov_store = markov_store
        self.server_reject_list = server_reject_list

    async def on(self, env: CommandEnvelope, channel_is_text_or_voice: bool) -> str:
        '''
        Turn markov on for the channel.

        ``channel_is_text_or_voice`` comes from the gateway, which knows the type of the channel
        the command was typed in; this pod has no Discord connection to look it up.
        '''
        if env.guild_id in self.server_reject_list:
            return await self.reply(env.guild_id, env.channel_id, 'Unable to turn on markov for server, in reject list')

        # Ensure channel not already on
        markov = await self.markov_store.get_channel(env.guild_id, env.channel_id)

        if markov:
            return await self.reply(env.guild_id, env.channel_id, 'Channel already has markov turned on')
        if not channel_is_text_or_voice:
            return await self.reply(env.guild_id, env.channel_id, 'Not a valid markov channel, cannot turn on markov')

        await self.markov_store.add_channel(env.guild_id, env.channel_id)
        self.logger.info(f'Adding new markov channel {env.channel_id} from server {env.guild_id}')
        return await self.reply(env.guild_id, env.channel_id, 'Markov turned on for channel')

    async def off(self, env: CommandEnvelope) -> str:
        '''Turn markov off for the channel.'''
        # False is "was not tracked", not a failure -- the store drops the
        # relations and the channel row in one transaction.
        removed = await self.markov_store.remove_channel(env.guild_id, env.channel_id)

        if not removed:
            return await self.reply(env.guild_id, env.channel_id, 'Channel does not have markov turned on')
        self.logger.info(f'Turning off markov channel {env.channel_id} from server {env.guild_id}')
        return await self.reply(env.guild_id, env.channel_id, 'Markov turned off for channel')

    async def list_channels(self, env: CommandEnvelope):
        '''List the channels markov is enabled for in this server.'''
        # Ids, not one-column Row tuples -- this used to index `row[0]`, which
        # is a driver artifact and not something an HTTP store would ever send.
        markov_channels = await self.markov_store.list_guild_channel_ids(env.guild_id)

        if not markov_channels:
            return await self.reply(env.guild_id, env.channel_id, 'Markov not enabled for any channels in server')

        headers = [
            Column('Channel', 64),
        ]

        table = DapperTable(columns=Columns(headers), pagination_options=PaginationLength(DISCORD_MAX_MESSAGE_LENGTH),
                            prefix='Channel List \n')
        for channel_id in markov_channels:
            table.add_row([f'<#{channel_id}>'])
        for output in table.render():
            await self.reply(env.guild_id, env.channel_id, output)
        return True

    async def speak(self, env: CommandEnvelope, first_word: Optional[str] = '',
                    sentence_length: Optional[int] = 32) -> str:
        '''Say a random sentence generated from this server's markov channels.'''
        if env.guild_id in self.server_reject_list:
            return await self.reply(env.guild_id, env.channel_id, 'Unable to use markov for server, in reject list')

        self.logger.info(f'Calling speak on server {env.guild_id}')
        all_words = []
        first = None
        if first_word:
            # Allow for multiple words to be given
            # If so, just grab last word
            starting_words = first_word.split(' ')
            # Make sure to add to all words here
            for start_words in starting_words[:-1]:
                all_words.append(start_words.lower())
            first = starting_words[-1].lower()

        # The whole walk in one call. Each word is chosen from the previous one
        # by postgres; `!268` made that a single query per word instead of
        # selecting the guild's entire relation-id set and choosing in python.
        # Asking the store per word would have put a round trip where that query
        # is, which is the same mistake one layer up.
        # At least one: a leader word was always fetched before, even when the
        # given prefix already met sentence_length, so `!markov speak "a b c" 2`
        # still answers with a word rather than "nothing to say".
        generated = await self.markov_store.generate_words(
            env.guild_id, max(1, sentence_length - len(all_words)), first_word=first)

        # Empty is the "nothing to say" answer, and a short list is a dead end:
        # retention can delete every relation in which a word leads while
        # keeping one where it follows.
        if not generated:
            if first_word:
                return await self.reply(env.guild_id, env.channel_id, f'No markov word matching "{first_word}"')
            return await self.reply(env.guild_id, env.channel_id, 'No markov words to pick from')

        all_words.extend(generated)
        return await self.reply(env.guild_id, env.channel_id, ' '.join(markov_word for markov_word in all_words))
