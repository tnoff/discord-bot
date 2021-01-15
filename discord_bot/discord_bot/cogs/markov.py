import asyncio
import re

from discord import TextChannel
from discord.ext import commands

from discord_bot.cogs.common import CogHelper
from discord_bot.database import MarkovChannel
from discord_bot.database import MarkovRelation, MarkovWord

# https://srome.github.io/Making-A-Markov-Chain-Twitter-Bot-In-Python/
def build_transition_matrix(corpus):
    '''
    corpus  :   Input message

    Returns dictionary of each word, and words following that word
    Ex:
    {
        "hello": ["there", "my"],
        "my": ["friend."],
        "there": ["my"],
        "friend.": ["hello"],
    }
    '''
    corpus = corpus.split(' ')
    transitions = {}
    for (k, word) in enumerate(corpus):
        if k != len(corpus) - 1: # Deal with last word
            next_word = corpus[k+1]
        else:
            next_word = corpus[0] # To loop back to the beginning

        if word not in transitions:
            transitions[word] = []

        transitions[word].append(next_word)
    return transitions

class Markov(CogHelper):
    '''
    Save markov relations to a database periodically
    '''
    def __init__(self, bot, db_session, logger):
        super().__init__(bot, db_session, logger)
        self.bot.loop.create_task(self.wait_loop())

    def __ensure_word(self, word, markov_channel):
        markov_word = self.db_session.query(MarkovWord).\
                filter(MarkovWord.word == word).\
                filter(MarkovWord.channel_id == markov_channel.id).first()
        if markov_word:
            return markov_word
        if len(word) > 1024:
            self.logger.warning(f'Cannot add word "{word}", is too long')
            return None
        new_word = MarkovWord(word=word, channel_id=markov_channel.id)
        self.db_session.add(new_word)
        self.db_session.commit()
        self.db_session.flush()
        return new_word

    def __ensure_relation(self, leader, follower):
        markov_relation = self.db_session.query(MarkovRelation).\
                filter(MarkovRelation.leader_id == leader.id).\
                filter(MarkovRelation.follower_id == follower.id).first()
        if markov_relation:
            return markov_relation
        new_relation = MarkovRelation(leader_id=leader.id,
                                      follower_id=follower.id,
                                      count=0)
        self.db_session.add(new_relation)
        self.db_session.commit()
        self.db_session.flush()
        return new_relation

    def __build_and_save_relations(self, message, markov_channel):
        transitions = build_transition_matrix(message)
        for leader, followers in transitions.items():
            leader_word = self.__ensure_word(leader, markov_channel)
            if leader_word is None:
                continue
            for follower in followers:
                follower_word = self.__ensure_word(follower, markov_channel)
                if follower_word is None:
                    continue
                relation = self.__ensure_relation(leader_word, follower_word)
                relation.count += 1
                self.db_session.commit()

    async def wait_loop(self):
        '''
        Our main loop.
        '''
        await self.bot.wait_until_ready()

        while not self.bot.is_closed():
            for markov_channel in self.db_session.query(MarkovChannel).all():
                channel = await self.bot.fetch_channel(markov_channel.channel_id)
                self.logger.info(f'Gathering markov messages for channel {markov_channel.channel_id}')
                # Start at the beginning of channel history, slowly make your way make to current day
                if not markov_channel.last_message_id:
                    messages = await channel.history(limit=100, oldest_first=True).flatten()
                else:
                    last_message = await channel.fetch_message(markov_channel.last_message_id)
                    messages = await channel.history(after=last_message, limit=100).flatten()


                for message in messages:
                    self.logger.debug(f'Gathering message {message.id} for channel {markov_channel.channel_id}')
                    markov_channel.last_message_id = message.id
                    # If no content continue or from a bot skip
                    if not message.content or message.author.bot:
                        continue
                    # If message begins with '!', assume it was a bot command
                    if message.content[0] == '!':
                        continue
                    # Remove web links and mentions from text
                    message_text = re.sub(r'(http?\://|https?\://|www|\<\@!\d+)\S+', '',
                                          message.content, flags=re.MULTILINE)
                    if not message_text:
                        continue
                    # Use lower() so we can re-use words better
                    message_text = message_text.lower()
                    self.logger.info(f'Attempting to add message_text "{message_text}" '
                                     f'to channel {markov_channel.channel_id}')
                    self.__build_and_save_relations(message_text, markov_channel)

                    self.db_session.commit()
                # Commit at the end in case the last message was skipped
                self.db_session.commit()

            await asyncio.sleep(180)

    @commands.group(name='markov', invoke_without_command=False)
    async def markov(self, ctx):
        '''
        Markov functions
        '''
        if ctx.invoked_subcommand is None:
            await ctx.send('Invalid sub command passed...')

    @markov.command(name='on')
    async def on(self, ctx):
        '''
        Turn markov on for channel
        '''
        # Ensure channel not already on
        markov = self.db_session.query(MarkovChannel).\
            filter(MarkovChannel.channel_id == str(ctx.channel.id)).\
            filter(MarkovChannel.server_id == str(ctx.guild.id)).first()

        if markov:
            return await ctx.send('Channel already has markov turned on')

        channel = await self.bot.fetch_channel(ctx.channel.id)
        if not isinstance(channel, TextChannel):
            await ctx.send('Channel is not text channel, cannot turn on markov')

        new_markov = MarkovChannel(channel_id=str(ctx.channel.id),
                                   server_id=str(ctx.guild.id),
                                   last_message_id=None)
        self.db_session.add(new_markov)
        self.db_session.commit()

        return await ctx.send('Markov turned on for channel')

    @markov.command(name='off')
    async def off(self, ctx):
        '''
        Turn markov off for channel
        '''
        # Ensure channel not already on
        markov = self.db_session.query(MarkovChannel).\
            filter(MarkovChannel.channel_id == str(ctx.channel.id)).\
            filter(MarkovChannel.server_id == str(ctx.guild.id)).first()

        if not markov:
            return await ctx.send('Channel does not have markov turned on')

        self.db_session.delete(markov)
        self.db_session.commit()

        return await ctx.send('Markov turned off for channel')
