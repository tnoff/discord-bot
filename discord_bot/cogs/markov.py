from asyncio import sleep
from datetime import datetime, timedelta, timezone
from functools import partial
from pathlib import Path
from random import choice
from re import match, sub, MULTILINE
from tempfile import NamedTemporaryFile
from typing import Optional, List

from dappertable import DapperTable
from discord import ChannelType
from discord.ext.commands import Bot, Context, group
from discord.errors import NotFound, DiscordServerError
from opentelemetry.trace import SpanKind
from opentelemetry.metrics import get_meter_provider
from opentelemetry.metrics import Observation
from sqlalchemy.engine.base import Engine
from sqlalchemy.orm.session import Session

from discord_bot.cogs.common import CogHelper
from discord_bot.database import MarkovChannel, MarkovRelation
from discord_bot.exceptions import CogMissingRequiredArg
from discord_bot.cogs.schema import SERVER_ID
from discord_bot.utils.common import retry_discord_message_command, async_retry_discord_message_command, return_loop_runner
from discord_bot.utils.common import create_observable_gauge
from discord_bot.utils.sql_retry import retry_database_commands
from discord_bot.utils.otel import otel_span_wrapper, command_wrapper, AttributeNaming, MetricNaming

# Default for how many days to keep messages around
MARKOV_HISTORY_RETENTION_DAYS_DEFAULT = 365

# Default for how to wait between each loop
LOOP_SLEEP_INTERVAL_DEFAULT = 300

# Limit for how many messages we grab on each history check
MESSAGE_CHECK_LIMIT = 16

# Markov config schema
MARKOV_SECTION_SCHEMA = {
    'type': 'object',
    'properties': {
        'loop_sleep_interval': {
            'type': 'number',
        },
        'message_check_limit': {
            'type': 'number',
        },
        'history_retention_days': {
            'type': 'number',

        },
        'server_reject_list': {
            'type': 'array',
            'items': SERVER_ID,
        },
    }
}

METER_PROVIDER = get_meter_provider().get_meter(__name__, '0.0.1')

def clean_message(content: str, emojis: List[str]):
    '''
    Clean channel message
    content :   Full message content to clean
    emojis  :   List of server emoji ids, so we can remove any not from server

    Returns "corpus", list of cleaned words
    '''
    # Remove web links and mentions from text
    message_text = sub(r'(https?\://|\<\@)\S+|\<\#\S+', '',
                       content, flags=MULTILINE)
    # Doesnt remove @here or @everyone
    message_text = message_text.replace('@here', '')
    message_text = message_text.replace('@everyone', '')
    # Strip blank ends
    message_text = message_text.strip()
    corpus = []
    emoji_ids = [emoji.id for emoji in emojis]
    for word in message_text.split(' '):
        if word in ('', ' '):
            continue
        # Check for commands again
        if word[0] == '!':
            continue
        # Check for emojis in message
        # If emoji, check if belongs to list, if not, disregard it
        # Emojis can be case sensitive so do not lower them
        # Custom emojis usually have <:emoji:id> format
        # Ex: <:fail:1231031923091032910390>
        match_result = match(r'^\ *<(?P<emoji>:\w+:)(?P<id>\d+)>\ *$', word)
        if match_result:
            if int(match_result.group('id')) in emoji_ids:
                corpus.append(word)
            continue
        corpus.append(word.lower())
    return corpus

def get_matching_markov_channel(db_session: Session, ctx: Context):
    '''
    Get channel that matches original context
    '''
    return db_session.query(MarkovChannel).\
        filter(MarkovChannel.channel_id == str(ctx.channel.id)).\
        filter(MarkovChannel.server_id == str(ctx.guild.id)).first()

def list_guild_channels(db_session: Session, ctx: Context):
    '''
    List guild channels
    '''
    return db_session.query(MarkovChannel.channel_id).\
        filter(MarkovChannel.server_id == str(ctx.guild.id))

class Markov(CogHelper):
    '''
    Save markov relations to a database periodically
    '''
    def __init__(self, bot: Bot, settings: dict, db_engine: Engine):
        if not db_engine:
            raise CogMissingRequiredArg('No db engine passed, cannot start markov')
        if not settings.get('general', {}).get('include', {}).get('markov', False):
            raise CogMissingRequiredArg('Markov cog not enabled')

        super().__init__(bot, settings, db_engine, settings_prefix='markov', section_schema=MARKOV_SECTION_SCHEMA)

        self.loop_sleep_interval = self.settings.get('markov', {}).get('loop_sleep_interval', LOOP_SLEEP_INTERVAL_DEFAULT)
        self.message_check_limit = self.settings.get('markov', {}).get('message_check_limit', MESSAGE_CHECK_LIMIT)
        self.history_retention_days = self.settings.get('markov', {}).get('history_retention_days', MARKOV_HISTORY_RETENTION_DAYS_DEFAULT)
        self.server_reject_list = self.settings.get('markov', {}).get('server_reject_list', [])

        self._task = None
        self.loop_checkfile = Path(NamedTemporaryFile(delete=False).name) #pylint:disable=consider-using-with
        create_observable_gauge(METER_PROVIDER, MetricNaming.HEARTBEAT.value, self.__loop_active_callback, 'Markov check loop heartbeat')

    def __loop_active_callback(self, _options):
        '''
        Loop active callback check
        '''
        value = int(self.loop_checkfile.read_text())
        return [
            Observation(value, attributes={
                AttributeNaming.BACKGROUND_JOB.value: 'markov_check'
            })
        ]

    async def cog_load(self):
        self._task = self.bot.loop.create_task(return_loop_runner(self.markov_message_check, self.bot, self.logger, self.loop_checkfile, continue_exceptions=(DiscordServerError, TimeoutError))())

    async def cog_unload(self):
        if self._task:
            self._task.cancel()
        if self.loop_checkfile.exists():
            self.loop_checkfile.unlink()

    # https://srome.github.io/Making-A-Markov-Chain-Twitter-Bot-In-Python/
    def build_and_save_relations(self, corpus: List[str], markov_channel_id: str, message_timestamp: datetime):
        '''
        Build and save relations to db
        corpus : List of strings from message, after cleaning
        markov_channel_id : Markov Channel ID (ID from DB)
        message_timestamp: Timestamp for db
        '''
        def ensure_word(word):
            if len(word) >= 255:
                self.logger.warning(f'Markov :: Cannot add word "{word}", is too long')
                return None
            return word

        def add_word(db_session: Session, new_relation: MarkovRelation):
            db_session.add(new_relation)
            db_session.commit()

        for (k, word) in enumerate(corpus):
            if k != len(corpus) - 1: # Deal with last word
                next_word = corpus[k+1]
            else:
                next_word = corpus[0] # To loop back to the beginning
            leader_word = ensure_word(word)
            if leader_word is None:
                continue
            follower_word = ensure_word(next_word)
            if follower_word is None:
                continue
            new_relation = MarkovRelation(channel_id=markov_channel_id,
                                          leader_word=leader_word,
                                          follower_word=follower_word,
                                          created_at=message_timestamp)
            with self.with_db_session() as db_session:
                retry_database_commands(db_session, partial(add_word, db_session, new_relation))

    def delete_channel_relations(self, db_session: Session, channel_id: str):
        '''
        Delete all relations related to channel
        
        db_session : Sqlalchemy db_session
        channel_id: Markov Channel ID (DB ID)
        '''
        def delete_records(db_session, channel_id):
            db_session.query(MarkovRelation).filter(MarkovRelation.channel_id == channel_id).delete()
            db_session.commit()

        retry_database_commands(db_session, partial(delete_records, db_session, channel_id))

    async def markov_message_check(self):
        '''
        Main loop runner
        '''
        def get_all_channels(db_session):
            return db_session.query(MarkovChannel).all()

        def delete_old_records(db_session):
            db_session.query(MarkovRelation).filter(MarkovRelation.created_at < retention_cutoff).delete()
            db_session.commit()

        await sleep(self.loop_sleep_interval)
        with otel_span_wrapper('markov.message_check', kind=SpanKind.CONSUMER):
            retention_cutoff = datetime.now(timezone.utc) - timedelta(days=self.history_retention_days)
            self.logger.debug(f'Entering message gather loop, using cutoff {retention_cutoff}')

            with self.with_db_session() as db_session:
                for markov_channel in retry_database_commands(db_session, partial(get_all_channels, db_session)):
                    with otel_span_wrapper('markov.channel_check', kind=SpanKind.INTERNAL, attributes={'discord.channel': markov_channel.channel_id}):
                        self.logger.debug(f'Checking channel id: {markov_channel.channel_id}, server id: {markov_channel.server_id}')
                        channel = await async_retry_discord_message_command(partial(self.bot.fetch_channel, markov_channel.channel_id))
                        server = await async_retry_discord_message_command(partial(self.bot.fetch_guild, markov_channel.server_id))
                        # Not sure why but this check in particular seems especially flakey
                        emojis = await async_retry_discord_message_command(partial(server.fetch_emojis, max_retries=5))
                        self.logger.info('Gathering markov messages for '
                                        f'channel {markov_channel.channel_id}')
                        # Start at the beginning of channel history,
                        # slowly make your way make to current day
                        if not markov_channel.last_message_id:
                            messages = [m async for m in retry_discord_message_command(partial(channel.history, limit=self.message_check_limit, after=retention_cutoff, oldest_first=True))]
                        else:
                            try:
                                last_message = await async_retry_discord_message_command(partial(channel.fetch_message, markov_channel.last_message_id))
                                messages = [m async for m in retry_discord_message_command(partial(channel.history, after=last_message, limit=self.message_check_limit, oldest_first=True))]
                            except NotFound:
                                self.logger.warning(f'Unable to find message {markov_channel.last_message_id}'
                                                    f' in channel {markov_channel.id}')
                                # Last message on record not found
                                # If this happens, wipe the channel clean and restart
                                self.delete_channel_relations(db_session, markov_channel.id)
                                markov_channel.last_message_id = None
                                self.retry_commit(db_session)
                                # Skip this channel for now
                                continue

                        if len(messages) == 0:
                            self.logger.debug(f'No new messages for channel {markov_channel.channel_id}')
                            continue


                        for message in messages:
                            self.logger.debug(f'Gathering message {message.id} '
                                                f'for channel {markov_channel.channel_id}')
                            add_message = True
                            if not message.content or message.author.bot:
                                add_message = False
                            elif message.content[0] == '!':
                                add_message = False
                            corpus = None
                            if add_message:
                                corpus = clean_message(message.content, emojis)
                            if corpus:
                                self.logger.info(f'Attempting to add corpus "{corpus}" '
                                                f'to channel {markov_channel.channel_id}')
                                self.build_and_save_relations(corpus, markov_channel.id, message.created_at)
                            markov_channel.last_message_id = str(message.id)
                            self.retry_commit(db_session)
                        self.logger.debug(f'Done with channel {markov_channel.channel_id}')

                # Clean up old messages
                with otel_span_wrapper('markov.message_delete', kind=SpanKind.INTERNAL):
                    retry_database_commands(db_session, partial(delete_old_records, db_session))
                    self.logger.debug('Deleted expired/old markov relations')

    @group(name='markov', invoke_without_command=False)
    async def markov(self, ctx: Context):
        '''
        Markov functions
        '''
        if ctx.invoked_subcommand is None:
            await ctx.send('Invalid sub command passed...')

    @markov.command(name='on')
    @command_wrapper
    async def on(self, ctx: Context):
        '''
        Turn markov on for channel
        '''

        def add_channel(db_session: Session, new_channel: MarkovChannel):
            db_session.add(new_channel)
            db_session.commit()

        if ctx.guild.id in self.server_reject_list:
            return await async_retry_discord_message_command(partial(ctx.send, 'Unable to turn on markov for server, in reject list'))

        with self.with_db_session() as db_session:
            # Ensure channel not already on
            markov = retry_database_commands(db_session, partial(get_matching_markov_channel, db_session, ctx))

            if markov:
                return await async_retry_discord_message_command(partial(ctx.send, 'Channel already has markov turned on'))

            channel = await self.bot.fetch_channel(ctx.channel.id)
            if channel.type not in [ChannelType.text, ChannelType.voice]:
                return await async_retry_discord_message_command(partial(ctx.send, 'Not a valid markov channel, cannot turn on markov'))

            new_markov = MarkovChannel(channel_id=str(ctx.channel.id),
                                    server_id=str(ctx.guild.id),
                                    last_message_id=None)
            retry_database_commands(db_session, partial(add_channel, db_session, new_markov))
            self.logger.info(f'Adding new markov channel {ctx.channel.id} from server {ctx.guild.id}')
            return await async_retry_discord_message_command(partial(ctx.send, 'Markov turned on for channel'))

    @markov.command(name='off')
    @command_wrapper
    async def off(self, ctx: Context):
        '''
        Turn markov off for channel
        '''
        def delete_channels(db_session: Session, markov_channel: MarkovChannel):
            db_session.delete(markov_channel)
            db_session.commit()

        with self.with_db_session() as db_session:
            # Ensure channel not already on
            markov_channel = retry_database_commands(db_session, partial(get_matching_markov_channel, db_session, ctx))

            if not markov_channel:
                return await async_retry_discord_message_command(partial(ctx.send, 'Channel does not have markov turned on'))
            self.logger.info(f'Turning off markov channel {ctx.channel.id} from server {ctx.guild.id}')

            self.delete_channel_relations(db_session, markov_channel.id)
            retry_database_commands(db_session, partial(delete_channels, db_session, markov_channel))
            return await async_retry_discord_message_command(partial(ctx.send, 'Markov turned off for channel'))

    @markov.command(name='list-channels')
    @command_wrapper
    async def list_channels(self, ctx: Context):
        '''
        List channels markov is enabled for in this server
        '''
        with self.with_db_session() as db_session:
            markov_channels = retry_database_commands(db_session, partial(list_guild_channels, db_session, ctx))

            if not markov_channels.count():
                return await async_retry_discord_message_command(partial(ctx.send, 'Markov not enabled for any channels in server'))

            table = DapperTable([{
                'name': 'Channel',
                'length': 64,
            }], rows_per_message=15)
            for channel_id in markov_channels:
                table.add_row([f'<#{channel_id[0]}>'])
            for output in table.print():
                await async_retry_discord_message_command(partial(ctx.send, output))
            return True

    @markov.command(name='speak')
    @command_wrapper
    async def speak(self, ctx: Context, #pylint:disable=too-many-locals
                    first_word: Optional[str] = '',
                    sentence_length: Optional[int] = 32):
        '''
        Say a random sentence generated by markov

        Note that this uses all markov channels setup for the server

        first_word  :   First word for markov string, if not given will be random.
        sentence_length :   Length of sentence

        Note that for first_word, multiple words can be given, but they must be in quotes
        Ex: !markov speak "hey whats up", or !markov speak "hey whats up" 64
        '''

        def get_possible_words(db_session: Session, ctx: Context, first_word: str = None):
            query = db_session.query(MarkovRelation.id).\
                        join(MarkovChannel, MarkovChannel.id == MarkovRelation.channel_id).\
                        filter(MarkovChannel.server_id == str(ctx.guild.id))
            if first_word:
                query = query.filter(MarkovRelation.leader_word == first_word)
            return [word[0] for word in query]

        def get_first_leader_word(db_session: Session, possible_words: List[int]):
            return db_session.get(MarkovRelation, choice(possible_words)).leader_word

        def get_first_follower_word(db_session: Session, possible_words: List[int]):
            return db_session.get(MarkovRelation, choice(possible_words)).follower_word

        if ctx.guild.id in self.server_reject_list:
            return await async_retry_discord_message_command(partial(ctx.send, 'Unable to use markov for server, in reject list'))

        self.logger.info(f'Calling speak on server {ctx.guild.id}')
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

        with self.with_db_session() as db_session:
            possible_words = retry_database_commands(db_session, partial(get_possible_words, db_session, ctx, first_word=first))

            if len(possible_words) == 0:
                if first_word:
                    return await async_retry_discord_message_command(partial(ctx.send, f'No markov word matching "{first_word}"'))
                return await async_retry_discord_message_command(partial(ctx.send, 'No markov words to pick from'))

            word = retry_database_commands(db_session, partial(get_first_leader_word, db_session, possible_words))
            all_words.append(word)

            remaining_word_num = sentence_length - len(all_words)
            for _ in range(remaining_word_num):
                # Get all leader ids first so you can pass it in
                relation_ids = retry_database_commands(db_session, partial(get_possible_words, db_session, ctx, first_word=word))
                # Get random choice of leader ids
                word = retry_database_commands(db_session, partial(get_first_follower_word, db_session, relation_ids))
                all_words.append(word)
            return await async_retry_discord_message_command(partial(ctx.send, ' '.join(markov_word for markov_word in all_words)))
