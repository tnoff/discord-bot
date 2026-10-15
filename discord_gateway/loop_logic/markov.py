from asyncio import sleep
from datetime import datetime, timedelta, timezone
from logging import Logger
from re import match, sub, MULTILINE
from typing import Awaitable, Callable, List

from opentelemetry.trace import SpanKind

from discord_core.utils.discord_context import DiscordContextNaming
from discord_core.utils.otel import async_otel_span_wrapper, span_links_from_context
from discord_core.types.dispatch_result import ChannelHistoryResult, GuildEmojisResult, is_not_found_error

from discord_core.interfaces.database_protocols import MarkovStore
from discord_core.types.markov import MarkovMessageWrite

# (guild_id, max_retries=): ask for the guild's emojis; the answer arrives later as a
# GuildEmojisResult on the cog's result queue.
RequestEmojis = Callable[..., Awaitable[None]]
# (guild_id, channel_id, limit=, after=, after_message_id=): ask for channel history; the
# answer arrives later as a ChannelHistoryResult.
RequestHistory = Callable[..., Awaitable[None]]


def clean_message(content: str, emojis: List[dict]):
    '''
    Clean channel message
    content :   Full message content to clean
    emojis  :   List of server emoji dicts ({'id', 'name', 'animated'}, as the
                dispatcher serialises them), so we can remove any not from server

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
    emoji_ids = [emoji['id'] for emoji in emojis]
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

class MarkovLoop:
    '''
    One iteration of each markov loop. Discord-free: see ``loop_logic``.

    ``emoji_cache`` is the guild id -> emoji list the result handler fills and
    ``clean_message`` reads; the cog owns it and passes the same dict every time.
    '''

    def __init__(self, logger: Logger, markov_store: MarkovStore, loop_sleep_interval: float,
                 message_check_limit: int, history_retention_days: int, emoji_cache: dict[int, list],
                 request_emojis: RequestEmojis, request_history: RequestHistory):
        self.logger = logger
        self.markov_store = markov_store
        self.loop_sleep_interval = loop_sleep_interval
        self.message_check_limit = message_check_limit
        self.history_retention_days = history_retention_days
        self.emoji_cache = emoji_cache
        self.request_emojis = request_emojis
        self.request_history = request_history

    # https://srome.github.io/Making-A-Markov-Chain-Twitter-Bot-In-Python/
    def build_word_pairs(self, corpus: List[str]) -> List[tuple]:
        '''
        Turn one message's corpus into leader/follower pairs.

        corpus : List of strings from message, after cleaning

        Pure: it builds the pairs and hands them back for the store to persist.
        It used to open a session and commit once per pair, which cost a fresh
        connection per pair under NullPool; `!267` moved the write onto the
        caller's session, and the store owns it now. Pair-building is the markov
        algorithm and stays here -- what left is every decision about sessions
        and commits.

        Words at or over the column width are dropped rather than truncated, the
        same as before, because a truncated word is a real word that was never
        said.
        '''
        def ensure_word(word):
            if len(word) >= 255:
                self.logger.debug(f'Markov :: Cannot add word "{word}", is too long')
                return None
            return word

        pairs = []
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
            pairs.append((leader_word, follower_word))
        return pairs

    async def request(self):
        '''
        Producer loop: submit Discord fetch requests for each tracked channel.
        '''
        await sleep(self.loop_sleep_interval)
        retention_cutoff = datetime.now(timezone.utc) - timedelta(days=self.history_retention_days)
        self.logger.debug(f'Entering message gather loop, using cutoff {retention_cutoff}')

        # Entries, and the session is closed before the first dispatch. This used
        # to iterate live rows inside `async with self.with_db_session()`, which
        # held one postgres connection open across every emoji fetch and history
        # request in the sweep -- under NullPool, for the whole fan-out.
        markov_channels = await self.markov_store.list_channels()
        for markov_channel in markov_channels:
            guild_id = markov_channel.server_id
            async with async_otel_span_wrapper('markov.channel_check', kind=SpanKind.INTERNAL,
                                               attributes={DiscordContextNaming.CHANNEL.value: markov_channel.channel_id,
                                                           DiscordContextNaming.GUILD.value: markov_channel.server_id}):
                self.logger.debug(f'Checking channel id: {markov_channel.channel_id}, server id: {markov_channel.server_id}')
                await self.request_emojis(guild_id, max_retries=5)
                self.logger.info('Gathering markov messages for '
                                f'channel {markov_channel.channel_id}')
                if not markov_channel.last_message_id:
                    await self.request_history(
                        guild_id, markov_channel.channel_id,
                        limit=self.message_check_limit,
                        after=retention_cutoff,
                    )
                else:
                    await self.request_history(
                        guild_id, markov_channel.channel_id,
                        limit=self.message_check_limit,
                        after_message_id=markov_channel.last_message_id,
                    )

        # Delete old records
        async with async_otel_span_wrapper('markov.message_delete', kind=SpanKind.INTERNAL):
            await self.markov_store.prune_relations_before(retention_cutoff)
            self.logger.debug('Deleted expired/old markov relations')

    async def process_emojis_result(self, result: GuildEmojisResult):
        '''
        Process a guild emoji result, caching the emoji list on success.

        A result carrying an error is still a handled iteration — the fetch failed
        upstream, the consumer did its job. An early return before the caller's
        record_success() would let a run of upstream errors read as a wedged
        consumer, so this never raises on result.error.
        '''
        async with async_otel_span_wrapper('markov.emojis_result', kind=SpanKind.CONSUMER,
                                           attributes={DiscordContextNaming.GUILD.value: result.guild_id},
                                           links=span_links_from_context(result.span_context)):
            if result.error:
                self.logger.error(f'Markov :: Failed to fetch emojis for server {result.guild_id}: {result.error}')
                return
            self.emoji_cache[result.guild_id] = result.emojis

    async def process_history_result(self, result: ChannelHistoryResult):
        '''
        Process a channel history result: filter messages and save to the Markov chain.

        Runs in the result-consumer task, long after every span that produced the
        request has closed, so it opens its own span linked back to the requesting
        one. Without that link these logs carry no trace at all.
        '''
        guild_id = result.guild_id
        channel_id = result.channel_id

        async with async_otel_span_wrapper('markov.history_result', kind=SpanKind.CONSUMER,
                                           attributes={DiscordContextNaming.CHANNEL.value: channel_id,
                                                       DiscordContextNaming.GUILD.value: guild_id},
                                           links=span_links_from_context(result.span_context)):
            return await self._apply_history_result(result, guild_id, channel_id)

    async def _apply_history_result(self, result: ChannelHistoryResult,
                                            guild_id: int, channel_id: int):
        '''Body of _process_history_result, run inside the consumer span.'''
        if result.error:
            # Matched on status rather than isinstance(result.error, NotFound):
            # results arriving through the dispatcher carry a DispatchRemoteError
            # rebuilt from JSON, so an isinstance check never matches in the split
            # deployment and the channel stays pinned to a dead message forever.
            if is_not_found_error(result.error) and result.after_message_id:
                self.logger.info(f'Unable to find message {result.after_message_id}'
                                 f' in channel {channel_id} in server {guild_id}, '
                                 'clearing relations and restarting from retention cutoff')
                # Clearing last_message_id makes the next check loop re-request
                # with after=retention_cutoff, so the channel rebuilds as far
                # back as retention allows rather than re-fetching a message
                # that no longer exists. Both halves are one transaction in the
                # store: a clear that dropped the id but kept the relations
                # would double every word it re-gathered.
                await self.markov_store.reset_channel(guild_id, channel_id)
            else:
                self.logger.error(
                    f'Markov :: Failed to fetch history for channel {channel_id} '
                    f'in server {guild_id}: {result.error}'
                )
            return

        if not result.messages:
            self.logger.debug(f'No new messages for channel {channel_id}')
            return

        emojis = self.emoji_cache.get(guild_id, [])
        writes = []
        for message in result.messages:
            self.logger.debug(f'Gathering message {message.id} '
                              f'for channel {channel_id}')
            add_message = True
            if not message.content or message.author_bot:
                add_message = False
            elif message.content[0] == '!':
                add_message = False
            corpus = None
            if add_message:
                corpus = clean_message(message.content, emojis)
            word_pairs = []
            if corpus:
                self.logger.info(f'Attempting to add corpus "{corpus}" '
                                 f'to channel {channel_id}')
                word_pairs = self.build_word_pairs(corpus)
            # A message that contributed nothing still advances last_message_id.
            # Skipping it here would re-fetch it every cycle forever.
            writes.append(MarkovMessageWrite(word_pairs=word_pairs,
                                             message_timestamp=message.created_at,
                                             last_message_id=message.id))

        # One call for the batch. The store still commits per message, so the
        # atomicity `!267` established survives -- what does not survive the
        # seam is a session held open across the loop.
        written = await self.markov_store.save_messages(guild_id, channel_id, writes)
        if written is None:
            self.logger.debug(f'Markov channel {channel_id} not found in DB, skipping')
            return
        self.logger.debug(f'Done with channel {channel_id}')
