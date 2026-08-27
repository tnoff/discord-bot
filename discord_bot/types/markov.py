'''
Serializable views of the markov tables.

The markov group is the second slice of projects/discord-db-tier-extraction to get a
Protocol, and these types are what make that Protocol implementable twice. The
rule is the one `types/video_cache.py` states: every value crossing the seam has
to survive a network hop, so nothing here is a live SQLAlchemy instance.

MarkovChannelEntry replaces the `MarkovChannel` rows the cog used to receive.
That mattered more here than it did for the video cache, because the cog did not
merely read those rows -- it **mutated** them (`markov_channel.last_message_id =
message.id`) and deleted them (`db_session.delete(markov_channel)`), both of
which are session-bound operations with no remote equivalent. Those are explicit
store methods now.

MarkovMessageWrite exists so a channel's whole history batch crosses in one
call. The natural per-message signature would be one round trip per message and,
in-process under NullPool, one postgres connection per message -- reintroducing
the exact cost `!267` removed by staging a message's relations on the caller's
session. The batch keeps the per-message commit boundary that makes a message
atomic (its relations and the channel's `last_message_id` land together) while
keeping the connection and the round trip per *batch*.
'''
from datetime import datetime
from typing import List, Optional, Tuple

from pydantic import BaseModel, Field


class MarkovChannelEntry(BaseModel):
    '''One tracked channel's `markov_channel` row, detached from any DB session.'''
    id: int
    channel_id: Optional[int] = None
    server_id: Optional[int] = None
    last_message_id: Optional[int] = None

    @classmethod
    def from_row(cls, row) -> 'MarkovChannelEntry':
        '''
        Build an entry from a live MarkovChannel, reading every column eagerly.

        The only place the ORM object is touched. Keeping the conversion here is
        what lets `discord_bot.database` leave a caller's import chain once this
        store is remote.

        row : A MarkovChannel instance, still attached to its session
        '''
        return cls(
            id=row.id,
            channel_id=row.channel_id,
            server_id=row.server_id,
            last_message_id=row.last_message_id,
        )


class MarkovMessageWrite(BaseModel):
    '''
    One message's contribution to the chain: its word pairs and its id.

    `word_pairs` is empty for a message that contributed nothing -- a bot post, a
    command, an image with no text. Those still advance `last_message_id`, which
    is why an empty list is a normal value here and not a reason to skip the
    write. Dropping them would re-fetch the same messages every cycle forever.
    '''
    word_pairs: List[Tuple[str, str]] = Field(default_factory=list)
    message_timestamp: datetime
    last_message_id: int
