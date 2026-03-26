from dataclasses import dataclass
from datetime import datetime


@dataclass
class FetchedMessage:
    '''Serializable representation of a Discord message returned by dispatch_channel_history.'''
    id: int
    content: str
    created_at: datetime
    author_bot: bool
