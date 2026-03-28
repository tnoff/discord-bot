from dataclasses import dataclass
from datetime import datetime


@dataclass
class FetchedMessage:
    '''Serializable representation of a Discord message returned by dispatch_channel_history.'''
    id: int
    content: str
    created_at: datetime
    author_bot: bool

    def to_dict(self) -> dict:
        '''Serialize to a JSON-safe dict.'''
        return {'id': self.id, 'content': self.content,
                'created_at': self.created_at.isoformat(), 'author_bot': self.author_bot}

    @classmethod
    def from_dict(cls, data: dict) -> 'FetchedMessage':
        '''Deserialize from a dict produced by to_dict().'''
        return cls(id=data['id'], content=data['content'],
                   created_at=datetime.fromisoformat(data['created_at']),
                   author_bot=data['author_bot'])
