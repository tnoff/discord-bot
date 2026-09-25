from datetime import datetime

from pydantic import BaseModel, field_serializer


class FetchedMessage(BaseModel):
    '''
    Serializable representation of a Discord message returned by dispatch_channel_history.

    A pydantic model rather than a dataclass because it IS a wire type: it is
    nested inside `ChannelHistoryResultBody`, and a dataclass cannot round-trip
    through a schema. See projects/seam-body-typing.

    **The serializer is load-bearing, not decoration.** Without it `model_dump()`
    returns a `datetime` object, `web.json_response` raises
    "Object of type datetime is not JSON serializable", and the failure lands at
    runtime on a real fetch while every unit test that compares `model_dump()`
    to a dict of datetimes passes. `to_dict()` has always emitted `.isoformat()`;
    this keeps that exactly, so the stored result bytes do not move.
    '''
    id: int
    content: str
    created_at: datetime
    author_bot: bool

    @field_serializer('created_at')
    def _created_at_isoformat(self, value: datetime) -> str:
        '''Emit the ISO string the wire has always carried.'''
        return value.isoformat()

    def to_dict(self) -> dict:
        '''Serialize to a JSON-safe dict.'''
        return self.model_dump()

    @classmethod
    def from_dict(cls, data: dict) -> 'FetchedMessage':
        '''Deserialize from a dict produced by to_dict().'''
        return cls.model_validate(data)
