import json
import uuid
from dataclasses import dataclass, field
from enum import StrEnum


class RequestType(StrEnum):
    '''Type tags for dispatch request messages.'''
    UPDATE_MUTABLE = 'update_mutable'
    REMOVE_MUTABLE = 'remove_mutable'
    UPDATE_MUTABLE_CHANNEL = 'update_mutable_channel'
    SEND = 'send'
    DELETE = 'delete'
    FETCH_HISTORY = 'fetch_history'
    FETCH_EMOJIS = 'fetch_emojis'


class ResultType(StrEnum):
    '''Type tags for dispatch result messages.'''
    OK = 'ok'
    ERROR = 'error'
    HISTORY = 'history'
    EMOJIS = 'emojis'



def new_request_id() -> str:
    '''Return a new unique request ID string.'''
    return str(uuid.uuid4())


@dataclass
class StreamEnvelope:
    '''Decoded representation of a dispatch request message on the input stream.'''
    req_type: str
    payload: dict
    process_id: str
    request_id: str
    trace_context: dict = field(default_factory=dict)

    def encode(self) -> dict:
        '''Return flat str dict suitable for XADD.'''
        return {
            'req_type': self.req_type,
            'payload': json.dumps(self.payload),
            'process_id': self.process_id,
            'request_id': self.request_id,
            'trace_context': json.dumps(self.trace_context),
        }

    @classmethod
    def decode(cls, fields: dict) -> 'StreamEnvelope':
        '''Decode a raw Redis fields dict into a StreamEnvelope.'''
        return cls(
            req_type=fields['req_type'],
            payload=json.loads(fields['payload']),
            process_id=fields['process_id'],
            request_id=fields['request_id'],
            trace_context=json.loads(fields.get('trace_context', '{}')),
        )


@dataclass
class StreamResult:
    '''Decoded representation of a dispatch result message on the result stream.'''
    req_type: str
    request_id: str
    result_type: str
    payload: dict

    def encode(self) -> dict:
        '''Return flat str dict suitable for XADD on the result stream.'''
        return {
            'req_type': self.req_type,
            'request_id': self.request_id,
            'result_type': self.result_type,
            'payload': json.dumps(self.payload),
        }

    @classmethod
    def decode(cls, fields: dict) -> 'StreamResult':
        '''Decode a raw Redis fields dict into a StreamResult.'''
        return cls(
            req_type=fields['req_type'],
            request_id=fields['request_id'],
            result_type=fields['result_type'],
            payload=json.loads(fields['payload']),
        )
