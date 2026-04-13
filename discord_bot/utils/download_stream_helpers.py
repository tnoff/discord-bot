from enum import Enum, StrEnum

from discord_bot.types.media_request import MediaRequest
from discord_bot.types.download import DownloadResult


class DownloadStreamKey(Enum):
    '''Redis Stream key templates for the download worker infrastructure.'''
    INPUT          = 'discord_bot:download:input'
    RESULT         = 'discord_bot:download:result:{process_id}'
    CONSUMER_GROUP = 'discord_bot:download:workers'


class DownloadResultType(StrEnum):
    '''Type tags for download result stream messages.'''
    DOWNLOAD_RESULT = 'download_result'


def download_input_stream_key() -> str:
    '''Return the Redis Stream key for the download worker input stream.'''
    return DownloadStreamKey.INPUT.value


def download_result_stream_key(process_id: str) -> str:
    '''Return the Redis Stream key for the result stream of the given process.'''
    return DownloadStreamKey.RESULT.value.format(process_id=process_id)


def encode_download_request(media_request: MediaRequest, priority: int | None = None) -> dict:
    '''Encode a MediaRequest as a flat dict suitable for XADD.'''
    return {
        'media_request': media_request.model_dump_json(),
        'priority': str(priority) if priority is not None else '',
    }


def decode_download_request(fields: dict) -> tuple[MediaRequest, int | None]:
    '''Decode a raw Redis fields dict into a (MediaRequest, priority) tuple.'''
    media_request = MediaRequest.model_validate_json(fields['media_request'])
    priority_str = fields.get('priority', '')
    priority = int(priority_str) if priority_str else None
    return media_request, priority


def encode_download_result(result: DownloadResult) -> dict:
    '''Encode a DownloadResult as a flat dict suitable for XADD.'''
    return {
        'result_type': DownloadResultType.DOWNLOAD_RESULT,
        'payload': result.model_dump_json(),
    }


def decode_download_result(fields: dict) -> DownloadResult:
    '''Decode a raw Redis fields dict into a DownloadResult.'''
    return DownloadResult.model_validate_json(fields['payload'])
