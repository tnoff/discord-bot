'''
The request bodies of the queue-worker seam (routes/queue_worker.py), served by the downloader at /downloads and by
the search pod at /search/ytmusic.

Here so the client builds and the server reads the SAME model rather than each writing out the field names, and so
the contract tests read them. The answers are in `types/queue_worker_responses.py`.
'''
from pydantic import BaseModel

from discord_core.types.playlist_add_request import AnyMediaRequest


class SubmitRequestBody(BaseModel):
    '''POST {prefix}'''
    guild_id: int
    media_request: AnyMediaRequest
    priority: int | None = None


class ClearRequestBody(BaseModel):
    '''POST {prefix}/clear'''
    guild_id: int
    # A predicate cannot cross HTTP, so True means "keep the requests that only add to a playlist".
    preserve_playlist_adds: bool = False


class BlockRequestBody(BaseModel):
    '''POST {prefix}/block'''
    guild_id: int
