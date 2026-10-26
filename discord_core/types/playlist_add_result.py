from pydantic import BaseModel


class PlaylistAddResult(BaseModel):
    '''Lightweight result from a metadata-only yt-dlp fetch for a playlist add.'''
    webpage_url: str
    title: str | None = None
    uploader: str | None = None
