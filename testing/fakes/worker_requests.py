'''The requests the queue-worker tests put on a queue.'''
from pathlib import Path

from discord_core.cogs.music_helpers.common import SearchType
from discord_core.types.media_download import MediaDownload
from discord_core.types.media_request import MediaRequest
from discord_core.types.search import SearchResult


def queue_request(*, guild_id=7, direct=False) -> MediaRequest:
    '''A MediaRequest for the guild: a direct URL when ``direct``, else a search string.'''
    return MediaRequest(
        guild_id=guild_id, channel_id=2, requester_name='tester', requester_id=9,
        search_result=SearchResult(
            search_type=SearchType.DIRECT if direct else SearchType.SEARCH,
            raw_search_string='https://example.com/v.mp4' if direct else 'song name',
        ),
    )


def direct_request(guild_id: int = 1, channel_id: int = 2) -> MediaRequest:
    '''A MediaRequest for a direct URL, as the broker tests register it.'''
    return MediaRequest(
        guild_id=guild_id,
        channel_id=channel_id,
        requester_name='tester',
        requester_id=9,
        search_result=SearchResult(
            search_type=SearchType.DIRECT,
            raw_search_string='https://example.com/video',
        ),
    )


async def register_track(broker, guild_id: int, title: str, cache_hit: bool = False) -> str:
    '''Register a direct request and its finished download with the broker; return the request uuid.'''
    request = MediaRequest(
        guild_id=guild_id, channel_id=2, requester_name='Alice', requester_id=9,
        search_result=SearchResult(search_type=SearchType.DIRECT,
                                   raw_search_string=f'https://example.com/{title}'))
    await broker.register_request(request)
    download = MediaDownload(
        Path(f'{title}.mp3'),
        {'id': title, 'title': title, 'webpage_url': f'https://example.com/{title}',
         'uploader': 'Someone', 'duration': 90, 'extractor': 'youtube'}, request)
    download.cache_hit = cache_hit
    await broker.register_download(download)
    return str(request.uuid)
