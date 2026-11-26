'''Redis-backed stand-in worker and request builders for the queue-worker tests.'''
import json

from discord_core.cogs.music_helpers.common import SearchType
from discord_core.types.media_request import MediaRequest
from discord_core.types.playlist_add_request import PlaylistAddRequest
from discord_core.types.search import SearchResult
from discord_core.workers.redis_guild_queue import (
    RedisGuildBlockMixin, build_status_snapshot, collect_queue_sizes, drain_guild_zset)

KEY_PREFIX = 'qtest:guild:'


def make_request(guild_id: int = 7, text: str = 'some song') -> MediaRequest:
    '''A downloadable request for the given guild.'''
    return MediaRequest(guild_id=guild_id, channel_id=11, requester_name='tester',
                        requester_id=13,
                        search_result=SearchResult(search_type=SearchType.SEARCH,
                                                   raw_search_string=text))


def make_playlist_add(guild_id: int = 7, text: str = 'kept song') -> PlaylistAddRequest:
    '''A metadata-only request that adds to playlist 5.'''
    return PlaylistAddRequest(guild_id=guild_id, channel_id=11, requester_name='tester',
                              requester_id=13, playlist_id=5,
                              search_result=SearchResult(search_type=SearchType.SEARCH,
                                                         raw_search_string=text))


class _Manager:
    '''Just enough of RedisManager: a `client` attribute.'''

    def __init__(self, client):
        self.client = client


class _Base:
    '''Terminal submit the block mixin delegates to; records what got through.'''

    def __init__(self):
        self.submitted: list = []

    async def submit(self, guild_id, media_request, priority=None):
        '''Record the accepted submission.'''
        self.submitted.append((guild_id, media_request.uuid, priority))


class RedisQueueStub(RedisGuildBlockMixin, _Base):
    '''A queue worker over a fake Redis built from the shared primitives.'''

    GUILD_KEY_PREFIX = KEY_PREFIX

    def __init__(self, client):
        super().__init__()
        self._manager = _Manager(client)
        self.snapshot_overrides: dict = {}

    def queue_key(self, guild_id: int) -> str:
        '''ZSET holding a guild's pending request uuids.'''
        return f'{KEY_PREFIX}{guild_id}'

    @staticmethod
    def request_key(request_uuid: str) -> str:
        '''String key holding one request payload.'''
        return f'qtest:request:{request_uuid}'

    async def enqueue(self, media_request, score: float) -> None:
        '''Place a request in its guild's ZSET with the given score.'''
        client = self._manager.client
        await client.set(self.request_key(media_request.uuid),
                         json.dumps(media_request.model_dump(mode='json')))
        await client.zadd(self.queue_key(media_request.guild_id),
                          {media_request.uuid: score})

    async def queue_size(self, guild_id: int) -> int:
        '''Pending count for one guild.'''
        return await self._manager.client.zcard(self.queue_key(guild_id))

    async def clear_guild_queue(self, guild_id, preserve_predicate=None):
        '''Drain the guild's ZSET, keeping what the predicate preserves.'''
        return await drain_guild_zset(self._manager.client, self.queue_key(guild_id),
                                      self.request_key, preserve_predicate)

    async def status_snapshot(self) -> dict:
        '''The status dict for every guild that has a queue.'''
        client = self._manager.client
        guilds = [key.removeprefix(KEY_PREFIX) async for key in client.scan_iter(f'{KEY_PREFIX}[0-9]*')
                  if not key.endswith(':blocked')]
        sizes = await collect_queue_sizes(guilds, self.queue_size)
        base = {'failure_summary': 'none', 'failure_count': 0, 'backoff_seconds': 0}
        base.update(self.snapshot_overrides)
        return build_status_snapshot(base['failure_summary'], base['failure_count'],
                                     base['backoff_seconds'], sizes)
