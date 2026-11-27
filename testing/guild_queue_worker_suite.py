'''
The tests the Redis guild-queue workers share.

RedisDownloadWorker and RedisYoutubeMusicSearchWorker both sit on RedisGuildBlockMixin and keep
a per-guild queue in Redis, so the guild block, the clear-with-predicate and the dangling-entry
behaviour is the same on both. A worker's test module subclasses ``GuildQueueWorkerSuite``,
supplies ``make_worker`` and ``make_request`` and keeps only what is specific to its pod.

Test names here must not be exactly 40 characters long: trufflehog's Lob detector matches
`test_` + 35 chars, the shape of a Lob test API key, and fails pr-check:secrets.
'''
# White-box tests: they inspect the worker's Redis client and key helpers directly.
# pylint: disable=protected-access
import pytest

from discord_core.types.queue import PutsBlocked
from discord_core.workers.redis_guild_queue import GUILD_BLOCK_TTL_SECONDS


def spy_redis_writes(worker) -> list:
    '''Record every SET / DELETE the worker's Redis client receives, as ('set'|'delete', key).'''
    writes = []
    client = worker._manager.client
    real_set, real_delete = client.set, client.delete

    async def _spy_set(*args, **kwargs):
        writes.append(('set', args[0]))
        return await real_set(*args, **kwargs)

    async def _spy_delete(*args, **kwargs):
        writes.append(('delete', args[0]))
        return await real_delete(*args, **kwargs)

    client.set, client.delete = _spy_set, _spy_delete
    return writes


class GuildQueueWorkerSuite:
    '''Cases every Redis guild-queue worker must pass.

    Not collected itself; a subclass in a ``test_*`` module supplies the hooks and attributes.
    '''
    # The ZSET that tracks which guilds have work queued, for a request from make_request().
    guilds_key = ''

    def make_worker(self):
        '''A worker over a fresh fake Redis.'''
        raise NotImplementedError

    def make_request(self, guild_id=7):
        '''A request this worker accepts.'''
        raise NotImplementedError

    @pytest.mark.asyncio
    async def test_block_guild_expires(self):
        '''The block carries a TTL, mirroring the transient in-process block. A key with no
        expiry would wedge the guild permanently instead.'''
        w = self.make_worker()
        await w.block_guild(7)
        ttl = await w._manager.client.ttl(w._guild_blocked_key(7))
        assert 0 < ttl <= GUILD_BLOCK_TTL_SECONDS

    @pytest.mark.asyncio
    async def test_submit_allowed_again_once_block_expires(self):
        '''Once the window lapses the guild accepts submissions again.'''
        w = self.make_worker()
        await w.block_guild(7)
        await w._manager.client.delete(w._guild_blocked_key(7))  # simulate expiry
        await w.submit(7, self.make_request(7))
        assert await w.queue_size(7) == 1

    @pytest.mark.asyncio
    async def test_block_is_scoped_to_one_guild(self):
        '''Blocking one guild leaves the others accepting work.'''
        w = self.make_worker()
        await w.block_guild(7)
        await w.submit(8, self.make_request(8))
        assert await w.queue_size(8) == 1

    @pytest.mark.asyncio
    async def test_submit_raises_puts_blocked_while_guild_blocked(self):
        '''The flag is enforced on submit — it used to be written and never read, so a
        blocked guild kept accepting work.'''
        w = self.make_worker()
        await w.block_guild(7)
        with pytest.raises(PutsBlocked):
            await w.submit(7, self.make_request(7))
        assert await w.queue_size(7) == 0

    @pytest.mark.asyncio
    async def test_clear_guild_queue_prunes_dangling_entry_with_missing_payload(self):
        '''A queue entry whose payload already TTL'd is removed and not returned.'''
        w = self.make_worker()
        mr = self.make_request(7)
        await w.submit(7, mr)
        # Simulate the request payload having TTL'd away.
        await w._manager.client.delete(w._request_key(str(mr.uuid)))
        dropped = await w.clear_guild_queue(7)
        assert dropped == []
        assert await w.queue_size(7) == 0

    @pytest.mark.asyncio
    async def test_clear_guild_queue_honours_preserve_predicate(self):
        '''A kept request stays queued (and the guild stays in the tracker).'''
        w = self.make_worker()
        keep = self.make_request(7)
        drop = self.make_request(7)
        await w.submit(7, keep)
        await w.submit(7, drop)
        dropped = await w.clear_guild_queue(
            7, preserve_predicate=lambda req: str(req.uuid) == str(keep.uuid))
        assert [str(d.uuid) for d in dropped] == [str(drop.uuid)]
        assert await w.queue_size(7) == 1
        assert await w._manager.client.zscore(self.guilds_key, '7') is not None
