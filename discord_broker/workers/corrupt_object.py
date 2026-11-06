'''
Evict a cached object a player found does not match the digest in its key.

A function over a broker rather than a method on it: the broker base class is at the limit of public
methods, and this reads three of its attributes and nothing else.
'''
import asyncio
import logging
from pathlib import Path
from tempfile import TemporaryDirectory

from discord_core.types.media_request import MediaRequest
from discord_core.utils.integrations.s3 import (
    ObjectDigestMismatch, ObjectStorageException, delete_file, get_file, key_digest,
)

from discord_broker.interfaces.broker_protocols import MediaBrokerBase, bucket_for_key

logger = logging.getLogger(__name__)


async def evict_corrupt_object(broker: MediaBrokerBase, media_request: MediaRequest, key: str) -> bool:
    '''
    Evict a cached object whose bytes a player found do not match the digest in its key.

    The report is a claim, not proof: a fetch can be cut short in transit, and a healthy song is
    not worth losing to that. So this reads the object itself, and evicts only if that read fails
    the digest check too. Returns True if the object and its catalog row were removed.

    Nothing is done (False) for a key with no digest, one the catalog does not hold for this
    request (already evicted, or replaced by another copy), or an object that cannot be read for any
    other reason, which proves nothing about its bytes.

    The row goes before the object, and the object only once the row is confirmed gone: a row left
    pointing at a missing object would serve cache hits that cannot be played, where an object left
    without its row is only wasted space.

    broker : The broker holding the catalog and the bucket names
    media_request : The request the player fetched the object for; names the catalog row
    key : The object key that failed the check
    '''
    if not broker.video_cache or key_digest(key) is None:
        return False
    cached = await broker.video_cache.get_webpage_url_item(media_request)
    if cached is None or str(cached.file_path) != key:
        return False
    bucket = bucket_for_key(key, broker.bucket_name, broker.cache_bucket_name)
    with TemporaryDirectory() as tmp_dir:
        try:
            await asyncio.to_thread(get_file, bucket, key, Path(tmp_dir) / Path(key).name)
        except ObjectDigestMismatch:
            pass
        except ObjectStorageException:
            logger.warning('Could not read %s to verify a corrupt report, leaving it', key, exc_info=True)
            return False
        else:
            logger.info('%s passed its digest check on re-read, leaving it', key)
            return False
    logger.warning('Evicting %s: its bytes do not match the digest in its key', key)
    await broker.video_cache.remove_video_cache([], base_paths=[key])
    # A db pod from before base_paths answers this call without removing anything. Deleting the
    # object then would leave a cache hit that can never play, so confirm the row is gone first.
    remaining = await broker.video_cache.get_webpage_url_item(media_request)
    if remaining is not None and str(remaining.file_path) == key:
        logger.error('The catalog still holds %s after asking it to drop the row, leaving the object', key)
        return False
    await asyncio.to_thread(delete_file, bucket, key)
    return True
