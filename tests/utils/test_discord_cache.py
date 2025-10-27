"""
Tests for Discord object caching utilities.
"""

import asyncio
import time
from unittest.mock import Mock, AsyncMock

import pytest

from discord_bot.utils.discord_cache import TTLCache, DiscordObjectCache, CachingHelper


class TestTTLCache:  #pylint:disable=protected-access
    """Tests for TTLCache class."""

    def test_init(self):
        """Test cache initialization."""
        cache = TTLCache(maxsize=100, ttl=3600)
        assert cache.maxsize == 100
        assert cache.ttl == 3600
        assert cache.size == 0
        assert cache.hit_rate == 0.0

    def test_set_and_get(self):
        """Test basic set and get operations."""
        cache = TTLCache(maxsize=10, ttl=3600)

        # Set value
        cache.set('key1', 'value1')
        assert cache.size == 1

        # Get value
        value = cache.get('key1')
        assert value == 'value1'
        assert cache._hits == 1
        assert cache._misses == 0

    def test_get_missing_key(self):
        """Test getting a key that doesn't exist."""
        cache = TTLCache()

        value = cache.get('nonexistent')
        assert value is None
        assert cache._misses == 1

    def test_ttl_expiration(self):
        """Test that entries expire after TTL."""
        cache = TTLCache(maxsize=10, ttl=1)  # 1 second TTL

        # Set value
        cache.set('key1', 'value1')
        assert cache.get('key1') == 'value1'

        # Wait for expiration
        time.sleep(1.1)

        # Should be expired now
        value = cache.get('key1')
        assert value is None
        assert cache.size == 0  # Should be removed
        assert cache._evictions == 1

    def test_maxsize_eviction(self):
        """Test that oldest entries are evicted when maxsize reached."""
        cache = TTLCache(maxsize=3, ttl=3600)

        # Fill cache
        cache.set('key1', 'value1')
        cache.set('key2', 'value2')
        cache.set('key3', 'value3')
        assert cache.size == 3

        # Add one more - should evict oldest (key1)
        cache.set('key4', 'value4')
        assert cache.size == 3
        assert cache.get('key1') is None  # Evicted
        assert cache.get('key2') == 'value2'  # Still there
        assert cache.get('key4') == 'value4'  # New entry
        assert cache._evictions == 1

    def test_update_existing_key(self):
        """Test updating an existing key."""
        cache = TTLCache(maxsize=10, ttl=3600)

        cache.set('key1', 'value1')
        assert cache.size == 1

        # Update with new value
        cache.set('key1', 'value2')
        assert cache.size == 1  # Size shouldn't change
        assert cache.get('key1') == 'value2'

    def test_delete(self):
        """Test deleting entries."""
        cache = TTLCache()

        cache.set('key1', 'value1')
        assert cache.size == 1

        # Delete existing key
        deleted = cache.delete('key1')
        assert deleted is True
        assert cache.size == 0

        # Delete non-existent key
        deleted = cache.delete('key2')
        assert deleted is False

    def test_clear(self):
        """Test clearing all entries."""
        cache = TTLCache()

        cache.set('key1', 'value1')
        cache.set('key2', 'value2')
        cache.set('key3', 'value3')
        assert cache.size == 3

        cache.clear()
        assert cache.size == 0
        assert cache._hits == 0
        assert cache._misses == 0
        assert cache._evictions == 0

    def test_cleanup_expired(self):
        """Test manual cleanup of expired entries."""
        cache = TTLCache(maxsize=10, ttl=1)  # 1 second TTL

        # Add multiple entries
        cache.set('key1', 'value1')
        cache.set('key2', 'value2')
        cache.set('key3', 'value3')

        # Wait for expiration
        time.sleep(1.1)

        # Manually clean up
        removed = cache.cleanup_expired()
        assert removed == 3
        assert cache.size == 0

    def test_hit_rate_calculation(self):
        """Test hit rate calculation."""
        cache = TTLCache()

        cache.set('key1', 'value1')

        # 1 hit
        cache.get('key1')
        assert cache.hit_rate == 1.0  # 1/1

        # 1 miss
        cache.get('key2')
        assert cache.hit_rate == 0.5  # 1/2

        # 2 more hits
        cache.get('key1')
        cache.get('key1')
        assert cache.hit_rate == 0.75  # 3/4

    def test_stats(self):
        """Test statistics reporting."""
        cache = TTLCache(maxsize=100, ttl=3600)

        cache.set('key1', 'value1')
        cache.get('key1')  # hit
        cache.get('key2')  # miss

        stats = cache.stats()
        assert stats['size'] == 1
        assert stats['maxsize'] == 100
        assert stats['ttl'] == 3600
        assert stats['hits'] == 1
        assert stats['misses'] == 1
        assert stats['evictions'] == 0
        assert stats['hit_rate'] == 0.5

    def test_lru_behavior(self):
        """Test LRU-like behavior when accessing entries."""
        cache = TTLCache(maxsize=3, ttl=3600)

        cache.set('key1', 'value1')
        cache.set('key2', 'value2')
        cache.set('key3', 'value3')

        # Access key1 to move it to end (most recently used)
        cache.get('key1')

        # Add new entry - should evict key2 (least recently used)
        cache.set('key4', 'value4')

        assert cache.get('key1') == 'value1'  # Still there
        assert cache.get('key2') is None      # Evicted
        assert cache.get('key3') == 'value3'  # Still there
        assert cache.get('key4') == 'value4'  # New entry


class TestDiscordObjectCache:  #pylint:disable=protected-access
    """Tests for DiscordObjectCache class."""

    def test_init(self):
        """Test cache initialization."""
        cache = DiscordObjectCache()

        assert cache.guild_cache.maxsize == 100
        assert cache.channel_cache.maxsize == 1000
        assert cache.message_cache.maxsize == 10000
        assert cache.emoji_cache.maxsize == 10000

    def test_init_with_custom_sizes(self):
        """Test initialization with custom sizes."""
        cache = DiscordObjectCache(
            guild_maxsize=50,
            guild_ttl=1800,
            channel_maxsize=500,
            message_ttl=300
        )

        assert cache.guild_cache.maxsize == 50
        assert cache.guild_cache.ttl == 1800
        assert cache.channel_cache.maxsize == 500
        assert cache.message_cache.ttl == 300

    def test_guild_cache_operations(self):
        """Test guild cache operations."""
        cache = DiscordObjectCache()
        mock_guild = Mock()
        mock_guild.id = 12345

        # Set guild
        cache.set_guild(12345, mock_guild)
        assert cache.guild_cache.size == 1

        # Get guild
        guild = cache.get_guild(12345)
        assert guild == mock_guild

        # Delete guild
        deleted = cache.delete_guild(12345)
        assert deleted is True
        assert cache.get_guild(12345) is None

    def test_channel_cache_operations(self):
        """Test channel cache operations."""
        cache = DiscordObjectCache()
        mock_channel = Mock()
        mock_channel.id = 67890

        cache.set_channel(67890, mock_channel)
        assert cache.get_channel(67890) == mock_channel

        cache.delete_channel(67890)
        assert cache.get_channel(67890) is None

    def test_message_cache_operations(self):
        """Test message cache operations."""
        cache = DiscordObjectCache()
        mock_message = Mock()
        mock_message.id = 111222

        cache.set_message(111222, mock_message)
        assert cache.get_message(111222) == mock_message

        cache.delete_message(111222)
        assert cache.get_message(111222) is None

    def test_emoji_cache_operations(self):
        """Test emoji cache operations."""
        cache = DiscordObjectCache()
        mock_emojis = [Mock(), Mock(), Mock()]

        cache.set_emojis(12345, mock_emojis)
        emojis = cache.get_emojis(12345)
        assert emojis == mock_emojis

        cache.delete_emojis(12345)
        assert cache.get_emojis(12345) is None

    def test_clear_all(self):
        """Test clearing all caches."""
        cache = DiscordObjectCache()

        # Populate all caches
        cache.set_guild(1, Mock())
        cache.set_channel(2, Mock())
        cache.set_message(3, Mock())
        cache.set_emojis(4, [Mock()])

        assert cache.guild_cache.size == 1
        assert cache.channel_cache.size == 1
        assert cache.message_cache.size == 1
        assert cache.emoji_cache.size == 1

        # Clear all
        cache.clear_all()

        assert cache.guild_cache.size == 0
        assert cache.channel_cache.size == 0
        assert cache.message_cache.size == 0
        assert cache.emoji_cache.size == 0

    def test_stats(self):
        """Test statistics reporting."""
        cache = DiscordObjectCache()

        cache.set_guild(1, Mock())
        cache.set_channel(2, Mock())
        cache.get_guild(1)  # hit
        cache.get_guild(999)  # miss

        stats = cache.stats()

        assert 'guild' in stats
        assert 'channel' in stats
        assert 'message' in stats
        assert 'emoji' in stats

        assert stats['guild']['size'] == 1
        assert stats['guild']['hits'] == 1
        assert stats['guild']['misses'] == 1
        assert stats['channel']['size'] == 1

    @pytest.mark.asyncio
    async def test_cleanup_task_lifecycle(self):
        """Test starting and stopping cleanup task."""
        cache = DiscordObjectCache()

        # Start cleanup task
        await cache.start_cleanup_task()
        assert cache._cleanup_task is not None
        assert not cache._cleanup_task.done()

        # Store reference before stopping
        task = cache._cleanup_task

        # Stop cleanup task
        await cache.stop_cleanup_task()
        assert task.cancelled() or task.done()
        assert cache._cleanup_task is None

    @pytest.mark.asyncio
    async def test_cleanup_task_removes_expired(self):
        """Test that cleanup task removes expired entries."""
        cache = DiscordObjectCache(
            guild_ttl=1,
            channel_ttl=1,
            message_ttl=1,
            emoji_ttl=1
        )

        # Add entries
        cache.set_guild(1, Mock())
        cache.set_channel(2, Mock())
        cache.set_message(3, Mock())
        cache.set_emojis(4, [Mock()])

        assert cache.guild_cache.size == 1
        assert cache.channel_cache.size == 1

        # Wait for expiration
        await asyncio.sleep(1.1)

        # Manually trigger cleanup (normally done by background task)
        guild_removed = cache.guild_cache.cleanup_expired()
        channel_removed = cache.channel_cache.cleanup_expired()
        message_removed = cache.message_cache.cleanup_expired()
        emoji_removed = cache.emoji_cache.cleanup_expired()

        assert guild_removed == 1
        assert channel_removed == 1
        assert message_removed == 1
        assert emoji_removed == 1


class TestCachingHelper:
    """Tests for CachingHelper class."""

    @pytest.mark.asyncio
    async def test_get_or_fetch_guild_cache_hit(self):
        """Test get_or_fetch_guild with cache hit."""
        cache = DiscordObjectCache()
        mock_bot = Mock()
        helper = CachingHelper(cache, mock_bot)

        # Pre-populate cache
        mock_guild = Mock()
        mock_guild.id = 12345
        cache.set_guild(12345, mock_guild)

        # Fetch - should use cache
        guild = await helper.get_or_fetch_guild(12345)

        assert guild == mock_guild
        assert not mock_bot.fetch_guild.called  # Should not fetch

    @pytest.mark.asyncio
    async def test_get_or_fetch_guild_cache_miss(self):
        """Test get_or_fetch_guild with cache miss."""
        cache = DiscordObjectCache()
        mock_bot = Mock()
        mock_guild = Mock()
        mock_guild.id = 12345
        mock_bot.fetch_guild = AsyncMock(return_value=mock_guild)

        helper = CachingHelper(cache, mock_bot)

        # Fetch - should call Discord API
        guild = await helper.get_or_fetch_guild(12345)

        assert guild == mock_guild
        mock_bot.fetch_guild.assert_called_once_with(12345)

        # Should now be in cache
        cached_guild = cache.get_guild(12345)
        assert cached_guild == mock_guild

    @pytest.mark.asyncio
    async def test_get_or_fetch_channel_cache_hit(self):
        """Test get_or_fetch_channel with cache hit."""
        cache = DiscordObjectCache()
        mock_bot = Mock()
        helper = CachingHelper(cache, mock_bot)

        # Pre-populate cache
        mock_channel = Mock()
        mock_channel.id = 67890
        cache.set_channel(67890, mock_channel)

        # Fetch - should use cache
        channel = await helper.get_or_fetch_channel(67890)

        assert channel == mock_channel
        assert not mock_bot.fetch_channel.called

    @pytest.mark.asyncio
    async def test_get_or_fetch_channel_cache_miss(self):
        """Test get_or_fetch_channel with cache miss."""
        cache = DiscordObjectCache()
        mock_bot = Mock()
        mock_channel = Mock()
        mock_channel.id = 67890
        mock_bot.fetch_channel = AsyncMock(return_value=mock_channel)

        helper = CachingHelper(cache, mock_bot)

        # Fetch - should call Discord API
        channel = await helper.get_or_fetch_channel(67890)

        assert channel == mock_channel
        mock_bot.fetch_channel.assert_called_once_with(67890)

        # Should now be in cache
        cached_channel = cache.get_channel(67890)
        assert cached_channel == mock_channel

    @pytest.mark.asyncio
    async def test_get_or_fetch_message_cache_hit(self):
        """Test get_or_fetch_message with cache hit."""
        cache = DiscordObjectCache()
        mock_bot = Mock()
        helper = CachingHelper(cache, mock_bot)

        # Pre-populate cache
        mock_message = Mock()
        mock_message.id = 111222
        cache.set_message(111222, mock_message)

        # Fetch - should use cache
        message = await helper.get_or_fetch_message(67890, 111222)

        assert message == mock_message
        assert not mock_bot.fetch_channel.called

    @pytest.mark.asyncio
    async def test_get_or_fetch_message_cache_miss(self):
        """Test get_or_fetch_message with cache miss."""
        cache = DiscordObjectCache()
        mock_bot = Mock()

        # Mock channel with fetch_message
        mock_channel = Mock()
        mock_message = Mock()
        mock_message.id = 111222
        mock_channel.fetch_message = AsyncMock(return_value=mock_message)
        mock_bot.fetch_channel = AsyncMock(return_value=mock_channel)

        helper = CachingHelper(cache, mock_bot)

        # Fetch - should call Discord API
        message = await helper.get_or_fetch_message(67890, 111222)

        assert message == mock_message
        mock_bot.fetch_channel.assert_called_once_with(67890)
        mock_channel.fetch_message.assert_called_once_with(111222)

        # Should now be in cache
        cached_message = cache.get_message(111222)
        assert cached_message == mock_message

        # Channel should also be cached
        cached_channel = cache.get_channel(67890)
        assert cached_channel == mock_channel

    @pytest.mark.asyncio
    async def test_get_or_fetch_emojis_cache_hit(self):
        """Test get_or_fetch_emojis with cache hit."""
        cache = DiscordObjectCache()
        mock_bot = Mock()
        helper = CachingHelper(cache, mock_bot)

        # Pre-populate cache
        mock_emojis = [Mock(), Mock()]
        cache.set_emojis(12345, mock_emojis)

        # Fetch - should use cache
        emojis = await helper.get_or_fetch_emojis(12345)

        assert emojis == mock_emojis
        assert not mock_bot.fetch_guild.called

    @pytest.mark.asyncio
    async def test_get_or_fetch_emojis_cache_miss(self):
        """Test get_or_fetch_emojis with cache miss."""
        cache = DiscordObjectCache()
        mock_bot = Mock()

        # Mock guild with fetch_emojis
        mock_guild = Mock()
        mock_emojis = [Mock(), Mock()]
        mock_guild.fetch_emojis = AsyncMock(return_value=mock_emojis)
        mock_bot.fetch_guild = AsyncMock(return_value=mock_guild)

        helper = CachingHelper(cache, mock_bot)

        # Fetch - should call Discord API
        emojis = await helper.get_or_fetch_emojis(12345)

        assert emojis == mock_emojis
        mock_bot.fetch_guild.assert_called_once_with(12345)
        mock_guild.fetch_emojis.assert_called_once()

        # Should now be in cache
        cached_emojis = cache.get_emojis(12345)
        assert cached_emojis == mock_emojis

        # Guild should also be cached
        cached_guild = cache.get_guild(12345)
        assert cached_guild == mock_guild

    @pytest.mark.asyncio
    async def test_multiple_cache_hits_no_api_calls(self):
        """Test that multiple fetches use cache without API calls."""
        cache = DiscordObjectCache()
        mock_bot = Mock()
        mock_guild = Mock()
        mock_guild.id = 12345
        mock_bot.fetch_guild = AsyncMock(return_value=mock_guild)

        helper = CachingHelper(cache, mock_bot)

        # First fetch - cache miss
        guild1 = await helper.get_or_fetch_guild(12345)
        assert mock_bot.fetch_guild.call_count == 1

        # Second fetch - cache hit
        guild2 = await helper.get_or_fetch_guild(12345)
        assert mock_bot.fetch_guild.call_count == 1  # No additional call

        # Third fetch - cache hit
        guild3 = await helper.get_or_fetch_guild(12345)
        assert mock_bot.fetch_guild.call_count == 1  # Still no additional call

        assert guild1 == guild2 == guild3
