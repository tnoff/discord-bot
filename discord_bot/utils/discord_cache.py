"""
Discord object caching utilities.

Provides TTL-based caches for Discord objects (guilds, channels, messages, emojis)
to reduce Discord API calls and improve performance.
"""

import time
from typing import Optional, Dict, Any, TypeVar, Generic
from collections import OrderedDict
import asyncio
from discord import Guild, TextChannel, Message


T = TypeVar('T')


class TTLCache(Generic[T]):
    """
    Time-To-Live cache with automatic expiration.

    Uses OrderedDict for LRU-like behavior when at capacity.
    Automatically evicts expired entries on access.
    """

    def __init__(self, maxsize: int = 1000, ttl: int = 3600):
        """
        Initialize TTL cache.

        Args:
            maxsize: Maximum number of entries to store
            ttl: Time-to-live in seconds (default: 1 hour)
        """
        self.maxsize = maxsize
        self.ttl = ttl
        self._cache: OrderedDict[Any, tuple[T, float]] = OrderedDict()
        self._hits = 0
        self._misses = 0
        self._evictions = 0

    def get(self, key: Any) -> Optional[T]:
        """
        Get value from cache.

        Args:
            key: Cache key

        Returns:
            Cached value if found and not expired, None otherwise
        """
        if key not in self._cache:
            self._misses += 1
            return None

        value, expiry_time = self._cache[key]

        # Check if expired
        if time.time() > expiry_time:
            # Expired - remove and return None
            del self._cache[key]
            self._misses += 1
            self._evictions += 1
            return None

        # Valid - move to end (LRU) and return
        self._cache.move_to_end(key)
        self._hits += 1
        return value

    def set(self, key: Any, value: T) -> None:
        """
        Set value in cache with TTL.

        Args:
            key: Cache key
            value: Value to cache
        """
        expiry_time = time.time() + self.ttl

        # If key exists, update it
        if key in self._cache:
            self._cache[key] = (value, expiry_time)
            self._cache.move_to_end(key)
            return

        # If at capacity, remove oldest
        if len(self._cache) >= self.maxsize:
            self._cache.popitem(last=False)
            self._evictions += 1

        # Add new entry
        self._cache[key] = (value, expiry_time)

    def delete(self, key: Any) -> bool:
        """
        Delete entry from cache.

        Args:
            key: Cache key

        Returns:
            True if entry was deleted, False if not found
        """
        if key in self._cache:
            del self._cache[key]
            return True
        return False

    def clear(self) -> None:
        """Clear all entries from cache."""
        self._cache.clear()
        self._hits = 0
        self._misses = 0
        self._evictions = 0

    def cleanup_expired(self) -> int:
        """
        Remove all expired entries.

        Returns:
            Number of entries removed
        """
        current_time = time.time()
        expired_keys = [
            key for key, (_, expiry_time) in self._cache.items()
            if current_time > expiry_time
        ]

        for key in expired_keys:
            del self._cache[key]
            self._evictions += 1

        return len(expired_keys)

    @property
    def size(self) -> int:
        """Current number of entries in cache."""
        return len(self._cache)

    @property
    def hit_rate(self) -> float:
        """
        Cache hit rate (0.0 to 1.0).

        Returns:
            Hit rate, or 0.0 if no accesses yet
        """
        total = self._hits + self._misses
        if total == 0:
            return 0.0
        return self._hits / total

    def stats(self) -> Dict[str, Any]:
        """
        Get cache statistics.

        Returns:
            Dictionary with cache stats
        """
        return {
            'size': self.size,
            'maxsize': self.maxsize,
            'ttl': self.ttl,
            'hits': self._hits,
            'misses': self._misses,
            'evictions': self._evictions,
            'hit_rate': self.hit_rate
        }


class DiscordObjectCache:
    """
    Multi-tier cache for Discord objects.

    Provides separate caches for different Discord object types with
    appropriate TTLs for each type.
    """

    def __init__(
        self,
        guild_maxsize: int = 100,
        guild_ttl: int = 3600,
        channel_maxsize: int = 1000,
        channel_ttl: int = 1800,
        message_maxsize: int = 10000,
        message_ttl: int = 600,
        emoji_maxsize: int = 10000,
        emoji_ttl: int = 3600
    ):
        """
        Initialize Discord object cache.

        Args:
            guild_maxsize: Max guilds to cache (default: 100)
            guild_ttl: Guild TTL in seconds (default: 1 hour)
            channel_maxsize: Max channels to cache (default: 1000)
            channel_ttl: Channel TTL in seconds (default: 30 minutes)
            message_maxsize: Max messages to cache (default: 10000)
            message_ttl: Message TTL in seconds (default: 10 minutes)
            emoji_maxsize: Max emoji sets to cache (default: 10000)
            emoji_ttl: Emoji TTL in seconds (default: 1 hour)
        """
        self.guild_cache = TTLCache[Guild](maxsize=guild_maxsize, ttl=guild_ttl)
        self.channel_cache = TTLCache[TextChannel](maxsize=channel_maxsize, ttl=channel_ttl)
        self.message_cache = TTLCache[Message](maxsize=message_maxsize, ttl=message_ttl)

        # Emoji cache uses guild_id as key, stores list of emojis
        self.emoji_cache = TTLCache[list](maxsize=emoji_maxsize, ttl=emoji_ttl)

        # Background cleanup task
        self._cleanup_task: Optional[asyncio.Task] = None
        self._cleanup_interval = 300  # 5 minutes

    # Guild cache methods

    def get_guild(self, guild_id: int) -> Optional[Guild]:
        """Get guild from cache."""
        return self.guild_cache.get(guild_id)

    def set_guild(self, guild_id: int, guild: Guild) -> None:
        """Cache a guild."""
        self.guild_cache.set(guild_id, guild)

    def delete_guild(self, guild_id: int) -> bool:
        """Remove guild from cache."""
        return self.guild_cache.delete(guild_id)

    # Channel cache methods

    def get_channel(self, channel_id: int) -> Optional[TextChannel]:
        """Get channel from cache."""
        return self.channel_cache.get(channel_id)

    def set_channel(self, channel_id: int, channel: TextChannel) -> None:
        """Cache a channel."""
        self.channel_cache.set(channel_id, channel)

    def delete_channel(self, channel_id: int) -> bool:
        """Remove channel from cache."""
        return self.channel_cache.delete(channel_id)

    # Message cache methods

    def get_message(self, message_id: int) -> Optional[Message]:
        """Get message from cache."""
        return self.message_cache.get(message_id)

    def set_message(self, message_id: int, message: Message) -> None:
        """Cache a message."""
        self.message_cache.set(message_id, message)

    def delete_message(self, message_id: int) -> bool:
        """Remove message from cache."""
        return self.message_cache.delete(message_id)

    # Emoji cache methods

    def get_emojis(self, guild_id: int) -> Optional[list]:
        """Get guild emojis from cache."""
        return self.emoji_cache.get(guild_id)

    def set_emojis(self, guild_id: int, emojis: list) -> None:
        """Cache guild emojis."""
        self.emoji_cache.set(guild_id, emojis)

    def delete_emojis(self, guild_id: int) -> bool:
        """Remove guild emojis from cache."""
        return self.emoji_cache.delete(guild_id)

    # Cleanup methods

    async def start_cleanup_task(self) -> None:
        """Start background cleanup task."""
        if self._cleanup_task is None or self._cleanup_task.done():
            self._cleanup_task = asyncio.create_task(self._cleanup_loop())

    async def stop_cleanup_task(self) -> None:
        """Stop background cleanup task."""
        if self._cleanup_task and not self._cleanup_task.done():
            self._cleanup_task.cancel()
            try:
                await self._cleanup_task
            except asyncio.CancelledError:
                pass
            self._cleanup_task = None

    async def _cleanup_loop(self) -> None:
        """Background loop to clean up expired entries."""
        while True:
            try:
                await asyncio.sleep(self._cleanup_interval)

                # Clean up expired entries from all caches
                guild_removed = self.guild_cache.cleanup_expired()
                channel_removed = self.channel_cache.cleanup_expired()
                message_removed = self.message_cache.cleanup_expired()
                emoji_removed = self.emoji_cache.cleanup_expired()

                total_removed = guild_removed + channel_removed + message_removed + emoji_removed

                if total_removed > 0:
                    # Could log this if logger is available
                    pass

            except asyncio.CancelledError:
                break
            except Exception:
                # Continue on error
                await asyncio.sleep(1)

    def clear_all(self) -> None:
        """Clear all caches."""
        self.guild_cache.clear()
        self.channel_cache.clear()
        self.message_cache.clear()
        self.emoji_cache.clear()

    def stats(self) -> Dict[str, Any]:
        """
        Get statistics for all caches.

        Returns:
            Dictionary with stats for each cache type
        """
        return {
            'guild': self.guild_cache.stats(),
            'channel': self.channel_cache.stats(),
            'message': self.message_cache.stats(),
            'emoji': self.emoji_cache.stats()
        }


class CachingHelper:
    """
    Helper class for async cache operations with fallback fetching.

    Provides "get or fetch" pattern for Discord objects.
    """

    def __init__(self, cache: DiscordObjectCache, bot):
        """
        Initialize caching helper.

        Args:
            cache: DiscordObjectCache instance
            bot: Discord bot instance for fetching
        """
        self.cache = cache
        self.bot = bot

    async def get_or_fetch_guild(self, guild_id: int) -> Guild:
        """
        Get guild from cache or fetch from Discord.

        Args:
            guild_id: Guild ID

        Returns:
            Guild object
        """
        # Try cache first
        guild = self.cache.get_guild(guild_id)
        if guild:
            return guild

        # Cache miss - fetch from Discord
        guild = await self.bot.fetch_guild(guild_id)
        self.cache.set_guild(guild_id, guild)
        return guild

    async def get_or_fetch_channel(self, channel_id: int) -> TextChannel:
        """
        Get channel from cache or fetch from Discord.

        Args:
            channel_id: Channel ID

        Returns:
            TextChannel object
        """
        # Try cache first
        channel = self.cache.get_channel(channel_id)
        if channel:
            return channel

        # Cache miss - fetch from Discord
        channel = await self.bot.fetch_channel(channel_id)
        self.cache.set_channel(channel_id, channel)
        return channel

    async def get_or_fetch_message(
        self,
        channel_id: int,
        message_id: int
    ) -> Message:
        """
        Get message from cache or fetch from Discord.

        Args:
            channel_id: Channel ID where message is located
            message_id: Message ID

        Returns:
            Message object
        """
        # Try cache first
        message = self.cache.get_message(message_id)
        if message:
            return message

        # Cache miss - need to fetch channel first, then message
        channel = await self.get_or_fetch_channel(channel_id)
        message = await channel.fetch_message(message_id)
        self.cache.set_message(message_id, message)
        return message

    async def get_or_fetch_emojis(self, guild_id: int) -> list:
        """
        Get guild emojis from cache or fetch from Discord.

        Args:
            guild_id: Guild ID

        Returns:
            List of emoji objects
        """
        # Try cache first
        emojis = self.cache.get_emojis(guild_id)
        if emojis is not None:
            return emojis

        # Cache miss - fetch from Discord
        guild = await self.get_or_fetch_guild(guild_id)
        emojis = await guild.fetch_emojis()

        # Cache as list
        emoji_list = list(emojis)
        self.cache.set_emojis(guild_id, emoji_list)
        return emoji_list
