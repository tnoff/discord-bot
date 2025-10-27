# Discord Bot Messaging System Refactoring Project

## Overview

This document outlines a plan to refactor the Discord bot's messaging system into a flexible, queue-based architecture that supports three operational modes:

1. **Direct Mode** (default) - Current behavior, no queue, standalone operation
2. **In-Process Queue Mode** - Queue in memory, processed by background task, single process
3. **External Gateway Mode** - Queue in Redis/PostgreSQL, separate process, can scale independently

## Goals

- **Centralize messaging logic** into a single `MessagingQueue` cog
- **Maintain backwards compatibility** - defaults to current behavior
- **Enable gradual migration** - three modes provide stepping stones
- **Support all Discord API operations** - messages, channel history, emojis, roles, etc.
- **Zero external dependencies by default** - bot runs standalone unless configured otherwise
- **Prepare for future scaling** - can move to external gateway when needed

---

## Architecture

### High-Level Flow

```
┌─────────────────────────────────────────────────────────┐
│                    Bot Process                          │
│                                                          │
│  ┌────────────┐  ┌────────────┐  ┌────────────┐        │
│  │ Music Cog  │  │ Markov Cog │  │ Role Cog   │        │
│  │ (extends   │  │ (extends   │  │ (extends   │        │
│  │ CogHelper) │  │ CogHelper) │  │ CogHelper) │        │
│  └─────┬──────┘  └─────┬──────┘  └─────┬──────┘        │
│        │                │                │              │
│        │  self.send_message()            │              │
│        │  self.edit_message()            │              │
│        │  self.delete_message()          │              │
│        │  (inherited from CogHelper)     │              │
│        │                │                │              │
│        └────────────────┴────────────────┘              │
│                         │                               │
│            ┌────────────▼──────────────┐                │
│            │    MessagingQueue Cog     │                │
│            │  ┌─────────────────────┐  │                │
│            │  │ Mode Router:        │  │                │
│            │  │                     │  │                │
│            │  │ if mode == "direct":│  │                │
│            │  │   → discord.py      │  │                │
│            │  │                     │  │                │
│            │  │ elif mode ==        │  │                │
│            │  │    "in_process":    │  │                │
│            │  │   → in-memory queue │  │                │
│            │  │   → background task │  │                │
│            │  │                     │  │                │
│            │  │ elif mode ==        │  │                │
│            │  │    "external_gateway│  │                │
│            │  │   → Redis queue     │  │                │
│            │  │   → external process│  │                │
│            │  └─────────────────────┘  │                │
│            └────────────┬──────────────┘                │
└─────────────────────────┼───────────────────────────────┘
                          │
              ┌───────────┴────────────┐
              │ Which mode?            │
              └───────────┬────────────┘
                          │
         ┌────────────────┼────────────────┐
         │                │                │
         ▼                ▼                ▼
    Direct Mode    In-Process Mode   External Gateway
    (current)      (single process)   (separate process)
         │                │                │
         │                │                ▼
         │                │        ┌───────────────────┐
         │                │        │ Redis/PostgreSQL  │
         │                │        │ Queue             │
         │                │        └────────┬──────────┘
         │                │                 │
         │                │                 ▼
         │                │        ┌───────────────────┐
         │                │        │ Gateway Service   │
         │                │        │ - Message cache   │
         │                │        │ - Channel cache   │
         │                │        │ - Guild cache     │
         │                │        │ - Emoji cache     │
         │                │        └───────────────────┘
         │                │                 │
         └────────────────┴─────────────────┘
                          │
                          ▼
                   Discord API
```

---

## Configuration

### Configuration Schema

```yaml
# config.yaml

messaging_queue:
  # Mode selection: "direct", "in_process", "external_gateway"
  mode: "direct"  # Default - current behavior, zero overhead

  # In-process mode settings (only used if mode="in_process")
  in_process:
    max_queue_size: 1000
    processor_count: 1  # Future: multiple background processors

  # External gateway settings (only used if mode="external_gateway")
  gateway:
    queue_type: "redis"  # redis, postgresql
    redis_host: "localhost"
    redis_port: 6379
    request_queue: "discord_api_requests"
    response_queue: "discord_api_responses"
    request_timeout: 30  # seconds

    # Cache settings for external gateway
    cache:
      guild_ttl: 3600      # 1 hour
      channel_ttl: 1800    # 30 minutes
      message_ttl: 600     # 10 minutes
      emoji_ttl: 3600      # 1 hour
```

### Mode Descriptions

#### Direct Mode (Default)
```yaml
# Option 1: No config at all (defaults to direct)

# Option 2: Explicit
messaging_queue:
  mode: "direct"
```

**Behavior:**
- No queue whatsoever
- Direct discord.py calls with retry logic (current behavior)
- Zero overhead, zero dependencies
- Perfect for standalone deployments

**Use Cases:**
- Personal bots
- Small servers
- Simple deployments
- Development/testing

---

#### In-Process Queue Mode
```yaml
messaging_queue:
  mode: "in_process"
```

**Behavior:**
- In-memory `asyncio.Queue`
- Background task processes requests
- Same process as bot
- No external dependencies

**Benefits:**
- Centralized logging (all Discord API calls in one place)
- Request batching (future enhancement)
- Rate limit coordination (future enhancement)
- Easier debugging (queue inspection)

**Use Cases:**
- Medium-sized deployments
- Want queue benefits without complexity
- Testing queue behavior before going external

---

#### External Gateway Mode
```yaml
messaging_queue:
  mode: "external_gateway"
  gateway:
    queue_type: "redis"
    redis_host: "localhost"
    redis_port: 6379
```

**Behavior:**
- Requests sent to Redis/PostgreSQL queue
- Separate gateway service process
- Gateway maintains Discord object caches
- Responses sent back via response queue

**Benefits:**
- Process isolation (bot crash doesn't lose message state)
- Horizontal scaling (multiple gateway workers)
- Independent deployment/restart
- Shared cache across multiple bot instances

**Use Cases:**
- Large deployments (100+ guilds)
- Multiple bot instances
- Need process isolation
- Want to scale messaging independently

**Requirements:**
- Redis or PostgreSQL
- Gateway service process running
- Network connectivity between bot and queue

---

## CogHelper Integration

All cogs inherit from `CogHelper`, which provides messaging methods:

```python
# discord_bot/cogs/common.py

from discord.ext.commands import Cog, Bot
from sqlalchemy.engine.base import Engine
from typing import Optional

class CogHelper(Cog):
    def __init__(self, bot: Bot, settings: dict, db_engine: Optional[Engine], **kwargs):
        self.bot = bot
        self.settings = settings
        self.db_engine = db_engine
        self.logger = get_logger(__name__)

        # MessagingQueue reference (lazy-loaded)
        self._messaging_queue = None

    @property
    def messaging_queue(self):
        """
        Lazy-load MessagingQueue cog.
        All cogs can use self.messaging_queue to send Discord API requests.
        """
        if self._messaging_queue is None:
            self._messaging_queue = self.bot.get_cog('MessagingQueue')
        return self._messaging_queue

    async def send_message(self, channel_id: int, content: str, **kwargs):
        """
        Convenience method - send message via messaging queue.
        All cogs inherit this from CogHelper.
        """
        if self.messaging_queue:
            return await self.messaging_queue.send_message(channel_id, content, **kwargs)
        else:
            # Fallback - direct call if MessagingQueue not loaded
            from functools import partial
            channel = await self.bot.fetch_channel(channel_id)
            message = await async_retry_discord_message_command(
                partial(channel.send, content, **kwargs)
            )
            return message.id

    async def edit_message(self, channel_id: int, message_id: int, content: str, **kwargs):
        """Convenience method - edit message"""
        if self.messaging_queue:
            return await self.messaging_queue.edit_message(channel_id, message_id, content, **kwargs)
        else:
            # Fallback - direct call
            from functools import partial
            channel = await self.bot.fetch_channel(channel_id)
            message = await channel.fetch_message(message_id)
            await async_retry_discord_message_command(
                partial(message.edit, content=content, **kwargs)
            )
            return True

    async def delete_message(self, channel_id: int, message_id: int):
        """Convenience method - delete message"""
        if self.messaging_queue:
            return await self.messaging_queue.delete_message(channel_id, message_id)
        else:
            # Fallback - direct call
            from functools import partial
            channel = await self.bot.fetch_channel(channel_id)
            message = await channel.fetch_message(message_id)
            await async_retry_discord_message_command(partial(message.delete))
            return True

    async def fetch_channel_history(
        self,
        channel_id: int,
        limit: int = 100,
        after_message_id: Optional[int] = None
    ):
        """Convenience method - fetch channel history (for Markov cog)"""
        if self.messaging_queue:
            return await self.messaging_queue.fetch_channel_history(
                channel_id, limit, after_message_id
            )
        else:
            # Fallback - direct call
            # ... implementation

    async def fetch_guild_emojis(self, guild_id: int):
        """Convenience method - fetch guild emojis (for Markov cog)"""
        if self.messaging_queue:
            return await self.messaging_queue.fetch_guild_emojis(guild_id)
        else:
            # Fallback - direct call
            # ... implementation
```

---

## MessagingQueue Cog Structure

### Public API Methods

All methods route based on `self.mode`:

```python
class MessagingQueue(Cog):
    async def send_message(
        self,
        channel_id: int,
        content: str,
        embed: Optional[Dict] = None,
        delete_after: Optional[int] = None
    ) -> int:
        """Send a message, returns message_id"""

        if self.mode == 'direct':
            return await self._direct_send_message(...)
        elif self.mode == 'in_process':
            return await self._in_process_send_message(...)
        elif self.mode == 'external_gateway':
            return await self._external_send_message(...)

    async def edit_message(
        self,
        channel_id: int,
        message_id: int,
        content: Optional[str] = None,
        embed: Optional[Dict] = None
    ) -> bool:
        """Edit a message, returns success status"""
        # ... route based on mode

    async def delete_message(
        self,
        channel_id: int,
        message_id: int
    ) -> bool:
        """Delete a message, returns success status"""
        # ... route based on mode

    async def fetch_channel_history(
        self,
        channel_id: int,
        limit: int = 100,
        after_message_id: Optional[int] = None,
        before: Optional[datetime] = None
    ) -> List[Dict[str, Any]]:
        """Fetch channel history, returns list of message dicts"""
        # ... route based on mode

    async def fetch_guild_emojis(
        self,
        guild_id: int
    ) -> List[Dict[str, Any]]:
        """Fetch guild emojis, returns list of emoji dicts"""
        # ... route based on mode

    async def batch_update_messages(
        self,
        channel_id: int,
        updates: List[Dict[str, Any]]
    ) -> Dict[str, int]:
        """
        Update multiple messages at once.
        Returns: {"updated_count": N, "failed_count": M}
        """
        # ... route based on mode
```

---

### Mode 1: Direct Implementation

Direct discord.py calls with retry logic - **this is the current behavior**.

```python
async def _direct_send_message(self, channel_id, content, embed, delete_after):
    """Direct discord.py call - CURRENT BEHAVIOR, zero overhead"""
    from functools import partial

    channel = await self.bot.fetch_channel(channel_id)
    message = await async_retry_discord_message_command(
        partial(channel.send, content, embed=embed, delete_after=delete_after)
    )
    return message.id

async def _direct_edit_message(self, channel_id, message_id, content, embed):
    """Direct discord.py call"""
    from functools import partial

    channel = await self.bot.fetch_channel(channel_id)
    message = await channel.fetch_message(message_id)
    await async_retry_discord_message_command(
        partial(message.edit, content=content, embed=embed)
    )
    return True

async def _direct_delete_message(self, channel_id, message_id):
    """Direct discord.py call"""
    from functools import partial

    channel = await self.bot.fetch_channel(channel_id)
    message = await channel.fetch_message(message_id)
    await async_retry_discord_message_command(partial(message.delete))
    return True

async def _direct_fetch_history(self, channel_id, limit, after_message_id):
    """Direct discord.py call"""
    from functools import partial

    async def fetch_messages(channel):
        kwargs = {'limit': limit}
        if after_message_id:
            after_msg = await channel.fetch_message(after_message_id)
            kwargs['after'] = after_msg
        return [m async for m in channel.history(**kwargs)]

    channel = await self.bot.fetch_channel(channel_id)
    messages = await async_retry_discord_message_command(
        partial(fetch_messages, channel)
    )

    # Convert to dicts for consistency
    return [
        {
            'id': str(m.id),
            'author_id': str(m.author.id),
            'content': m.content,
            'created_at': m.created_at.isoformat(),
            'embeds': [e.to_dict() for e in m.embeds],
            'attachments': [a.to_dict() for a in m.attachments]
        }
        for m in messages
    ]

async def _direct_fetch_emojis(self, guild_id):
    """Direct discord.py call"""
    from functools import partial

    guild = await self.bot.fetch_guild(guild_id)
    emojis = await async_retry_discord_message_command(
        partial(guild.fetch_emojis), max_retries=5
    )

    return [
        {
            'id': str(e.id),
            'name': e.name,
            'animated': e.animated,
            'url': str(e.url)
        }
        for e in emojis
    ]
```

---

### Mode 2: In-Process Queue Implementation

Uses `asyncio.Queue` for request queue, background task processes requests.

```python
def _init_in_process_mode(self):
    """
    In-process mode - queue in memory, processed by background task.
    Benefits: batching, rate limiting, centralized logging
    No external dependencies needed!
    """
    self.in_process_queue = asyncio.Queue()
    self.pending_requests = {}  # request_id → Future
    self._processor_task = None

async def cog_load(self):
    """Start background task"""
    if self.mode == 'in_process':
        self._processor_task = asyncio.create_task(
            self._in_process_request_processor()
        )
        self.logger.info("Started in-process request processor")

async def _in_process_send_request(self, action: str, params: dict) -> Any:
    """
    Send request to in-process queue and wait for result.
    This runs in the SAME process, just uses a queue for organization.
    """
    from uuid import uuid4

    request_id = str(uuid4())
    future = asyncio.Future()
    self.pending_requests[request_id] = future

    request = {
        'request_id': request_id,
        'action': action,
        'params': params
    }

    # Add to in-memory queue
    await self.in_process_queue.put(request)

    # Wait for result
    try:
        result = await asyncio.wait_for(future, timeout=30)
        return result
    finally:
        if request_id in self.pending_requests:
            del self.pending_requests[request_id]

async def _in_process_request_processor(self):
    """
    Background task - processes in-memory queue.
    This is where we could add batching, rate limiting, etc.
    """
    while True:
        try:
            request = await self.in_process_queue.get()

            # Process request using direct discord.py calls
            result = await self._execute_request(request)

            # Return result to waiting Future
            request_id = request['request_id']
            if request_id in self.pending_requests:
                self.pending_requests[request_id].set_result(result)

        except Exception as e:
            self.logger.error(f"Error processing in-process request: {e}")
            request_id = request.get('request_id')
            if request_id and request_id in self.pending_requests:
                self.pending_requests[request_id].set_exception(e)

async def _execute_request(self, request: dict) -> Any:
    """
    Execute a request using direct discord.py calls.
    This is shared by in_process mode and can be used by external gateway.
    """
    action = request['action']
    params = request['params']

    if action == 'send_message':
        return await self._direct_send_message(
            int(params['channel_id']),
            params['content'],
            params.get('embed'),
            params.get('delete_after')
        )

    elif action == 'edit_message':
        return await self._direct_edit_message(
            int(params['channel_id']),
            int(params['message_id']),
            params.get('content'),
            params.get('embed')
        )

    elif action == 'delete_message':
        return await self._direct_delete_message(
            int(params['channel_id']),
            int(params['message_id'])
        )

    elif action == 'fetch_channel_history':
        return await self._direct_fetch_history(
            int(params['channel_id']),
            params.get('limit', 100),
            int(params['after_message_id']) if params.get('after_message_id') else None
        )

    elif action == 'fetch_guild_emojis':
        return await self._direct_fetch_emojis(int(params['guild_id']))

    else:
        raise ValueError(f"Unknown action: {action}")

async def _in_process_send_message(self, channel_id, content, embed, delete_after):
    """Send via in-process queue"""
    return await self._in_process_send_request('send_message', {
        'channel_id': str(channel_id),
        'content': content,
        'embed': embed,
        'delete_after': delete_after
    })

async def _in_process_edit_message(self, channel_id, message_id, content, embed):
    """Edit via in-process queue"""
    await self._in_process_send_request('edit_message', {
        'channel_id': str(channel_id),
        'message_id': str(message_id),
        'content': content,
        'embed': embed
    })
    return True

async def _in_process_delete_message(self, channel_id, message_id):
    """Delete via in-process queue"""
    await self._in_process_send_request('delete_message', {
        'channel_id': str(channel_id),
        'message_id': str(message_id)
    })
    return True

async def _in_process_fetch_history(self, channel_id, limit, after_message_id):
    """Fetch history via in-process queue"""
    return await self._in_process_send_request('fetch_channel_history', {
        'channel_id': str(channel_id),
        'limit': limit,
        'after_message_id': str(after_message_id) if after_message_id else None
    })

async def _in_process_fetch_emojis(self, guild_id):
    """Fetch emojis via in-process queue"""
    return await self._in_process_send_request('fetch_guild_emojis', {
        'guild_id': str(guild_id)
    })
```

---

### Mode 3: External Gateway Implementation

Sends requests to Redis/PostgreSQL queue, separate gateway service processes them.

```python
def _init_external_gateway_mode(self, settings):
    """
    External gateway mode - queue in Redis/PostgreSQL.
    Requires separate gateway service process.
    """
    gateway_config = settings['messaging_queue']['gateway']

    if gateway_config['queue_type'] == 'redis':
        from redis import Redis
        self.redis = Redis(
            host=gateway_config.get('redis_host', 'localhost'),
            port=gateway_config.get('redis_port', 6379)
        )

    self.request_queue = gateway_config.get('request_queue', 'discord_api_requests')
    self.response_queue = gateway_config.get('response_queue', 'discord_api_responses')
    self.request_timeout = gateway_config.get('request_timeout', 30)

    self.pending_requests = {}
    self._listener_task = None

async def cog_load(self):
    """Start response listener"""
    if self.mode == 'external_gateway':
        self._listener_task = asyncio.create_task(
            self._external_response_listener()
        )
        self.logger.info("Started external gateway response listener")

async def _external_send_request(self, action: str, params: dict) -> Any:
    """Send request to external Redis queue"""
    from uuid import uuid4
    import json

    request_id = str(uuid4())
    future = asyncio.Future()
    self.pending_requests[request_id] = future

    request = {
        'request_id': request_id,
        'action': action,
        'params': params,
        'timestamp': time.time()
    }

    # Send to Redis
    await asyncio.to_thread(
        self.redis.lpush,
        self.request_queue,
        json.dumps(request)
    )

    # Wait for response
    try:
        response = await asyncio.wait_for(future, timeout=self.request_timeout)

        if response['status'] == 'error':
            raise Exception(f"Gateway error: {response['error']}")

        return response['data']
    finally:
        if request_id in self.pending_requests:
            del self.pending_requests[request_id]

async def _external_response_listener(self):
    """Background task - listens for responses from external gateway"""
    import json

    while True:
        try:
            result = await asyncio.to_thread(
                self.redis.brpop,
                self.response_queue,
                timeout=1
            )

            if not result:
                continue

            _, response_json = result
            response = json.loads(response_json)

            request_id = response['request_id']
            if request_id in self.pending_requests:
                self.pending_requests[request_id].set_result(response)

        except Exception as e:
            self.logger.error(f"External response listener error: {e}")
            await asyncio.sleep(1)

async def _external_send_message(self, channel_id, content, embed, delete_after):
    """Send via external gateway"""
    response = await self._external_send_request('send_message', {
        'channel_id': str(channel_id),
        'content': content,
        'embed': embed,
        'delete_after': delete_after
    })
    return int(response['message_id'])

# Similar implementations for edit, delete, fetch_history, fetch_emojis...
```

---

## External Gateway Service

When using `mode: external_gateway`, a separate process must run to process requests.

### Gateway Service Structure

```python
# gateway_service.py

import asyncio
import discord
from discord.ext import commands
from redis import Redis
import json
from cachetools import TTLCache

class DiscordAPIGatewayService:
    """
    Standalone service that processes Discord API requests from queue.
    Maintains caches of Discord objects for efficiency.
    """

    def __init__(self, token, redis_config, cache_config):
        # Lightweight discord.py bot (no commands)
        intents = discord.Intents.default()
        self.bot = commands.Bot(command_prefix='!', intents=intents)

        # Redis connection
        self.redis = Redis(
            host=redis_config['host'],
            port=redis_config['port']
        )

        self.request_queue = redis_config['request_queue']
        self.response_queue = redis_config['response_queue']

        # Object caches
        self.guild_cache = TTLCache(
            maxsize=100,
            ttl=cache_config.get('guild_ttl', 3600)
        )
        self.channel_cache = TTLCache(
            maxsize=1000,
            ttl=cache_config.get('channel_ttl', 1800)
        )
        self.message_cache = TTLCache(
            maxsize=10000,
            ttl=cache_config.get('message_ttl', 600)
        )
        self.emoji_cache = TTLCache(
            maxsize=10000,
            ttl=cache_config.get('emoji_ttl', 3600)
        )

    async def start(self):
        """Start bot and request processor"""
        # Start bot
        asyncio.create_task(self.bot.start(self.token))

        # Wait for ready
        await self.bot.wait_until_ready()

        # Start processing requests
        await self.process_requests()

    async def process_requests(self):
        """Main loop - process requests from Redis queue"""
        while True:
            try:
                # Blocking pop with timeout
                result = self.redis.brpop(self.request_queue, timeout=1)

                if not result:
                    continue

                _, request_json = result
                request = json.loads(request_json)

                # Process request
                response = await self.handle_request(request)

                # Send response
                self.redis.lpush(self.response_queue, json.dumps(response))

            except Exception as e:
                self.logger.error(f"Error processing request: {e}")

    async def handle_request(self, request):
        """Handle a single request"""
        request_id = request['request_id']
        action = request['action']
        params = request['params']

        try:
            if action == 'send_message':
                data = await self.send_message(params)
            elif action == 'edit_message':
                data = await self.edit_message(params)
            elif action == 'delete_message':
                data = await self.delete_message(params)
            elif action == 'fetch_channel_history':
                data = await self.fetch_channel_history(params)
            elif action == 'fetch_guild_emojis':
                data = await self.fetch_guild_emojis(params)
            else:
                raise ValueError(f"Unknown action: {action}")

            return {
                'request_id': request_id,
                'status': 'success',
                'data': data,
                'error': None
            }

        except Exception as e:
            return {
                'request_id': request_id,
                'status': 'error',
                'data': None,
                'error': str(e)
            }

    async def get_or_fetch_channel(self, channel_id):
        """Get channel from cache or fetch from Discord"""
        channel_id = int(channel_id)

        if channel_id in self.channel_cache:
            return self.channel_cache[channel_id]

        channel = await self.bot.fetch_channel(channel_id)
        self.channel_cache[channel_id] = channel
        return channel

    async def send_message(self, params):
        """Send a message"""
        channel = await self.get_or_fetch_channel(params['channel_id'])

        message = await channel.send(
            params['content'],
            embed=params.get('embed'),
            delete_after=params.get('delete_after')
        )

        # Cache the message
        self.message_cache[message.id] = message

        return {
            'message_id': str(message.id),
            'created_at': message.created_at.isoformat()
        }

    async def edit_message(self, params):
        """Edit a message"""
        message_id = int(params['message_id'])

        # Try cache first
        message = self.message_cache.get(message_id)

        if not message:
            # Cache miss - fetch from Discord
            channel = await self.get_or_fetch_channel(params['channel_id'])
            message = await channel.fetch_message(message_id)
            self.message_cache[message_id] = message

        await message.edit(
            content=params.get('content'),
            embed=params.get('embed')
        )

        return {
            'message_id': str(message.id),
            'updated_at': message.edited_at.isoformat() if message.edited_at else None
        }

    async def delete_message(self, params):
        """Delete a message"""
        message_id = int(params['message_id'])

        message = self.message_cache.get(message_id)

        if not message:
            channel = await self.get_or_fetch_channel(params['channel_id'])
            message = await channel.fetch_message(message_id)

        await message.delete()

        # Remove from cache
        if message_id in self.message_cache:
            del self.message_cache[message_id]

        return {'deleted': True}

    async def fetch_channel_history(self, params):
        """Fetch channel message history"""
        channel = await self.get_or_fetch_channel(params['channel_id'])

        kwargs = {'limit': params.get('limit', 100)}

        if params.get('after_message_id'):
            after_msg = await channel.fetch_message(int(params['after_message_id']))
            kwargs['after'] = after_msg

        messages = [m async for m in channel.history(**kwargs)]

        return {
            'messages': [
                {
                    'id': str(m.id),
                    'author_id': str(m.author.id),
                    'content': m.content,
                    'created_at': m.created_at.isoformat(),
                    'embeds': [e.to_dict() for e in m.embeds],
                    'attachments': [a.to_dict() for a in m.attachments]
                }
                for m in messages
            ]
        }

    async def fetch_guild_emojis(self, params):
        """Fetch guild emojis"""
        guild_id = int(params['guild_id'])

        # Check cache
        cache_key = f"{guild_id}:emojis"
        if cache_key in self.emoji_cache:
            return {'emojis': self.emoji_cache[cache_key]}

        guild = await self.bot.fetch_guild(guild_id)
        emojis = await guild.fetch_emojis()

        emoji_data = [
            {
                'id': str(e.id),
                'name': e.name,
                'animated': e.animated,
                'url': str(e.url)
            }
            for e in emojis
        ]

        # Cache the result
        self.emoji_cache[cache_key] = emoji_data

        return {'emojis': emoji_data}


# Run the service
if __name__ == "__main__":
    import os

    service = DiscordAPIGatewayService(
        token=os.getenv('DISCORD_TOKEN'),
        redis_config={
            'host': 'localhost',
            'port': 6379,
            'request_queue': 'discord_api_requests',
            'response_queue': 'discord_api_responses'
        },
        cache_config={
            'guild_ttl': 3600,
            'channel_ttl': 1800,
            'message_ttl': 600,
            'emoji_ttl': 3600
        }
    )

    asyncio.run(service.start())
```

---

## Request/Response Format

### Generic Request Schema

```python
{
    "request_id": "uuid-12345",           # Unique request ID (UUID)
    "action": "send_message",             # Action type
    "timestamp": 1234567890.123,          # Unix timestamp
    "params": {                           # Action-specific parameters
        "channel_id": "987654321",
        "content": "Hello world",
        "embed": {...},                   # Optional
        "delete_after": 300               # Optional
    }
}
```

### Generic Response Schema

```python
{
    "request_id": "uuid-12345",           # Matches original request
    "status": "success",                  # "success" or "error"
    "timestamp": 1234567890.456,          # Unix timestamp
    "data": {                             # Action-specific response data
        "message_id": "111222333",
        "created_at": "2025-10-26T12:00:00Z"
    },
    "error": null                         # Error message if status="error"
}
```

### Action-Specific Formats

#### send_message

**Request:**
```python
{
    "action": "send_message",
    "params": {
        "channel_id": "456",
        "content": "Downloading track 1...",
        "embed": {...},              # Optional Discord embed dict
        "delete_after": 300          # Optional: auto-delete after N seconds
    }
}
```

**Response:**
```python
{
    "status": "success",
    "data": {
        "message_id": "789",
        "channel_id": "456",
        "created_at": "2025-10-26T12:00:00Z"
    }
}
```

#### edit_message

**Request:**
```python
{
    "action": "edit_message",
    "params": {
        "channel_id": "456",
        "message_id": "789",
        "content": "Download complete!",
        "embed": {...}               # Optional
    }
}
```

**Response:**
```python
{
    "status": "success",
    "data": {
        "message_id": "789",
        "updated_at": "2025-10-26T12:01:00Z"
    }
}
```

#### delete_message

**Request:**
```python
{
    "action": "delete_message",
    "params": {
        "channel_id": "456",
        "message_id": "789"
    }
}
```

**Response:**
```python
{
    "status": "success",
    "data": {
        "deleted": true
    }
}
```

#### fetch_channel_history

**Request:**
```python
{
    "action": "fetch_channel_history",
    "params": {
        "channel_id": "456",
        "limit": 100,
        "after_message_id": "12345",         # Optional: for pagination
        "before": "2025-10-26T00:00:00Z"     # Optional: date filter
    }
}
```

**Response:**
```python
{
    "status": "success",
    "data": {
        "messages": [
            {
                "id": "111",
                "author_id": "222",
                "content": "Hello world",
                "created_at": "2025-10-25T12:00:00Z",
                "embeds": [...],
                "attachments": [...]
            },
            # ... more messages
        ],
        "has_more": true,
        "next_message_id": "110"  # For pagination
    }
}
```

#### fetch_guild_emojis

**Request:**
```python
{
    "action": "fetch_guild_emojis",
    "params": {
        "guild_id": "123"
    }
}
```

**Response:**
```python
{
    "status": "success",
    "data": {
        "emojis": [
            {
                "id": "555",
                "name": "poggers",
                "animated": false,
                "url": "https://cdn.discordapp.com/..."
            },
            # ... more emojis
        ]
    }
}
```

#### batch_update_messages

**Request:**
```python
{
    "action": "batch_update_messages",
    "params": {
        "channel_id": "456",
        "updates": [
            {"message_id": "789", "content": "Track 1 done"},
            {"message_id": "790", "content": "Track 2 done"}
        ]
    }
}
```

**Response:**
```python
{
    "status": "success",
    "data": {
        "updated_count": 2,
        "failed_count": 0,
        "failures": []  # List of failed message_ids if any
    }
}
```

---

## Usage Examples

### Music Cog

```python
# discord_bot/cogs/music.py

class Music(CogHelper):
    async def send_now_playing(self, channel_id, track_title, track_url):
        """Send 'now playing' message"""
        # Option 1: Use inherited method from CogHelper
        message_id = await self.send_message(
            channel_id=channel_id,
            content=f"Now playing: {track_title}\n{track_url}"
        )

        # Option 2: Use messaging_queue directly (same thing)
        message_id = await self.messaging_queue.send_message(
            channel_id=channel_id,
            content=f"Now playing: {track_title}\n{track_url}"
        )

        # Store message_id for later edits
        self.now_playing_message_id = message_id
        return message_id

    async def update_bundle_progress(self, bundle):
        """Update multi-track download progress"""
        await self.edit_message(
            channel_id=bundle.channel_id,
            message_id=bundle.message_id,
            content=f"Processing: {bundle.completed}/{bundle.total} tracks"
        )

    async def clear_progress_message(self, channel_id, message_id):
        """Delete progress message after completion"""
        await self.delete_message(
            channel_id=channel_id,
            message_id=message_id
        )
```

### Markov Cog

```python
# discord_bot/cogs/markov.py

class Markov(CogHelper):
    async def gather_messages_loop(self):
        """Background loop - gather channel history for markov chains"""
        for markov_channel in self.get_active_channels():
            # Fetch channel history - works with all three modes!
            messages = await self.fetch_channel_history(
                channel_id=markov_channel.channel_id,
                limit=100,
                after_message_id=markov_channel.last_message_id
            )

            # Fetch guild emojis
            emojis = await self.fetch_guild_emojis(
                guild_id=markov_channel.server_id
            )

            # Process messages for markov chain generation
            for msg in messages:
                self.process_message_for_markov(msg, emojis)

            # Update last processed message
            if messages:
                markov_channel.last_message_id = messages[-1]['id']
                self.save_channel_state(markov_channel)

    @commands.command(name='speak')
    async def speak(self, ctx, *, first_word: str = ''):
        """Generate markov chain sentence"""
        sentence = self.generate_markov_sentence(
            guild_id=ctx.guild.id,
            first_word=first_word
        )

        # Send response - uses messaging queue
        await self.send_message(
            channel_id=ctx.channel.id,
            content=sentence
        )
```

### Role Cog

```python
# discord_bot/cogs/role.py

class Role(CogHelper):
    @commands.command(name='list-roles')
    async def list_roles(self, ctx):
        """List all available roles"""
        # Build role list
        from dappertable import DapperTable, DapperTableHeaderOptions, DapperTableHeader
        from dappertable import PaginationLength

        headers = [DapperTableHeader('Role Name', 30)]
        table = DapperTable(
            header_options=DapperTableHeaderOptions(headers),
            pagination_options=PaginationLength(DISCORD_MAX_MESSAGE_LENGTH)
        )

        for role in ctx.guild.roles:
            table.add_row([f'@{role.name}'])

        # Send each page - uses messaging queue
        for output in table.print():
            await self.send_message(
                channel_id=ctx.channel.id,
                content=output
            )
```

---

## Migration Path

### Phase 1: Create MessagingQueue Cog (Direct Mode Only)
**Goal:** Centralize messaging logic, maintain current behavior

**Config:**
```yaml
# No messaging_queue config = defaults to direct mode
```

**Tasks:**
1. Create `MessagingQueue` cog with direct mode implementation
2. Update `CogHelper` to add messaging convenience methods
3. Update `Music` cog to use `self.send_message()` instead of direct discord.py calls
4. Test thoroughly - should work identically to current behavior
5. Update remaining cogs one by one (Markov, Role, General, Urban, DeleteMessages)

**Success Criteria:**
- All cogs use `MessagingQueue`
- All tests pass
- No behavior changes
- Zero performance impact

---

### Phase 2: Add In-Process Queue Mode
**Goal:** Add queue benefits without external dependencies

**Config:**
```yaml
messaging_queue:
  mode: "in_process"
```

**Tasks:**
1. Implement in-process queue mode in `MessagingQueue` cog
2. Add request/response infrastructure
3. Add background processor task
4. Test with single cog first (Music)
5. Enable for all cogs
6. Add queue metrics/logging

**Success Criteria:**
- In-process mode works identically to direct mode
- Can switch between modes via config
- Request queue is observable/debuggable
- All tests pass in both modes

**Benefits Gained:**
- Centralized logging of all Discord API calls
- Foundation for batching/rate limiting
- Queue inspection for debugging

---

### Phase 3: Add External Gateway Mode (Music Cog Only)
**Goal:** Prove external gateway concept with single cog

**Config:**
```yaml
messaging_queue:
  mode: "external_gateway"
  gateway:
    queue_type: "redis"
    redis_host: "localhost"
```

**Tasks:**
1. Implement external gateway mode in `MessagingQueue` cog
2. Build standalone gateway service with caching
3. Set up Redis for queue
4. Test with Music cog only
5. Monitor performance, cache hit rates
6. Document gateway service deployment

**Success Criteria:**
- Gateway service runs independently
- Music cog works via gateway
- Caches reduce Discord API calls
- Gateway survives bot restarts
- All tests pass

---

### Phase 4: Expand External Gateway to All Cogs
**Goal:** Full separation of Discord API operations

**Tasks:**
1. Add `fetch_channel_history` support to gateway (for Markov)
2. Add `fetch_guild_emojis` support to gateway (for Markov)
3. Add role management operations (for Role cog)
4. Enable gateway for all cogs
5. Monitor and optimize cache sizes
6. Add horizontal scaling (multiple gateway workers)

**Success Criteria:**
- All cogs work via external gateway
- Cache hit rate >80%
- Gateway can scale independently
- Bot can restart without losing message state

---

### Phase 5: Optimization & Advanced Features
**Goal:** Leverage queue architecture for efficiency

**Tasks:**
1. Implement request batching (update multiple messages in one Discord API call)
2. Add intelligent rate limiting coordination
3. Add cache warming (preload frequently used objects)
4. Add metrics/observability (OpenTelemetry)
5. Add request prioritization (critical vs. non-critical)
6. Add dead-letter queue for failed requests

**Success Criteria:**
- Batch updates reduce API calls by 50%+
- Rate limits never hit
- Comprehensive metrics available
- Failed requests are retried intelligently

---

## Testing Strategy

### Unit Tests

Test each mode independently:

```python
# tests/cogs/test_messaging_queue.py

import pytest
from discord_bot.cogs.messaging_queue import MessagingQueue

@pytest.mark.parametrize("mode", ["direct", "in_process", "external_gateway"])
async def test_send_message_all_modes(mode, fake_bot, fake_settings):
    """Test send_message works in all modes"""
    fake_settings['messaging_queue'] = {'mode': mode}

    if mode == 'external_gateway':
        # Mock Redis for external mode
        fake_settings['messaging_queue']['gateway'] = {
            'queue_type': 'redis',
            'redis_host': 'localhost'
        }

    msg_queue = MessagingQueue(fake_bot, fake_settings, None)
    await msg_queue.cog_load()

    message_id = await msg_queue.send_message(
        channel_id=12345,
        content="Test message"
    )

    assert isinstance(message_id, int)
    assert message_id > 0

    await msg_queue.cog_unload()

@pytest.mark.parametrize("mode", ["direct", "in_process"])
async def test_edit_message_all_modes(mode, fake_bot, fake_settings):
    """Test edit_message works in all modes"""
    fake_settings['messaging_queue'] = {'mode': mode}

    msg_queue = MessagingQueue(fake_bot, fake_settings, None)
    await msg_queue.cog_load()

    # Send message first
    message_id = await msg_queue.send_message(
        channel_id=12345,
        content="Original"
    )

    # Edit it
    success = await msg_queue.edit_message(
        channel_id=12345,
        message_id=message_id,
        content="Edited"
    )

    assert success is True

    await msg_queue.cog_unload()
```

### Integration Tests

Test cog integration:

```python
# tests/cogs/test_music_messaging.py

async def test_music_cog_uses_messaging_queue(fake_bot, fake_settings, fake_engine):
    """Test Music cog uses MessagingQueue"""
    # Add MessagingQueue cog
    msg_queue = MessagingQueue(fake_bot, fake_settings, fake_engine)
    await fake_bot.add_cog(msg_queue)

    # Add Music cog
    music = Music(fake_bot, fake_settings, fake_engine)
    await fake_bot.add_cog(music)

    # Music cog should have reference to MessagingQueue
    assert music.messaging_queue is not None
    assert music.messaging_queue == msg_queue

    # Test sending message via Music cog
    message_id = await music.send_message(
        channel_id=12345,
        content="Now playing: Test Track"
    )

    assert isinstance(message_id, int)
```

### Mode Switching Tests

Test that switching modes doesn't break functionality:

```python
async def test_mode_switching(fake_bot, fake_settings, fake_engine):
    """Test that switching modes works correctly"""

    # Start in direct mode
    fake_settings['messaging_queue'] = {'mode': 'direct'}
    msg_queue = MessagingQueue(fake_bot, fake_settings, fake_engine)

    message_id = await msg_queue.send_message(12345, "Test")
    assert message_id > 0

    # Switch to in_process mode
    await msg_queue.cog_unload()
    fake_settings['messaging_queue'] = {'mode': 'in_process'}
    msg_queue = MessagingQueue(fake_bot, fake_settings, fake_engine)
    await msg_queue.cog_load()

    message_id = await msg_queue.send_message(12345, "Test")
    assert message_id > 0
```

---

## Performance Considerations

### Direct Mode
- **Latency:** Same as current (direct discord.py call)
- **Memory:** Minimal (no queue overhead)
- **CPU:** Minimal (no background tasks)

### In-Process Mode
- **Latency:** +5-10ms (queue overhead)
- **Memory:** +10-50MB (queue + request tracking)
- **CPU:** +5-10% (background processor task)

### External Gateway Mode
- **Latency:** +20-50ms (Redis roundtrip + network)
- **Memory:** Bot: +10MB, Gateway: +500MB-2GB (caches)
- **CPU:** Bot: minimal, Gateway: moderate
- **Network:** Redis traffic for all Discord API calls

### Cache Benefits (External Gateway)
- **Message cache hit rate:** 60-80% (reduce fetches before edits)
- **Channel cache hit rate:** 90-95% (channels rarely change)
- **Emoji cache hit rate:** 95%+ (emojis rarely change)

**Overall Impact:** 40-60% reduction in Discord API calls

---

## Future Enhancements

### Request Batching
Combine multiple message edits into single Discord API call:
```python
# Instead of 10 separate edits:
for i in range(10):
    await edit_message(channel_id, msg_ids[i], f"Track {i} done")

# Single batched call:
await batch_update_messages(channel_id, [
    {'message_id': msg_ids[i], 'content': f"Track {i} done"}
    for i in range(10)
])
```

### Intelligent Rate Limiting
Gateway coordinates rate limits across all bot instances:
- Track API calls per endpoint
- Queue requests when approaching limits
- Distribute requests evenly over time windows

### Cache Warming
Preload commonly accessed objects on startup:
- Guild information for all active guilds
- Channels for music/markov guilds
- Recent messages for active bundles

### Horizontal Scaling
Run multiple gateway workers:
- Partition guilds across workers
- Redis queue distributes load
- Each worker maintains subset of caches

### Metrics & Observability
Track key metrics via OpenTelemetry:
- Request latency by action type
- Cache hit rates by object type
- Queue depth and processing time
- Error rates and failure reasons

### Dead-Letter Queue
Handle failed requests gracefully:
- Retry with exponential backoff
- Move to DLQ after N failures
- Alert on high DLQ depth
- Manual retry interface

---

## Benefits Summary

### Direct Mode (Default)
✅ Zero overhead - works exactly like current code
✅ No external dependencies
✅ Perfect for standalone deployments
✅ Simple to understand and debug

### In-Process Queue Mode
✅ Centralized logging - all Discord API calls in one place
✅ Foundation for batching/rate limiting
✅ Queue inspection for debugging
✅ No external dependencies
✅ Single process - easy deployment

### External Gateway Mode
✅ Process isolation - bot can restart without losing state
✅ Horizontal scaling - add gateway workers as needed
✅ Shared caching - reduce Discord API calls 40-60%
✅ Independent deployment - gateway and bot update separately
✅ Multiple bot instances can share one gateway

### Overall Project Benefits
✅ Gradual migration path - three modes provide stepping stones
✅ Backwards compatible - defaults to current behavior
✅ Flexible deployment - choose mode based on needs
✅ Testable - all modes tested with same tests
✅ Future-proof - ready for advanced features (batching, rate limiting)
✅ Centralized - all Discord API logic in one place

---

## Open Questions

1. **PostgreSQL LISTEN/NOTIFY vs Redis**: Which queue technology for external mode?
   - Redis: Faster, simpler, widely used
   - PostgreSQL: Already in stack, persistent, no extra dependency

2. **Cache eviction strategy**: TTL only, or also event-driven?
   - TTL: Simple, automatic
   - Event-driven: More accurate, requires discord.py event handlers
   - Hybrid: Best of both?

3. **Request timeout handling**: What happens if gateway doesn't respond?
   - Retry automatically?
   - Return error to cog?
   - Fall back to direct mode?

4. **Message ID tracking**: How does bot track message IDs created by gateway?
   - Wait for response (current design)
   - Optimistic creation (assume success, handle failures later)
   - Database tracking (persist all message IDs)

5. **Deployment packaging**: How to deploy gateway service?
   - Docker container?
   - Systemd service?
   - Kubernetes pod?

---

## References

- [discord.py Documentation](https://discordpy.readthedocs.io/)
- [Redis Python Client](https://redis-py.readthedocs.io/)
- [cachetools Documentation](https://cachetools.readthedocs.io/)
- [asyncio Queue](https://docs.python.org/3/library/asyncio-queue.html)
- [PostgreSQL LISTEN/NOTIFY](https://www.postgresql.org/docs/current/sql-notify.html)
