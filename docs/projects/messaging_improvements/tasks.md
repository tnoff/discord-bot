# Messaging System Refactoring - Task List

This document breaks down the messaging system refactoring project into small, testable tasks.

See [messaging_project.md](./messaging_project.md) for the complete design and architecture.

---

## Task Status Legend

- 🔲 Not Started
- 🔄 In Progress
- ✅ Complete
- ⏸️ Blocked

---

## Phase 1: Foundation (Direct Mode Only)

**Goal:** Centralize messaging logic in MessagingQueue cog, maintain current behavior

### Task 1.1: Create MessagingQueue Cog Skeleton 🔲

**Goal:** Create basic cog structure with direct mode only

**Subtasks:**
1. Create `discord_bot/cogs/messaging_queue.py`
2. Implement `__init__` with mode detection (hardcode to 'direct' for now)
3. Implement `_init_direct_mode()` (empty for now)
4. Add `cog_load()` and `cog_unload()` hooks
5. Add `async def setup(bot)` function

**Test:**
```python
def test_messaging_queue_cog_loads():
    msg_queue = MessagingQueue(fake_bot, {}, None)
    assert msg_queue.mode == 'direct'
```

**Files Changed:**
- `discord_bot/cogs/messaging_queue.py` (new)

**Acceptance Criteria:**
- Cog loads without errors
- Mode defaults to 'direct'
- Cog can be added to bot

**Estimated Time:** 30 minutes

---

### Task 1.2: Implement Direct Mode - send_message 🔲

**Goal:** Implement `send_message()` in direct mode

**Subtasks:**
1. Add `async def send_message(self, channel_id, content, **kwargs)` to MessagingQueue
2. Implement `_direct_send_message()` with discord.py + retry logic
3. Route from `send_message()` to `_direct_send_message()`

**Test:**
```python
async def test_direct_send_message(fake_bot):
    msg_queue = MessagingQueue(fake_bot, {}, None)
    message_id = await msg_queue.send_message(12345, "Test")
    assert isinstance(message_id, int)
    assert message_id > 0
```

**Files Changed:**
- `discord_bot/cogs/messaging_queue.py`

**Acceptance Criteria:**
- Can send messages via `send_message()`
- Returns valid message_id (int)
- Uses `async_retry_discord_message_command` for retries

**Estimated Time:** 45 minutes

---

### Task 1.3: Implement Direct Mode - edit_message 🔲

**Goal:** Implement `edit_message()` in direct mode

**Subtasks:**
1. Add `async def edit_message(self, channel_id, message_id, content, **kwargs)` to MessagingQueue
2. Implement `_direct_edit_message()` using discord.py
3. Route from `edit_message()` to `_direct_edit_message()`

**Test:**
```python
async def test_direct_edit_message(fake_bot):
    msg_queue = MessagingQueue(fake_bot, {}, None)
    msg_id = await msg_queue.send_message(12345, "Original")
    success = await msg_queue.edit_message(12345, msg_id, "Edited")
    assert success is True
```

**Files Changed:**
- `discord_bot/cogs/messaging_queue.py`

**Acceptance Criteria:**
- Can edit existing messages
- Returns True on success
- Message content is updated

**Estimated Time:** 30 minutes

---

### Task 1.4: Implement Direct Mode - delete_message 🔲

**Goal:** Implement `delete_message()` in direct mode

**Subtasks:**
1. Add `async def delete_message(self, channel_id, message_id)` to MessagingQueue
2. Implement `_direct_delete_message()` using discord.py
3. Route from `delete_message()` to `_direct_delete_message()`

**Test:**
```python
async def test_direct_delete_message(fake_bot):
    msg_queue = MessagingQueue(fake_bot, {}, None)
    msg_id = await msg_queue.send_message(12345, "Test")
    success = await msg_queue.delete_message(12345, msg_id)
    assert success is True
```

**Files Changed:**
- `discord_bot/cogs/messaging_queue.py`

**Acceptance Criteria:**
- Can delete messages
- Returns True on success
- Message is removed from Discord

**Estimated Time:** 30 minutes

---

### Task 1.5: Update CogHelper with MessagingQueue Integration 🔲

**Goal:** Add messaging methods to `CogHelper` so all cogs can use them

**Subtasks:**
1. Add `_messaging_queue` attribute to `CogHelper.__init__`
2. Add `@property messaging_queue` with lazy loading via `self.bot.get_cog('MessagingQueue')`
3. Add `async def send_message(self, channel_id, content, **kwargs)` convenience method
4. Add `async def edit_message(self, channel_id, message_id, content, **kwargs)` convenience method
5. Add `async def delete_message(self, channel_id, message_id)` convenience method
6. Add fallback logic if MessagingQueue not loaded (call discord.py directly)

**Test:**
```python
async def test_cog_helper_has_messaging_methods(fake_bot):
    # Create MessagingQueue
    msg_queue = MessagingQueue(fake_bot, {}, None)
    await fake_bot.add_cog(msg_queue)

    # Create any cog that extends CogHelper
    class TestCog(CogHelper):
        pass

    test_cog = TestCog(fake_bot, {}, None)

    # Should have messaging methods
    assert hasattr(test_cog, 'send_message')
    assert hasattr(test_cog, 'edit_message')
    assert hasattr(test_cog, 'delete_message')
    assert test_cog.messaging_queue is not None

    # Test sending message
    msg_id = await test_cog.send_message(12345, "Test")
    assert isinstance(msg_id, int)
```

**Files Changed:**
- `discord_bot/cogs/common.py`

**Acceptance Criteria:**
- All CogHelper subclasses have `send_message()`, `edit_message()`, `delete_message()` methods
- Methods route through MessagingQueue when available
- Fallback to direct discord.py calls if MessagingQueue not loaded
- Lazy loading of MessagingQueue reference works

**Estimated Time:** 1 hour

---

### Task 1.6: Migrate Music Cog - Now Playing Messages 🔲

**Goal:** Replace direct discord.py calls in Music cog with messaging queue

**Subtasks:**
1. Search Music cog for all `ctx.send()` calls related to "now playing"
2. Replace with `await self.send_message(ctx.channel.id, ...)`
3. Store returned `message_id` for later edits/deletes
4. Update any existing edit/delete calls to use messaging queue
5. Test now playing functionality

**Test:**
```python
async def test_music_now_playing_uses_messaging_queue(fake_bot, fake_context):
    # Setup
    msg_queue = MessagingQueue(fake_bot, {}, None)
    await fake_bot.add_cog(msg_queue)

    music = Music(fake_bot, {}, fake_engine)
    await fake_bot.add_cog(music)

    # Verify Music has reference to MessagingQueue
    assert music.messaging_queue is not None

    # Play a track
    # (Will need to mock player and download)

    # Verify "now playing" message was sent via messaging queue
```

**Files Changed:**
- `discord_bot/cogs/music.py`

**Acceptance Criteria:**
- Music cog sends "now playing" messages via MessagingQueue
- Message IDs are tracked for future edits
- No direct discord.py calls for now playing messages
- All existing tests pass
- Manual testing confirms now playing works

**Estimated Time:** 1.5 hours

---

### Task 1.7: Migrate Music Cog - Bundle Progress Messages 🔲

**Goal:** Migrate bundle progress tracking to messaging queue

**Subtasks:**
1. Review how `MultiMediaRequestBundle` sends progress messages
2. Update bundle to use `messaging_queue` instead of direct discord.py
3. Replace message edit calls with `messaging_queue.edit_message()`
4. Test bundle lifecycle with messaging queue

**Test:**
```python
async def test_bundle_uses_messaging_queue(fake_bot, fake_context):
    msg_queue = MessagingQueue(fake_bot, {}, None)
    await fake_bot.add_cog(msg_queue)

    music = Music(fake_bot, {}, fake_engine)
    await fake_bot.add_cog(music)

    # Create a bundle
    bundle = MultiMediaRequestBundle(
        guild_id=fake_context.guild.id,
        channel_id=fake_context.channel.id,
        text_channel=fake_context.channel
    )

    # Add some media requests
    bundle.set_initial_search("test playlist")
    # ... add requests ...

    # Verify bundle updates messages via messaging queue
```

**Files Changed:**
- `discord_bot/cogs/music_helpers/media_request.py` (if bundle sends messages directly)
- `discord_bot/cogs/music.py` (if music cog manages bundle messages)

**Acceptance Criteria:**
- Bundle progress messages use MessagingQueue
- Multi-track download progress updates work correctly
- Bundle completion/deletion works
- All bundle tests pass

**Estimated Time:** 2 hours

---

### Task 1.8: Add Configuration Support 🔲

**Goal:** Add config schema and mode selection via YAML config

**Subtasks:**
1. Define `MESSAGING_QUEUE_SCHEMA` in `discord_bot/cogs/schema.py`
2. Update `MessagingQueue.__init__` to read `settings.get('messaging_queue', {})`
3. Read `mode` from config, default to 'direct' if not present
4. Validate config against schema on startup
5. Add logging to show which mode is active

**Test:**
```python
def test_messaging_queue_config_defaults_to_direct():
    # No config provided
    msg_queue = MessagingQueue(fake_bot, {}, None)
    assert msg_queue.mode == 'direct'

def test_messaging_queue_respects_config():
    # Explicit config
    settings = {'messaging_queue': {'mode': 'direct'}}
    msg_queue = MessagingQueue(fake_bot, settings, None)
    assert msg_queue.mode == 'direct'

def test_messaging_queue_config_validation():
    # Invalid mode
    settings = {'messaging_queue': {'mode': 'invalid'}}
    with pytest.raises(ValueError):
        msg_queue = MessagingQueue(fake_bot, settings, None)
```

**Schema:**
```python
MESSAGING_QUEUE_SCHEMA = {
    'type': 'object',
    'properties': {
        'mode': {
            'type': 'string',
            'enum': ['direct', 'in_process', 'external_gateway']
        },
        'in_process': {
            'type': 'object',
            'properties': {
                'max_queue_size': {'type': 'integer'},
            }
        },
        'gateway': {
            'type': 'object',
            'properties': {
                'queue_type': {'type': 'string', 'enum': ['redis', 'postgresql']},
                'redis_host': {'type': 'string'},
                'redis_port': {'type': 'integer'},
            }
        }
    }
}
```

**Files Changed:**
- `discord_bot/cogs/schema.py`
- `discord_bot/cogs/messaging_queue.py`

**Acceptance Criteria:**
- Config is validated on startup
- Mode defaults to 'direct' if no config
- Invalid modes raise ValueError
- Logs show active mode on startup

**Estimated Time:** 1 hour

---

### Task 1.9: Integration Testing - Music Cog End-to-End 🔲

**Goal:** Test complete flow through Music cog with MessagingQueue

**Subtasks:**
1. Create comprehensive integration test for `!play` command
2. Test single track play (sends now playing message)
3. Test multi-track playlist (sends bundle progress messages)
4. Test message cleanup after completion
5. Verify no regressions in existing functionality
6. Run full test suite

**Test:**
```python
async def test_play_command_full_flow(fake_bot, fake_context, fake_engine):
    # Setup
    msg_queue = MessagingQueue(fake_bot, {}, None)
    await fake_bot.add_cog(msg_queue)

    music = Music(fake_bot, {}, fake_engine)
    await fake_bot.add_cog(music)

    # Mock search client to return results
    # Mock download client to return files

    # Execute !play command with single song
    await music.play_(fake_context, search="test song")

    # Verify:
    # - Search was performed
    # - Download was initiated
    # - Progress message was sent via messaging queue
    # - Now playing message was sent
    # - Message IDs were tracked

async def test_play_playlist_full_flow(fake_bot, fake_context, fake_engine):
    # Setup as above

    # Execute !play with playlist
    await music.play_(fake_context, search="https://spotify.com/playlist/abc")

    # Verify:
    # - Bundle was created
    # - Progress messages were sent
    # - Multiple tracks queued
    # - Bundle completion message sent
```

**Files Changed:**
- `tests/cogs/test_music.py` (add new integration tests)

**Acceptance Criteria:**
- Full `!play` flow works with MessagingQueue
- Single track play works
- Playlist play works
- Progress messages sent correctly
- All existing Music cog tests pass
- No regressions in functionality

**Estimated Time:** 2 hours

---

## Phase 1 Completion Checklist

Before moving to Phase 2, verify:
- ✅ All tasks 1.1-1.9 complete
- ✅ All tests passing (510/510)
- ✅ Pylint score 10/10 for all modified files
- ✅ Music cog fully migrated to MessagingQueue
- ✅ No behavior changes (identical to pre-refactor)
- ✅ Manual testing confirms all features work
- ✅ Documentation updated

---

## Phase 2: In-Process Queue Mode

**Goal:** Add in-memory queue mode for batching and centralized logging

### Task 2.1: Implement In-Process Queue Infrastructure 🔲

**Goal:** Add in-process queue mode skeleton

**Subtasks:**
1. Implement `_init_in_process_mode()` method
2. Create `self.in_process_queue = asyncio.Queue()`
3. Create `self.pending_requests = {}` dict for tracking
4. Update mode routing in `send_message()`, `edit_message()`, `delete_message()`

**Test:**
```python
def test_in_process_mode_initializes():
    settings = {'messaging_queue': {'mode': 'in_process'}}
    msg_queue = MessagingQueue(fake_bot, settings, None)
    assert msg_queue.mode == 'in_process'
    assert hasattr(msg_queue, 'in_process_queue')
    assert hasattr(msg_queue, 'pending_requests')
    assert isinstance(msg_queue.in_process_queue, asyncio.Queue)
```

**Files Changed:**
- `discord_bot/cogs/messaging_queue.py`

**Acceptance Criteria:**
- In-process mode initializes correctly
- Queue and pending_requests dict created
- Mode routing updated for all methods

**Estimated Time:** 45 minutes

---

### Task 2.2: Implement Request/Response Pattern 🔲

**Goal:** Build request/response infrastructure for in-process queue

**Subtasks:**
1. Implement `_in_process_send_request(action, params)` method
2. Generate unique request_id using `uuid4()`
3. Create `asyncio.Future()` and store in `pending_requests`
4. Put request dict in `in_process_queue`
5. Wait for Future with timeout (30 seconds)
6. Handle timeout exceptions

**Test:**
```python
async def test_in_process_send_request():
    settings = {'messaging_queue': {'mode': 'in_process'}}
    msg_queue = MessagingQueue(fake_bot, settings, None)

    # Mock processor to immediately respond
    async def mock_processor():
        request = await msg_queue.in_process_queue.get()
        request_id = request['request_id']
        msg_queue.pending_requests[request_id].set_result("test_result")

    asyncio.create_task(mock_processor())

    result = await msg_queue._in_process_send_request('test_action', {})
    assert result == "test_result"

async def test_in_process_send_request_timeout():
    settings = {'messaging_queue': {'mode': 'in_process'}}
    msg_queue = MessagingQueue(fake_bot, settings, None)

    # No processor - should timeout
    with pytest.raises(asyncio.TimeoutError):
        await msg_queue._in_process_send_request('test_action', {})
```

**Files Changed:**
- `discord_bot/cogs/messaging_queue.py`

**Acceptance Criteria:**
- Request/response pattern works
- Futures are created and resolved correctly
- Timeouts are handled
- Request IDs are unique

**Estimated Time:** 1 hour

---

### Task 2.3: Implement Background Processor 🔲

**Goal:** Create background task to process in-memory queue

**Subtasks:**
1. Implement `_in_process_request_processor()` infinite loop
2. Get requests from `in_process_queue` with `await queue.get()`
3. Execute requests via `_execute_request(request)`
4. Set result on Future from `pending_requests`
5. Handle exceptions and set exception on Future
6. Start processor in `cog_load()`
7. Cancel processor in `cog_unload()`

**Test:**
```python
async def test_background_processor_starts_and_stops():
    settings = {'messaging_queue': {'mode': 'in_process'}}
    msg_queue = MessagingQueue(fake_bot, settings, None)

    await msg_queue.cog_load()
    assert msg_queue._processor_task is not None
    assert not msg_queue._processor_task.done()

    await msg_queue.cog_unload()
    assert msg_queue._processor_task.cancelled()

async def test_background_processor_handles_requests():
    settings = {'messaging_queue': {'mode': 'in_process'}}
    msg_queue = MessagingQueue(fake_bot, settings, None)
    await msg_queue.cog_load()

    # Send request
    result = await msg_queue._in_process_send_request('send_message', {
        'channel_id': '12345',
        'content': 'test'
    })

    assert isinstance(result, int)  # message_id

    await msg_queue.cog_unload()
```

**Files Changed:**
- `discord_bot/cogs/messaging_queue.py`

**Acceptance Criteria:**
- Background processor task starts on cog load
- Background processor task stops on cog unload
- Processor executes requests and returns results
- Exceptions are handled gracefully

**Estimated Time:** 1.5 hours

---

### Task 2.4: Implement _execute_request() Router 🔲

**Goal:** Route requests to appropriate direct mode implementations

**Subtasks:**
1. Implement `_execute_request(request)` method
2. Parse `action` and `params` from request dict
3. Route to appropriate `_direct_*` method based on action
4. Return result from direct method
5. Handle unknown actions with ValueError

**Test:**
```python
async def test_execute_request_routes_send_message():
    msg_queue = MessagingQueue(fake_bot, {}, None)

    request = {
        'request_id': 'test-123',
        'action': 'send_message',
        'params': {'channel_id': '12345', 'content': 'test'}
    }

    result = await msg_queue._execute_request(request)
    assert isinstance(result, int)  # message_id

async def test_execute_request_routes_edit_message():
    msg_queue = MessagingQueue(fake_bot, {}, None)

    # Send message first
    msg_id = await msg_queue._direct_send_message(12345, 'test', None, None)

    request = {
        'action': 'edit_message',
        'params': {
            'channel_id': '12345',
            'message_id': str(msg_id),
            'content': 'edited'
        }
    }

    result = await msg_queue._execute_request(request)
    assert result is True

async def test_execute_request_unknown_action():
    msg_queue = MessagingQueue(fake_bot, {}, None)

    request = {
        'action': 'unknown_action',
        'params': {}
    }

    with pytest.raises(ValueError, match="Unknown action"):
        await msg_queue._execute_request(request)
```

**Files Changed:**
- `discord_bot/cogs/messaging_queue.py`

**Acceptance Criteria:**
- Routes all supported actions correctly
- Returns appropriate results
- Raises ValueError for unknown actions
- Works with all action types (send, edit, delete)

**Estimated Time:** 1 hour

---

### Task 2.5: Implement In-Process send_message 🔲

**Goal:** Route send_message through in-process queue

**Subtasks:**
1. Implement `_in_process_send_message(channel_id, content, embed, delete_after)`
2. Call `_in_process_send_request('send_message', params)` with formatted params
3. Return result (message_id)

**Test:**
```python
async def test_in_process_send_message():
    settings = {'messaging_queue': {'mode': 'in_process'}}
    msg_queue = MessagingQueue(fake_bot, settings, None)
    await msg_queue.cog_load()

    message_id = await msg_queue.send_message(12345, "Test")
    assert isinstance(message_id, int)
    assert message_id > 0

    await msg_queue.cog_unload()

async def test_in_process_send_message_with_options():
    settings = {'messaging_queue': {'mode': 'in_process'}}
    msg_queue = MessagingQueue(fake_bot, settings, None)
    await msg_queue.cog_load()

    message_id = await msg_queue.send_message(
        12345,
        "Test",
        embed={'title': 'Test'},
        delete_after=300
    )
    assert isinstance(message_id, int)

    await msg_queue.cog_unload()
```

**Files Changed:**
- `discord_bot/cogs/messaging_queue.py`

**Acceptance Criteria:**
- send_message works in in-process mode
- Returns valid message_id
- Supports all parameters (content, embed, delete_after)
- Works identically to direct mode

**Estimated Time:** 30 minutes

---

### Task 2.6: Implement In-Process edit/delete 🔲

**Goal:** Add edit and delete support for in-process mode

**Subtasks:**
1. Implement `_in_process_edit_message(channel_id, message_id, content, embed)`
2. Implement `_in_process_delete_message(channel_id, message_id)`
3. Test both methods thoroughly

**Test:**
```python
async def test_in_process_edit_message():
    settings = {'messaging_queue': {'mode': 'in_process'}}
    msg_queue = MessagingQueue(fake_bot, settings, None)
    await msg_queue.cog_load()

    msg_id = await msg_queue.send_message(12345, "Original")
    success = await msg_queue.edit_message(12345, msg_id, "Edited")
    assert success is True

    await msg_queue.cog_unload()

async def test_in_process_delete_message():
    settings = {'messaging_queue': {'mode': 'in_process'}}
    msg_queue = MessagingQueue(fake_bot, settings, None)
    await msg_queue.cog_load()

    msg_id = await msg_queue.send_message(12345, "Test")
    success = await msg_queue.delete_message(12345, msg_id)
    assert success is True

    await msg_queue.cog_unload()

async def test_in_process_full_lifecycle():
    settings = {'messaging_queue': {'mode': 'in_process'}}
    msg_queue = MessagingQueue(fake_bot, settings, None)
    await msg_queue.cog_load()

    # Send -> Edit -> Delete
    msg_id = await msg_queue.send_message(12345, "Original")
    await msg_queue.edit_message(12345, msg_id, "Edited")
    await msg_queue.delete_message(12345, msg_id)

    await msg_queue.cog_unload()
```

**Files Changed:**
- `discord_bot/cogs/messaging_queue.py`

**Acceptance Criteria:**
- Edit works in in-process mode
- Delete works in in-process mode
- Full lifecycle (send → edit → delete) works
- Works identically to direct mode

**Estimated Time:** 45 minutes

---

### Task 2.7: Mode Comparison Testing 🔲

**Goal:** Verify both modes (direct and in_process) behave identically

**Subtasks:**
1. Create parametrized tests that run for both modes
2. Test all operations (send, edit, delete)
3. Verify identical results
4. Test edge cases in both modes
5. Test concurrent operations in both modes

**Test:**
```python
@pytest.mark.parametrize("mode", ["direct", "in_process"])
async def test_send_message_both_modes(mode, fake_bot):
    settings = {'messaging_queue': {'mode': mode}}
    msg_queue = MessagingQueue(fake_bot, settings, None)
    if mode == 'in_process':
        await msg_queue.cog_load()

    message_id = await msg_queue.send_message(12345, "Test")
    assert isinstance(message_id, int)
    assert message_id > 0

    if mode == 'in_process':
        await msg_queue.cog_unload()

@pytest.mark.parametrize("mode", ["direct", "in_process"])
async def test_edit_message_both_modes(mode, fake_bot):
    settings = {'messaging_queue': {'mode': mode}}
    msg_queue = MessagingQueue(fake_bot, settings, None)
    if mode == 'in_process':
        await msg_queue.cog_load()

    msg_id = await msg_queue.send_message(12345, "Original")
    success = await msg_queue.edit_message(12345, msg_id, "Edited")
    assert success is True

    if mode == 'in_process':
        await msg_queue.cog_unload()

@pytest.mark.parametrize("mode", ["direct", "in_process"])
async def test_delete_message_both_modes(mode, fake_bot):
    settings = {'messaging_queue': {'mode': mode}}
    msg_queue = MessagingQueue(fake_bot, settings, None)
    if mode == 'in_process':
        await msg_queue.cog_load()

    msg_id = await msg_queue.send_message(12345, "Test")
    success = await msg_queue.delete_message(12345, msg_id)
    assert success is True

    if mode == 'in_process':
        await msg_queue.cog_unload()

@pytest.mark.parametrize("mode", ["direct", "in_process"])
async def test_concurrent_operations_both_modes(mode, fake_bot):
    """Test multiple concurrent message operations"""
    settings = {'messaging_queue': {'mode': mode}}
    msg_queue = MessagingQueue(fake_bot, settings, None)
    if mode == 'in_process':
        await msg_queue.cog_load()

    # Send 10 messages concurrently
    tasks = [
        msg_queue.send_message(12345, f"Message {i}")
        for i in range(10)
    ]
    message_ids = await asyncio.gather(*tasks)

    assert len(message_ids) == 10
    assert all(isinstance(mid, int) for mid in message_ids)

    if mode == 'in_process':
        await msg_queue.cog_unload()
```

**Files Changed:**
- `tests/cogs/test_messaging_queue.py`

**Acceptance Criteria:**
- All operations work identically in both modes
- Tests pass for both direct and in_process modes
- Concurrent operations handled correctly
- No regressions between modes

**Estimated Time:** 1 hour

---

## Phase 2 Completion Checklist

Before moving to Phase 3, verify:
- ✅ All tasks 2.1-2.7 complete
- ✅ In-process mode works identically to direct mode
- ✅ All tests passing (510+ tests)
- ✅ Pylint score 10/10
- ✅ Can switch between modes via config
- ✅ Background processor starts/stops correctly
- ✅ Manual testing confirms both modes work

---

## Phase 3: Markov Cog Support

**Goal:** Add support for fetching channel history and emojis (for Markov cog)

### Task 3.1: Implement fetch_channel_history (Direct) 🔲

**Goal:** Add channel history fetching capability

**Subtasks:**
1. Add `async def fetch_channel_history(channel_id, limit, after_message_id)` to MessagingQueue
2. Implement `_direct_fetch_history()` using discord.py
3. Convert discord.py messages to dict format for consistency
4. Route based on mode (direct only for now)

**Test:**
```python
async def test_direct_fetch_history(fake_bot):
    msg_queue = MessagingQueue(fake_bot, {}, None)

    # Send some messages first
    await msg_queue.send_message(12345, "Message 1")
    await msg_queue.send_message(12345, "Message 2")
    await msg_queue.send_message(12345, "Message 3")

    # Fetch history
    messages = await msg_queue.fetch_channel_history(12345, limit=10)

    assert isinstance(messages, list)
    assert len(messages) > 0
    assert all('id' in m for m in messages)
    assert all('content' in m for m in messages)
    assert all('author_id' in m for m in messages)

async def test_fetch_history_with_pagination(fake_bot):
    msg_queue = MessagingQueue(fake_bot, {}, None)

    # Send messages
    msg_ids = []
    for i in range(5):
        msg_id = await msg_queue.send_message(12345, f"Message {i}")
        msg_ids.append(msg_id)

    # Fetch after first message
    messages = await msg_queue.fetch_channel_history(
        12345,
        limit=10,
        after_message_id=msg_ids[0]
    )

    assert len(messages) == 4  # Should get messages after first one
```

**Files Changed:**
- `discord_bot/cogs/messaging_queue.py`

**Acceptance Criteria:**
- Can fetch channel history
- Returns list of message dicts
- Pagination with `after_message_id` works
- Message dict format is consistent

**Estimated Time:** 1 hour

---

### Task 3.2: Implement fetch_guild_emojis (Direct) 🔲

**Goal:** Add emoji fetching capability

**Subtasks:**
1. Add `async def fetch_guild_emojis(guild_id)` to MessagingQueue
2. Implement `_direct_fetch_emojis()` using discord.py
3. Convert discord.py emojis to dict format
4. Route based on mode

**Test:**
```python
async def test_direct_fetch_emojis(fake_bot):
    msg_queue = MessagingQueue(fake_bot, {}, None)

    emojis = await msg_queue.fetch_guild_emojis(12345)

    assert isinstance(emojis, list)
    # May be empty if test guild has no emojis
    if emojis:
        assert all('id' in e for e in emojis)
        assert all('name' in e for e in emojis)
        assert all('animated' in e for e in emojis)
```

**Files Changed:**
- `discord_bot/cogs/messaging_queue.py`

**Acceptance Criteria:**
- Can fetch guild emojis
- Returns list of emoji dicts
- Emoji dict format is consistent
- Works with guilds that have no emojis

**Estimated Time:** 45 minutes

---

### Task 3.3: Add fetch Methods to CogHelper 🔲

**Goal:** Expose fetch methods in CogHelper for all cogs to use

**Subtasks:**
1. Add `async def fetch_channel_history(...)` to CogHelper
2. Add `async def fetch_guild_emojis(...)` to CogHelper
3. Add fallback logic if MessagingQueue not loaded
4. Test from CogHelper subclass

**Test:**
```python
async def test_cog_helper_fetch_channel_history(fake_bot):
    msg_queue = MessagingQueue(fake_bot, {}, None)
    await fake_bot.add_cog(msg_queue)

    class TestCog(CogHelper):
        pass

    test_cog = TestCog(fake_bot, {}, None)

    # Should have fetch methods
    assert hasattr(test_cog, 'fetch_channel_history')
    assert hasattr(test_cog, 'fetch_guild_emojis')

    # Test fetching
    messages = await test_cog.fetch_channel_history(12345, limit=10)
    assert isinstance(messages, list)

async def test_cog_helper_fetch_emojis(fake_bot):
    msg_queue = MessagingQueue(fake_bot, {}, None)
    await fake_bot.add_cog(msg_queue)

    class TestCog(CogHelper):
        pass

    test_cog = TestCog(fake_bot, {}, None)

    emojis = await test_cog.fetch_guild_emojis(12345)
    assert isinstance(emojis, list)
```

**Files Changed:**
- `discord_bot/cogs/common.py`

**Acceptance Criteria:**
- CogHelper has `fetch_channel_history()` method
- CogHelper has `fetch_guild_emojis()` method
- Methods route through MessagingQueue
- Fallback works if MessagingQueue not loaded

**Estimated Time:** 45 minutes

---

### Task 3.4: Add In-Process Support for Fetch Methods 🔲

**Goal:** Add in-process mode support for history and emoji fetching

**Subtasks:**
1. Add `fetch_channel_history` action to `_execute_request()`
2. Add `fetch_guild_emojis` action to `_execute_request()`
3. Implement `_in_process_fetch_history()`
4. Implement `_in_process_fetch_emojis()`
5. Test both methods in in-process mode

**Test:**
```python
async def test_in_process_fetch_history():
    settings = {'messaging_queue': {'mode': 'in_process'}}
    msg_queue = MessagingQueue(fake_bot, settings, None)
    await msg_queue.cog_load()

    messages = await msg_queue.fetch_channel_history(12345, limit=10)
    assert isinstance(messages, list)

    await msg_queue.cog_unload()

@pytest.mark.parametrize("mode", ["direct", "in_process"])
async def test_fetch_history_both_modes(mode, fake_bot):
    settings = {'messaging_queue': {'mode': mode}}
    msg_queue = MessagingQueue(fake_bot, settings, None)
    if mode == 'in_process':
        await msg_queue.cog_load()

    messages = await msg_queue.fetch_channel_history(12345, limit=10)
    assert isinstance(messages, list)

    if mode == 'in_process':
        await msg_queue.cog_unload()
```

**Files Changed:**
- `discord_bot/cogs/messaging_queue.py`

**Acceptance Criteria:**
- Fetch methods work in in-process mode
- Results identical to direct mode
- Parametrized tests pass for both modes

**Estimated Time:** 1 hour

---

### Task 3.5: Migrate Markov Cog 🔲

**Goal:** Update Markov cog to use MessagingQueue for all Discord API calls

**Subtasks:**
1. Find all `async_retry_discord_message_command` calls in Markov cog
2. Replace `fetch_channel()` calls with `self.fetch_channel_history()`
3. Replace `fetch_emojis()` calls with `self.fetch_guild_emojis()`
4. Replace message sending with `self.send_message()`
5. Test markov background loop
6. Test markov commands (`!markov speak`, etc.)

**Test:**
```python
async def test_markov_uses_messaging_queue(fake_bot, fake_engine):
    msg_queue = MessagingQueue(fake_bot, {}, None)
    await fake_bot.add_cog(msg_queue)

    markov = Markov(fake_bot, {}, fake_engine)
    await fake_bot.add_cog(markov)

    # Verify Markov has reference to MessagingQueue
    assert markov.messaging_queue is not None

    # Test gathering messages (would need to mock channel history)
    # await markov.gather_messages_loop() - one iteration

    # Verify it used messaging_queue methods

async def test_markov_speak_uses_messaging_queue(fake_bot, fake_context, fake_engine):
    msg_queue = MessagingQueue(fake_bot, {}, None)
    await fake_bot.add_cog(msg_queue)

    markov = Markov(fake_bot, {}, fake_engine)
    await fake_bot.add_cog(markov)

    # Execute speak command
    await markov.speak(fake_context)

    # Verify message was sent via messaging_queue
```

**Files Changed:**
- `discord_bot/cogs/markov.py`

**Acceptance Criteria:**
- Markov cog uses MessagingQueue for all Discord API calls
- Background loop uses `fetch_channel_history()`
- Commands use `send_message()`
- No direct discord.py calls (except via MessagingQueue)
- All markov tests pass

**Estimated Time:** 2 hours

---

## Phase 3 Completion Checklist

Before moving to Phase 4, verify:
- ✅ All tasks 3.1-3.5 complete
- ✅ Markov cog fully migrated
- ✅ Fetch methods work in both modes
- ✅ All tests passing
- ✅ Pylint score 10/10
- ✅ Manual testing confirms markov functionality

---

## Phase 4: Migrate Remaining Cogs

**Goal:** Migrate all remaining cogs to use MessagingQueue

### Task 4.1: Migrate Role Cog 🔲

**Goal:** Update Role cog to use MessagingQueue

**Subtasks:**
1. Replace all `ctx.send()` with `self.send_message()`
2. Replace all `async_retry_discord_message_command` calls
3. Test all role commands
4. Verify DapperTable outputs still work

**Files Changed:**
- `discord_bot/cogs/role.py`

**Acceptance Criteria:**
- Role cog uses MessagingQueue
- All role commands work
- No regressions

**Estimated Time:** 1.5 hours

---

### Task 4.2: Migrate General Cog 🔲

**Goal:** Update General cog to use MessagingQueue

**Subtasks:**
1. Replace `ctx.send()` calls with `self.send_message()`
2. Test `!hello`, `!roll`, `!meta` commands

**Files Changed:**
- `discord_bot/cogs/general.py`

**Acceptance Criteria:**
- General cog uses MessagingQueue
- All commands work

**Estimated Time:** 30 minutes

---

### Task 4.3: Migrate Urban Cog 🔲

**Goal:** Update Urban cog to use MessagingQueue

**Subtasks:**
1. Replace `ctx.send()` calls with `self.send_message()`
2. Test `!urban` command

**Files Changed:**
- `discord_bot/cogs/urban.py`

**Acceptance Criteria:**
- Urban cog uses MessagingQueue
- Urban dictionary lookup works

**Estimated Time:** 30 minutes

---

### Task 4.4: Migrate DeleteMessages Cog 🔲

**Goal:** Update DeleteMessages cog to use MessagingQueue

**Subtasks:**
1. Replace `async_retry_discord_message_command` calls
2. Use `self.delete_message()` for message deletion
3. Test background loop

**Files Changed:**
- `discord_bot/cogs/delete_messages.py`

**Acceptance Criteria:**
- DeleteMessages cog uses MessagingQueue
- Background deletion loop works

**Estimated Time:** 45 minutes

---

## Phase 4 Completion Checklist

Before declaring Phase 4 complete:
- ✅ All tasks 4.1-4.4 complete
- ✅ All cogs migrated to MessagingQueue
- ✅ All 510+ tests passing
- ✅ No direct discord.py calls outside MessagingQueue
- ✅ Pylint score 10/10
- ✅ Manual testing of all cogs

---

## Phase 5: External Gateway Mode (Future)

**Note:** These tasks are for future implementation and can be broken down further when we reach this phase.

### Task 5.1: Create Gateway Service Skeleton 🔲
### Task 5.2: Implement Redis Queue 🔲
### Task 5.3: Implement Gateway Caches 🔲
### Task 5.4: Implement External Mode in MessagingQueue 🔲
### Task 5.5: Deployment & Testing 🔲

---

## Progress Tracking

### Overall Progress
- **Phase 1:** 0/9 tasks complete (0%)
- **Phase 2:** 0/7 tasks complete (0%)
- **Phase 3:** 0/5 tasks complete (0%)
- **Phase 4:** 0/4 tasks complete (0%)
- **Phase 5:** Not started

### Total Tasks: 25 (not counting Phase 5 subtasks)

---

## Notes

- Each task should be completed in a separate branch/commit
- All tests must pass before moving to next task
- Pylint score must be 10/10 for modified files
- Manual testing should be performed for user-facing changes
- Update this file as tasks are completed
