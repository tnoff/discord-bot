# Discord Bot Messaging System - Architecture Explainer

## Overview

The messaging system is a multi-layer architecture that manages real-time
Discord message updates across every pod, via the shared `MessageDispatcher`
worker — not just music. It achieves efficient API usage through inline
message edits/deletes and maintains stable ordering through carefully managed
data structures. Music is the system's largest and most complex consumer
(bundles, queue operations, download progress), so most of the detailed
examples below are drawn from it, but the dispatcher and bundle model
themselves are pod-agnostic — this page lives alongside
[docs/message_dispatcher.md](./message_dispatcher.md) rather than under
`docs/music/`.

---

## Core Components

### 1. **Message Dispatcher** (`MessageDispatcher`)

`MessageDispatcher` is not a cog — it is the sole worker of the standalone
`discord-dispatcher` pod (`discord_dispatcher/workers/message_dispatcher.py`),
the dispatcher for all Discord API calls across every other pod, reached
over HTTP via `HttpDispatchClient`. See
[docs/message_dispatcher.md](./message_dispatcher.md) for the full
reference; this page focuses on the messaging/bundle model specifically.

**Two Message Types**:

1. **`SINGLE_IMMUTABLE`** - One-off messages sent once and deleted after timeout (e.g., error messages)
2. **`MULTIPLE_MUTABLE`** - Bundles of messages that update in-place via edits

**Processing Flow**:
- One Redis-backed priority work queue per guild (a sorted set); one
  `BZPOPMIN` worker per active guild — not an in-process `asyncio.PriorityQueue`,
  since this now runs in its own pod
- Mutable sentinel items (HIGH priority) flush the current bundle display
- Immutable sends and deletes run at NORMAL priority
- Background reads (fetch_message, channel history) run at LOW priority

---

### 2. **Message Bundle System** (`MessageMutableBundle`)

This manages multiple related Discord messages as a cohesive unit. Defined
in `discord_dispatcher/workers/message_dispatcher.py`.

**Key Features**:

- **Sticky Messages**: Ensures bundle stays at the bottom of the channel by deleting and resending when other messages appear below
- **Smart Diffing**: Compares existing messages with new content and generates minimal edit/delete/send operations
- **Message Contexts**: Each context tracks a single Discord message with its content, ID, and dispatch function

**Key Fields**:
- `message_contexts` - List of MessageContext objects tracking individual messages
- `sticky_messages` - Boolean flag to keep messages at bottom of channel

---

### 3. **Media Request Bundle** (`BundleState` + `BundleRenderer`)

There is no single `MultiMediaRequestBundle` class any more — it was split
into `BundleState` (Pydantic, the persisted data) and `BundleRenderer` (the
transient `DapperTable` wrapper), both in
`discord_broker/workers/media_bundle.py`, since HA broker mode needs bundle
state persisted in Redis independently of any one bot process. The exact
field names moved with the split (e.g. `BundleState` has `uuid`,
`pagination_length`, `input_string`; check the source directly rather than
trusting a field list here, since this doc predates the split and hasn't
been re-verified field-by-field). Manages the lifecycle of multiple media
requests (playlists, albums, searches):
- `total`, `completed`, `failed`, `rejected`, `discarded` - Counters for tracking progress

`rejected` counts requests that reached FAILED because the *video* was declined
(too long, banned, private, age restricted, unavailable, format missing) rather
than because the download broke. They render as "Media request rejected" and are
summarised under their own "Details for Rejected Requests" header; the banner
tail reads `N failed, M rejected` (the `0 failed` half is dropped when nothing
actually failed).

**Request Tracking Structure**:

Each media request is tracked in a `BundledRequestState`
(`discord_broker/workers/media_bundle.py`), which embeds the `MediaRequest`
itself (for `search_string`, `status`/lifecycle stage, `uuid`) plus:
- `table_index` - Index in DapperTable
- `row_collection_index` - Pagination collection index
- `row_index_in_collection` - Row index within collection

---

## Static Ordering Mechanism

### **Problem**: How to maintain consistent message order when content changes?

**Solution**: Two-phase indexing system

#### **Phase 1: Dynamic Table Building** (Before `all_requests_added()`)

- Requests added to `DapperTable` dynamically
- `table_index` tracks position in table
- Messages show search/queue status

#### **Phase 2: Static Pagination** (After `all_requests_added()`)

Once `all_requests_added()` is called:
1. The table is frozen into paginated collections via `self.row_collections = self.table.get_paginated_rows()`
2. A static index mapping is built from `table_index` to `(collection_idx, row_idx)` positions
3. Each media request gets assigned its frozen position in the paginated structure

**Key Insight**: Once frozen, the pagination structure never changes. Updates use `_edit_row_data()` to edit both:
1. The underlying `DapperTable` (source of truth)
2. The cached `row_collections` (for performance)

This ensures:
- ✅ **Stable ordering**: Items never move position
- ✅ **Consistent pagination**: Page boundaries stay fixed
- ✅ **Reliable updates**: Edits target exact row positions

---

## Minimizing API Calls

### **Strategy 1: Inline Edits**

Instead of deleting and resending, the system performs intelligent diffing:

**Process**:
1. Match existing messages with new content using `_match_existing_message_content()`
2. Skip messages where content is unchanged (no API call)
3. Edit messages where content changed (1 API call instead of 2)

**Example**: Updating download progress from "Downloading 1/10" → "Downloading 2/10"
- ❌ **Naive**: Delete old message + Send new message = **2 API calls**
- ✅ **Optimized**: Edit existing message = **1 API call**

### **Strategy 2: Intelligent Deletion**

When messages are removed (e.g., completing downloads):

**Process**:
1. Find which messages can be preserved by matching content
2. Delete only non-matching messages
3. Edit preserved messages with new content

**Example**: Bundle with 5 items, 2 complete
- Shows: `[Item1: Downloading..., Item2: Downloading..., Item3: Queued, Item4: Queued, Item5: Queued]`
- After completion: `[Item3: Downloading..., Item4: Queued, Item5: Queued]`
- Operation: Edit message 1 (Item3 content), Edit message 2 (Item4 content), Edit message 3 (Item5 content), Delete messages 4-5
- **Result**: 5 API calls instead of 8 (delete 5 + send 3)

### **Strategy 3: Sticky Message Optimization**

The `should_clear_messages()` method checks if messages are still at the bottom of the channel by comparing message IDs with recent channel history.

**Benefit**: Only deletes/resends when messages are pushed up (sticky=True), avoiding unnecessary operations when messages remain at the bottom.

---

## Queue System Integration

### **Player Queue** (`discord_broker/workers/play_order.py`)

Shows current playback and upcoming tracks. The broker renders it and updates it on every change to the guild's queue; the gateway no longer does.

**Behavior**:
- Displays "Now Playing" message followed by queue table
- Table includes position, wait time, title, and uploader
- Multiple messages if queue spans pages

**Index Name**: `play_order-{guild_id}`
**Sticky**: True (always shown at bottom)

### **Download/Search Queues**

There is no `DistributedQueue` any more — download and YouTube Music search
each run as a per-guild Redis-backed worker in their own standalone pod
(`discord-downloader`, `discord-search`), not an in-process queue the bot
drains itself. The gateway submits requests to those pods over HTTP and
polls the broker for results.

These are **separate from messaging** but trigger bundle updates:

1. **Search** (`discord-search` pod) → YouTube Music API lookup → Updates bundle status to `QUEUED`
2. **Download** (`discord-downloader` pod) → yt-dlp download → Updates bundle status to `IN_PROGRESS` → `COMPLETED`/`FAILED`

---

## Complete Message Lifecycle Example

**User types**: `/play spotify:album:abc123` (10 tracks)

### **1. Bundle Creation**

Process:
1. Create a `BundleState`/`BundleRenderer` pair for the album, on the broker
2. Set initial search string: "spotify:album:abc123"
3. Add each track as a `MediaRequest` with `SEARCHING` status
4. Submit each request to the `discord-search` pod over HTTP
5. Call `bundle.all_requests_added()` to freeze pagination
6. Register bundle with the dispatcher for message updates

**Messages Sent**:
```
Processing "spotify:album:abc123"
Media request queued for download: "Track 1"
Media request queued for download: "Track 2"
...
Media request queued for download: "Track 10"
```

### **2. Search Phase**

Each track runs through YouTube Music search to find the actual YouTube video URL.

**Messages Updated** (inline edits):
```
Processing "spotify:album:abc123"
0/10 media_requests processed, 0 failed
Media request queued for download: "Track 1"  ← EDITED
...
```

### **3. Download Phase**

For each track:
1. **Backoff**: Wait for YouTube rate limiting → "Waiting for youtube backoff..."
2. **Download**: Run yt-dlp → "Downloading and processing: Track 1"
3. **Completion**: Add to play queue → Row cleared (empty message)

**Messages Updated** (for Track 1):
```
Processing "spotify:album:abc123"
1/10 media_requests processed, 0 failed  ← HEADER EDITED
                                          ← Track 1 row CLEARED
Downloading and processing: "Track 2"    ← Track 2 row EDITED
Media request queued for download: "Track 3"
...
```

### **4. Bundle Completion**

When all tracks are processed, the bundle is marked as finished and removed from the active bundles dictionary.

**Final Message** (deleted after 5 minutes):
```
Completed processing of "spotify:album:abc123"
10/10 media_requests processed, 0 failed
```

---

## API Call Optimization Summary

| Operation | Naive Approach | Optimized Approach | Savings |
|-----------|---------------|-------------------|---------|
| Update 1 track status | Delete + Send (2 calls) | Edit (1 call) | **50%** |
| Complete 5/10 tracks | Delete 10 + Send 5 (15 calls) | Edit 5 + Delete 5 (10 calls) | **33%** |
| Sticky re-position | Delete N + Send N (2N calls) | Only when pushed up | **~90%** |
| Unchanged content | Send duplicate (1 call) | Skip (0 calls) | **100%** |

**Real-world impact**: For a 50-track playlist:
- Naive: ~150+ API calls
- Optimized: ~60 API calls (**60% reduction**)

---

## Key Design Principles

1. **Separation of Concerns**:
   - `MediaRequest`: User intent (what to play)
   - `MediaDownload`: Downloaded file (where it is)
   - `BundleState`/`BundleRenderer`: Progress tracking (how it's going)
   - `MessageMutableBundle`: Discord presentation (what user sees)

2. **Immutable Pagination**: Once `all_requests_added()` is called, row positions are frozen, enabling stable inline edits

3. **Dual Indexing**: Both `table_index` (logical) and `row_collection_index`/`row_index_in_collection` (physical) allow efficient lookups

4. **Lazy Deletion**: Messages deleted only when content shrinks or bundle completes, not on every status change

5. **Smart Diffing**: Content comparison prevents redundant edits when messages unchanged

This architecture enables real-time progress updates for complex multi-track operations while respecting Discord's rate limits through aggressive API call minimization.
