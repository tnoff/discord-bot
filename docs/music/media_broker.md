# Media Broker

## Overview

The media broker is a lifecycle registry that tracks every piece of media flowing through the music system — from the moment a `MediaRequest` enters the pipeline to the moment the player finishes with the downloaded file and it can safely be evicted from cache.

It solves two problems that currently have no clean solution:

1. **Cache cleanup**: today the system uses a scattered "mark for deletion" pattern to decide when a file is safe to delete. The broker replaces this with reference counting — a file is evictable when nothing holds a reference to it.

2. **Player restart / refresh**: if a player disconnects and reconnects, it needs a way to know what files it had queued and whether those files are still available. The broker is the single place that holds that view.

It is intentionally designed as an in-process sidecar first. The interface is defined so the backing store can later be replaced with a remote service (Redis, SQS, HTTP) without callers needing to change.

---

## Relationship to the Request Lifecycle State Machine

The `MediaRequestStateMachine` and the media broker are **separate systems** that track different things and serve different consumers.

| | State Machine | Media Broker |
|---|---|---|
| Tracks | Individual `MediaRequest` lifecycle | All active media across the system |
| Consumer | Bundle UI (user-facing status messages) | Cache cleanup, restart recovery |
| Lifetime | Request creation → terminal stage | Download completion → file eviction |
| Scope | Per-request | System-wide aggregate |

They have a clean handoff point at `COMPLETED`:

```
State machine:  SEARCHING → QUEUED → IN_PROGRESS → COMPLETED  ← ends here
                                                        ↓
Broker:                                       registers file → AVAILABLE → CHECKED_OUT → evictable
```

The broker subscribes to state machine transitions via the existing `on_change` callback rather than requiring explicit calls at every transition point. The cog's `_on_request_state_change` method is the natural place to forward relevant transitions to the broker.

---

## The Three Zones

All media in the system lives in exactly one of three zones at any given time:

```
┌─────────────────┐    ┌──────────────────────┐    ┌───────────────────────┐
│   IN_FLIGHT     │    │      AVAILABLE        │    │     CHECKED_OUT       │
│                 │    │                       │    │                       │
│ MediaRequest    │    │ MediaDownload          │    │ MediaDownload         │
│ exists, no file │    │ file on disk,          │    │ player has it queued  │
│ yet             │    │ nobody playing it yet  │    │ or is playing it now  │
└────────┬────────┘    └──────────┬────────────┘    └──────────┬────────────┘
         │                        │                             │
    download                 player takes               player finishes
    completes                  the file                  or disconnects
         │                        │                             │
         └──────────→─────────────┘                            │
                                         ┌─────────────────────┘
                                         ↓
                                    reference count → 0
                                    file is evictable
```

**IN_FLIGHT**: a `MediaRequest` is active (searching, queued, downloading) but no file exists on disk yet. The broker tracks these so the system can answer "what is currently being worked on?" across all guilds.

**AVAILABLE**: the file is on disk and the download is complete, but the player has not yet consumed it. This zone is currently invisible in the codebase — nothing tracks it today. The broker makes it explicit.

**CHECKED_OUT**: a player holds a reference to this file. It may be queued behind the current track or actively playing. The file must not be evicted while any player holds a reference.

---

## Data Model

The broker maintains a single registry keyed on `MediaRequest.uuid`. This UUID is the through-line for the entire lifecycle — the `MediaDownload` carries a reference to its originating `MediaRequest`, so the broker can link them at download completion without a separate lookup.

```python
{
    media_request_uuid: BrokerEntry(
        request=MediaRequest,
        download=MediaDownload | None,   # None until download completes
        zone=Zone,                        # IN_FLIGHT | AVAILABLE | CHECKED_OUT
        checked_out_by=guild_id | None,   # set when player takes the file
    )
}
```

`MediaDownload` gets its own UUID in addition to carrying the `MediaRequest` reference. This matters for two reasons:
- The same `webpage_url` can appear multiple times (same song queued twice), so URL is not a safe unique key
- In a future microservice split, a download artifact may need to be identified and handed off independently of its originating request

---

## State Transitions

```
register_request(media_request)
    → entry created, zone = IN_FLIGHT

register_download(media_download)
    → entry updated with download, zone = AVAILABLE
    → triggered when state machine fires COMPLETED

prefetch(queue_items, guild_id, guild_path, limit)
    → stages the next `limit` AVAILABLE items to local disk via checkout()
    → already CHECKED_OUT items count toward the limit
    → no-op in local mode (no bucket_name configured)

checkout(media_request_uuid, guild_id, guild_path)
    → zone = CHECKED_OUT, checked_out_by = guild_id
    → stages the file to guild_path (S3 download or local copy)
    → if already CHECKED_OUT with a valid staged file, returns immediately (idempotent)
    → triggered when player takes the file from the queue, or by prefetch()

release(media_request_uuid)
    → deletes the guild-specific staged copy, removes entry from registry
    → triggered when player finishes the track or is cleaned up

remove(media_request_uuid)
    → removes an AVAILABLE entry without touching any files
    → used when a queued item is discarded before checkout

discard(media_request_uuid)
    → removes an entry that could not be enqueued
    → deletes the underlying file (S3 object or local) if no video cache is configured
```

---

## Integration Points

### Download client → broker
When a download completes and a `MediaDownload` is created, the download client (or the cog, at the point where the file is handed to the player) calls `register_download`. This moves the entry from IN_FLIGHT to AVAILABLE.

### State machine → broker
The cog's `_on_request_state_change` callback is the central place to forward transitions:
- Entry into the pipeline (first `SEARCHING` transition) → `register_request`
- `COMPLETED` → `register_download`
- `FAILED` or `DISCARDED` → `remove`

### Player → broker
When the player takes the next item from its queue → `checkout`.
When the player finishes a track or is cleaned up → `release`.

### Cache cleanup → broker
Instead of checking a "marked for deletion" flag on the file, the cache cleanup logic asks the broker `can_evict(media_request_uuid)`. The broker returns true only when the entry is in the AVAILABLE zone (not checked out, not in-flight).

---

## S3 Prefetch Window

In S3 mode, `checkout()` downloads the file from S3 the moment the player dequeues it. With typical song durations of 3+ minutes, this latency is imperceptible. However, the broker also supports a configurable **prefetch window** that pre-stages the next N songs to local disk so they are ready the instant the player needs them, and provides a predictable disk usage ceiling.

```
add_source_to_player()
    → register_download() → Zone.AVAILABLE
    → _trigger_prefetch(): prefetch() next N AVAILABLE items → local disk

player_loop() dequeues
    → checkout() → file already staged, no S3 download needed

player_loop() finishes → release() → local file deleted
    → prefetch() slides next item into the window
```

The number of items pre-staged is controlled by `storage.prefetch_limit` in the config (default: 5). Setting it to `0` restores fully lazy behaviour — only 1 song on local disk at a time.

Already-CHECKED_OUT items count toward the limit, so the window never over-stages. The `checkout()` call is idempotent: if a prefetched item reaches the player while its staged file still exists, no second S3 download is made.

## Player Restart / Refresh

When a player disconnects and reconnects (future capability), it can query the broker for all entries currently in CHECKED_OUT state for its guild:

```python
broker.get_checked_out_by(guild_id)
    → list of BrokerEntry where checked_out_by == guild_id
```

Each entry has the `MediaDownload` with the file path and all metadata needed to reconstruct the player queue. Files still on disk can be re-queued immediately. Files that were evicted would need to be re-downloaded (re-registered as IN_FLIGHT and routed through the download queue again).

## Future Considerations

The in-process design is intentional for the first iteration — no HTTP, no serialization, just a Python class with an in-memory dict. The interface is stable enough that the backing store can be swapped later:

- **Redis**: replace the dict with Redis hash operations, making the broker visible across multiple bot processes or shards
- **Remote HTTP service**: the transition methods become HTTP calls, files are transferred rather than referenced by local path
- **SQS-style handoff**: broker entries become messages on queues, consumed by separate download or playback workers

The `MediaBroker` class is the seam where that migration happens. Callers (cog, player, cache cleanup) do not need to change when the backing store changes.
