# Media Request Trace Linking

Explains how OpenTelemetry span links connect the full lifecycle of a music
request — from the Discord command that triggered it through download and
into the audio player — across async task boundaries where normal
parent-child relationships are not possible.

## Background

The music pipeline is split across several independent background tasks:

- `_youtube_search_task` — resolves song names via YouTube Music API
- `_download_task` — downloads audio files via yt-dlp
- `_result_task` — routes completed downloads to players

Because these tasks run concurrently and are not awaited by the originating
command, they have no active parent span when they execute. Spans created
inside them appear as completely separate, unconnected traces in any OTEL
backend.

Span **links** solve this. A link is a peer reference — it says "this span
was caused by span X in trace Y" without making it a child. Each background
span carries a link back to the command span that submitted the request, so
you can navigate the full journey from a single request in your trace backend.

## How context is captured and carried

### 1. Capture at enqueue time — `enqueue_media_requests()`

```
music.py :: enqueue_media_requests()
```

This is the single choke point through which all media requests pass before
entering any queue. While the `@command_wrapper` span (e.g. `music.play_`)
is still active, `capture_span_context()` snapshots the current span as a
plain dict:

```python
{'trace_id': int, 'span_id': int, 'trace_flags': int}
```

This dict is stored on `MediaRequest.span_context`. It is JSON-serialisable
and Pydantic-compatible, so it travels with the request through any queue.

The capture happens once per batch and is written to every request in the
batch that does not already have a context set. The existing guard in
`download_client.submit()` acts as a fallback for any path that bypasses
`enqueue_media_requests`.

### 2. Reconstruct links from context — `span_links_from_context()`

```
utils/otel.py :: span_links_from_context(span_context)
```

When a background task creates a span it calls this helper, which
reconstructs a `trace.Link` from the stored dict. If the dict is `None` or
the IDs are zero (invalid), an empty list is returned and the span is created
without links — no error is raised.

## Span chain for a typical request

### Direct YouTube / URL path

```
music.play_  (SERVER)                    ← @command_wrapper, Discord ctx attributes
  │
  │  [MediaRequest.span_context captured here]
  │
  └─ [link] music.create_source  (CLIENT)           ← _download_task, yt-dlp call
               └─ [link] music.process_download_results  (CONSUMER)  ← _result_task
                            └─ [link] music.add_source_to_player  (INTERNAL)
```

### YouTube Music search path

Requests that are not a direct URL or YouTube link go through the YT Music
search queue first:

```
music.play_  (SERVER)
  │
  │  [MediaRequest.span_context captured here]
  │
  └─ [link] music.search_youtube_music  (CLIENT)    ← _youtube_search_task
               └─ [link] music.create_source  (CLIENT)
                            └─ [link] music.process_download_results  (CONSUMER)
                                         └─ [link] music.add_source_to_player  (INTERNAL)
```

If a YT Music search is rate-limited and retried, `search_youtube_music` is
called again for the same request. Each retry produces a new linked span so
you can see every attempt.

### Playlist play path

```
music.playlist play  (SERVER)
  │
  │  [MediaRequest.span_context captured here, same context for all items in the batch]
  │
  └─ [link] music.create_source  (CLIENT)  ← one span per playlist item
  └─ [link] music.create_source  (CLIENT)
  ...
```

## Span reference

| Span name | Kind | Location | Notes |
|-----------|------|----------|-------|
| `music.play_` | SERVER | `music.py` | Created by `@command_wrapper`; carries full Discord context attributes |
| `music.search_youtube_music` | CLIENT | `music.py` | Only present for non-URL searches; marked ERROR on rate-limit failure |
| `music.create_source` | CLIENT | `download_client.py` | yt-dlp download; carries media request attributes |
| `music.process_download_results` | CONSUMER | `music.py` | Routes completed result to player or returns error to user |
| `music.add_source_to_player` | INTERNAL | `music.py` | Final handoff into the player queue |

## Navigating linked spans in a trace backend

Most backends (Grafana Tempo, Jaeger, Honeycomb) display links as clickable
references on the span detail panel.

**From a background span to the originating command:**
Open any of the background spans (e.g. `music.create_source`), find the
Links section, and follow the link to the `music.play_` span and its trace.

**From the command to downstream spans:**
Most backends do not index links bidirectionally, so you cannot directly
click from `music.play_` to its linked children. Use the `trace_id` from the
originating span as a search term, or search for
`media_request.uuid` attribute — all spans in the chain set this attribute
via `media_request_attributes()`.

## Span attributes set on all media request spans

These attributes are set via `media_request_attributes()` and appear on
every span in the chain:

| Attribute | Source |
|-----------|--------|
| `music.media_request.uuid` | `MediaRequest.uuid` |
| `music.media_request.search_string` | Raw user input |
| `music.media_request.requester` | Discord user ID |
| `music.media_request.guild` | Discord guild ID |
| `music.media_request.search_type` | `YOUTUBE`, `DIRECT`, `YOUTUBE_MUSIC`, etc. |

The originating command span additionally carries Discord context attributes
(`discord.author`, `discord.channel`, `discord.guild`,
`discord.context.command`, `discord.context.message`) set by
`@command_wrapper`.

## Implementation files

| File | Role |
|------|------|
| `discord_bot/utils/otel.py` | `capture_span_context()`, `span_links_from_context()` |
| `discord_bot/types/media_request.py` | `MediaRequest.span_context` field |
| `discord_bot/cogs/music.py` | Capture in `enqueue_media_requests()`; links in `search_youtube_music()`, `process_download_results()`, `add_source_to_player()` |
| `discord_bot/cogs/music_helpers/download_client.py` | Fallback capture in `submit()`; link in `__prepare_data_source()` |
