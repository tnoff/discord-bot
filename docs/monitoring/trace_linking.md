# Media Request Trace Linking

Explains how OpenTelemetry span links connect the full lifecycle of a music
request — from the Discord command that triggered it through download and
into the audio player — across async task boundaries where normal
parent-child relationships are not possible. The same underlying link
mechanism also carries trace context across the dispatcher's HTTP+Redis
hop; see [Dispatcher trace propagation](#dispatcher-trace-propagation) below
for that specific case.

## Background

The music pipeline is split across pods now, not just background tasks
within one process: search happens in the `discord-search` pod, downloads in
the `discord-downloader` pod, and `discord-gateway` (the bot) only submits
requests and later consumes results. Within the bot, `_result_task` and
`_search_result_task` are the two consumers pulling those results back; there
is no `_youtube_search_task` or `_download_task` any more — those became
`discord_search/workers/youtube_music_search_driver.py` and
`discord_downloader/interfaces/download_protocols.py` in their own pods.

Because none of this runs in the same process as the originating Discord
command, and is not awaited by it, there is no active parent span when a
downstream span is created — whether it's a background task in the same
process or a worker in a different pod entirely. Spans created there appear
as completely separate, unconnected traces in any OTEL backend.

Span **links** solve this. A link is a peer reference — it says "this span
was caused by span X in trace Y" without making it a child. Each background
span carries a link back to the command span that submitted the request, so
you can navigate the full journey from a single request in your trace backend.

## How context is captured and carried

### 1. Capture at enqueue time — `enqueue_media_requests()`

```
discord_gateway/cogs/music.py :: enqueue_media_requests()
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
batch that does not already have a context set. The MediaRequest carries this
dict wherever it travels — including over HTTP/Redis to the `discord-search`
and `discord-downloader` pods, since it's JSON-serialisable — so the link
survives the pod boundary the same way it survived the old in-process queue
boundary. The existing guard in `discord_downloader/interfaces/download_protocols.py`
(`DownloadWorkerBase.submit()`) acts as a fallback for any path that
bypasses `enqueue_media_requests`.

### 2. Reconstruct links from context — `span_links_from_context()`

```
discord_core/utils/otel.py :: span_links_from_context(span_context)
```

When a background task creates a span it calls this helper, which
reconstructs a `trace.Link` from the stored dict. If the dict is `None` or
the IDs are zero (invalid), an empty list is returned and the span is created
without links — no error is raised.

## Span chain for a typical request

### Direct YouTube / URL path

```
music.play_  (SERVER)                    ← @command_wrapper, Discord ctx attributes, discord-gateway pod
  │
  │  [MediaRequest.span_context captured here]
  │
  └─ [link] music.download_client.create_source  (CLIENT)   ← yt-dlp call, discord-downloader pod
               └─ [link] music.process_download_results  (CONSUMER)  ← discord-gateway pod, pulls the result back
                            └─ [link] music.add_source_to_player  (INTERNAL)  ← discord-gateway pod
```

### YouTube Music search path

Requests that are not a direct URL or YouTube link go through the YT Music
search queue first:

```
music.play_  (SERVER)                                       ← discord-gateway pod
  │
  │  [MediaRequest.span_context captured here]
  │
  └─ [link] music.search_youtube_music  (CLIENT)    ← discord-search pod
               └─ [link] music.download_client.create_source  (CLIENT)  ← discord-downloader pod
                            └─ [link] music.process_download_results  (CONSUMER)  ← discord-gateway pod
                                         └─ [link] music.add_source_to_player  (INTERNAL)  ← discord-gateway pod
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
  └─ [link] music.download_client.create_source  (CLIENT)  ← one span per playlist item, discord-downloader pod
  └─ [link] music.download_client.create_source  (CLIENT)
  ...
```

## Span reference

| Span name | Kind | Pod / location | Notes |
|-----------|------|----------|-------|
| `music.play_` | SERVER | `discord-gateway`, `discord_gateway/cogs/music.py` | Created by `@command_wrapper`; carries full Discord context attributes |
| `music.search_youtube_music` | CLIENT | `discord-search`, `discord_search/workers/youtube_music_search_driver.py` | Only present for non-URL searches; marked ERROR on rate-limit failure |
| `music.download_client.create_source` | CLIENT | `discord-downloader`, `discord_downloader/interfaces/download_protocols.py` | yt-dlp download; carries media request attributes |
| `music.process_download_results` | CONSUMER | `discord-gateway`, `discord_gateway/cogs/music.py` | Routes completed result to player or returns error to user |
| `music.add_source_to_player` | INTERNAL | `discord-gateway`, `discord_gateway/cogs/music.py` | Final handoff into the player queue |

Other spans `discord_gateway/cogs/music.py` emits (`music.process_search_results`,
`music.post_play_processing`, `music.resume_player_session`,
`music.get_player`, `music.ensure_player`, `music.cleanup`,
`music.cog_unload`) follow the same `span_links_from_context` pattern where
they carry a `MediaRequest`; see the source for the current full list rather
than treating this table as exhaustive.

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

## Dispatcher trace propagation

Distinct from the media-request pipeline above, but built on the same
`span_links_from_context()` primitive: the dispatcher hop uses two different
techniques depending on whether the call is fire-and-forget or awaitable.

### Fire-and-forget (send, delete, mutable updates)

W3C `traceparent` headers flow from cog pod → dispatcher pod → Redis payload:

- `HttpDispatchClient` (via `HttpClientMixin._trace_headers()`) injects the
  active span's traceparent into every outbound POST with
  `opentelemetry.propagate.inject()`.
- `DispatchHttpServer` extracts it with
  `opentelemetry.propagate.extract(request.headers)` and opens a
  `SpanKind.SERVER` span for the handler
  (`discord_dispatcher/servers/dispatch_server.py`).
- The request is then enqueued to Redis for async execution, so the handler
  span has already closed by the time a worker picks up the payload — there
  is no active parent to inherit from. Handlers reuse the same mechanism
  `enqueue_media_requests()` uses above: the span context is captured into
  the payload's `span_context` dict and reconstructed as a `trace.Link` via
  `span_links_from_context()` when the worker span opens
  (`discord_dispatcher/workers/message_dispatcher.py`).

### Awaitable fetches (channel history, guild emojis)

These block the caller on a result, so they stay on the header-propagation
path throughout instead of detaching into a link: `dispatch_client.fetch_history`
/ `fetch_emojis` (CLIENT, bot pod, `discord_core/clients/dispatch_client_base.py`)
open while the traceparent header is in flight; the dispatcher's matching
`dispatch.fetch_history` / `fetch_emojis` (SERVER) handler spans extract it
the same way as the fire-and-forget handlers, and the eventual Redis-worker
execution span still links back via `span_context`, same as every other
handler.

### Span reference

Measured from the real `otel_span_wrapper`/`async_otel_span_wrapper` call
sites, not hand-transcribed — this table went stale twice in one session
before it was generated (a leftover `_redis` suffix on three names, then
four real spans missing outright). See `tests/cli/_span_census.py`. **Pod**
is which package the span-creating code is *defined* in, not which pod
necessarily emits it at runtime — `dispatch_client.*` lives in
`discord_core` (`DispatchClientBase`) and is reachable from any pod that
constructs an `HttpDispatchClient`; in practice only the bot pod's cogs
call the history/emoji fetch path today.

<!-- BEGIN GENERATED(dispatcher-span-table) by tests/cli/test_span_census.py. Do not edit this block by hand.
     Regenerate with: UPDATE_SPAN_CENSUS=1 pytest tests/cli/test_span_census.py -->

| Span name | Kind | Pod | Source |
|---|---|---|---|
| `dispatch.delete` | SERVER | dispatcher | `discord_dispatcher/servers/dispatch_server.py:110` |
| `dispatch.fetch_emojis` | SERVER | dispatcher | `discord_dispatcher/servers/dispatch_server.py:170` |
| `dispatch.fetch_history` | SERVER | dispatcher | `discord_dispatcher/servers/dispatch_server.py:157` |
| `dispatch.remove_mutable` | SERVER | dispatcher | `discord_dispatcher/servers/dispatch_server.py:126` |
| `dispatch.send` | SERVER | dispatcher | `discord_dispatcher/servers/dispatch_server.py:101` |
| `dispatch.update_mutable` | SERVER | dispatcher | `discord_dispatcher/servers/dispatch_server.py:118` |
| `dispatch.update_mutable_channel` | SERVER | dispatcher | `discord_dispatcher/servers/dispatch_server.py:133` |
| `dispatch_client.fetch_emojis` | CLIENT | core (shared) | `discord_core/clients/dispatch_client_base.py:169` |
| `dispatch_client.fetch_history` | CLIENT | core (shared) | `discord_core/clients/dispatch_client_base.py:141` |
| `message_dispatcher.delete` | INTERNAL | dispatcher | `discord_dispatcher/workers/message_dispatcher.py:737` |
| `message_dispatcher.fetch_emojis` | INTERNAL | dispatcher | `discord_dispatcher/workers/message_dispatcher.py:786` |
| `message_dispatcher.fetch_history` | INTERNAL | dispatcher | `discord_dispatcher/workers/message_dispatcher.py:765` |
| `message_dispatcher.process_mutable` | INTERNAL | dispatcher | `discord_dispatcher/workers/message_dispatcher.py:611` |
| `message_dispatcher.remove_mutable` | INTERNAL | dispatcher | `discord_dispatcher/workers/message_dispatcher.py:698` |
| `message_dispatcher.send` | INTERNAL | dispatcher | `discord_dispatcher/workers/message_dispatcher.py:726` |
| `message_dispatcher.update_mutable_channel` | INTERNAL | dispatcher | `discord_dispatcher/workers/message_dispatcher.py:750` |

<!-- END GENERATED(dispatcher-span-table) -->

Redis commands issued by the dispatcher (and by the broker, downloader, and
search pods) are traced automatically by `RedisInstrumentor`, one CLIENT
span per command — see [OTLP configuration](./otlp_configuration.md#high-volume-span-filtering)
for why that volume is filtered at the collector rather than in-process,
and note that instrumentation is enabled process-wide (every pod, when OTLP
is on) even though only the pods that actually issue Redis commands produce
any spans from it.

## Implementation files

| File | Role |
|------|------|
| `discord_core/utils/otel.py` | `capture_span_context()`, `span_links_from_context()` |
| `discord_core/types/media_request.py` | `MediaRequest.span_context` field |
| `discord_gateway/cogs/music.py` | Capture in `enqueue_media_requests()`; links in `process_download_results()`, `process_search_results()`, `add_source_to_player()` |
| `discord_search/workers/youtube_music_search_driver.py` | `music.search_youtube_music` span, `discord-search` pod |
| `discord_downloader/interfaces/download_protocols.py` | Fallback capture in `DownloadWorkerBase.submit()`; `music.download_client.create_source` span, `discord-downloader` pod |
| `discord_core/clients/http_client_base.py` | `_trace_headers()` — W3C traceparent injection shared by every `Http*Client` |
| `discord_dispatcher/servers/dispatch_server.py` | Header extraction + `SpanKind.SERVER` handler spans (`dispatch.*`) |
| `discord_core/clients/dispatch_client_base.py` | `dispatch_client.fetch_history` / `fetch_emojis` (CLIENT) |
| `discord_dispatcher/workers/message_dispatcher.py` | `message_dispatcher.*` (INTERNAL) worker execution spans |

## Spans not statically determinable

<!-- BEGIN GENERATED(exempt-span-appendix) by tests/cli/test_span_census.py. Do not edit this block by hand.
     Regenerate with: UPDATE_SPAN_CENSUS=1 pytest tests/cli/test_span_census.py -->

Every `otel_span_wrapper`/`async_otel_span_wrapper` call site this
census can't reduce to a literal string, repo-wide -- not just the
dispatcher. Each one is a genuine runtime value (a route name passed
as a parameter, a Discord command name resolved from `ctx`), not a
gap in the resolver: see `tests/cli/_otel_resolve.py` for exactly
which constructs it does and does not follow.

| Source | Why | Expression |
|---|---|---|
| `discord_core/clients/http_store_base.py:101` | span name not statically determinable | `f'{self.SPAN_PREFIX}.{route}'` |
| `discord_db/servers/database_server.py:251` | span name not statically determinable | `f'{SPAN_PREFIX}.{span_name}'` |
| `discord_gateway/utils/otel_command.py:44` | span name not statically determinable | `span_name` |

<!-- END GENERATED(exempt-span-appendix) -->
