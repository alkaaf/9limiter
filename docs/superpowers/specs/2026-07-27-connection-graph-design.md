# Connection Graph — Real-time Neural Network Visualizer

**Feature:** Live bipartite graph showing API key ↔ Model connections in real time, with animated edges colored/thickened by tokens/sec and node sizes by requests/sec.

**Status:** Spec

## 1. Motivation

Existing dashboard (`/dashboard`) shows rate-limit cards + request log table. Existing stats (`/stats`) shows historical bar charts. Neither gives instant visual intuition about **who is hitting which model, how fast, and how many requests per second**. This page fills that gap with a single-screen graph that answers at a glance:

- Which API keys are most active right now (node size = rps)
- Which models are under load (node size = rps)
- How fast each connection is (edge speed/color/thickness = tokens/sec)
- Are requests flowing or stuck (edge direction/animations)

## 2. Architecture

### Zero changes to existing data pipeline

No new channels, no new SQLite tables, no new config fields. Everything builds on the existing `tokio::sync::broadcast::Sender<AppEvent>`.

Only new code: trivial route + WS handler in a new `src/graph.rs` module (cloning the `dashboard.rs` pattern). This is minimal server-side glue — the bulk of the feature is client-side Canvas JS.

```
Event type        Fields used                    Source
────────────────────────────────────────────────────────────────────
Request           id, api_key, model, method,    AppEvent::Request
                  path, owner, timestamp
RequestEnd        id, status, latency_ms,         AppEvent::RequestEnd
                  input_tokens, output_tokens,
                  cache_tokens
```

### Scope

| Component | Files | Lines |
|-----------|-------|-------|
| Route + page | `src/main.rs`, `src/graph.html` | ~5 + 1 file |
| WS handler | `src/graph.rs` (new module, 1 route + 1 handler cloning dashboard pattern) | ~20 |
| Canvas engine | inline in `src/graph.html` | ~400 JS |

**Total:** ~430 lines.

Delete `src/graph.rs` when? If another page also needs a filtered WS handler — then extract a shared helper. Until then, YAGNI.

## 3. Server Side

### 3.1 New route in `src/main.rs`

```rust
mod graph;

// In router:
.route("/graph", axum::routing::get(graph::graph_page_handler))
.route("/_ws_graph", axum::routing::get(graph::graph_ws_handler))
```

**Why separate `/_ws_graph` instead of reusing `/_ws`?** The graph WS handler doesn't need `rate_limit` events, `snapshot`, `sync` command, or `clock` command. A separate handler means simpler code and no event filtering overhead on the existing dashboard path. The two WS handlers share the same `event_tx` broadcast channel — both receive all events; each filters what it needs on the client side.

### 3.2 New module `src/graph.rs`

```rust
pub async fn graph_page_handler() -> impl IntoResponse {
    Html(include_str!("graph.html"))
}

pub async fn graph_ws_handler(
    ws: WebSocketUpgrade,
    State(state): State<AppState>,
) -> impl IntoResponse {
    ws.on_upgrade(move |socket| handle_socket(socket, state))
}

async fn handle_socket(socket: WebSocket, state: AppState) {
    let mut rx = state.event_tx.subscribe();
    let (mut sender, _receiver) = socket.split();

    loop {
        match rx.recv().await {
            Ok(event @ AppEvent::Request(_)) => {
                let json = serde_json::to_string(&event).unwrap();
                let _ = sender.send(Message::Text(json.into())).await;
            }
            Ok(event @ AppEvent::RequestEnd(_)) => {
                let json = serde_json::to_string(&event).unwrap();
                let _ = sender.send(Message::Text(json.into())).await;
            }
            Ok(_) => continue, // ignore rate_limit, log
            Err(_) => break,
        }
    }
}
```

- Ignores incoming messages (no `sync`, `clock`, or any commands)
- Only forwards `AppEvent::Request` and `AppEvent::RequestEnd` variants — drops `RateLimit` and `Log` events
- Each forwarded event carries its `#[serde(tag = "type")]` discriminator (e.g. `"type":"request"`, `"type":"request_end"`)
- No snapshot, no rate_limit events, no logs
- `_receiver` is dropped — client cannot send commands

### 3.3 WebSocket Data Contract

Server sends these two event types over `/_ws_graph`. Both use `#[serde(tag = "type")]` from the existing `AppEvent` enum — no serialization code needed, no new structs.

#### `type: "request"` (from `AppEvent::Request`)

| Field | Type | Source struct field | Example |
|-------|------|--------------------|---------|
| `type` | `"request"` | (enum tag) | |
| `id` | string | `request: RequestStartEvent.id` | `"abc-123"` |
| `api_key` | string | `.api_key` | `"sk-proj-8x...a3F2"` |
| `model` | string | `.model` | `"gpt-4"` |
| `method` | string | `.method` | `"POST"` |
| `path` | string | `.path` | `"/v1/chat/completions"` |
| `owner` | string | `.owner` | `"Alice"` |
| `timestamp` | string (RFC3339) | `.timestamp` | `"2026-07-27T10:00:00+07:00"` |

```json
{"type":"request","id":"abc-123","api_key":"sk-proj-8x...a3F2","model":"gpt-4","method":"POST","path":"/v1/chat","owner":"Alice","timestamp":"2026-07-27T10:00:00+07:00"}
```

Client action: create/update key+model nodes, record RPS, create edge in request phase.

#### `type: "request_end"` (from `AppEvent::RequestEnd`)

| Field | Type | Source struct field | Example |
|-------|------|--------------------|---------|
| `type` | `"request_end"` | (enum tag) | |
| `id` | string | `request_end: RequestEndEvent.id` | `"abc-123"` |
| `status` | u16 | `.status` | `200` |
| `latency_ms` | u64 | `.latency_ms` | `850` |
| `input_tokens` | u64 | `.input_tokens` | `120` |
| `output_tokens` | u64 | `.output_tokens` | `450` |
| `cache_tokens` | u64 | `.cache_tokens` | `30` |

```json
{"type":"request_end","id":"abc-123","status":200,"latency_ms":850,"input_tokens":120,"output_tokens":450,"cache_tokens":30}
```

Client action: find edge by `id`, compute tps, switch to response phase.

#### What is NOT sent

| Not sent | Reason |
|----------|--------|
| `RateLimitEvent` | Dashboard concern, not graph |
| `LogEvent` | Dashboard concern, not graph |
| `AppEvent::Log` | Filtered out in handler |
| Snapshot / `sync` command | Graph is purely event-driven, no polling |
| `clock` | Graph doesn't display server time |

### 3.4 Sidebar in `graph.html`

Same sidebar pattern as `dashboard.html` and `stats.html`:

```html
<a href="/dashboard">Dashboard</a>
<a href="/stats">Stats</a>
<a href="/graph" class="active">Graph</a>
```

## 4. Client Side — Graph Engine

### 4.1 Data Structures (JS)

```js
// Node types
{ id: string, owner: string, rps: number, conns: Map }

// Edge types
{
  id: string,
  keyIdx: number, modelIdx: number,
  phase: 'request' | 'response',
  progress: number,
  inp: number, out: number, total: number,
  tps: number, // = total / (latency_ms / 1000)
  latencyMs: number,
  reqSpeed: number,  // normalized progress/frame from inp tokens
  resSpeed: number,  // normalized progress/frame from out tokens
  alive: boolean,
}
```

### 4.2 Event → State Mapping

Events arrive as tagged JSON from the Rust `#[serde(tag = "type")]` enum:

```json
{"type":"request","id":"abc","api_key":"sk-...","model":"gpt-4","method":"POST","path":"/v1/chat","owner":"Alice","timestamp":"2026-07-27T10:00:00+07:00"}
{"type":"request_end","id":"abc","status":200,"latency_ms":850,"input_tokens":120,"output_tokens":450,"cache_tokens":30}
```

```
On "request" (RequestStartEvent):
  → upsert key node (id = api_key)
  → upsert model node (id = model)
  → record event in RPS window for both nodes
  → create edge with phase='request', progress=0

On "request_end" (RequestEndEvent):
  → find edge by matching id
  → set edge.latencyMs, edge.inp, edge.out, edge.total
  → compute tps = total / (latencyMs / 1000)
  → set reqSpeed = normalize(inp / (latencyMs / 1000))
  → set resSpeed = normalize(out / (latencyMs / 1000))
  → switch phase to 'response', reset progress to 0

Animation frame:
  for each alive edge:
    if phase='request': progress += reqSpeed
    if phase='response': progress += resSpeed
    if progress >= 1:
      if phase='request': switch to 'response'
      else: mark edge as dead (removed after 10-frame timeout)
```

### 4.3 Node Size = Requests/sec (RPS)

Computed client-side using rolling window:

```js
const RPS_WINDOW_MS = 2000; // 2 seconds

// On each RequestStartEvent:
recordRequest(apiKey, model);
  → push Date.now() into per-node queues
  → on each frame, filter queue to [now - 2s, now]
  → rps = queue.length / 2
```

Node radius formula:

```js
function nodeRadius(rps) {
  return Math.min(44, Math.max(16, 18 + rps * 6));
}
```

- Base 18px at 0 rps
- +6px per rps
- Caps at 44px max, 16px min

### 4.4 Edge Speed = Tokens/sec

Normalize tps to animation progress per frame (60 fps target):

```js
// Map tps to progress/frame (0.002 - 0.06)
// At 50000 tps → 0.06/frame → crosses screen in ~16 frames (267ms)
// At 1000 tps → 0.02/frame → crosses screen in ~50 frames (833ms)
function tpsToSpeed(tps) {
  return Math.min(0.06, Math.max(0.002, tps / 50000));
}
```

Edge visual properties:

```js
function getSpeedColor(tps, alpha) {
  if (tps > 500) return `rgba(102, 187, 106, ${alpha})`; // green
  if (tps > 50)  return `rgba(255, 183, 77, ${alpha})`;  // orange
  return `rgba(244, 67, 54, ${alpha})`;                    // red
}

function getWidth(tps) {
  if (tps > 500) return 3.5;
  if (tps > 50)  return 2.5;
  return 1.5;
}
```

| t/s range | Color | Width | Description |
|-----------|-------|-------|-------------|
| >500      | Green | 3.5px | Fast streaming (gemini-flash, gpt-4o-mini) |
| 50–500    | Orange | 2.5px | Normal throughput (gpt-4o) |
| <50       | Red | 1.5px | Slow/latency (claude-sonnet thinking) |

Edge dash animation: dashed line with `lineDashOffset -= frame * 2` creates moving dash effect. Request phase dashes move left-to-right; response phase dashes move right-to-left.

### 4.5 Node Layout

```
┌──────────────────────────────────────────────────────┐
│  Header: title, LIVE badge, K/M/E/tps/rps counters   │
├──────────────────────────────────────────────────────┤
│                                                       │
│   🔑Alice ────────────────► gpt-4o              🟦   │
│   (big)  ◄────────────────       (big)                │
│                                                       │
│   🔑Bob   ──────► claude-sonnet                  🟦   │
│   (small)                    (small)                  │
│                                                       │
│   🔑Charlie ───────────────► gemini-flash         🟦   │
│   (medium) ◄───────────────       (big)               │
│                                                       │
│   🔑Diana  ──────► gpt-4o-mini                    🟦   │
│   (medium)                    (medium)                 │
│                                                       │
│                                                       │
│  Legend                            Status              │
└──────────────────────────────────────────────────────┘
```

- Key nodes on the left (column 1): X = `PAD_LEFT` (130px)
- Model nodes on the right (column 2): X = `canvas.width - PAD_RIGHT` (160px from right)
- Y positions evenly distributed: `PAD_TOP + (canvas.height - PAD_TOP - PAD_BOTTOM) * (i + 0.5) / count`
- PAD_TOP = 90px (below header), PAD_BOTTOM = 80px (above legend)
- Nodes never overlap because they're in fixed columns; if count exceeds 10, consider vertical scroll or canvas shrink

### 4.6 Node Rendering

Circle with radial gradient for 3D effect:

**Key nodes** (orange gradient):
```js
const grad = ctx.createRadialGradient(x-5, y-5, 0, x, y, radius);
grad.addColorStop(0, '#ffb74d');   // light orange center
grad.addColorStop(1, '#e65100');   // dark orange edge
```

**Model nodes** (blue gradient):
```js
const grad = ctx.createRadialGradient(x-5, y-5, 0, x, y, radius);
grad.addColorStop(0, '#81d4fa');   // light blue center
grad.addColorStop(1, '#1565c0');   // dark blue edge
```

**Glow**: when node has active connections, add `ctx.shadowBlur = 18` with matching shadow color (orange for keys, blue for models). Inactive nodes get `shadowBlur = Math.min(8, radius * 0.3)`.

**Labels**: to the right of model nodes (left-aligned), to the left of key nodes (right-aligned). Font: 11px monospace for ID, 10px sans-serif for owner. Offset by `radius + 6px`.

**RPS badge**: below each node when rps > 0.5, centered: `Math.round(rps * 10) / 10 + ' r/s'` in bold 10px monospace.

### 4.7 Header Stats

```html
<span>🔑 <strong>6</strong> keys</span>         — unique API keys seen in last 2s
<span>📦 <strong>4</strong> models</span>        — unique models seen in last 2s
<span>🔗 <strong>8</strong> active</span>        — connections currently animating
<span>⚡ <strong>450</strong> t/s avg</span>      — average tokens/sec across active connections
<span>📊 <strong>3.2</strong> req/s total</span>  — sum of all node rps
```

All counters update every frame (60fps). Values are computed from the same rolling window (2-second) and active edges list.

### 4.8 Edge Removal / Idle Timeout

- Completed edges (response dot reached model node) stay dead for 10 animation frames (~167ms) then removed from array.
- No "keep-alive" edge concept needed — HTTP requests are request/response. If real keep-alive exists in future (e.g., streaming), edge would stay in `response` phase with `progress` oscillating or held below 1.

### 4.9 Error / Empty States

| State | Behavior |
|-------|----------|
| WS connecting | Canvas shows dim nodes, no edges. Header shows `connecting...` if implemented |
| WS disconnected after connected | Existing edges frozen (no animation), nodes dim. Reconnect every 2s |
| No events for 60s | Canvas shows empty graph (just title + legend). "No active connections" message overlaid |
| Single key, single model | Both nodes render at base size (18px). No edges |
| 0 keys, 0 models | Blank canvas with centered "Waiting for connections..." |
| Browser resize | `window.requestAnimationFrame` loop handles this — layout recalculates every frame |
| >100 keys or models | Layout compresses. When node count > 15, text labels may overlap. `ponytail:` — add scroll container or zoom when needed |

### 4.10 Performance

- Canvas, not DOM — single render surface, no style recalculations
- `requestAnimationFrame` loop throttles to display refresh rate
- Target: 2000 edges + 200 nodes at 30fps minimum
- Edge array capped at 1000 (oldest removed beyond that)
- RPS window trimming runs every frame via `filter` over timestamps arrays

### 4.11 Canvas Sizing

Full viewport: `canvas.width = window.innerWidth`, `canvas.height = window.innerHeight` on init and resize.

### 4.12 Legend

Fixed bottom-left, always visible:

```html
🟠 API Key
🟦 Model
◻ Node size = req/s
── High t/s (>500)
── Medium (50–500)
── Low (<50)
```

### 4.13 WS Reconnection

```js
function connect() {
  const ws = new WebSocket('ws://' + location.host + '/_ws_graph');
  ws.onclose = () => setTimeout(connect, 2000);
  ws.onmessage = handleEvent;
}
```

No exponential backoff — 2s fixed interval is fine because reconnection just clears stale edges.

## 5. Files Changed / Created

| File | Action | Description |
|------|--------|-------------|
| `src/graph.rs` | **Create** | ~20 lines: `graph_page_handler`, `graph_ws_handler`, `handle_socket` |
| `src/graph.html` | **Create** | ~400 lines: canvas, CSS, JS engine |
| `src/main.rs` | **Edit** | Add `mod graph;`, add 2 routes to Router |

## 6. Test Plan

| Topic | Test | Type |
|-------|------|------|
| WS handler | `test_graph_ws_filters_rate_limit_events` — connect WS, send a `RateLimit` event, assert it is NOT forwarded | Integration |
| WS handler | `test_graph_ws_forwards_request_events` — connect WS, send `RequestStartEvent`, assert client receives it | Integration |
| HTML page | `test_graph_page_returns_html` — hit `/graph`, assert 200 and `text/html` | Integration via axum test |

Note: The JS canvas logic (node sizing, edge speed, color mapping, RPS tracking, layout math) is all client-side and not tested by Rust. `ponytail:` — add Playwright or headless browser test when/if graph logic becomes complex enough. For now, manual verification in browser suffices.

## 7. Edge Cases

- **Unknown model**: If model field is empty or `*` (from extract_model fallback), render as "unknown" node. Still show edges.
- **Concurrent requests to same key+model**: Multiple edges between same two nodes — allowed and expected. Edge IDs are unique per request.
- **Node timeout**: Nodes persist as long as they have rps > 0 in the 2s window. Once a key/model stops receiving requests, its rps decays to 0 and its label + RPS badge fades. Node is removed after 30s of zero rps.
- **Very fast request**: If latency < 100ms, the edge's request phase may complete before a single frame renders it. In practice, the dot snaps across and the response phase starts immediately — user sees a brief green flicker. Acceptable.
- **WebSocket full channel**: broadcast channel 256 capacity. If full, oldest event dropped. Graph may miss a request. No crash — just a brief gap. The rolling RPS window smooths this out.

## 8. Dependencies

None. Canvas is native browser API. No D3.js, no Three.js, no vis.js.
