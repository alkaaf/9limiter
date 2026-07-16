# 9limiter — Design Spec

Standalone HTTP reverse proxy for sliding-window RPM rate limiting of OpenAI/Anthropic API requests. In-memory, no DB, YAML config with hot-reload.

## Architecture

```
Client → 9limiter:${PORT} → upstream-llm (OpenAI / Anthropic / …)
```

1. Accept HTTP request
2. Extract `Authorization: Bearer <key>` + parse body for `model`
3. Look up key's ruleset (or fallback). Find matching rules (model, day, time window)
4. All matching rules' sliding windows checked. Any over limit → 429
5. Under → proxy request to upstream, stream response back

No request body modification. API key passed through unchanged.

## Config

```yaml
listen: ":8080"

upstreams:
  - path_prefix: "/v1/chat/completions"
    base_url: "https://api.openai.com"
  - path_prefix: "/v1/messages"
    base_url: "https://api.anthropic.com"

fallback_ruleset: default

rulesets:
  - name: default
    rules:
      - model: "*"
        limit: 10
        window_secs: 3600
        time_start: "00:00"
        time_end: "23:59"
        days: [Mon, Tue, Wed, Thu, Fri, Sat, Sun]

  - name: premium
    rules:
      - model: "*"
        limit: 500
        window_secs: 3600
        time_start: "07:00"
        time_end: "22:00"
        days: [Mon, Tue, Wed, Thu, Fri]

api_keys:
  - ruleset: premium
    keys:
      - "sk-abc-123"
      - "sk-def-456"
```

## Components

### Config
- Parse YAML file, validate at startup & on hot-reload
- Error at startup → exit. Error on hot-reload → log + keep old config
- Validation: time format, day names, limit/window > 0, ruleset references exist, overlap warning

### Rate Limiter (in-memory)
- Per (api_key, model, rule): `VecDeque<Instant>` of recent request timestamps
- On check: pop expired (< now - window_secs), count remaining, if ≥ limit → deny
- `Arc<Mutex<HashMap<Key, VecDeque>>>` — shared across threads

### Proxy
- Match request path against upstreams by `path_prefix` (longest match wins)
- No match → 404 Not Found
- Forward request via `reqwest` with streaming body
- Return upstream response as-is (status, headers, body)

### CLI overrides config
- `--listen` flag overrides `listen:` field in YAML

### Hot Reload
- `notify` file watcher on config file
- On change: re-parse, validate. Pass → atomically swap `Arc<Config>`. Fail → log, retain old.

## Decisions / Non-goals

| Decision | Rationale |
|----------|-----------|
| No DB, no Redis | "by runtime" — counter reset on restart |
| RPM only, no TPM | Token counting adds complexity, not requested |
| No auth validation | Every key considered legitimate |
| YAGNI: admin UI, caching, request modification | Add when requested |

## Web UI Dashboard

Embedded live dashboard served at `/dashboard` (and root `/`). Vanilla HTML/CSS/JS — no build, no CDN, no framework. Embedded via `include_str!` in the binary.

### Architecture

```
Rate limit check ──┐
Request start ─────┤──▶ broadcast::Sender ──▶ WS /_ws
Request end ───────┘                              │
                                          Dashboard JS (WebSocket client)
```

### Broadcast Protocol (JSON over WebSocket `/ws`)

```json
{"type":"rate_limit","data":{"api_key":"sk-...","model":"gpt-4",
 "rule":{"limit":100,"window_secs":3600},"count":45,"remaining":55,
 "reset_after_secs":2340}}

{"type":"request","data":{"id":"uuid","api_key":"sk-...",
 "model":"gpt-4","method":"POST","path":"/v1/chat/completions",
 "status":null,"latency_ms":null}}
// status + latency populated on response. In-flight row is dimmed.
```

### Dashboard Layout

Two-panel:
- **Left panel** — Rate limit cards per (api_key, rule). Progress bar with color (green → orange → red), count/limit, remaining, ruleset badge
- **Right panel** — Live request log table. Columns: Time, Key (truncated), Model, Status, Latency. Newest first. In-flight rows dimmed.

### Implementation
- `_ws` path → WebSocket upgrade, subscribes to broadcast channel
- `/dashboard` → HTML page served as static asset
- `/` → redirects to `/dashboard`
- Broadcast: `tokio::sync::broadcast` channel, capacity 256, drop-oldest on overflow

## Rate Limit Response

```json
HTTP 429
{"error": "rate_limit_exceeded", "message": "Rate limit exceeded for model gpt-4", "reset_after_secs": 1234}
```

## CLI

```
9limiter [--config path] [--listen addr] [--log-level info]
```

Defaults: `config.yaml`, `:8080`, `info`.

## Error Handling

| Condition | Behavior |
|-----------|----------|
| Key not found + no fallback | 401 Unauthorized |
| Body unparseable (no model) | Pass through |
| Upstream timeout/error | Return error code (502/504) |
| Upstream down | 502 |
| Hot-reload parse fail | Log, keep old config |
| Overlapping rules | Log warning at startup |
