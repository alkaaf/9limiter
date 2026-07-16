# 9limiter

Standalone Rust HTTP reverse proxy with rate limiting for OpenAI/Anthropic-compatible APIs.

## Features

- **Sliding window** — per-key, per-model VecDeque counters, no DB, no Redis
- **Per-key rulesets** — YAML config with hot-reload (notify debounced)
- **Multi-dimensional rules** — model match (string or array), time windows (HH:MM), day-of-week, overnight support
- **Upstream routing** — prefix-based, round-robin per group, prefix auto-strip
- **PostgreSQL key lookup** — resolve API key → owner name from 9router's DB (optional, fail-soft)
- **Timezone** — configurable fixed offset (`+07:00` default)
- **Live dashboard** — embedded WS-realtime HTML UI with rate limit cards, request table, log bar, server clock
- **All in-memory** — reset on restart, no external deps except optional PostgreSQL

## Usage

```bash
ninelimiter --config config.yaml
```

### Options

| Flag | Default | Description |
|------|---------|-------------|
| `--config` | `config.yaml` | Config file path |
| `--listen` | from config | Override listen address |
| `--log-level` | `info` | Tracing level |

## Config

```yaml
listen: "0.0.0.0:20127"
timezone: "+07:00"

upstreams:
  - path_prefix: "/v1"
    base_url: "http://localhost:20128/v1"

fallback_ruleset: default

database:                          # optional, PostgreSQL key→owner lookup
  host: "localhost"
  port: 5433
  user: "rahasia"
  password: "rahasia"
  dbname: "9router_test"

rulesets:
  - name: default
    rules:
      - model: ["zyr/gpt-5.5", "zyr/gpt-5.6-sol"]
        limit: 10
        window_secs: 3600
        time_start: "07:00"
        time_end: "17:30"
        days: [Mon, Tue, Wed, Thu, Fri, Sat, Sun]

api_keys:
  - ruleset: premium
    keys: ["sk-abc-123"]

rulesets:
  - name: default              # fallback for unlisted keys
    rules: [ ...
```

`model` accepts string or array. `time_start` > `time_end` = overnight window (22:00-07:00). Multiple rules AND'd, first limit hit returns 429.

## Dashboard

Open `http://<host>:` in browser. WebSocket realtime:

- **Rate Limit Status** — per-key+model cards, live bar, owner name
- **Live Requests** — streaming request table with status/latency
- **Logs** — server log bar at bottom

## Build

```bash
cargo build --release
```
