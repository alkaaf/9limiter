# 9limiter Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a standalone Rust HTTP reverse proxy that rate-limits OpenAI/Anthropic API requests using configurable rulesets with sliding windows, hot-reloadable YAML config, and an embedded live dashboard.

**Architecture:** Axum server with three independent subsystems — proxy (forward requests), rate limiter (sliding-window VecDeque per key+model+rule), dashboard (vanilla HTML/JS served via HTTP, live updates via WebSocket over a shared broadcast channel). Config drives all behavior; config reload atomsically swaps `Arc<Config>`.

**Tech Stack:** Rust, Axum, Reqwest, `tokio::sync::broadcast`, `serde_yaml`, `notify`, `clap`, vanilla HTML/CSS/JS (zero framework, embedded via `include_str!`).

## Global Constraints

- Single self-contained binary, no DB, no Redis, no external deps beyond crates
- All state in-memory, resets on restart
- Web UI: vanilla HTML/CSS/JS only — no build tool, no framework, no CDN
- Config hot-reload via `notify` file watcher, atomic Arc swap
- Config validation at startup (fail hard) and hot-reload (fail soft, log + keep old)
- Rate limit check is AND across all matching rules — first hit = 429
- Unknown API keys use `fallback_ruleset` if configured, else 401
- CLI overrides YAML for `--listen`

---
### Task 1: Project scaffold, Cargo.toml, and main entrypoint

**Files:**
- Create: `Cargo.toml`
- Create: `src/main.rs`

**Interfaces:**
- Consumes: nothing
- Produces: binary skeleton with CLI parsing

- [ ] **Step 1: Create Cargo.toml**

```toml
[package]
name = "9limiter"
version = "0.1.0"
edition = "2021"

[dependencies]
axum = { version = "0.8", features = ["ws"] }
axum-extra = "0.10"
tokio = { version = "1", features = ["full"] }
tower = "0.5"
tower-http = { version = "0.6", features = ["cors", "set-header"] }
reqwest = { version = "0.12", features = ["stream", "json"] }
serde = { version = "1", features = ["derive"] }
serde_yaml = "0.9"
tracing = "0.1"
tracing-subscriber = { version = "0.3", features = ["env-filter", "json"] }
clap = { version = "4", features = ["derive"] }
notify = { version = "7", features = ["macos_kqueue"] }
uuid = { version = "1", features = ["v4"] }
chrono = { version = "0.4", features = ["serde"] }
futures-util = "0.3"
```

- [ ] **Step 2: Create src/main.rs with CLI + server skeleton**

```rust
use clap::Parser;

#[derive(Parser)]
struct Args {
    #[arg(long, default_value = "config.yaml")]
    config: String,
    #[arg(long)]
    listen: Option<String>,
    #[arg(long, default_value = "info")]
    log_level: String,
}

#[tokio::main]
async fn main() {
    let args = Args::parse();
    tracing_subscriber::fmt()
        .with_env_filter(&args.log_level)
        .init();

    tracing::info!("starting 9limiter");
    // placeholder — tasks will wire components here

    tokio::signal::ctrl_c().await.unwrap();
    tracing::info!("shutdown complete");
}
```

- [ ] **Step 3: Verify it compiles and runs**

```
cd /home/alkaaf/project/9limiter && cargo check
```

Expected: compiles without error.

```bash
cd /home/alkaaf/project/9limiter && cargo run -- --help
```

Expected: prints CLI help text.

- [ ] **Step 4: Commit**

```bash
git add Cargo.toml src/main.rs
git commit -m "chore: scaffold rust project"
```

---
### Task 2: Config types + YAML parsing + validation

**Files:**
- Create: `src/config.rs`

**Interfaces:**
- Consumes: nothing
- Produces: `Config`, `Ruleset`, `Rule`, `ApiKeyEntry`, `UpstreamEntry` structs + `Config::from_file(path) -> Result<Config>` + `Config::validate(&self) -> Result<()>`

- [ ] **Step 1: Define config types**

```rust
use serde::Deserialize;
use std::collections::HashMap;

#[derive(Debug, Clone, Deserialize)]
pub struct Config {
    pub listen: Option<String>,
    pub upstreams: Vec<UpstreamEntry>,
    pub fallback_ruleset: Option<String>,
    pub rulesets: Vec<Ruleset>,
    pub api_keys: Vec<ApiKeyEntry>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct UpstreamEntry {
    pub path_prefix: String,
    pub base_url: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct Ruleset {
    pub name: String,
    pub rules: Vec<Rule>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct Rule {
    pub model: String,
    pub limit: u32,
    pub window_secs: u64,
    pub time_start: String,
    pub time_end: String,
    pub days: Vec<String>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ApiKeyEntry {
    pub ruleset: String,
    pub keys: Vec<String>,
}
```

- [ ] **Step 2: Implement Config::from_file and validate**

```rust
impl Config {
    pub fn from_file(path: &str) -> Result<Self, Box<dyn std::error::Error>> {
        let content = std::fs::read_to_string(path)?;
        let config: Config = serde_yaml::from_str(&content)?;
        config.validate()?;
        Ok(config)
    }

    pub fn validate(&self) -> Result<(), Box<dyn std::error::Error>> {
        // Build set of valid ruleset names
        let ruleset_names: std::collections::HashSet<&str> =
            self.rulesets.iter().map(|r| r.name.as_str()).collect();

        // Validate fallback_ruleset exists if set
        if let Some(ref fallback) = self.fallback_ruleset {
            if !ruleset_names.contains(fallback.as_str()) {
                return Err(format!("fallback_ruleset '{}' not found in rulesets", fallback).into());
            }
        }

        // Validate each api_key entry references existing ruleset
        for entry in &self.api_keys {
            if !ruleset_names.contains(entry.ruleset.as_str()) {
                return Err(format!("api_key ruleset '{}' not found", entry.ruleset).into());
            }
        }

        // Validate each rule
        for rs in &self.rulesets {
            for rule in &rs.rules {
                if rule.limit == 0 {
                    return Err(format!("ruleset '{}': limit must be > 0", rs.name).into());
                }
                if rule.window_secs == 0 {
                    return Err(format!("ruleset '{}': window_secs must be > 0", rs.name).into());
                }
                // Validate time format HH:MM
                for time in [&rule.time_start, &rule.time_end] {
                    let parts: Vec<&str> = time.split(':').collect();
                    if parts.len() != 2 {
                        return Err(format!("ruleset '{}': invalid time format '{}'", rs.name, time).into());
                    }
                    let h: u8 = parts[0].parse().map_err(|_| format!("invalid hour '{}'", parts[0]))?;
                    let m: u8 = parts[1].parse().map_err(|_| format!("invalid minute '{}'", parts[1]))?;
                    if h > 23 || m > 59 {
                        return Err(format!("ruleset '{}': time '{}' out of range", rs.name, time).into());
                    }
                }
                // Validate time_start < time_end
                if rule.time_start >= rule.time_end {
                    return Err(format!("ruleset '{}': time_start must be < time_end", rs.name).into());
                }
                // Validate days
                let valid_days = ["Mon","Tue","Wed","Thu","Fri","Sat","Sun"];
                for d in &rule.days {
                    if !valid_days.contains(&d.as_str()) {
                        return Err(format!("ruleset '{}': invalid day '{}'", rs.name, d).into());
                    }
                }
            }

            // Warn about overlapping rules within same ruleset
            // (same model + overlapping days + overlapping time)
            for i in 0..rs.rules.len() {
                for j in (i + 1)..rs.rules.len() {
                    if rules_overlap(&rs.rules[i], &rs.rules[j]) {
                        tracing::warn!(
                            "ruleset '{}': rules {} and {} may overlap",
                            rs.name, i, j
                        );
                    }
                }
            }
        }

        Ok(())
    }
}

fn rules_overlap(a: &Rule, b: &Rule) -> bool {
    // Models match if same string or either is "*"
    let model_match = a.model == "*" || b.model == "*" || a.model == b.model;
    if !model_match { return false; }

    // Day overlap
    let days_a: std::collections::HashSet<&str> = a.days.iter().map(|s| s.as_str()).collect();
    let days_b: std::collections::HashSet<&str> = b.days.iter().map(|s| s.as_str()).collect();
    if days_a.intersection(&days_b).next().is_none() { return false; }

    // Time overlap: two ranges [s1,e1) [s2,e2) overlap if s1 < e2 && s2 < e1
    a.time_start < b.time_end && b.time_start < a.time_end
}
```

- [ ] **Step 3: Add `mod config;` to main.rs and test parsing**

```bash
cd /home/alkaaf/project/9limiter && cargo check
```

Expected: compiles.

- [ ] **Step 4: Write a test config.yaml**

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

Write the test config to `config.yaml` in the project root.

- [ ] **Step 5: Write a quick validation test in main.rs**

```rust
#[test]
fn test_parse_config() {
    let cfg = config::Config::from_file("config.yaml").unwrap();
    assert_eq!(cfg.rulesets.len(), 2);
    assert_eq!(cfg.api_keys.len(), 1);
    assert_eq!(cfg.api_keys[0].keys.len(), 2);
}
```

```bash
cd /home/alkaaf/project/9limiter && cargo test
```

Expected: test passes.

- [ ] **Step 6: Commit**

```bash
git add src/config.rs config.yaml
git add -u
git commit -m "feat: config parsing and validation"
```

---
### Task 3: Rate limiter core (SlidingWindow)

**Files:**
- Create: `src/limiter.rs`

**Interfaces:**
- Consumes: `Config`, `Rule`
- Produces: `SlidingWindow::check(key, rule) -> Result<(), OverLimit>` — thread-safe, shared via Arc
- Uses `tokio::sync::broadcast::Sender` to emit rate_limit events

- [ ] **Step 1: Define RateLimitEvent + limiter state**

```rust
use std::collections::HashMap;
use std::time::Instant;
use tokio::sync::broadcast;
use std::collections::VecDeque;
use std::sync::{Arc, Mutex};

#[derive(Debug, Clone)]
pub struct RateLimitEvent {
    pub api_key: String,
    pub model: String,
    pub rule_limit: u32,
    pub rule_window_secs: u64,
    pub count: usize,
    pub remaining: u32,
    pub reset_after_secs: u64,
}

#[derive(Clone)]
pub struct SlidingLimiter {
    state: Arc<Mutex<HashMap<String, VecDeque<Instant>>>>,
    tx: broadcast::Sender<RateLimitEvent>,
}

impl SlidingLimiter {
    pub fn new(tx: broadcast::Sender<RateLimitEvent>) -> Self {
        Self {
            state: Arc::new(Mutex::new(HashMap::new())),
            tx,
        }
    }
}
```

- [ ] **Step 2: Implement check method**

```rust
impl SlidingLimiter {
    pub fn check(&self, api_key: &str, model: &str, limit: u32, window_secs: u64) -> bool {
        let key = format!("{}:{}:{}:{}", api_key, model, limit, window_secs);
        let now = Instant::now();
        let window = std::time::Duration::from_secs(window_secs);

        let mut state = self.state.lock().unwrap();
        let deque = state.entry(key.clone()).or_insert_with(VecDeque::new);

        // Pop expired timestamps
        while let Some(&t) = deque.front() {
            if now.duration_since(t) >= window {
                deque.pop_front();
            } else {
                break;
            }
        }

        let count = deque.len();
        let passed = count < limit as usize;

        if passed {
            deque.push_back(now);
        }

        // Calculate reset time from oldest entry
        let reset_after_secs = deque.front()
            .map(|oldest| {
                let elapsed = now.duration_since(*oldest).as_secs();
                window_secs.saturating_sub(elapsed)
            })
            .unwrap_or(0);

        let remaining = limit.saturating_sub(count as u32);

        // Broadcast event
        let _ = self.tx.send(RateLimitEvent {
            api_key: api_key.to_string(),
            model: model.to_string(),
            rule_limit: limit,
            rule_window_secs: window_secs,
            count,
            remaining,
            reset_after_secs,
        });

        passed
    }
}
```

- [ ] **Step 3: Add `mod limiter;` to main.rs, compile**

```bash
cd /home/alkaaf/project/9limiter && cargo check
```

Expected: compiles.

- [ ] **Step 4: Write a test**

```rust
#[test]
fn test_sliding_limiter() {
    let (tx, _rx) = tokio::sync::broadcast::channel(16);
    let limiter = limiter::SlidingLimiter::new(tx);

    // First 3 should pass (limit=3)
    assert!(limiter.check("key1", "gpt-4", 3, 60));
    assert!(limiter.check("key1", "gpt-4", 3, 60));
    assert!(limiter.check("key1", "gpt-4", 3, 60));
    // 4th should fail
    assert!(!limiter.check("key1", "gpt-4", 3, 60));

    // Different key should pass
    assert!(limiter.check("key2", "gpt-4", 3, 60));
}
```

```bash
cd /home/alkaaf/project/9limiter && cargo test
```

Expected: passes.

- [ ] **Step 5: Commit**

```bash
git add src/limiter.rs
git add -u
git commit -m "feat: sliding window rate limiter"
```

---
### Task 4: Broadcast events + request tracking

**Files:**
- Create: `src/events.rs`

**Interfaces:**
- Consumes: nothing
- Produces: `AppEvent` enum + `RequestStartEvent`, `RequestEndEvent` + shared broadcast channels

- [ ] **Step 1: Define event types**

```rust
use serde::Serialize;

#[derive(Debug, Clone, Serialize)]
pub struct RequestStartEvent {
    pub id: String,
    pub api_key: String,
    pub model: String,
    pub method: String,
    pub path: String,
    pub timestamp: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct RequestEndEvent {
    pub id: String,
    pub status: u16,
    pub latency_ms: u64,
}

#[derive(Debug, Clone, Serialize)]
#[serde(tag = "type")]
pub enum AppEvent {
    #[serde(rename = "rate_limit")]
    RateLimit(limiter::RateLimitEvent),
    #[serde(rename = "request")]
    Request(RequestStartEvent),
    #[serde(rename = "request_end")]
    RequestEnd(RequestEndEvent),
}
```

- [ ] **Step 2: Add `mod events;` to main.rs, compile**

```bash
cd /home/alkaaf/project/9limiter && cargo check
```

Expected: compiles.

- [ ] **Step 3: Commit**

```bash
git add src/events.rs
git add -u
git commit -m "feat: event types for broadcast"
```

---
### Task 5: HTTP proxy forwarding

**Files:**
- Create: `src/proxy.rs`

**Interfaces:**
- Consumes: `<ReqBody>` from Axum, upstream config, `SlidingLimiter`, `broadcast::Sender<AppEvent>`
- Produces: Axum handler that checks rate limit → proxies to upstream
- Signature: `async fn proxy_handler(axum::extract::State<AppState>, req: axum::http::Request<Body>) -> Response`

- [ ] **Step 1: Define proxy handler**

```rust
use axum::{
    body::Body,
    extract::{State, Path},
    http::{Request, StatusCode, HeaderMap, header},
    response::{Response, IntoResponse},
};
use std::sync::Arc;
use crate::{config::Config, limiter::SlidingLimiter, events::{AppEvent, RequestStartEvent, RequestEndEvent}};

#[derive(Clone)]
pub struct AppState {
    pub config: Arc<tokio::sync::RwLock<Config>>,
    pub limiter: SlidingLimiter,
    pub event_tx: tokio::sync::broadcast::Sender<AppEvent>,
}

pub async fn proxy_handler(
    State(state): State<AppState>,
    req: Request<Body>,
) -> Response {
    // Extract API key from Authorization header
    let api_key = req.headers()
        .get(header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.strip_prefix("Bearer "))
        .unwrap_or("")
        .to_string();

    if api_key.is_empty() {
        return (StatusCode::UNAUTHORIZED, "missing Authorization header").into_response();
    }

    // Read body for model extraction
    let (parts, body) = req.into_parts();
    let bytes = match axum::body::to_bytes(body, 1024 * 1024).await {
        Ok(b) => b,
        Err(_) => return (StatusCode::BAD_REQUEST, "failed to read body").into_response(),
    };

    // Extract model from JSON body
    let model = extract_model(&bytes).unwrap_or("*");

    let config = state.config.read().await;
    let ruleset_name = lookup_ruleset(&config, &api_key);

    let ruleset_name = match ruleset_name {
        Some(name) => name,
        None => return (StatusCode::UNAUTHORIZED, "unauthorized").into_response(),
    };

    // Find matching rules
    let now = chrono::Local::now();
    let day_name = now.format("%a").to_string();
    let time_str = now.format("%H:%M").to_string();

    let matching_rules: Vec<_> = config.rulesets.iter()
        .find(|rs| rs.name == ruleset_name)
        .map(|rs| rs.rules.iter().filter(|rule| {
            rule_matches(rule, &model, &day_name, &time_str)
        }).collect())
        .unwrap_or_default();

    // Check all matching rules
    for rule in &matching_rules {
        if !state.limiter.check(&api_key, &model, rule.limit, rule.window_secs) {
            let reset = rule.window_secs; // approximate
            let body = serde_json::json!({
                "error": "rate_limit_exceeded",
                "message": format!("Rate limit exceeded for model {}", model),
                "reset_after_secs": reset,
            });
            return (StatusCode::TOO_MANY_REQUESTS, serde_json::to_string(&body).unwrap()).into_response();
        }
    }

    // Find matching upstream
    let upstream = config.upstreams.iter()
        .max_by_key(|u| u.path_prefix.len())
        .find(|u| parts.uri.path().starts_with(&u.path_prefix));

    let upstream = match upstream {
        Some(u) => u,
        None => return (StatusCode::NOT_FOUND, "no matching upstream").into_response(),
    };

    drop(config);
    // const request_start
    // Then proxy
    proxy_to_upstream(&state, parts, bytes, upstream, &api_key, &model).await
}
```

- [ ] **Step 2: Implement rule_matches helper**

```rust
fn rule_matches(rule: &crate::config::Rule, model: &str, day: &str, time: &str) -> bool {
    // Model match
    if rule.model != "*" && rule.model != model {
        return false;
    }
    // Day match
    if !rule.days.iter().any(|d| d == day) {
        return false;
    }
    // Time match
    time >= &rule.time_start && time < &rule.time_end
}

fn lookup_ruleset<'a>(config: &'a crate::config::Config, api_key: &str) -> Option<&'a str> {
    for entry in &config.api_keys {
        if entry.keys.iter().any(|k| k == api_key) {
            return Some(&entry.ruleset);
        }
    }
    config.fallback_ruleset.as_deref()
}

fn extract_model(body: &[u8]) -> Option<String> {
    let v: serde_json::Value = serde_json::from_slice(body).ok()?;
    v.get("model").and_then(|m| m.as_str()).map(|s| s.to_string())
}
```

- [ ] **Step 3: Implement proxy_to_upstream**

```rust
async fn proxy_to_upstream(
    state: &AppState,
    parts: axum::http::request::Parts,
    body_bytes: bytes::Bytes,
    upstream: &crate::config::UpstreamEntry,
    api_key: &str,
    model: &str,
) -> Response {
    let req_id = uuid::Uuid::new_v4().to_string();
    let start = std::time::Instant::now();

    // Emit request start event
    let _ = state.event_tx.send(AppEvent::Request(RequestStartEvent {
        id: req_id.clone(),
        api_key: api_key.to_string(),
        model: model.to_string(),
        method: parts.method.to_string(),
        path: parts.uri.path_and_query()
            .map(|pq| pq.to_string())
            .unwrap_or_default(),
        timestamp: chrono::Local::now().to_rfc3339(),
    }));

    // Build upstream URL
    let path = parts.uri.path_and_query()
        .map(|pq| pq.as_str())
        .unwrap_or("");
    let upstream_url = format!("{}{}", upstream.base_url.trim_end_matches('/'), path);

    // Build reqwest request
    let client = reqwest::Client::new();
    let mut upstream_req = client.request(parts.method.clone(), &upstream_url)
        .body(body_bytes);

    // Forward headers (skip host, keep auth)
    for (name, value) in &parts.headers {
        if name != header::HOST {
            upstream_req = upstream_req.header(name, value);
        }
    }

    match upstream_req.send().await {
        Ok(resp) => {
            let status = resp.status().as_u16();
            let latency = start.elapsed().as_millis() as u64;

            // Emit request end event
            let _ = state.event_tx.send(AppEvent::RequestEnd(RequestEndEvent {
                id: req_id,
                status,
                latency_ms: latency,
            }));

            // Build response with streaming body
            let body = axum::body::Body::from_stream(resp.bytes_stream());
            let mut response = Response::new(body);
            *response.status_mut() = axum::http::StatusCode::from_u16(status).unwrap();
            response.headers_mut().insert(
                header::CONTENT_TYPE,
                resp.headers().get(header::CONTENT_TYPE)
                    .cloned()
                    .unwrap_or_else(|| header::HeaderValue::from_static("application/json")),
            );
            response
        }
        Err(e) => {
            tracing::error!("upstream error: {}", e);
            let status = if e.is_timeout() { 504 } else { 502 };
            let _ = state.event_tx.send(AppEvent::RequestEnd(RequestEndEvent {
                id: req_id,
                status,
                latency_ms: start.elapsed().as_millis() as u64,
            }));
            (StatusCode::from_u16(status).unwrap(), "upstream error").into_response()
        }
    }
}
```

- [ ] **Step 4: Add `mod proxy;` to main.rs, compile**

```bash
cd /home/alkaaf/project/9limiter && cargo check
```

Expected: compiles.

- [ ] **Step 5: Commit**

```bash
git add src/proxy.rs
git add -u
git commit -m "feat: HTTP proxy with rate limit check"
```

---
### Task 6: Dashboard HTML + WebSocket

**Files:**
- Create: `src/dashboard.rs`
- Create: `src/dashboard_html.rs` (or embed .html directly)

**Interfaces:**
- Consumes: `broadcast::Receiver<AppEvent>`
- Produces: Axum handler for `/dashboard` (HTML) and `/_ws` (WebSocket)

- [ ] **Step 1: Create the dashboard HTML**

Create `src/dashboard.html`:

```html
<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>9limiter — Dashboard</title>
<style>
  *{box-sizing:border-box;margin:0;padding:0}
  body{font-family:-apple-system,BlinkMacSystemFont,sans-serif;background:#0f0f1a;color:#e0e0e0;height:100vh;display:flex;flex-direction:column}
  header{background:#1a1a2e;padding:12px 24px;display:flex;justify-content:space-between;align-items:center;border-bottom:1px solid #2a2a4a}
  header h1{font-size:18px;font-weight:600}
  .live-indicator{display:flex;align-items:center;gap:8px;font-size:13px;color:#666}
  .live-indicator .dot{width:8px;height:8px;border-radius:50%;background:#4caf50}
  .live-indicator .dot.disconnected{background:#f44336}
  .dashboard{display:flex;flex:1;overflow:hidden}
  .panel{flex:1;display:flex;flex-direction:column;overflow:hidden}
  .panel-left{border-right:1px solid #2a2a4a}
  .panel h2{font-size:13px;text-transform:uppercase;color:#888;padding:12px 16px;border-bottom:1px solid #2a2a4a;letter-spacing:0.5px}
  .scroll{flex:1;overflow-y:auto;padding:12px}
  .key-card{background:#1a1a2e;border-radius:8px;padding:14px;margin-bottom:10px;border:1px solid #2a2a4a}
  .key-card .header{display:flex;justify-content:space-between;align-items:center;margin-bottom:6px}
  .key-card .api-key{font-family:monospace;font-size:13px}
  .key-card .ruleset{font-size:11px;padding:2px 8px;border-radius:10px;background:#2a2a4a;color:#aaa}
  .key-card .ruleset.premium{background:#1a3a2e;color:#4caf50}
  .key-card .meta{font-size:11px;color:#888;margin-bottom:8px}
  .bar-track{height:8px;background:#2a2a4a;border-radius:4px;overflow:hidden;margin-bottom:4px}
  .bar-fill{height:100%;border-radius:4px;transition:width 0.3s,background 0.3s}
  .bar-fill.green{background:#4caf50}
  .bar-fill.orange{background:#ff9800}
  .bar-fill.red{background:#f44336}
  .stats{display:flex;justify-content:space-between;font-size:12px}
  .stats .count{font-family:monospace;font-weight:600}
  .stats .remaining{color:#888}
  table{width:100%;border-collapse:collapse;font-size:12px}
  th{text-align:left;padding:6px 8px;color:#888;font-weight:500;border-bottom:1px solid #2a2a4a;position:sticky;top:0;background:#0f0f1a}
  td{padding:6px 8px;border-bottom:1px solid #1a1a2e;font-family:monospace;font-size:11px}
  tr.in-flight{opacity:0.5}
  .status-200{color:#4caf50}
  .status-429{color:#f44336}
  .status-err{color:#ff9800}
  td .key-trunc{max-width:80px;overflow:hidden;text-overflow:ellipsis;display:inline-block;white-space:nowrap;vertical-align:middle}
  .rate-info{font-size:12px;color:#888;display:flex;gap:16px;padding:8px 16px;border-bottom:1px solid #2a2a4a}
  @media(max-width:768px){.dashboard{flex-direction:column}}
</style>
</head>
<body>
<header>
  <h1>9limiter</h1>
  <div class="live-indicator">
    <span class="dot" id="dot"></span>
    <span id="status-text">connecting...</span>
  </div>
</header>
<div class="rate-info">
  <span id="keys-count">0 keys</span>
  <span id="reqs-count">0 req/s</span>
</div>
<div class="dashboard">
  <div class="panel panel-left">
    <h2>Rate Limit Status</h2>
    <div class="scroll" id="rate-cards"></div>
  </div>
  <div class="panel panel-right">
    <h2>Live Requests</h2>
    <div class="scroll" id="log-table">
      <table><thead><tr>
        <th>Time</th><th>Key</th><th>Model</th><th>Status</th><th>Latency</th>
      </tr></thead><tbody id="log-body"></tbody></table>
    </div>
  </div>
</div>
<script>
const MAX_LOG = 200;
let logCount = 0;

function connect() {
  const proto = location.protocol === 'https:' ? 'wss:' : 'ws:';
  const ws = new WebSocket(`${proto}//${location.host}/_ws`);
  const dot = document.getElementById('dot');
  const statusText = document.getElementById('status-text');

  ws.onopen = () => { dot.className = 'dot'; statusText.textContent = 'connected'; };
  ws.onclose = () => { dot.className = 'dot disconnected'; statusText.textContent = 'disconnected'; setTimeout(connect, 2000); };
  ws.onerror = () => ws.close();

  ws.onmessage = (msg) => {
    try {
      const event = JSON.parse(msg.data);
      if (event.type === 'rate_limit') renderRateCard(event.data);
      else if (event.type === 'request') renderLogRow(event.data);
      else if (event.type === 'request_end') updateLogRow(event.data);
    } catch(e) {}
  };
}

function renderRateCard(d) {
  const container = document.getElementById('rate-cards');
  const cardId = `card-${btoa(d.api_key + d.model + d.rule_limit)}`;
  let card = document.getElementById(cardId);
  if (!card) {
    card = document.createElement('div');
    card.id = cardId;
    card.className = 'key-card';
    card.innerHTML = `<div class="header"><span class="api-key">${d.api_key}</span><span class="ruleset"></span></div>
      <div class="meta">Model: ${d.model} · window ${d.rule_window_secs}s</div>
      <div class="bar-track"><div class="bar-fill"></div></div>
      <div class="stats"><span class="count">0 / ${d.rule_limit}</span><span class="remaining">0 remaining</span></div>`;
    container.prepend(card);
  }
  const pct = d.count / d.rule_limit;
  const bar = card.querySelector('.bar-fill');
  bar.style.width = `${Math.min(pct * 100, 100)}%`;
  bar.className = `bar-fill ${pct < 0.7 ? 'green' : pct < 0.9 ? 'orange' : 'red'}`;
  card.querySelector('.count').textContent = `${d.count} / ${d.rule_limit}`;
  card.querySelector('.remaining').textContent = `${d.remaining} remaining`;
}

function renderLogRow(d) {
  const tbody = document.getElementById('log-body');
  const row = document.createElement('tr');
  row.id = `log-${d.id}`;
  row.className = 'in-flight';
  row.innerHTML = `<td>${new Date().toLocaleTimeString()}</td>
    <td><span class="key-trunc" title="${d.api_key}">${d.api_key.slice(0, 10)}…</span></td>
    <td>${d.model}</td>
    <td><span class="status-err">…</span></td>
    <td>—</td>`;
  tbody.prepend(row);
  logCount++;
  while (tbody.children.length > MAX_LOG) tbody.removeChild(tbody.lastChild);
  document.getElementById('reqs-count').textContent = `${Math.min(logCount, MAX_LOG)} requests`;
}

function updateLogRow(d) {
  const row = document.getElementById(`log-${d.id}`);
  if (!row) return;
  row.className = '';
  const statusClass = d.status === 200 ? 'status-200' : d.status === 429 ? 'status-429' : 'status-err';
  row.querySelector('td:nth-child(4)').innerHTML = `<span class="${statusClass}">${d.status}</span>`;
  row.querySelector('td:nth-child(5)').textContent = d.latency_ms ? `${(d.latency_ms / 1000).toFixed(1)}s` : '—';
}

connect();
</script>
</body>
</html>
```

- [ ] **Step 2: Create dashboard.rs handler**

```rust
use axum::{
    extract::{ws::{WebSocket, WebSocketUpgrade, Message}, State},
    response::{Html, IntoResponse},
};
use futures_util::{SinkExt, StreamExt};
use crate::events::AppEvent;
use crate::proxy::AppState;

pub async fn dashboard_handler() -> impl IntoResponse {
    Html(include_str!("dashboard.html"))
}

pub async fn ws_handler(
    ws: WebSocketUpgrade,
    State(state): State<AppState>,
) -> impl IntoResponse {
    ws.on_upgrade(move |socket| handle_socket(socket, state))
}

async fn handle_socket(socket: WebSocket, state: AppState) {
    let mut rx = state.event_tx.subscribe();
    let (mut sender, _receiver) = socket.split();

    while let Ok(event) = rx.recv().await {
        let json = serde_json::to_string(&event).unwrap_or_default();
        if sender.send(Message::Text(json.into())).await.is_err() {
            break;
        }
    }
}
```

- [ ] **Step 3: Wire into main.rs routing**

```rust
// In main() after building AppState:
let app = axum::Router::new()
    .route("/", axum::routing::get(dashboard::dashboard_handler))
    .route("/dashboard", axum::routing::get(dashboard::dashboard_handler))
    .route("/_ws", axum::routing::get(dashboard::ws_handler))
    // ... proxy catch-all route
    .with_state(state);
```

- [ ] **Step 4: Compile and verify**

```bash
cd /home/alkaaf/project/9limiter && cargo check
```

Expected: compiles.

- [ ] **Step 5: Commit**

```bash
git add src/dashboard.rs src/dashboard.html
git add -u
git commit -m "feat: embedded dashboard with WebSocket live updates"
```

---
### Task 7: Config hot-reload

**Files:**
- Modify: `src/main.rs` (add notify watcher + config reload loop)

- [ ] **Step 1: Add hot-reload loop to main.rs**

```rust
use notify::{Config as NotifyConfig, Event, EventKind, RecommendedWatcher, Watcher};
use std::sync::Arc;

async fn config_reload_loop(
    config_path: String,
    app_state: proxy::AppState,
) {
    let (tx, mut rx) = tokio::sync::mpsc::channel(1);
    let mut watcher = RecommendedWatcher::new(
        move |res: Result<Event, notify::Error>| {
            if let Ok(event) = res {
                if matches!(event.kind, EventKind::Modify(_)) {
                    let _ = tx.blocking_send(());
                }
            }
        },
        NotifyConfig::default(),
    )
    .expect("failed to create file watcher");

    if let Err(e) = watcher.watch(&config_path, notify::RecursiveMode::NonRecursive) {
        tracing::error!("failed to watch config: {}", e);
        return;
    }

    while rx.recv().await.is_some() {
        tracing::info!("config file changed, reloading...");
        match crate::config::Config::from_file(&config_path) {
            Ok(new_config) => {
                *app_state.config.write().await = new_config;
                tracing::info!("config reloaded successfully");
            }
            Err(e) => {
                tracing::error!("config reload failed (keeping old config): {}", e);
            }
        }
    }
}
```

- [ ] **Step 2: Wire hot-reload into main()**

```rust
let config = config::Config::from_file(&args.config)
    .expect("failed to parse config");

let config = Arc::new(tokio::sync::RwLock::new(config));
let (event_tx, _) = tokio::sync::broadcast::channel(256);
let limiter = limiter::SlidingLimiter::new(event_tx.clone());

let state = proxy::AppState {
    config: config.clone(),
    limiter,
    event_tx,
};

// Start hot-reload watcher
tokio::spawn(config_reload_loop(args.config.clone(), state.clone()));

let app = axum::Router::new()
    .route("/", axum::routing::get(dashboard::dashboard_handler))
    .route("/dashboard", axum::routing::get(dashboard::dashboard_handler))
    .route("/_ws", axum::routing::get(dashboard::ws_handler))
    .fallback(proxy::proxy_handler)
    .with_state(state);

let listen_addr = config.read().await.listen.clone()
    .or(args.listen)
    .unwrap_or_else(|| ":8080".to_string());

let listener = tokio::net::TcpListener::bind(&listen_addr).await.unwrap();
tracing::info!("listening on {}", listen_addr);
axum::serve(listener, app).await.unwrap();
```

- [ ] **Step 3: Compile and verify**

```bash
cd /home/alkaaf/project/9limiter && cargo check
```

Expected: compiles.

- [ ] **Step 4: Quick integration run**

```bash
cd /home/alkaaf/project/9limiter && cargo run &
sleep 2
curl -s http://localhost:8080/ | head -5
kill %1 2>/dev/null
```

Expected: dashboard HTML returned.

- [ ] **Step 5: Commit**

```bash
git add -A
git commit -m "feat: config hot-reload and full server wiring"
```

---
### Task 8: Integration self-check

**Files:**
- Modify: `src/main.rs` (add a test config + integration test)

- [ ] **Step 1: Write an integration test**

```rust
#[cfg(test)]
mod integration {
    use super::*;

    #[tokio::test]
    async fn test_full_config() {
        let cfg = config::Config::from_file("config.yaml").unwrap();
        assert_eq!(cfg.upstreams.len(), 2);
        assert!(cfg.upstreams.iter().any(|u| u.base_url.contains("openai")));
        assert!(cfg.upstreams.iter().any(|u| u.base_url.contains("anthropic")));
    }

    #[test]
    fn test_rule_matching() {
        // Create a rule for Mon-Fri 07:00-17:00
        let rule = config::Rule {
            model: "gpt-4".into(),
            limit: 100,
            window_secs: 3600,
            time_start: "07:00".into(),
            time_end: "17:00".into(),
            days: vec!["Mon".into(), "Tue".into(), "Wed".into(), "Thu".into(), "Fri".into()],
        };
        assert!(proxy::rule_matches(&rule, "gpt-4", "Mon", "10:00"));
        assert!(!proxy::rule_matches(&rule, "gpt-4", "Sat", "10:00"));
        assert!(!proxy::rule_matches(&rule, "gpt-4", "Mon", "18:00"));
        assert!(!proxy::rule_matches(&rule, "claude-3", "Mon", "10:00"));
    }
}
```

Note: `proxy::rule_matches` must be made `pub` in proxy.rs.

- [ ] **Step 2: Run tests**

```bash
cd /home/alkaaf/project/9limiter && cargo test
```

Expected: all tests pass.

- [ ] **Step 3: Final commit**

```bash
git add -A
git commit -m "chore: integration tests"
```

---
## Self-Review

### Spec coverage
- Proxy with rate limiting: Tasks 3, 5 ✓
- YAML config with fallback, upstreams, rulesets, api_keys: Task 2 ✓
- Config validation including overlap warning: Task 2 ✓
- Hot-reload via notify: Task 7 ✓
- CLI flags: Task 1 ✓
- Embedded vanilla dashboard + WS: Task 6 ✓
- Rate limit response JSON: Task 5 ✓
- Error handling (401, 404, 502/504, unparseable body): Task 5 ✓

### Type consistency check
- `AppState` used consistently across proxy, dashboard, and main ✓
- `AppEvent` enum with `rate_limit`, `request`, `request_end` variants ✓
- `SlidingLimiter::check` returns `bool` (true = pass) ✓
- Config structs match serde field names ✓

No gaps found.
