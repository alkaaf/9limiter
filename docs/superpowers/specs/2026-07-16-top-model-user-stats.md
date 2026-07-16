# Top Model & Top User Stats

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development to implement this plan task-by-task.

**Goal:** Add a `/stats` page with a dual-handle timeline slider that shows leaderboards of top models and top users by token count. Data collected from upstream response bodies.

**Architecture:** In-memory channel → batch SQLite writer → queryable stats endpoint → live UI.

**Tech Stack:** rusqlite, tokio::sync::mpsc, chrono, existing dashboard HTML/CSS pattern.

## Global Constraints

- Data directory: `~/.9limiter/` — SQLite file at `~/.9limiter/stats.db`
- SQLite path passed via AppState
- All proxy-path data collection is non-blocking — write errors are logged and skipped
- Model name from `usage` field in upstream response body
- Model family extracted as the part before `/` (e.g. `zyr/gpt-5.5` → family `zyr`) for grouping
- Api key → owner name resolved from existing `state.key_owners`
- UI matches dashboard theme (dark, monospace, same header/nav)
- Dual-range slider: left handle = start, right handle = end, range = 7 weeks back
- Smooth animation on leaderboard transitions
- Retention: 2 months, cleanup runs hourly
- Aggregation: hourly buckets, inserted with INSERT OR REPLACE

---

### Task 1: Add SQLite dependency

**Files:**
- Modify: `Cargo.toml`

**Interfaces:**
- Consumes: project dependencies
- Produces: `rusqlite` available as `rusqlite` with `bundled` feature

- [ ] **Step 1: Add rusqlite**

```toml
rusqlite = { version = "0.31", features = ["bundled"] }
```

- [ ] **Step 2: Verify compilation**

```bash
cargo check
```

- [ ] **Step 3: Commit**

```bash
git add Cargo.toml Cargo.lock
git commit -m "chore: add rusqlite dependency"
```

---

### Task 2: Stats data collector — in-memory channel + SQLite writer

**Files:**
- Create: `src/stats.rs`

**Interfaces:**
- Consumes: `(api_key: String, model_family: String, hour_bucket: String, tokens: u64)` via mpsc channel
- Produces: `StatsCollector { sender, db_path }` — public start/stop, query method

`StatsCollector` runs a background tokio task that:
1. Drains mpsc receiver every 5 seconds
2. Batches INSERT OR REPLACE into `usage(hour, api_key, model, tokens)` 
3. Resolves owner from `key_owners` HashMap stored inside collector
4. Runs hourly cleanup: `DELETE FROM usage WHERE hour < datetime('now', '-2 months', '+7 hours')`

Query method:
```rust
pub fn query(&self, start: &str, end: &str) -> (Vec<ModelStat>, Vec<UserStat>)
```
- Models: `SELECT model, SUM(tokens) as tokens FROM usage WHERE hour >= ?1 AND hour <= ?2 GROUP BY model ORDER BY tokens DESC LIMIT 10`
- Users: `SELECT api_key, SUM(tokens) as tokens FROM usage WHERE hour >= ?1 AND hour <= ?2 GROUP BY api_key ORDER BY tokens DESC LIMIT 10`

`ModelStat { model: String, tokens: u64 }`
`UserStat { api_key: String, owner: String, tokens: u64 }`

- [ ] **Step 1: Create src/stats.rs**
- [ ] **Step 2: Verify compilation**
- [ ] **Step 3: Commit**

---

### Task 3: Intercept token usage in proxy handler

**Files:**
- Modify: `src/proxy.rs`

**Interfaces:**
- Consumes: `StatsCollector` (via `AppState`)
- Produces: token usage extracted from upstream response body

Flow:
1. Before returning response, read the full response body bytes
2. Parse JSON, extract `usage.total_tokens` (OpenAI) or `usage.output_tokens` + `usage.input_tokens` (Anthropic)
3. Sum to get total
4. Extract model family: `model.split('/').next()`
5. Send `(api_key, model_family, hour_bucket, tokens)` to mpsc sender
6. Return reconstructed response with body bytes

Hour bucket format: `YYYY-MM-DDTHH:00:00+07` — formatted with `state.tz`

- [ ] **Step 1: Add body reading + parsing in proxy_to_upstream**
- [ ] **Step 2: Send to stats collector**
- [ ] **Step 3: Verify compilation**
- [ ] **Step 4: Commit**

---

### Task 4: Stats API endpoint

**Files:**
- Modify: `src/stats.rs` (add axum handler)
- Modify: `src/main.rs` (add route)

**Interfaces:**
- Produces: `GET /api/stats?start=...&end=... → JSON`

Route: `/api/stats`
Query params: `start`, `end` — ISO datetime strings
Response:
```json
{
  "models": [{"model": "gpt-4", "tokens": 150000}],
  "users": [{"api_key": "sk-ae10…65f", "owner": "alkaaf", "tokens": 95000}]
}
```

- [ ] **Step 1: Add stats_handler function**
- [ ] **Step 2: Register route in main.rs**
- [ ] **Step 3: Add AppState field for StatsCollector sender or Arc<Mutex<...>>**
- [ ] **Step 4: Verify compilation**
- [ ] **Step 5: Commit**

---

### Task 5: Stats page UI

**Files:**
- Create: `src/stats.html`
- Modify: `src/stats.rs` (serve HTML)

UI layout:
```
[Header] 9limiter | [Dashboard] [Stats]  ← nav tabs
[Range slider — dual handle — timeline]
[Top Models — horizontal bar chart, 10 items]
[Top Users — horizontal bar chart, 10 items]
```

Slider behaviour:
- Two `<input type="range">` elements layered, min/max = 7 weeks
- On `change` (mouse release), fetch `/api/stats?start=X&end=Y`
- Animate bar widths with CSS transition
- Display formatted time labels above handles

Design matches dashboard: `background:#0f0f1a`, cards `#1a1a2e`, monospace

- [ ] **Step 1: Create stats.html with full structure**
- [ ] **Step 2: Wire stats_handler to serve HTML**
- [ ] **Step 3: Verify compilation**
- [ ] **Step 4: Commit**

---

### Task 6: Wire everything together in main.rs

**Files:**
- Modify: `src/main.rs`

**Interfaces:**
- Consumes: all prior tasks
- Produces: working `/stats` page

Add to AppState:
```rust
pub stats: Arc<stats::StatsCollector>,
```

Initialize StatsCollector in main, spawn background writer.

- [ ] **Step 1: Import stats module, add to AppState**
- [ ] **Step 2: Initialize with db path `~/.9limiter/stats.db`**
- [ ] **Step 3: Pass to axum state and routes**
- [ ] **Step 4: Run full build + test**
- [ ] **Step 5: Commit**

---

### Task 7: Send usage data from proxy endpoint

**Files:**
- Modify: `src/proxy.rs`

For OpenAI format: `response_obj.usage.total_tokens`
For Anthropic format: `response_obj.usage.input_tokens + response_obj.usage.output_tokens`

Implementation:
```rust
fn extract_usage(body: &[u8]) -> Option<u64> {
    let v: serde_json::Value = serde_json::from_slice(body).ok()?;
    if let Some(total) = v.get("usage")?.get("total_tokens")?.as_u64() {
        return Some(total);
    }
    // Anthropic
    if let Some(u) = v.get("usage") {
        let inp = u.get("input_tokens").and_then(|x| x.as_u64()).unwrap_or(0);
        let out = u.get("output_tokens").and_then(|x| x.as_u64()).unwrap_or(0);
        if inp + out > 0 { return Some(inp + out); }
    }
    None
}
```

Call this after getting the upstream response body, before returning.

- [ ] **Step 1: Add extract_usage function**
- [ ] **Step 2: Integrate into proxy_to_upstream**
- [ ] **Step 3: Send to stats collector**
- [ ] **Step 4: Run tests**
- [ ] **Step 5: Commit**
