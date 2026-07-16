# Request Log Persistence Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Persist request logs to SQLite so dashboard survives refresh. Add range filter (24h/3d/7d/15d/30d). Live WS stays.

**Architecture:** Follow existing `StatsCollector` pattern — one more mpsc channel → batch flush every 5s → SQLite table `request_logs`. New endpoint `/api/requests?range=24h` for initial load. Frontend fetch on connect + dropdown filter.

**Tech Stack:** rusqlite (bundled already), tokio::sync::mpsc, chrono, axum

## Global Constraints

- File SQLite sama: `~/.9limiter/stats.db`
- Retention: 2 months, cleanup di merge sama cleanup stats yang sudah ada tiap jam
- Max 200 rows per query
- Range values exact: `24h`, `3d`, `7d`, `15d`, `30d`
- API key di frontend tetap pake `shortKey()` (12 chars + `…`)
- WS live update (`request` + `request_end` events) tetap jalan seperti sekarang, gak diubah

---

### Task 1: Backend — RequestLog struct, SQLite table, writer, cleanup

**Files:**
- Modify: `src/stats.rs`

**Interfaces:**
- Produces: `RequestLogWriter` struct dengan `start()`, `flush_batch()`, `cleanup()`, `query()`
- Produces: `RequestLog` struct — `#[derive(Debug, Clone, Serialize)]`
- Produces: `requests_handler(State, Query) -> impl IntoResponse`
- Sender type: `tokio::sync::mpsc::UnboundedSender<RequestLog>`

- [ ] **Step 1: Write failing test — flush & query**

Add inside `#[cfg(test)]` module in `src/stats.rs`:

```rust
#[test]
fn test_request_log_flush_and_query() {
    let uid = uuid::Uuid::new_v4().to_string();
    let dir = std::env::temp_dir().join(format!("9limiter_rl_test_{}", uid));
    let _ = std::fs::create_dir_all(&dir);
    let path = dir.join("test_rl.db").to_string_lossy().to_string();

    RequestLogWriter::init_db(&path);

    // Insert batch
    let logs = vec![
        RequestLog { id: "r1".into(), api_key: "sk-key1".into(), owner: "alice".into(), model: "gpt-4".into(), method: "POST".into(), path: "/v1/chat".into(), status: 200, latency_ms: 500, timestamp: "2026-07-16T10:00:00+07:00".into() },
        RequestLog { id: "r2".into(), api_key: "sk-key2".into(), owner: "bob".into(), model: "claude-3".into(), method: "POST".into(), path: "/v1/messages".into(), status: 429, latency_ms: 0, timestamp: "2026-07-16T12:00:00+07:00".into() },
    ];
    RequestLogWriter::flush_batch(&path, &logs).unwrap();

    // Query all
    let results = RequestLogWriter::query_logs(&path, "2026-07-16T00:00:00+07:00", "2026-07-17T00:00:00+07:00", 200).unwrap();
    assert_eq!(results.len(), 2);
    assert_eq!(results[0].id, "r2"); // DESC order
    assert_eq!(results[1].model, "gpt-4");

    // Query partial range — hit only r2
    let results = RequestLogWriter::query_logs(&path, "2026-07-16T11:00:00+07:00", "2026-07-16T13:00:00+07:00", 200).unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].id, "r2");

    // Query empty range
    let results = RequestLogWriter::query_logs(&path, "2026-07-17T00:00:00+07:00", "2026-07-18T00:00:00+07:00", 200).unwrap();
    assert!(results.is_empty());

    let _ = std::fs::remove_dir_all(&dir);
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test --release test_request_log_flush_and_query 2>&1`
Expected: FAIL — `RequestLogWriter` not found

- [ ] **Step 3: Write minimal implementation**

Add to `src/stats.rs`:

```rust
#[derive(Debug, Clone, serde::Serialize)]
pub struct RequestLog {
    pub id: String,
    pub api_key: String,
    pub owner: String,
    pub model: String,
    pub method: String,
    pub path: String,
    pub status: u16,
    pub latency_ms: u64,
    pub timestamp: String,
}

pub struct RequestLogWriter;

impl RequestLogWriter {
    pub fn init_db(path: &str) {
        let conn = Connection::open(path).expect("failed to open stats db");
        conn.execute_batch(
            "CREATE TABLE IF NOT EXISTS request_logs (
                id         TEXT PRIMARY KEY,
                api_key    TEXT NOT NULL,
                owner      TEXT NOT NULL DEFAULT '',
                model      TEXT NOT NULL,
                method     TEXT NOT NULL,
                path       TEXT NOT NULL,
                status     INTEGER NOT NULL DEFAULT 0,
                latency_ms INTEGER NOT NULL DEFAULT 0,
                timestamp  TEXT NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_request_logs_ts ON request_logs(timestamp);"
        ).expect("failed to init request_logs table");
    }

    fn flush_batch(db_path: &str, batch: &[RequestLog]) -> Result<(), rusqlite::Error> {
        let conn = Connection::open(db_path)?;
        let tx = conn.unchecked_transaction()?;
        {
            let mut stmt = tx.prepare(
                "INSERT OR IGNORE INTO request_logs (id, api_key, owner, model, method, path, status, latency_ms, timestamp)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)"
            )?;
            for log in batch {
                stmt.execute(rusqlite::params![
                    log.id, log.api_key, log.owner, log.model,
                    log.method, log.path, log.status, log.latency_ms, log.timestamp
                ])?;
            }
        }
        tx.commit()?;
        Ok(())
    }

    fn query_logs(db_path: &str, start: &str, end: &str, limit: usize) -> Result<Vec<RequestLog>, rusqlite::Error> {
        let conn = Connection::open(db_path)?;
        let mut stmt = conn.prepare(
            "SELECT id, api_key, owner, model, method, path, status, latency_ms, timestamp
             FROM request_logs
             WHERE timestamp >= ?1 AND timestamp <= ?2
             ORDER BY timestamp DESC
             LIMIT ?3"
        )?;
        let rows = stmt.query_map(
            rusqlite::params![start, end, limit as i64],
            |row| {
                Ok(RequestLog {
                    id: row.get(0)?,
                    api_key: row.get(1)?,
                    owner: row.get(2)?,
                    model: row.get(3)?,
                    method: row.get(4)?,
                    path: row.get(5)?,
                    status: row.get::<_, i32>(6)? as u16,
                    latency_ms: row.get::<_, i64>(7)? as u64,
                    timestamp: row.get(8)?,
                })
            }
        )?;
        rows.collect()
    }
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cargo test --release test_request_log_flush_and_query 2>&1`
Expected: PASS

- [ ] **Step 5: Write failing test — writer + cleanup**

```rust
#[test]
fn test_request_log_writer_cleanup() {
    let uid = uuid::Uuid::new_v4().to_string();
    let dir = std::env::temp_dir().join(format!("9limiter_rl_test_{}", uid));
    let _ = std::fs::create_dir_all(&dir);
    let path = dir.join("test_rl.db").to_string_lossy().to_string();
    RequestLogWriter::init_db(&path);

    // Insert old + new data
    let logs = vec![
        RequestLog { id: "old".into(), api_key: "sk-k".into(), owner: "".into(), model: "gpt-4".into(), method: "POST".into(), path: "/v1/chat".into(), status: 200, latency_ms: 100, timestamp: "2026-05-01T00:00:00+07:00".into() },
        RequestLog { id: "new".into(), api_key: "sk-k".into(), owner: "".into(), model: "gpt-4".into(), method: "POST".into(), path: "/v1/chat".into(), status: 200, latency_ms: 100, timestamp: "2026-07-16T00:00:00+07:00".into() },
    ];
    RequestLogWriter::flush_batch(&path, &logs).unwrap();

    // Cleanup removes entries < 2 months
    let conn = Connection::open(&path).unwrap();
    conn.execute("DELETE FROM request_logs WHERE timestamp < datetime('now', '-60 days')", []).unwrap();

    let results = RequestLogWriter::query_logs(&path, "2000-01-01", "2099-12-31", 200).unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].id, "new");

    let _ = std::fs::remove_dir_all(&dir);
}
```

- [ ] **Step 6: Run test**

Run: `cargo test --release test_request_log_writer_cleanup 2>&1`
Expected: PASS

- [ ] **Step 7: Commit**

```bash
git add -A && git commit -m "feat: add RequestLog, SQLite table, flush, query, cleanup

Co-Authored-By: Claude <noreply@anthropic.com>"
```

---

### Task 2: Backend — writer loop + `/api/requests` endpoint + wire proxy

**Files:**
- Modify: `src/stats.rs`
- Modify: `src/proxy.rs` (AppState, proxy_to_upstream)
- Modify: `src/main.rs` (init + route)

**Interfaces:**
- Consumes: `RequestLog`, `RequestLogWriter` dari Task 1
- Produces: `AppState.request_log_tx: UnboundedSender<RequestLog>`
- Produces: Route `GET /api/requests?range=24h`

- [ ] **Step 1: Write test — range parsing**

```rust
#[test]
fn test_request_log_parse_range() {
    fn parse_range(range: &str) -> (String, String) {
        let now = chrono::Utc::now();
        let (start, end) = match range {
            "24h" => (now - chrono::Duration::hours(24), now),
            "3d"  => (now - chrono::Duration::days(3), now),
            "7d"  => (now - chrono::Duration::days(7), now),
            "15d" => (now - chrono::Duration::days(15), now),
            "30d" => (now - chrono::Duration::days(30), now),
            _ => (now - chrono::Duration::days(1), now),
        };
        (start.to_rfc3339(), end.to_rfc3339())
    }

    let (s, e) = parse_range("24h");
    assert!(s < e);
    let (s, e) = parse_range("30d");
    assert!(s < e);
}
```

- [ ] **Step 2: Run test**

Run: `cargo test --release test_request_log_parse_range 2>&1`
Expected: PASS

- [ ] **Step 3: Add writer loop + `requests_handler` + range parsing**

In `src/stats.rs`, add after `RequestLogWriter`:

```rust
pub fn spawn_request_log_writer(db_path: String, rx: Arc<Mutex<tokio::sync::mpsc::UnboundedReceiver<RequestLog>>>) {
    tokio::spawn(async move {
        let mut last_cleanup = std::time::Instant::now();
        loop {
            tokio::time::sleep(std::time::Duration::from_secs(5)).await;
            let mut batch = Vec::new();
            {
                let mut rx = rx.lock().unwrap();
                while let Ok(item) = rx.try_recv() {
                    batch.push(item);
                }
            }
            if !batch.is_empty() {
                if let Err(e) = RequestLogWriter::flush_batch(&db_path, &batch) {
                    tracing::error!("request_log flush error: {}", e);
                }
            }
            // Merge cleanup with stats cleanup — gak perlu cleanup sendiri
            // karena di stats cleanup jam 1 jam sekali juga hapus request_logs
        }
    });
}

pub async fn requests_handler(
    axum::extract::Query(params): axum::extract::Query<HashMap<String, String>>,
) -> impl IntoResponse {
    let range = params.get("range").map(|s| s.as_str()).unwrap_or("24h");
    let now = chrono::Utc::now();
    let (start, end): (chrono::DateTime<chrono::Utc>, chrono::DateTime<chrono::Utc>) = match range {
        "3d"  => (now - chrono::Duration::days(3), now),
        "7d"  => (now - chrono::Duration::days(7), now),
        "15d" => (now - chrono::Duration::days(15), now),
        "30d" => (now - chrono::Duration::days(30), now),
        _     => (now - chrono::Duration::hours(24), now), // default 24h
    };

    let db_path = /* path ke stats.db — butuh dari state */ "".to_string();
    match RequestLogWriter::query_logs(&db_path, &start.to_rfc3339(), &end.to_rfc3339(), 200) {
        Ok(logs) => axum::Json(serde_json::json!(logs)),
        Err(_) => axum::Json(serde_json::json!([])),
    }
}
```

- [ ] **Step 4: Update cleanup — merge request_log cleanup**

In `StatsHandle::cleanup()`, add after the `DELETE FROM usage`:

```rust
let _ = conn.execute(
    "DELETE FROM request_logs WHERE timestamp < datetime('now', '-60 days')",
    [],
);
```

- [ ] **Step 5: Add `request_log_tx` to AppState**

In `src/proxy.rs`, add field:

```rust
pub request_log_tx: tokio::sync::mpsc::UnboundedSender<stats::RequestLog>,
```

- [ ] **Step 6: Send request log on request end in `proxy_to_upstream`**

In `src/proxy.rs`, after the success `state.event_tx.send(AppEvent::RequestEnd(...))` for Ok path (~line 242), add:

```rust
let _ = state.request_log_tx.send(stats::RequestLog {
    id: req_id.clone(),
    api_key: api_key.to_string(),
    owner: owner.clone(),
    model: model.to_string(),
    method: parts.method.to_string(),
    path: parts.uri.path_and_query().map(|pq| pq.to_string()).unwrap_or_default(),
    status,
    latency_ms: latency,
    timestamp: chrono::Utc::now().with_timezone(&state.tz).to_rfc3339(),
});
```

Also in the error path (~line 258), after `AppEvent::RequestEnd(...)`:

```rust
let _ = state.request_log_tx.send(stats::RequestLog {
    id: req_id.clone(),
    api_key: api_key.to_string(),
    owner,
    model: model.to_string(),
    method: parts.method.to_string(),
    path: parts.uri.path_and_query().map(|pq| pq.to_string()).unwrap_or_default(),
    status: status as u16,
    latency_ms: start.elapsed().as_millis() as u64,
    timestamp: chrono::Utc::now().with_timezone(&state.tz).to_rfc3339(),
});
```

Note: in the error handler, `parts` has been partially moved but `parts.method` and `parts.uri` are still accessible. The `parts` variable is still available because it was created in `proxy_handler` and borrowed/used by reference in the match arm. If needed, clone `parts.uri` before the match.

- [ ] **Step 7: Init writer + route in main.rs**

In `main.rs`:
- After stats init (~line 230), init request_log writer:

```rust
let (rl_tx, rl_rx) = tokio::sync::mpsc::unbounded_channel();
let rl_rx = Arc::new(Mutex::new(rl_rx));
stats::RequestLogWriter::init_db(&stats_db);
stats::spawn_request_log_writer(stats_db.clone(), rl_rx);
```

- Add `request_log_tx: rl_tx.clone()` to AppState
- Add route:

```rust
.route("/api/requests", axum::routing::get(stats::requests_handler))
```

Note: `requests_handler` hanya butuh `axum::extract::Query(params)` — gak perlu State untuk handler sederhana. Tapi `requests_handler` perlu `stats_db` path. Solusi: simpan `stats_db` di AppState juga, atau bikin handler jadi method di `StatsHandle`.

Simplify: pindahkan query ke method di `RequestLogWriter` yang static, dan `requests_handler` accept `State(state)` tapi `state` perlu bawa path. Cara ter-simple: buat `requests_handler` jadi `async fn requests_handler(State(state): State<AppState>, Query(params): Query<...>)`.

Tambahkan field ke AppState:

```rust
pub request_log_tx: tokio::sync::mpsc::UnboundedSender<stats::RequestLog>,
```

Tambahkan juga path ke AppState atau bikin `requests_handler` panggil `RequestLogWriter::query_logs` dengan db_path yang disimpan di `state.stats_db` — tapi `stats_db` belum ada di AppState.

Solusi paling simpel: tambah `stats_db_path: String` di `AppState`.

- [ ] **Step 8: Build + test**

Run: `cargo test --release 2>&1`
Expected: all tests pass

- [ ] **Step 9: Commit**

```bash
git add -A && git commit -m "feat: wire request log writer, endpoint, proxy send

Co-Authored-By: Claude <noreply@anthropic.com>"
```

---

### Task 3: Frontend — filter dropdown + initial fetch

**Files:**
- Modify: `src/dashboard.html`

**Interfaces:**
- Consumes: endpoint `GET /api/requests?range=X` dari Task 2
- Consumes: WS events `request` + `request_end` (existing, unchanged)

- [ ] **Step 1: Add filter dropdown to HTML**

In `<div class="top-bar">`, after `rate-info` div, before `live-indicator`, add:

```html
<div class="range-filter">
  <select id="range-select">
    <option value="24h" selected>24 Hours</option>
    <option value="3d">3 Days</option>
    <option value="7d">7 Days</option>
    <option value="15d">15 Days</option>
    <option value="30d">30 Days</option>
  </select>
</div>
```

Add CSS:
```css
.range-filter select{background:#2a2a4a;color:#e0e0e0;border:1px solid #3a3a5a;border-radius:4px;padding:4px 8px;font-size:12px;cursor:pointer}
.range-filter select:focus{outline:none;border-color:#4fc3f7}
```

- [ ] **Step 2: Add `loadRequests()` function**

```javascript
let currentRange = '24h';

async function loadRequests(range) {
  currentRange = range;
  try {
    const r = await fetch('/api/requests?range=' + range);
    const data = await r.json();
    const tbody = document.getElementById('log-body');
    tbody.innerHTML = '';
    logCount = data.length;
    for (const d of data) {
      const tr = document.createElement('tr');
      tr.id = 'log-' + d.id;
      tr.innerHTML = '<td>' + d.timestamp.slice(11,19) + '</td>' +
        '<td title="'+d.api_key+'">' + shortKey(d.api_key) + '</td>' +
        '<td>' + (d.owner || '') + '</td>' +
        '<td>' + d.model + '</td>' +
        '<td><span class="'+(d.status===200?'status-200':d.status===429?'status-429':'status-err')+'">'+d.status+'</span></td>' +
        '<td>' + (d.latency_ms ? (d.latency_ms/1000).toFixed(1)+'s' : '&mdash;') + '</td>';
      tbody.appendChild(tr);
    }
    document.getElementById('reqs-count').textContent = logCount + ' requests';
  } catch(e) {}
}
```

- [ ] **Step 3: Wire select onChange + initial load**

```javascript
document.getElementById('range-select').addEventListener('change', function(e) {
  loadRequests(e.target.value);
});

// On page load
loadRequests('24h');
```

- [ ] **Step 4: Also update `updateRow()` to check range**

When WS `request_end` arrives, also append to table even if loaded from API. This already works because `renderRow` (`request` event) prepends and `updateRow` (`request_end` event) updates by ID.

But after `loadRequests()` replaces tbody, WS events still create new rows via `renderRow`. That's fine — new rows appear at top, pre-existing rows from API stay below until they naturally age out at MAX_LOG.

- [ ] **Step 5: Build + test**

Run: `cargo build --release 2>&1`
Expected: success

- [ ] **Step 6: Commit**

```bash
git add -A && git commit -m "feat: dashboard range filter + load persisted requests

Co-Authored-By: Claude <noreply@anthropic.com>"
```
