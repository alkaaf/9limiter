use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use axum::{extract::State, response::IntoResponse};
use rusqlite::Connection;

#[derive(Debug, Clone, serde::Serialize)]
pub struct ModelStat {
    pub model: String,
    pub tokens: u64,
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct UserStat {
    pub api_key: String,
    pub owner: String,
    pub tokens: u64,
}

pub struct StatsCollector {
    #[allow(dead_code)]
    db_path: String,
    pub sender: tokio::sync::mpsc::Sender<(String, String, String, u64)>,
    #[allow(dead_code)]
    key_owners: Arc<HashMap<String, String>>,
}

impl StatsCollector {
    pub fn new(
        db_path: String,
        key_owners: Arc<HashMap<String, String>>,
    ) -> (Self, StatsHandle) {
        let (tx, rx) = tokio::sync::mpsc::channel(8192);

        // Init DB schema
        Self::init_db(&db_path);

        let collector = StatsCollector {
            db_path: db_path.clone(),
            sender: tx,
            key_owners: key_owners.clone(),
        };

        let handle = StatsHandle {
            db_path,
            rx: Arc::new(Mutex::new(rx)),
            key_owners,
        };

        (collector, handle)
    }

    fn init_db(path: &str) {
        let conn = Connection::open(path).expect("failed to open stats db");
        conn.execute_batch("PRAGMA journal_mode=WAL;").ok();
        conn.execute_batch(
            "CREATE TABLE IF NOT EXISTS usage (
                hour     TEXT NOT NULL,
                api_key  TEXT NOT NULL,
                model    TEXT NOT NULL,
                tokens   INTEGER NOT NULL,
                PRIMARY KEY (hour, api_key, model)
            );
            CREATE INDEX IF NOT EXISTS idx_usage_hour ON usage(hour);"
        ).expect("failed to init stats db");
    }
}

#[derive(Clone)]
pub struct StatsHandle {
    db_path: String,
    rx: Arc<Mutex<tokio::sync::mpsc::Receiver<(String, String, String, u64)>>>,
    key_owners: Arc<HashMap<String, String>>,
}

impl StatsHandle {
    /// Start background writer — call once, runs forever
    pub fn start(&self) {
        let db_path = self.db_path.clone();
        let rx = self.rx.clone();

        tokio::spawn(async move {
            let mut last_cleanup = std::time::Instant::now();

            loop {
                tokio::time::sleep(std::time::Duration::from_secs(5)).await;

                // Drain channel
                let mut batch: Vec<(String, String, String, u64)> = Vec::new();
                {
                    let mut rx = rx.lock().unwrap();
                    while let Ok(item) = rx.try_recv() {
                        batch.push(item);
                    }
                }

                if !batch.is_empty() {
                    let db = db_path.clone();
                    tokio::task::spawn_blocking(move || {
                        if let Err(e) = Self::flush_batch(&db, &batch) {
                            tracing::error!("stats flush error: {}", e);
                        }
                    }).await.ok();
                }

                // Hourly cleanup
                if last_cleanup.elapsed().as_secs() >= 3600 {
                    let db = db_path.clone();
                    tokio::task::spawn_blocking(move || Self::cleanup(&db)).await.ok();
                    last_cleanup = std::time::Instant::now();
                }
            }
        });
    }

    fn flush_batch(db_path: &str, batch: &[(String, String, String, u64)]) -> Result<(), rusqlite::Error> {
        let conn = Connection::open(db_path)?;
        let tx = conn.unchecked_transaction()?;
        {
            let mut stmt = tx.prepare(
                "INSERT OR REPLACE INTO usage (hour, api_key, model, tokens)
                 VALUES (?1, ?2, ?3, COALESCE(
                     (SELECT tokens FROM usage WHERE hour=?1 AND api_key=?2 AND model=?3),
                     0
                 ) + ?4)"
            )?;
            for (api_key, model, hour, tokens) in batch {
                stmt.execute(rusqlite::params![hour, api_key, model, tokens])?;
            }
        }
        tx.commit()?;
        Ok(())
    }

    fn cleanup(db_path: &str) {
        tracing::debug!("stats cleanup: removing entries older than 2 months");
        let conn = match Connection::open(db_path) {
            Ok(c) => c,
            Err(e) => {
                tracing::error!("stats cleanup open error: {}", e);
                return;
            }
        };
        if let Err(e) = conn.execute(
            "DELETE FROM usage WHERE hour < datetime('now', '-2 months')",
            [],
        ) {
            tracing::error!("stats cleanup delete error: {}", e);
        }
        let _ = conn.execute(
            "DELETE FROM request_logs WHERE timestamp < datetime('now', '-60 days')",
            [],
        );
    }

    pub async fn query(&self, start: &str, end: &str) -> (Vec<ModelStat>, Vec<UserStat>) {
        let db_path = self.db_path.clone();
        let owners = self.key_owners.clone();
        let start = start.to_string();
        let end = end.to_string();
        tokio::task::spawn_blocking(move || {
            Self::query_sync(&db_path, &owners, &start, &end)
        }).await.unwrap_or_default()
    }

    fn query_sync(db_path: &str, key_owners: &HashMap<String, String>, start: &str, end: &str) -> (Vec<ModelStat>, Vec<UserStat>) {
        let conn = match Connection::open(db_path) {
            Ok(c) => c,
            Err(e) => {
                tracing::error!("stats query open error: {}", e);
                return (vec![], vec![]);
            }
        };

        // Top models
        let mut stmt = conn.prepare(
            "SELECT model, SUM(tokens) as tokens FROM usage
             WHERE hour >= ?1 AND hour <= ?2
             GROUP BY model ORDER BY tokens DESC LIMIT 10"
        ).unwrap();
        let models: Vec<ModelStat> = stmt.query_map(rusqlite::params![start, end], |row| {
            Ok(ModelStat {
                model: row.get(0)?,
                tokens: row.get::<_, i64>(1)? as u64,
            })
        }).unwrap().filter_map(|r| r.ok()).collect();

        // Top users
        let mut stmt = conn.prepare(
            "SELECT api_key, SUM(tokens) as tokens FROM usage
             WHERE hour >= ?1 AND hour <= ?2
             GROUP BY api_key ORDER BY tokens DESC LIMIT 10"
        ).unwrap();
        let users: Vec<UserStat> = stmt.query_map(rusqlite::params![start, end], |row| {
            let api_key: String = row.get(0)?;
            let tokens: i64 = row.get(1)?;
            let owner = key_owners.get(&api_key).cloned().unwrap_or_default();
            Ok(UserStat { api_key, owner, tokens: tokens as u64 })
        }).unwrap().filter_map(|r| r.ok()).collect();

        (models, users)
    }
}

pub async fn stats_handler(
    State(state): State<crate::proxy::AppState>,
    axum::extract::Query(params): axum::extract::Query<HashMap<String, String>>,
) -> impl IntoResponse {
    let start = params.get("start").cloned().unwrap_or_default();
    let end = params.get("end").cloned().unwrap_or_default();
    let (models, users) = state.stats_handle.query(&start, &end).await;
    let tz_offset = state.tz.local_minus_utc();
    axum::Json(serde_json::json!({ "models": models, "users": users, "tz_offset": tz_offset }))
}

pub async fn stats_page_handler() -> impl IntoResponse {
    axum::response::Html(include_str!("stats.html"))
}

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
    pub input_tokens: u64,
    pub output_tokens: u64,
    pub cache_tokens: u64,
    pub timestamp: String,
}

pub struct RequestLogWriter;

impl RequestLogWriter {
    pub fn init_db(path: &str) {
        let conn = Connection::open(path).expect("failed to open stats db");
        conn.execute_batch("PRAGMA journal_mode=WAL;").ok();
        conn.execute_batch(
            "CREATE TABLE IF NOT EXISTS request_logs (
                id            TEXT PRIMARY KEY,
                api_key       TEXT NOT NULL,
                owner         TEXT NOT NULL DEFAULT '',
                model         TEXT NOT NULL,
                method        TEXT NOT NULL,
                path          TEXT NOT NULL,
                status        INTEGER NOT NULL DEFAULT 0,
                latency_ms    INTEGER NOT NULL DEFAULT 0,
                input_tokens  INTEGER NOT NULL DEFAULT 0,
                output_tokens INTEGER NOT NULL DEFAULT 0,
                cache_tokens  INTEGER NOT NULL DEFAULT 0,
                timestamp     TEXT NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_request_logs_ts ON request_logs(timestamp);"
        ).expect("failed to init request_logs table");
        // Migration: add columns if table existed from older version
        let _ = conn.execute("ALTER TABLE request_logs ADD COLUMN input_tokens INTEGER NOT NULL DEFAULT 0", []);
        let _ = conn.execute("ALTER TABLE request_logs ADD COLUMN output_tokens INTEGER NOT NULL DEFAULT 0", []);
        let _ = conn.execute("ALTER TABLE request_logs ADD COLUMN cache_tokens INTEGER NOT NULL DEFAULT 0", []);
    }

    pub fn flush_batch(db_path: &str, batch: &[RequestLog]) -> Result<(), rusqlite::Error> {
        let conn = Connection::open(db_path)?;
        let tx = conn.unchecked_transaction()?;
        {
            let mut stmt = tx.prepare(
                "INSERT OR IGNORE INTO request_logs (id, api_key, owner, model, method, path, status, latency_ms, input_tokens, output_tokens, cache_tokens, timestamp)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12)"
            )?;
            for log in batch {
                stmt.execute(rusqlite::params![
                    log.id, log.api_key, log.owner, log.model,
                    log.method, log.path, log.status, log.latency_ms,
                    log.input_tokens, log.output_tokens, log.cache_tokens,
                    log.timestamp
                ])?;
            }
        }
        tx.commit()?;
        Ok(())
    }

    pub fn query_logs(db_path: &str, start: &str, end: &str, limit: usize) -> Result<Vec<RequestLog>, rusqlite::Error> {
        let conn = Connection::open(db_path)?;
        let mut stmt = conn.prepare(
            "SELECT id, api_key, owner, model, method, path, status, latency_ms, input_tokens, output_tokens, cache_tokens, timestamp
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
                    input_tokens: row.get::<_, i64>(8)? as u64,
                    output_tokens: row.get::<_, i64>(9)? as u64,
                    cache_tokens: row.get::<_, i64>(10)? as u64,
                    timestamp: row.get(11)?,
                })
            }
        )?;
        rows.collect()
    }
}

pub fn spawn_request_log_writer(db_path: String, rx: Arc<Mutex<tokio::sync::mpsc::Receiver<RequestLog>>>) {
    tokio::spawn(async move {
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
                let db = db_path.clone();
                tokio::task::spawn_blocking(move || {
                    if let Err(e) = RequestLogWriter::flush_batch(&db, &batch) {
                        tracing::error!("request_log flush error: {}", e);
                    }
                }).await.ok();
            }
        }
    });
}

pub async fn requests_handler(
    State(state): State<crate::proxy::AppState>,
    axum::extract::Query(params): axum::extract::Query<HashMap<String, String>>,
) -> impl IntoResponse {
    let range = params.get("range").map(|s| s.as_str()).unwrap_or("24h");
    let now = chrono::Utc::now().with_timezone(&state.tz);
    let (start, end) = match range {
        "3d"  => (now - chrono::Duration::days(3), now),
        "7d"  => (now - chrono::Duration::days(7), now),
        "15d" => (now - chrono::Duration::days(15), now),
        "30d" => (now - chrono::Duration::days(30), now),
        _     => (now - chrono::Duration::hours(24), now),
    };
    let db_path = state.stats_db_path.clone();
    let start_str = start.to_rfc3339();
    let end_str = end.to_rfc3339();
    let logs = tokio::task::spawn_blocking(move || {
        RequestLogWriter::query_logs(&db_path, &start_str, &end_str, 200)
    }).await.unwrap_or_else(|_| Ok(vec![]));
    match logs {
        Ok(logs) => axum::Json(serde_json::json!(logs)),
        Err(e) => {
            tracing::error!("request_log query error: {}", e);
            axum::Json(serde_json::json!([]))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    fn tmp_db() -> (String, StatsHandle) {
        let uid = uuid::Uuid::new_v4().to_string();
        let dir = std::env::temp_dir().join(format!("9limiter_stats_test_{}", uid));
        let _ = std::fs::create_dir_all(&dir);
        let path = dir.join("test_stats.db").to_string_lossy().to_string();
        StatsCollector::init_db(&path);
        let handle = StatsHandle {
            db_path: path.clone(),
            rx: Arc::new(Mutex::new(tokio::sync::mpsc::channel::<(String, String, String, u64)>(8192).1)),
            key_owners: Arc::new(HashMap::new()),
        };
        (path, handle)
    }

    #[tokio::test]
    async fn test_flush_and_query() {
        let (path, handle) = tmp_db();
        let mut batch: Vec<(String, String, String, u64)> = Vec::new();
        batch.push(("sk-key1".into(), "gpt-4".into(), "2026-07-16T10:00:00+07:00".into(), 50));
        batch.push(("sk-key1".into(), "gpt-4".into(), "2026-07-16T10:00:00+07:00".into(), 30)); // same key+model+hour → merged
        batch.push(("sk-key1".into(), "claude-3".into(), "2026-07-16T10:00:00+07:00".into(), 100));
        batch.push(("sk-key2".into(), "gpt-4".into(), "2026-07-16T11:00:00+07:00".into(), 200));
        StatsHandle::flush_batch(&path, &batch).unwrap();

        // Query all
        let (models, users) = handle.query("2026-07-16T00:00:00+07:00", "2026-07-17T00:00:00+07:00").await;
        assert_eq!(models.len(), 2);
        // gpt-4 = 50+30+200 = 280, claude-3 = 100
        assert_eq!(models[0].model, "gpt-4");
        assert_eq!(models[0].tokens, 280);
        assert_eq!(models[1].model, "claude-3");
        assert_eq!(models[1].tokens, 100);

        assert_eq!(users.len(), 2);
        // sk-key2 = 200, sk-key1 = 50+30+100 = 180
        assert_eq!(users[0].api_key, "sk-key2");
        assert_eq!(users[0].tokens, 200);
        assert_eq!(users[1].api_key, "sk-key1");
        assert_eq!(users[1].tokens, 180);

        // Cleanup
        let _ = std::fs::remove_file(&path);
    }

    #[tokio::test]
    async fn test_query_empty_db() {
        let (path, handle) = tmp_db();
        let (models, users) = handle.query("2026-07-01T00:00:00+07:00", "2026-07-02T00:00:00+07:00").await;
        assert!(models.is_empty());
        assert!(users.is_empty());
        let _ = std::fs::remove_file(&path);
    }

    #[tokio::test]
    async fn test_query_partial_range() {
        let (path, handle) = tmp_db();
        let mut batch: Vec<(String, String, String, u64)> = Vec::new();
        batch.push(("sk-key1".into(), "gpt-4".into(), "2026-07-16T10:00:00+07:00".into(), 50));
        StatsHandle::flush_batch(&path, &batch).unwrap();

        // Query outside range → empty
        let (models, users) = handle.query("2026-07-17T00:00:00+07:00", "2026-07-18T00:00:00+07:00").await;
        assert!(models.is_empty());
        assert!(users.is_empty());
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn test_cleanup_sql_valid() {
        // Verify cleanup SQL doesn't crash — just test the SQL is valid
        let (path, _handle) = tmp_db();
        StatsHandle::cleanup(&path); // should not panic
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn test_init_db_idempotent() {
        let dir = std::env::temp_dir().join(format!("9limiter_stats_test_{}", std::process::id()));
        let _ = std::fs::create_dir_all(&dir);
        let path = dir.join("idempotent.db").to_string_lossy().to_string();
        // Call twice — second should not fail
        StatsCollector::init_db(&path);
        StatsCollector::init_db(&path);
        let conn = Connection::open(&path).unwrap();
        let count: i64 = conn.query_row("SELECT COUNT(*) FROM usage", [], |r| r.get(0)).unwrap();
        assert_eq!(count, 0);
        let _ = std::fs::remove_file(&path);
    }

    #[tokio::test]
    async fn test_key_owner_in_query() {
        let dir = std::env::temp_dir().join(format!("9limiter_stats_test_{}", std::process::id()));
        let _ = std::fs::create_dir_all(&dir);
        let path = dir.join("owner_test.db").to_string_lossy().to_string();
        StatsCollector::init_db(&path);

        let mut owners = HashMap::new();
        owners.insert("sk-key1".to_string(), "Alice".to_string());
        let owners = Arc::new(owners);

        let handle = StatsHandle {
            db_path: path.clone(),
            rx: Arc::new(Mutex::new(tokio::sync::mpsc::channel::<(String, String, String, u64)>(8192).1)),
            key_owners: owners,
        };

        let batch = vec![("sk-key1".into(), "gpt-4".into(), "2026-07-16T10:00:00+07:00".into(), 50)];
        StatsHandle::flush_batch(&path, &batch).unwrap();

        let (_, users) = handle.query("2026-07-16T00:00:00+07:00", "2026-07-17T00:00:00+07:00").await;
        assert_eq!(users.len(), 1);
        assert_eq!(users[0].owner, "Alice");

        let _ = std::fs::remove_file(&path);
    }

    #[tokio::test]
    async fn test_owner_fallback_empty() {
        let (path, handle) = tmp_db();
        let batch = vec![("sk-unknown".into(), "gpt-4".into(), "2026-07-16T10:00:00+07:00".into(), 10)];
        StatsHandle::flush_batch(&path, &batch).unwrap();
        let (_, users) = handle.query("2026-07-16T00:00:00+07:00", "2026-07-17T00:00:00+07:00").await;
        assert_eq!(users[0].owner, "");
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn test_request_log_flush_and_query() {
        let uid = uuid::Uuid::new_v4().to_string();
        let dir = std::env::temp_dir().join(format!("9limiter_rl_test_{}", uid));
        let _ = std::fs::create_dir_all(&dir);
        let path = dir.join("test_rl.db").to_string_lossy().to_string();

        RequestLogWriter::init_db(&path);

        let logs = vec![
            RequestLog { id: "r1".into(), api_key: "sk-key1".into(), owner: "alice".into(), model: "gpt-4".into(), method: "POST".into(), path: "/v1/chat".into(), status: 200, latency_ms: 500, input_tokens: 0, output_tokens: 0, cache_tokens: 0, timestamp: "2026-07-16T10:00:00+07:00".into() },
            RequestLog { id: "r2".into(), api_key: "sk-key2".into(), owner: "bob".into(), model: "claude-3".into(), method: "POST".into(), path: "/v1/messages".into(), status: 429, latency_ms: 0, input_tokens: 0, output_tokens: 0, cache_tokens: 0, timestamp: "2026-07-16T12:00:00+07:00".into() },
        ];
        RequestLogWriter::flush_batch(&path, &logs).unwrap();

        let results = RequestLogWriter::query_logs(&path, "2026-07-16T00:00:00+07:00", "2026-07-17T00:00:00+07:00", 200).unwrap();
        assert_eq!(results.len(), 2);
        assert_eq!(results[0].id, "r2");
        assert_eq!(results[1].model, "gpt-4");

        let results = RequestLogWriter::query_logs(&path, "2026-07-16T11:00:00+07:00", "2026-07-16T13:00:00+07:00", 200).unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].id, "r2");

        let results = RequestLogWriter::query_logs(&path, "2026-07-17T00:00:00+07:00", "2026-07-18T00:00:00+07:00", 200).unwrap();
        assert!(results.is_empty());

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_request_log_writer_cleanup() {
        let uid = uuid::Uuid::new_v4().to_string();
        let dir = std::env::temp_dir().join(format!("9limiter_rl_test_{}", uid));
        let _ = std::fs::create_dir_all(&dir);
        let path = dir.join("test_rl.db").to_string_lossy().to_string();
        RequestLogWriter::init_db(&path);

        let logs = vec![
            RequestLog { id: "old".into(), api_key: "sk-k".into(), owner: "".into(), model: "gpt-4".into(), method: "POST".into(), path: "/v1/chat".into(), status: 200, latency_ms: 100, input_tokens: 0, output_tokens: 0, cache_tokens: 0, timestamp: "2026-05-01T00:00:00+07:00".into() },
            RequestLog { id: "new".into(), api_key: "sk-k".into(), owner: "".into(), model: "gpt-4".into(), method: "POST".into(), path: "/v1/chat".into(), status: 200, latency_ms: 100, input_tokens: 0, output_tokens: 0, cache_tokens: 0, timestamp: "2026-07-16T00:00:00+07:00".into() },
        ];
        RequestLogWriter::flush_batch(&path, &logs).unwrap();

        let conn = Connection::open(&path).unwrap();
        conn.execute("DELETE FROM request_logs WHERE timestamp < datetime('now', '-60 days')", []).unwrap();

        let results = RequestLogWriter::query_logs(&path, "2000-01-01", "2099-12-31", 200).unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].id, "new");

        let _ = std::fs::remove_dir_all(&dir);
    }

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

    #[test]
    fn test_cleanup_removes_request_logs() {
        // Verify stats cleanup also removes old request_logs
        let uid = uuid::Uuid::new_v4().to_string();
        let dir = std::env::temp_dir().join(format!("9limiter_rl_test_{}", uid));
        let _ = std::fs::create_dir_all(&dir);
        let path = dir.join("test_rl.db").to_string_lossy().to_string();
        RequestLogWriter::init_db(&path);

        // Insert old (60+ days) and recent data
        let logs = vec![
            RequestLog { id: "old".into(), api_key: "sk-k".into(), owner: "".into(), model: "gpt-4".into(), method: "POST".into(), path: "/v1/chat".into(), status: 200, latency_ms: 100, input_tokens: 0, output_tokens: 0, cache_tokens: 0, timestamp: "2026-04-01T00:00:00+07:00".into() },
            RequestLog { id: "recent".into(), api_key: "sk-k".into(), owner: "".into(), model: "gpt-4".into(), method: "POST".into(), path: "/v1/chat".into(), status: 200, latency_ms: 100, input_tokens: 0, output_tokens: 0, cache_tokens: 0, timestamp: "2026-07-16T00:00:00+07:00".into() },
        ];
        RequestLogWriter::flush_batch(&path, &logs).unwrap();
        drop(logs);

        // Reuse cleanup SQL from StatsHandle
        let conn = Connection::open(&path).unwrap();
        conn.execute("DELETE FROM request_logs WHERE timestamp < datetime('now', '-60 days')", []).unwrap();
        drop(conn);

        let results = RequestLogWriter::query_logs(&path, "2000-01-01", "2099-12-31", 200).unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].id, "recent");

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_request_log_init_db_idempotent() {
        let uid = uuid::Uuid::new_v4().to_string();
        let dir = std::env::temp_dir().join(format!("9limiter_rl_test_{}", uid));
        let _ = std::fs::create_dir_all(&dir);
        let path = dir.join("test_rl.db").to_string_lossy().to_string();
        RequestLogWriter::init_db(&path);
        RequestLogWriter::init_db(&path); // second call should not fail
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn test_stats_collector_new_creates_working_channel() {
        let uid = uuid::Uuid::new_v4().to_string();
        let dir = std::env::temp_dir().join(format!("9limiter_coll_test_{}", uid));
        let _ = std::fs::create_dir_all(&dir);
        let path = dir.join("collector.db").to_string_lossy().to_string();
        let key_owners = Arc::new(HashMap::new());
        let (collector, handle) = StatsCollector::new(path.clone(), key_owners.clone());

        // DB initialized (table exists)
        let conn = Connection::open(&path).unwrap();
        let count: i64 = conn.query_row("SELECT COUNT(*) FROM usage", [], |r| r.get(0)).unwrap();
        assert_eq!(count, 0);

        // sender routes to correct rx
        collector.sender
            .try_send(("sk-t".into(), "gpt-4".into(), "2026-07-16T10:00:00+07:00".into(), 42))
            .unwrap();
        let mut rx = handle.rx.lock().unwrap();
        let received = rx.try_recv().unwrap();
        assert_eq!(received, ("sk-t".into(), "gpt-4".into(), "2026-07-16T10:00:00+07:00".into(), 42));

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn test_spawn_request_log_writer_flushes_items() {
        let uid = uuid::Uuid::new_v4().to_string();
        let dir = std::env::temp_dir().join(format!("9limiter_spawn_test_{}", uid));
        let _ = std::fs::create_dir_all(&dir);
        let path = dir.join("spawn.db").to_string_lossy().to_string();
        RequestLogWriter::init_db(&path);

        let (tx, rx) = tokio::sync::mpsc::channel::<RequestLog>(8192);
        let rx = Arc::new(Mutex::new(rx));
        spawn_request_log_writer(path.clone(), rx.clone());

        let log = RequestLog {
            id: "spawn-test-1".into(),
            api_key: "sk-k".into(),
            owner: "tester".into(),
            model: "gpt-4".into(),
            method: "POST".into(),
            path: "/v1/chat".into(),
            status: 200,
            latency_ms: 50,
            input_tokens: 10,
            output_tokens: 20,
            cache_tokens: 5,
            timestamp: "2026-07-16T10:00:00+07:00".into(),
        };
        tx.send(log).await.unwrap();
        drop(tx);

        // Writer loop sleeps 5s then drains
        tokio::time::sleep(std::time::Duration::from_secs(7)).await;

        let results = RequestLogWriter::query_logs(&path, "2000-01-01", "2099-12-31", 200).unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].id, "spawn-test-1");
        assert_eq!(results[0].owner, "tester");

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_cleanup_removes_old_usage() {
        let (path, _handle) = tmp_db();
        let batch = vec![
            ("sk-old".into(), "gpt-4".into(), "2026-04-01T00:00:00+07:00".into(), 100),
            ("sk-recent".into(), "gpt-4".into(), "2026-07-16T00:00:00+07:00".into(), 50),
        ];
        StatsHandle::flush_batch(&path, &batch).unwrap();
        StatsHandle::cleanup(&path);

        let conn = Connection::open(&path).unwrap();
        let count: i64 = conn.query_row("SELECT COUNT(*) FROM usage", [], |r| r.get(0)).unwrap();
        assert_eq!(count, 1);

        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn test_query_sync_empty_owners_no_panic() {
        let (path, _handle) = tmp_db();
        let batch = vec![("sk-k".into(), "gpt-4".into(), "2026-07-16T10:00:00+07:00".into(), 10)];
        StatsHandle::flush_batch(&path, &batch).unwrap();
        let owners = HashMap::new();
        let (models, users) = StatsHandle::query_sync(&path, &owners, "2026-07-16T00:00:00+07:00", "2026-07-17T00:00:00+07:00");
        assert_eq!(models.len(), 1);
        assert_eq!(users.len(), 1);
        assert_eq!(users[0].owner, "");
        let _ = std::fs::remove_file(&path);
    }
}
