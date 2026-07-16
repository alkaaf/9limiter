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
    pub sender: tokio::sync::mpsc::UnboundedSender<(String, String, String, u64)>,
    #[allow(dead_code)]
    key_owners: Arc<HashMap<String, String>>,
}

impl StatsCollector {
    pub fn new(
        db_path: String,
        key_owners: Arc<HashMap<String, String>>,
    ) -> (Self, StatsHandle) {
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();

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

pub struct StatsHandle {
    db_path: String,
    rx: Arc<Mutex<tokio::sync::mpsc::UnboundedReceiver<(String, String, String, u64)>>>,
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
                    if let Err(e) = Self::flush_batch(&db_path, &batch) {
                        tracing::error!("stats flush error: {}", e);
                    }
                }

                // Hourly cleanup
                if last_cleanup.elapsed().as_secs() >= 3600 {
                    Self::cleanup(&db_path);
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
    }

    pub fn query(&self, start: &str, end: &str) -> (Vec<ModelStat>, Vec<UserStat>) {
        let conn = match Connection::open(&self.db_path) {
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
            let owner = self.key_owners.get(&api_key).cloned().unwrap_or_default();
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
    let (models, users) = state.stats_handle.query(&start, &end);
    axum::Json(serde_json::json!({ "models": models, "users": users }))
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

    pub fn flush_batch(db_path: &str, batch: &[RequestLog]) -> Result<(), rusqlite::Error> {
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

    pub fn query_logs(db_path: &str, start: &str, end: &str, limit: usize) -> Result<Vec<RequestLog>, rusqlite::Error> {
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
            rx: Arc::new(Mutex::new(tokio::sync::mpsc::unbounded_channel::<(String, String, String, u64)>().1)),
            key_owners: Arc::new(HashMap::new()),
        };
        (path, handle)
    }

    #[test]
    fn test_flush_and_query() {
        let (path, handle) = tmp_db();
        let mut batch: Vec<(String, String, String, u64)> = Vec::new();
        batch.push(("sk-key1".into(), "gpt-4".into(), "2026-07-16T10:00:00+07:00".into(), 50));
        batch.push(("sk-key1".into(), "gpt-4".into(), "2026-07-16T10:00:00+07:00".into(), 30)); // same key+model+hour → merged
        batch.push(("sk-key1".into(), "claude-3".into(), "2026-07-16T10:00:00+07:00".into(), 100));
        batch.push(("sk-key2".into(), "gpt-4".into(), "2026-07-16T11:00:00+07:00".into(), 200));
        StatsHandle::flush_batch(&path, &batch).unwrap();

        // Query all
        let (models, users) = handle.query("2026-07-16T00:00:00+07:00", "2026-07-17T00:00:00+07:00");
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

    #[test]
    fn test_query_empty_db() {
        let (path, handle) = tmp_db();
        let (models, users) = handle.query("2026-07-01T00:00:00+07:00", "2026-07-02T00:00:00+07:00");
        assert!(models.is_empty());
        assert!(users.is_empty());
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn test_query_partial_range() {
        let (path, handle) = tmp_db();
        let mut batch: Vec<(String, String, String, u64)> = Vec::new();
        batch.push(("sk-key1".into(), "gpt-4".into(), "2026-07-16T10:00:00+07:00".into(), 50));
        StatsHandle::flush_batch(&path, &batch).unwrap();

        // Query outside range → empty
        let (models, users) = handle.query("2026-07-17T00:00:00+07:00", "2026-07-18T00:00:00+07:00");
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

    #[test]
    fn test_key_owner_in_query() {
        let dir = std::env::temp_dir().join(format!("9limiter_stats_test_{}", std::process::id()));
        let _ = std::fs::create_dir_all(&dir);
        let path = dir.join("owner_test.db").to_string_lossy().to_string();
        StatsCollector::init_db(&path);

        let mut owners = HashMap::new();
        owners.insert("sk-key1".to_string(), "Alice".to_string());
        let owners = Arc::new(owners);

        let handle = StatsHandle {
            db_path: path.clone(),
            rx: Arc::new(Mutex::new(tokio::sync::mpsc::unbounded_channel::<(String, String, String, u64)>().1)),
            key_owners: owners,
        };

        let batch = vec![("sk-key1".into(), "gpt-4".into(), "2026-07-16T10:00:00+07:00".into(), 50)];
        StatsHandle::flush_batch(&path, &batch).unwrap();

        let (_, users) = handle.query("2026-07-16T00:00:00+07:00", "2026-07-17T00:00:00+07:00");
        assert_eq!(users.len(), 1);
        assert_eq!(users[0].owner, "Alice");

        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn test_owner_fallback_empty() {
        let (path, handle) = tmp_db();
        let batch = vec![("sk-unknown".into(), "gpt-4".into(), "2026-07-16T10:00:00+07:00".into(), 10)];
        StatsHandle::flush_batch(&path, &batch).unwrap();
        let (_, users) = handle.query("2026-07-16T00:00:00+07:00", "2026-07-17T00:00:00+07:00");
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
            RequestLog { id: "r1".into(), api_key: "sk-key1".into(), owner: "alice".into(), model: "gpt-4".into(), method: "POST".into(), path: "/v1/chat".into(), status: 200, latency_ms: 500, timestamp: "2026-07-16T10:00:00+07:00".into() },
            RequestLog { id: "r2".into(), api_key: "sk-key2".into(), owner: "bob".into(), model: "claude-3".into(), method: "POST".into(), path: "/v1/messages".into(), status: 429, latency_ms: 0, timestamp: "2026-07-16T12:00:00+07:00".into() },
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
            RequestLog { id: "old".into(), api_key: "sk-k".into(), owner: "".into(), model: "gpt-4".into(), method: "POST".into(), path: "/v1/chat".into(), status: 200, latency_ms: 100, timestamp: "2026-05-01T00:00:00+07:00".into() },
            RequestLog { id: "new".into(), api_key: "sk-k".into(), owner: "".into(), model: "gpt-4".into(), method: "POST".into(), path: "/v1/chat".into(), status: 200, latency_ms: 100, timestamp: "2026-07-16T00:00:00+07:00".into() },
        ];
        RequestLogWriter::flush_batch(&path, &logs).unwrap();

        let conn = Connection::open(&path).unwrap();
        conn.execute("DELETE FROM request_logs WHERE timestamp < datetime('now', '-60 days')", []).unwrap();

        let results = RequestLogWriter::query_logs(&path, "2000-01-01", "2099-12-31", 200).unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].id, "new");

        let _ = std::fs::remove_dir_all(&dir);
    }
}
