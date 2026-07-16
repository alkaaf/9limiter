use std::collections::HashMap;
use std::sync::{Arc, Mutex};
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
    db_path: String,
    pub sender: tokio::sync::mpsc::UnboundedSender<(String, String, String, u64)>,
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
            for (hour, api_key, model, tokens) in batch {
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
