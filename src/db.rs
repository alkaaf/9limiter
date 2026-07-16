use std::collections::HashMap;
use sqlx::Connection;

/// Fetch all active API keys and their owner names from PostgreSQL.
/// Returns empty map on any failure (DB unavailable, bad config — log the error, keep running).
pub async fn fetch_key_names(
    host: &str,
    port: u16,
    user: &str,
    password: &str,
    dbname: &str,
) -> HashMap<String, String> {
    let dsn = format!("postgres://{}:{}@{}:{}/{}", user, password, host, port, dbname);
    tracing::info!("connecting to postgresql at {}:{}", host, port);

    let mut conn = match sqlx::postgres::PgConnection::connect(&dsn).await {
        Ok(c) => c,
        Err(e) => {
            tracing::error!("failed to connect to postgresql: {}", e);
            return HashMap::new();
        }
    };

    let rows: Vec<(String, String)> = match sqlx::query_as(
        "SELECT key, name FROM apikeys WHERE isactive = true OR isactive IS NULL"
    )
    .fetch_all(&mut conn)
    .await
    {
        Ok(r) => r,
        Err(e) => {
            tracing::error!("failed to query apikeys: {}", e);
            return HashMap::new();
        }
    };

    let map: HashMap<String, String> = rows.into_iter().collect();
    tracing::info!("loaded {} api key owners from postgresql", map.len());
    map
}
