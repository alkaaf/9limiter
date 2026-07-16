pub mod config;
mod limiter;
mod events;
mod proxy;
mod dashboard;

use clap::Parser;
use notify::{Config as NotifyConfig, EventKind, RecommendedWatcher, Watcher};
use std::sync::Arc;

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

    let cfg = config::Config::from_file(&args.config)
        .expect("failed to parse config");

    let config = Arc::new(tokio::sync::RwLock::new(cfg));
    let (event_tx, _) = tokio::sync::broadcast::channel(256);
    let limiter = limiter::SlidingLimiter::new();

    let state = proxy::AppState {
        config: config.clone(),
        limiter,
        event_tx: event_tx.clone(),
    };

    // Start hot-reload watcher
    let reload_state = state.clone();
    let cfg_path = args.config.clone();
    tokio::spawn(config_reload_loop(cfg_path, reload_state));

    let app = axum::Router::new()
        .route("/", axum::routing::get(dashboard::dashboard_handler))
        .route("/dashboard", axum::routing::get(dashboard::dashboard_handler))
        .route("/_ws", axum::routing::get(dashboard::ws_handler))
        .fallback(proxy::proxy_handler)
        .with_state(state);

    let listen_addr = {
        let cfg = config.read().await;
        args.listen.clone().or_else(|| cfg.listen.clone()).unwrap_or_else(|| ":8080".to_string())
    };

    let listener = tokio::net::TcpListener::bind(&listen_addr).await.unwrap();
    tracing::info!("listening on {}", listen_addr);
    axum::serve(listener, app).await.unwrap();
}

async fn config_reload_loop(
    config_path: String,
    app_state: proxy::AppState,
) {
    let (tx, mut rx) = tokio::sync::mpsc::channel(1);
    let mut watcher = match RecommendedWatcher::new(
        move |res: Result<notify::Event, notify::Error>| {
            if let Ok(event) = res {
                if matches!(event.kind, EventKind::Modify(_)) {
                    let _ = tx.blocking_send(());
                }
            }
        },
        NotifyConfig::default(),
    ) {
        Ok(w) => w,
        Err(e) => {
            tracing::error!("failed to create file watcher: {}", e);
            return;
        }
    };

    if let Err(e) = watcher.watch(std::path::Path::new(&config_path), notify::RecursiveMode::NonRecursive) {
        tracing::error!("failed to watch config: {}", e);
        return;
    }

    while rx.recv().await.is_some() {
        tracing::info!("config file changed, reloading...");
        match config::Config::from_file(&config_path) {
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_config() {
        let cfg = config::Config::from_file("config.yaml").unwrap();
        assert_eq!(cfg.rulesets.len(), 2);
        assert_eq!(cfg.api_keys.len(), 1);
        assert_eq!(cfg.api_keys[0].keys.len(), 2);
    }

    #[test]
    fn test_sliding_limiter() {
        let limiter = limiter::SlidingLimiter::new();

        assert!(limiter.check("key1", "gpt-4", 3, 60).0);
        assert!(limiter.check("key1", "gpt-4", 3, 60).0);
        assert!(limiter.check("key1", "gpt-4", 3, 60).0);
        assert!(!limiter.check("key1", "gpt-4", 3, 60).0);

        assert!(limiter.check("key2", "gpt-4", 3, 60).0);
    }
}
