pub mod config;
mod limiter;
mod events;
mod proxy;
mod dashboard;
mod db;
mod stats;

use clap::{Parser, Subcommand};
use notify::{Config as NotifyConfig, EventKind, RecommendedWatcher, Watcher};
use std::collections::HashMap;
use std::sync::Arc;
use std::path::PathBuf;

fn default_config_path() -> String {
    let user = std::env::var("SUDO_USER").or_else(|_| std::env::var("USER")).unwrap_or_else(|_| "root".to_string());
    if user == "root" {
        "/root/.9limiter/config.yaml".to_string()
    } else {
        format!("/home/{}/.9limiter/config.yaml", user)
    }
}

fn parse_tz(s: &str) -> chrono::FixedOffset {
    s.parse().unwrap_or_else(|_| {
        tracing::warn!("invalid timezone '{}', falling back to +07:00", s);
        chrono::FixedOffset::east_opt(7 * 3600).unwrap()
    })
}

fn binary_path() -> String {
    std::env::current_exe()
        .map(|p| p.to_string_lossy().to_string())
        .unwrap_or_else(|_| "/usr/local/bin/ninelimiter".to_string())
}

fn service_unit(config: &str) -> String {
    let cfg_path = if config.starts_with('/') {
        config.to_string()
    } else {
        let cwd = std::env::current_dir().map(|p| p.to_string_lossy().to_string()).unwrap_or_default();
        format!("{}/{}", cwd, config)
    };
    let exe = binary_path();
    format!(
        "[Unit]\n\
         Description=9limiter rate-limiting proxy\n\
         After=network.target\n\
         \n\
         [Service]\n\
         Type=simple\n\
         ExecStart={} --config {}\n\
         Restart=always\n\
         RestartSec=5\n\
         StandardOutput=journal\n\
         StandardError=journal\n\
         \n\
         [Install]\n\
         WantedBy=multi-user.target\n",
        exe, cfg_path
    )
}

fn unit_path() -> PathBuf {
    PathBuf::from("/etc/systemd/system/ninelimiter.service")
}

#[derive(Parser)]
struct Args {
    #[command(subcommand)]
    cmd: Option<DaemonCmd>,

    #[arg(long, default_value_t = default_config_path())]
    config: String,
    #[arg(long)]
    listen: Option<String>,
    #[arg(long, default_value = "info")]
    log_level: String,
}

#[derive(Subcommand)]
enum DaemonCmd {
    /// Install, start, and enable systemd service
    Install {},
    /// Start service
    Start {},
    /// Stop service
    Stop {},
    /// Restart service
    Restart {},
    /// Stop and remove service file
    Remove {},
}

fn run_daemon(cmd: &DaemonCmd, config: &str) {
    // Re-exec via sudo if not root
    fn is_root() -> bool { std::process::Command::new("id").arg("-u").output().map(|o| std::str::from_utf8(&o.stdout).unwrap_or("").trim() == "0").unwrap_or(false) }
    if !is_root() {
        let exe = binary_path();
        let subcmd = match cmd {
            DaemonCmd::Install { .. } => "install",
            DaemonCmd::Start {} => "start",
            DaemonCmd::Stop {} => "stop",
            DaemonCmd::Restart {} => "restart",
            DaemonCmd::Remove {} => "remove",
        };
        let status = std::process::Command::new("sudo")
            .args([&exe, "--config", config, subcmd])
            .status()
            .expect("failed to exec sudo");
        std::process::exit(status.code().unwrap_or(1));
    }

    let unit = unit_path();
    match cmd {
        DaemonCmd::Install { .. } => {
            let content = service_unit(config);
            std::fs::write(&unit, content).expect("failed to write systemd unit (root?)");
            std::process::Command::new("systemctl")
                .args(["daemon-reload"])
                .status().ok();
            std::process::Command::new("systemctl")
                .args(["enable", "ninelimiter"])
                .status().ok();
            std::process::Command::new("systemctl")
                .args(["start", "ninelimiter"])
                .status().ok();
            println!("9limiter service installed and started.");
        }
        DaemonCmd::Start {} => {
            std::process::Command::new("systemctl")
                .args(["start", "ninelimiter"])
                .status().ok();
            println!("9limiter service started.");
        }
        DaemonCmd::Stop {} => {
            std::process::Command::new("systemctl")
                .args(["stop", "ninelimiter"])
                .status().ok();
            println!("9limiter service stopped.");
        }
        DaemonCmd::Restart {} => {
            std::process::Command::new("systemctl")
                .args(["restart", "ninelimiter"])
                .status().ok();
            println!("9limiter service restarted.");
        }
        DaemonCmd::Remove {} => {
            std::process::Command::new("systemctl")
                .args(["stop", "ninelimiter"])
                .status().ok();
            std::process::Command::new("systemctl")
                .args(["disable", "ninelimiter"])
                .status().ok();
            let _ = std::fs::remove_file(&unit);
            std::process::Command::new("systemctl")
                .args(["daemon-reload"])
                .status().ok();
            println!("9limiter service removed.");
        }
    }
}

#[tokio::main]
async fn main() {
    let args = Args::parse();

    // Ensure config file exists — write default if missing
    let config_path = PathBuf::from(&args.config);
    if !config_path.exists() {
        if let Some(parent) = config_path.parent() {
            let _ = std::fs::create_dir_all(parent);
        }
        let default = include_str!("../config.default.yaml");
        std::fs::write(&config_path, default).expect("failed to write default config");
        tracing_subscriber::fmt()
            .with_env_filter(&args.log_level)
            .init();
        tracing::info!("created default config at {}", config_path.display());
    }

    if let Some(ref cmd) = args.cmd {
        run_daemon(cmd, &args.config);
        return;
    }

    tracing_subscriber::fmt()
        .with_env_filter(&args.log_level)
        .init();

    let cfg = config::Config::from_file(&args.config)
        .expect("failed to parse config");

    tracing::info!("loaded config from {}", args.config);
    tracing::info!("{} rulesets configured", cfg.rulesets.len());
    tracing::info!("{} api key entries configured", cfg.api_keys.iter().map(|e| e.keys.len()).sum::<usize>());

    let config = Arc::new(tokio::sync::RwLock::new(cfg));

    // Load API key owners from PostgreSQL if configured
    let key_owners = {
        let cfg = config.read().await;
        match &cfg.database {
            Some(db) => {
                let host = db.host.as_deref().unwrap_or("localhost");
                let port = db.port.unwrap_or(5432);
                let user = db.user.as_deref().unwrap_or("postgres");
                let password = db.password.as_deref().unwrap_or("postgres");
                let dbname = db.dbname.as_deref().unwrap_or("postgres");
                db::fetch_key_names(host, port, user, password, dbname).await
            }
            None => {
                tracing::info!("no database configured, skipping key owner lookup");
                HashMap::new()
            }
        }
    };
    tracing::info!("loaded {} api key owners", key_owners.len());

    let tz = {
        let cfg = config.read().await;
        parse_tz(cfg.timezone.as_deref().unwrap_or("+07:00"))
    };

    let (event_tx, _) = tokio::sync::broadcast::channel(256);
    let limiter = limiter::SlidingLimiter::new();

    // Init stats collector
    let key_owners = Arc::new(key_owners);
    let stats_db = format!("{}/stats.db", config_path.parent().unwrap_or(std::path::Path::new("~/.9limiter")).display());
    let (stats_collector, stats_handle) = stats::StatsCollector::new(stats_db.clone(), key_owners.clone());
    let stats_handle = Arc::new(stats_handle);
    stats_handle.start();

    // Init request log writer
    let (rl_tx, rl_rx) = tokio::sync::mpsc::unbounded_channel();
    let rl_rx = Arc::new(std::sync::Mutex::new(rl_rx));
    stats::RequestLogWriter::init_db(&stats_db);
    stats::spawn_request_log_writer(stats_db.clone(), rl_rx);

    let http_client = reqwest::Client::new();
    let circuit_breaker = Arc::new(std::sync::Mutex::new(HashMap::new()));

    let state = proxy::AppState {
        config: config.clone(),
        limiter,
        event_tx: event_tx.clone(),
        upstream_idx: Default::default(),
        key_owners: key_owners.clone(),
        tz,
        stats_tx: stats_collector.sender.clone(),
        stats_handle: stats_handle.clone(),
        request_log_tx: rl_tx.clone(),
        stats_db_path: stats_db.clone(),
        http_client,
        circuit_breaker,
    };

    // Start hot-reload watcher
    let reload_state = state.clone();
    let cfg_path = args.config.clone();
    tokio::spawn(config_reload_loop(cfg_path, reload_state));

    let app = axum::Router::new()
        .route("/", axum::routing::get(dashboard::dashboard_handler))
        .route("/dashboard", axum::routing::get(dashboard::dashboard_handler))
        .route("/stats", axum::routing::get(stats::stats_page_handler))
        .route("/api/stats", axum::routing::get(stats::stats_handler))
        .route("/api/requests", axum::routing::get(stats::requests_handler))
        .route("/_ws", axum::routing::get(dashboard::ws_handler))
        .fallback(proxy::proxy_handler)
        .with_state(state);

    let listen_addr = {
        let cfg = config.read().await;
        args.listen.clone().or_else(|| cfg.listen.clone()).unwrap_or_else(|| "0.0.0.0:8080".to_string())
    };

    let listener = tokio::net::TcpListener::bind(&listen_addr).await
        .expect(&format!("failed to bind to {}", listen_addr));
    tracing::info!("proxy ready on {}", listen_addr);
    let upstream_count = config.read().await.upstreams.len();
    tracing::info!("upstreams configured: {}", upstream_count);
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
        // debounce: wait for writes to settle, drain duplicate events
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
        while rx.try_recv().is_ok() {}

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
    use crate::config::Rule;

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

    #[test]
    fn test_rate_limit_blocks() {
        let limiter = limiter::SlidingLimiter::new();
        for _ in 0..5 {
            assert!(limiter.check("k", "m", 5, 60).0);
        }
        for _ in 0..10 {
            assert!(!limiter.check("k", "m", 5, 60).0);
        }
    }

    #[test]
    fn test_rule_matching() {
        let rule = Rule {
            models: vec!["gpt-4".into()],
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
        assert!(!proxy::rule_matches(&rule, "gpt-4", "Mon", "06:59"));
    }

    #[test]
    fn test_multiple_rules() {
        let limiter = limiter::SlidingLimiter::new();
        // Same (key, model) hit by 2 rules: limit=2 and limit=3
        let (passed, _) = limiter.check("k", "m", 2, 60);
        assert!(passed);
        let (passed, _) = limiter.check("k", "m", 3, 60);
        assert!(passed);
        let (passed, _) = limiter.check("k", "m", 2, 60);
        assert!(passed);
        let (passed, _) = limiter.check("k", "m", 3, 60);
        assert!(passed);
        // 3rd request for limit=2 rule blocked
        let (passed, _) = limiter.check("k", "m", 2, 60);
        assert!(!passed);
    }

    #[test]
    fn test_overlap_does_not_crash() {
        let yaml = r#"
upstreams:
  - path_prefix: "/v1/chat"
    base_url: "https://api.openai.com"
rulesets:
  - name: test
    rules:
      - model: "gpt-4"
        limit: 100
        window_secs: 3600
        time_start: "09:00"
        time_end: "17:00"
        days: [Mon, Tue, Wed, Thu, Fri]
      - model: "gpt-4"
        limit: 50
        window_secs: 3600
        time_start: "12:00"
        time_end: "14:00"
        days: [Mon, Wed, Fri]
api_keys:
  - ruleset: test
    keys: ["sk-test"]
"#;
        let cfg: config::Config = serde_yaml::from_str(yaml).unwrap();
        cfg.validate().unwrap();
    }

    #[tokio::test]
    async fn test_config_swap() {
        let cfg = config::Config::from_file("config.yaml").unwrap();
        let config = Arc::new(tokio::sync::RwLock::new(cfg));

        let before = { config.read().await.listen.clone() };
        let mut new_cfg = config::Config::from_file("config.yaml").unwrap();
        new_cfg.listen = Some(":9090".into());
        *config.write().await = new_cfg;
        let after = { config.read().await.listen.clone() };

        assert_ne!(before, after);
        assert_eq!(after, Some(":9090".into()));
    }
}
