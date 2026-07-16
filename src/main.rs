pub mod config;
mod limiter;
mod events;
mod proxy;
mod dashboard;

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
