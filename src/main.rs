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
