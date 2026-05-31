//! Command-line entry point for the deployable matchmaker server.

use clap::Parser;

#[derive(Debug, Parser)]
struct Args {
    #[arg(
        long,
        default_value = "examples/bevy_local_static/config/matchmaker.local.toml"
    )]
    config: String,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "lightyear_matchmaker_server=info,tower_http=info".into()),
        )
        .init();
    let args = Args::parse();
    lightyear_matchmaker_server::run_from_config_path(args.config).await
}
