#[allow(unused_imports)]
use anyhow::Context;
use clap::Parser;
use log::LevelFilter;
use relay::http::Server;
use relay::postgres::pool::PgPool;
use relay::postgres::store::PgStore;
use sqlx::postgres::PgConnectOptions;
use sqlx::ConnectOptions;
use std::env;
use std::str::FromStr;
use std::time::Duration;
use tracing::info;

const VERSION: &str = env!("CARGO_PKG_VERSION");

#[derive(Debug, Parser)]
#[clap(version = env!("CARGO_PKG_VERSION"), author = env!("CARGO_PKG_AUTHORS"), about = env!("CARGO_PKG_DESCRIPTION"))]
pub struct Opts {
    /// HTTP Port to bind to.
    #[clap(long, default_value = "8080", env = "HTTP_PORT")]
    pub http_port: String,

    /// Metrics Port to bind to.
    #[cfg(feature = "metrics-prometheus")]
    #[clap(long, default_value = "5001", env = "METRICS_PORT")]
    pub metrics_port: String,

    /// DATABASE URL to connect to.
    #[clap(
        long,
        default_value = "postgres://username:pass@localhost:5432/relay?sslmode=disable",
        env = "DATABASE_URL"
    )]
    pub database_url: String,

    /// Maximum allowed database connections
    #[clap(long, default_value = "10", env = "DATABASE_MAX_CONNECTIONS")]
    pub database_max_connections: usize,

    /// This time interval, in seconds, between runs checking for retries and failed jobs.
    #[clap(long, default_value = "5", env = "REAP_INTERVAL")]
    pub reap_interval: u64,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    match std::env::var("RUST_LOG") {
        Err(_) => env::set_var("RUST_LOG", "info"),
        Ok(v) => {
            if v.trim() == "" {
                env::set_var("RUST_LOG", "info");
            }
        }
    };

    // install global collector configured based on RUST_LOG env var.
    tracing_subscriber::fmt::init();

    info!("Starting relay version {}", VERSION);

    let opts: Opts = Opts::parse();

    #[cfg(feature = "metrics-prometheus")]
    metrics_exporter_prometheus::PrometheusBuilder::new()
        .with_http_listener(
            format!("0.0.0.0:{}", &opts.metrics_port)
                .parse::<std::net::SocketAddr>()
                .context("invalid prometheus address")?,
        )
        .idle_timeout(
            metrics_util::MetricKindMask::COUNTER | metrics_util::MetricKindMask::HISTOGRAM,
            Some(std::time::Duration::from_secs(30)),
        )
        .add_global_label("app", "relay_rs")
        .install()
        .context("failed to install Prometheus recorder")?;

    let options = PgConnectOptions::from_str(&opts.database_url)?
        .log_statements(LevelFilter::Off)
        .log_slow_statements(LevelFilter::Warn, Duration::from_secs(1))
        .clone();

    let pool = PgPool::new(options, opts.database_max_connections)?;
    let pg = PgStore::new(pool).await?;

    Server::run(
        pg,
        &format!("0.0.0.0:{}", opts.http_port),
        Duration::from_secs(opts.reap_interval),
    )
    .await
}
