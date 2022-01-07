#[allow(unused_imports)]
use anyhow::Context;
use clap::Parser;
use relay_rs::store::memory::backing;
use relay_rs::store::memory::Store as MemoryStore;
use relay_rs::store::server::http::Server;
use std::env;

#[derive(Debug, Parser)]
#[clap(version = env!("CARGO_PKG_VERSION"), author = env!("CARGO_PKG_AUTHORS"), about = env!("CARGO_PKG_DESCRIPTION"))]
pub struct Opts {
    /// HTTP Port to bind to.
    #[clap(long, default_value = "8080", env = "SERVER_PORT")]
    pub server_port: String,

    /// Metrics Port to bind to.
    #[cfg(feature = "prometheus_metrics")]
    #[clap(long, default_value = "5001", env = "METRICS_PORT")]
    pub metrics_port: String,

    /// DATABASE_URL to connect to.
    #[cfg(feature = "sqlite_backing")]
    #[clap(short, long, default_value = "test.jobs.db", env = "DATABASE_URL")]
    pub database_url: String,

    /// DATABASE_URL to connect to.
    #[cfg(feature = "redis_backing")]
    #[clap(
        short,
        long,
        default_value = "redis://localhost:6379/",
        env = "DATABASE_URL"
    )]
    pub database_url: String,

    /// DATABASE_URL to connect to.
    #[cfg(feature = "postgres_backing")]
    #[clap(
        short,
        long,
        default_value = "postgres://username:pass@localhost:5432/dev?sslmode=disable",
        env = "DATABASE_URL"
    )]
    pub database_url: String,

    #[cfg(feature = "dynamodb_backing")]
    #[clap(long, default_value = "localhost", env = "AWS_REGION")]
    pub region: String,

    #[cfg(feature = "dynamodb_backing")]
    #[clap(long, default_value = "relay_backing", env = "TABLE")]
    pub table: String,
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

    let opts: Opts = Opts::parse();

    #[cfg(feature = "prometheus_metrics")]
    metrics_exporter_prometheus::PrometheusBuilder::new()
        .listen_address(
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

    #[cfg(not(any(
        feature = "sqlite_backing",
        feature = "postgres_backing",
        feature = "redis_backing",
        feature = "dynamodb_backing"
    )))]
    let backing = backing::noop::Store::default();

    #[cfg(feature = "sqlite_backing")]
    let backing = backing::sqlite::Store::default(&opts.database_url).await?;

    #[cfg(feature = "postgres_backing")]
    let backing = backing::postgres::Store::default(&opts.database_url).await?;

    #[cfg(feature = "redis_backing")]
    let backing = backing::redis::Store::default(&opts.database_url).await?;

    #[cfg(feature = "dynamodb_backing")]
    let backing = backing::dynamodb::Store::default(opts.region, opts.table).await?;

    let memory_store = MemoryStore::new(backing).await?;

    Server::run(memory_store, &format!("0.0.0.0:{}", opts.server_port)).await
}
