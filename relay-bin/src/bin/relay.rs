#[allow(unused_imports)]
use anyhow::Context;
use clap::Parser;
use relay::memory_store::Store;
use relay_server_http::Server;
use std::env;

#[derive(Debug, Parser)]
#[clap(version = env!("CARGO_PKG_VERSION"), author = env!("CARGO_PKG_AUTHORS"), about = env!("CARGO_PKG_DESCRIPTION"))]
pub struct Opts {
    /// HTTP Port to bind to.
    #[clap(long, default_value = "8080", env = "SERVER_PORT")]
    pub server_port: String,

    /// Metrics Port to bind to.
    #[cfg(feature = "metrics-prometheus")]
    #[clap(long, default_value = "5001", env = "METRICS_PORT")]
    pub metrics_port: String,

    /// DATABASE_URL to connect to.
    #[cfg(feature = "backing-sqlite")]
    #[clap(short, long, default_value = "test.jobs.db", env = "DATABASE_URL")]
    pub database_url: String,

    /// DATABASE_URL to connect to.
    #[cfg(feature = "backing-redis")]
    #[clap(
        short,
        long,
        default_value = "redis://localhost:6379/",
        env = "DATABASE_URL"
    )]
    pub database_url: String,

    /// DATABASE_URL to connect to.
    #[cfg(feature = "backing-postgres")]
    #[clap(
        short,
        long,
        default_value = "postgres://username:pass@localhost:5432/dev?sslmode=disable",
        env = "DATABASE_URL"
    )]
    pub database_url: String,

    #[cfg(feature = "backing-dynamodb")]
    #[clap(long, default_value = "localhost", env = "AWS_REGION")]
    pub region: String,

    #[cfg(feature = "backing-dynamodb")]
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

    #[cfg(not(any(
        feature = "backing-sqlite",
        feature = "backing-postgres",
        feature = "backing-redis",
        feature = "backing-dynamodb"
    )))]
    let backing = relay::memory_store::backing::noop::Store::default();

    #[cfg(feature = "backing-sqlite")]
    let backing = relay_backing_sqlite::Store::default(&opts.database_url).await?;

    #[cfg(feature = "backing-postgres")]
    let backing = relay_backing_postgres::Store::default(&opts.database_url).await?;

    #[cfg(feature = "backing-redis")]
    let backing = relay_backing_redis::Store::default(&opts.database_url).await?;

    #[cfg(feature = "backing-dynamodb")]
    let backing = relay_backing_dynamodb::Store::default(opts.region, opts.table).await?;

    let memory_store = Store::new(backing).await?;

    Server::run(memory_store, &format!("0.0.0.0:{}", opts.server_port)).await
}
