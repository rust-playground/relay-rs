#[allow(unused_imports)]
use anyhow::Context;
use clap::Parser;
use deadpool_postgres::{
    ClientWrapper, Hook, HookError, HookErrorCause, Manager, ManagerConfig, Pool, RecyclingMethod,
};
use relay::http::Server;
use relay::postgres::PgStore;
use std::env;
use std::str::FromStr;
use std::time::Duration;
use tokio_postgres::{Config as PostgresConfig, NoTls};

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

    let mut pg_config = PostgresConfig::from_str(&opts.database_url)?;
    if pg_config.get_connect_timeout().is_none() {
        pg_config.connect_timeout(Duration::from_secs(5));
    }
    if pg_config.get_application_name().is_none() {
        pg_config.application_name("relay");
    }
    let mgr = Manager::from_config(
        pg_config,
        NoTls,
        ManagerConfig {
            recycling_method: RecyclingMethod::Fast,
        },
    );
    let pool = Pool::builder(mgr)
        .max_size(opts.database_max_connections)
        .post_create(Hook::async_fn(|client: &mut ClientWrapper, _| {
            Box::pin(async move {
                client
                    .simple_query("SET default_transaction_isolation TO 'read committed'")
                    .await
                    .map_err(|e| HookError::Abort(HookErrorCause::Backend(e)))?;
                Ok(())
            })
        }))
        .build()?;

    let pg = PgStore::new_with_pool(pool).await?;

    Server::run(
        pg,
        &format!("0.0.0.0:{}", opts.http_port),
        Duration::from_secs(opts.reap_interval),
    )
    .await
}
