#[allow(unused_imports)]
use anyhow::Context;
use clap::Parser;
use relay_core::Backend;
use serde_json::value::RawValue;
use std::env;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::oneshot;
use tracing::{error, info};

#[derive(Debug, Parser)]
#[clap(version = env!("CARGO_PKG_VERSION"), author = env!("CARGO_PKG_AUTHORS"), about = env!("CARGO_PKG_DESCRIPTION"))]
pub struct Opts {
    /// HTTP Port to bind to.
    #[cfg(feature = "frontend-http")]
    #[clap(long, default_value = "8080", env = "HTTP_PORT")]
    pub http_port: String,

    /// Metrics Port to bind to.
    #[cfg(feature = "metrics-prometheus")]
    #[clap(long, default_value = "5001", env = "METRICS_PORT")]
    pub metrics_port: String,

    /// DATABASE URL to connect to.
    #[cfg(feature = "backend-postgres")]
    #[clap(
        long,
        default_value = "postgres://username:pass@localhost:5432/relay?sslmode=disable",
        env = "DATABASE_URL"
    )]
    pub database_url: String,

    /// Maximum allowed database connections
    #[cfg(feature = "backend-postgres")]
    #[clap(long, default_value = "10", env = "DATABASE_MAX_CONNECTIONS")]
    pub database_max_connections: u32,

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
            Some(Duration::from_secs(30)),
        )
        .add_global_label("app", "relay_rs")
        .install()
        .context("failed to install Prometheus recorder")?;

    #[cfg(feature = "backend-postgres")]
    let backend = Arc::new(init_postgres(&opts).await?);
    let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();
    let reap_be = backend.clone();
    let reaper = tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_secs(opts.reap_interval as u64));
        interval.reset();
        tokio::pin!(shutdown_rx);

        loop {
            tokio::select! {
                _ = interval.tick() => {
                    if let Err(e) = reap_be.reap(opts.reap_interval).await {
                        error!("error occurred reaping jobs. {}", e.to_string());
                    }
                    interval.reset();
                },
                _ = (&mut shutdown_rx) => break
            }
        }
    });

    #[cfg(feature = "frontend-http")]
    relay_frontend_http::Server::run(backend, &format!("0.0.0.0:{}", opts.http_port)).await?;

    drop(shutdown_tx);

    reaper.await?;
    info!("Reaper shutdown");

    Ok(())
}

#[cfg(feature = "backend-postgres")]
async fn init_postgres(opts: &Opts) -> anyhow::Result<impl Backend<Box<RawValue>, Box<RawValue>>> {
    let min_connections = if opts.database_max_connections < 10 {
        1
    } else {
        10
    };
    Ok(relay_backend_postgres::PgStore::new(
        &opts.database_url,
        min_connections,
        opts.database_max_connections,
    )
    .await?)
}
