use crate::postgres::PgStore;
use crate::{Error, Job};
use actix_web::http::StatusCode;
use actix_web::middleware::Logger;
use actix_web::{web, App, HttpResponse, HttpServer};
use metrics::increment_counter;
use serde::Deserialize;
use serde_json::value::RawValue;
use std::time::Duration;
use tokio::sync::oneshot;
use tracing::{error, info};

/// The internal HTTP server representation for Jobs.
pub struct Server;

async fn enqueue(data: web::Data<Data>, job: web::Json<Job>) -> HttpResponse {
    increment_counter!("http_request", "endpoint" => "enqueued", "queue" => job.0.queue.clone());

    if let Err(e) = data.pg_store.enqueue(&job.0).await {
        increment_counter!("errors", "endpoint" => "enqueued", "type" => e.error_type(), "queue" => e.queue());
        match e {
            Error::JobExists { .. } => {
                HttpResponse::build(StatusCode::CONFLICT).body(e.to_string())
            }
            Error::Postgres { .. } => {
                if e.is_retryable() {
                    HttpResponse::build(StatusCode::TOO_MANY_REQUESTS).body(e.to_string())
                } else {
                    HttpResponse::build(StatusCode::UNPROCESSABLE_ENTITY).body(e.to_string())
                }
            }
            Error::JobNotFound { .. } => {
                HttpResponse::build(StatusCode::INTERNAL_SERVER_ERROR).body(e.to_string())
            }
        }
    } else {
        HttpResponse::build(StatusCode::ACCEPTED).finish()
    }
}

#[derive(Deserialize)]
struct NextInfo {
    queue: String,
}

async fn next(data: web::Data<Data>, info: web::Query<NextInfo>) -> HttpResponse {
    increment_counter!("http_request", "endpoint" => "next", "queue" => info.queue.clone());

    match data.pg_store.next(&info.queue).await {
        Err(e) => {
            increment_counter!("errors", "endpoint" => "next", "type" => e.error_type(), "queue" => e.queue());
            if let Error::Postgres { .. } = e {
                if e.is_retryable() {
                    HttpResponse::build(StatusCode::TOO_MANY_REQUESTS).body(e.to_string())
                } else {
                    HttpResponse::build(StatusCode::INTERNAL_SERVER_ERROR).body(e.to_string())
                }
            } else {
                HttpResponse::build(StatusCode::INTERNAL_SERVER_ERROR).body(e.to_string())
            }
        }
        Ok(job) => match job {
            None => HttpResponse::build(StatusCode::NO_CONTENT).finish(),
            Some(job) => HttpResponse::build(StatusCode::OK).json(job),
        },
    }
}

#[derive(Deserialize)]
struct HeartbeatInfo {
    queue: String,
    job_id: String,
}

async fn heartbeat(
    data: web::Data<Data>,
    info: web::Query<HeartbeatInfo>,
    state: Option<web::Json<Box<RawValue>>>,
) -> HttpResponse {
    increment_counter!("http_request", "endpoint" => "heartbeat", "queue" => info.queue.clone());

    let state = match state {
        None => None,
        Some(state) => Some(state.0),
    };
    if let Err(e) = data.pg_store.update(&info.queue, &info.job_id, state).await {
        increment_counter!("errors", "endpoint" => "heartbeat", "type" => e.error_type(), "queue" => e.queue());
        match e {
            Error::JobNotFound { .. } => {
                HttpResponse::build(StatusCode::NOT_FOUND).body(e.to_string())
            }
            Error::Postgres { .. } => {
                if e.is_retryable() {
                    HttpResponse::build(StatusCode::TOO_MANY_REQUESTS).body(e.to_string())
                } else {
                    HttpResponse::build(StatusCode::UNPROCESSABLE_ENTITY).body(e.to_string())
                }
            }
            Error::JobExists { .. } => {
                HttpResponse::build(StatusCode::INTERNAL_SERVER_ERROR).body(e.to_string())
            }
        }
    } else {
        HttpResponse::build(StatusCode::ACCEPTED).finish()
    }
}

#[derive(Deserialize)]
struct CompleteInfo {
    queue: String,
    job_id: String,
}

async fn complete(data: web::Data<Data>, info: web::Query<CompleteInfo>) -> HttpResponse {
    increment_counter!("http_request", "endpoint" => "complete", "queue" => info.queue.clone());

    if let Err(e) = data.pg_store.remove(&info.queue, &info.job_id).await {
        increment_counter!("errors", "endpoint" => "complete", "type" => e.error_type(), "queue" => e.queue());
        match e {
            Error::JobNotFound { .. } => {
                HttpResponse::build(StatusCode::NOT_FOUND).body(e.to_string())
            }
            Error::Postgres { .. } => {
                if e.is_retryable() {
                    HttpResponse::build(StatusCode::TOO_MANY_REQUESTS).body(e.to_string())
                } else {
                    HttpResponse::build(StatusCode::UNPROCESSABLE_ENTITY).body(e.to_string())
                }
            }
            Error::JobExists { .. } => {
                HttpResponse::build(StatusCode::INTERNAL_SERVER_ERROR).body(e.to_string())
            }
        }
    } else {
        HttpResponse::build(StatusCode::OK).finish()
    }
}

fn health() -> HttpResponse {
    HttpResponse::build(StatusCode::OK).finish()
}

struct Data {
    pg_store: PgStore,
}

impl Server {
    /// starts the HTTP server and waits for a shutdown signal before returning.
    ///
    /// # Errors
    ///
    /// Will return `Err` if the server fails to start.
    ///
    /// # Panics
    ///
    /// Will panic the reaper async thread fails, which can only happen if the timer and channel
    /// both die.
    #[inline]
    pub async fn run(pg_store: PgStore, addr: &str) -> anyhow::Result<()> {
        let store = web::Data::new(Data { pg_store });

        let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();
        let reap_store = store.clone();
        let reaper = tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(5));
            interval.reset();
            tokio::pin!(shutdown_rx);

            loop {
                tokio::select! {
                    _ = interval.tick() => {
                        if let Err(e) = reap_store.pg_store.reap_timeouts().await {
                            error!("error occurred reaping jobs. {}", e.to_string());
                        }
                        interval.reset();
                    },
                    _ = (&mut shutdown_rx) => break
                }
            }
        });

        HttpServer::new(move || {
            App::new()
                .app_data(store.clone())
                .wrap(Logger::new("%a %r %s %Dms"))
                .route("/enqueue", web::post().to(enqueue))
                .route("/heartbeat", web::patch().to(heartbeat))
                .route("/complete", web::delete().to(complete))
                .route("/next", web::get().to(next))
                .route("/health", web::get().to(health))
        })
        .bind(addr)?
        .run()
        .await?;

        drop(shutdown_tx);

        reaper.await?;
        info!("Reaper shutdown");
        Ok(())
    }
}
