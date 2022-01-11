use actix_web::http::StatusCode;
use actix_web::middleware::Logger;
use actix_web::{web, App, HttpResponse, HttpServer};
use metrics::increment_counter;
use relay::memory_store::backing::Backing;
use relay::memory_store::{Error, Store};
use relay::Job;
use serde::Deserialize;
use serde_json::value::RawValue;
use std::time::Duration;
use tokio::sync::oneshot;
use tokio_stream::StreamExt;
use tracing::{error, info, warn};

/// The internal HTTP server representation for Jobs.
pub struct Server;

async fn enqueue<B>(data: web::Data<Data<B>>, job: web::Json<Job>) -> HttpResponse
where
    B: Backing + Send + Sync,
{
    increment_counter!("http_request", "endpoint" => "enqueued", "queue" => job.0.queue.clone());

    if let Err(e) = data.job_store.enqueue(job.0).await {
        increment_counter!("errors", "endpoint" => "enqueued", "type" => e.error_type(), "queue" => e.queue());
        match e {
            Error::JobExists { .. } => {
                HttpResponse::build(StatusCode::CONFLICT).body(e.to_string())
            }
            Error::Backing(e) => {
                if e.is_retryable() {
                    HttpResponse::build(StatusCode::TOO_MANY_REQUESTS).body(e.to_string())
                } else {
                    HttpResponse::build(StatusCode::UNPROCESSABLE_ENTITY).body(e.to_string())
                }
            }
            _ => HttpResponse::build(StatusCode::INTERNAL_SERVER_ERROR).body(e.to_string()),
        }
    } else {
        HttpResponse::build(StatusCode::ACCEPTED).finish()
    }
}

#[derive(Deserialize)]
struct NextInfo {
    queue: String,
}

async fn next<B>(data: web::Data<Data<B>>, info: web::Query<NextInfo>) -> HttpResponse
where
    B: Backing + Send + Sync,
{
    increment_counter!("http_request", "endpoint" => "next", "queue" => info.queue.clone());

    match data.job_store.next(&info.queue).await {
        Err(e) => {
            increment_counter!("errors", "endpoint" => "next", "type" => e.error_type(), "queue" => e.queue());
            if let Error::Backing(e) = e {
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

async fn heartbeat<B>(
    data: web::Data<Data<B>>,
    info: web::Query<HeartbeatInfo>,
    state: Option<web::Json<Box<RawValue>>>,
) -> HttpResponse
where
    B: Backing + Send + Sync,
{
    increment_counter!("http_request", "endpoint" => "heartbeat", "queue" => info.queue.clone());

    let state = match state {
        None => None,
        Some(state) => Some(state.0),
    };
    if let Err(e) = data.job_store.touch(&info.queue, &info.job_id, state).await {
        increment_counter!("errors", "endpoint" => "heartbeat", "type" => e.error_type(), "queue" => e.queue());
        match e {
            Error::JobNotFound { .. } => {
                HttpResponse::build(StatusCode::NOT_FOUND).body(e.to_string())
            }
            Error::Backing(e) => {
                if e.is_retryable() {
                    HttpResponse::build(StatusCode::TOO_MANY_REQUESTS).body(e.to_string())
                } else {
                    HttpResponse::build(StatusCode::UNPROCESSABLE_ENTITY).body(e.to_string())
                }
            }
            _ => HttpResponse::build(StatusCode::INTERNAL_SERVER_ERROR).body(e.to_string()),
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

async fn complete<B>(data: web::Data<Data<B>>, info: web::Query<CompleteInfo>) -> HttpResponse
where
    B: Backing + Send + Sync,
{
    increment_counter!("http_request", "endpoint" => "complete", "queue" => info.queue.clone());

    if let Err(e) = data.job_store.complete(&info.queue, &info.job_id).await {
        increment_counter!("errors", "endpoint" => "complete", "type" => e.error_type(), "queue" => e.queue());
        match e {
            Error::JobNotFound { .. } => {
                HttpResponse::build(StatusCode::NOT_FOUND).body(e.to_string())
            }
            Error::Backing(e) => {
                if e.is_retryable() {
                    HttpResponse::build(StatusCode::TOO_MANY_REQUESTS).body(e.to_string())
                } else {
                    HttpResponse::build(StatusCode::UNPROCESSABLE_ENTITY).body(e.to_string())
                }
            }
            _ => HttpResponse::build(StatusCode::INTERNAL_SERVER_ERROR).body(e.to_string()),
        }
    } else {
        HttpResponse::build(StatusCode::OK).finish()
    }
}

fn health() -> HttpResponse {
    HttpResponse::build(StatusCode::OK).finish()
}

struct Data<B>
where
    B: Backing + Send + Sync,
{
    job_store: Store<B>,
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
    pub async fn run<B>(memory_store: Store<B>, addr: &str) -> anyhow::Result<()>
    where
        B: Backing + Send + Sync + 'static,
    {
        let store = web::Data::new(Data {
            job_store: memory_store,
        });

        let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();
        let reaper_store = store.clone();
        let reaper = tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(3));
            interval.reset();
            tokio::pin!(shutdown_rx);

            loop {
                tokio::select! {
                    _ = interval.tick() => {
                        {
                            let  stream = reaper_store.job_store.reap_timeouts().await;
                            tokio::pin!(stream);

                            while let Some(result)= stream.next().await{
                                match result {
                                    Err(e)=>{
                                        error!("error occurred reaping jobs. {}", e.to_string());
                                    },
                                    Ok(job)=>{
                                        increment_counter!("errors", "type" => "job_timeout", "queue" => job.queue.clone());
                                        warn!("job failed after reaching it's max {} attempt(s) on queue: {} with job id: {}", job.max_retries, job.queue, job.id);
                                    }
                                };

                            }
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
                .route("/enqueue", web::post().to(enqueue::<B>))
                .route("/heartbeat", web::patch().to(heartbeat::<B>))
                .route("/complete", web::delete().to(complete::<B>))
                .route("/next", web::get().to(next::<B>))
                .route("/health", web::get().to(health))
        })
        .keep_alive(2)
        .max_connection_rate(512)
        .bind(addr)?
        .run()
        .await?;

        drop(shutdown_tx);

        reaper.await?;
        info!("Reaper shutdown");
        Ok(())
    }
}
