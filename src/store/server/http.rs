use crate::store::memory::backing::Backing;
use crate::store::memory::{Error as MemoryStoreError, Store as MemoryStore};
use crate::store::Job;
use actix_web::http::StatusCode;
use actix_web::middleware::Logger;
use actix_web::{web, App, HttpResponse, HttpServer};
use metrics::{counter, increment_counter};
use serde::Deserialize;
use serde_json::value::RawValue;
use std::borrow::BorrowMut;
use std::time::Duration;
use tokio::sync::{oneshot, Mutex};
use tokio_stream::StreamExt;
use tracing::{error, info, warn};

/// The internal HTTP server representation for Jobs.
pub struct Server;

async fn enqueue<B>(data: web::Data<Data<B>>, job: web::Json<Job>) -> HttpResponse
where
    B: Backing + Send + Sync,
{
    let result;
    {
        let mut lock = data.job_store.lock().await;
        result = lock.borrow_mut().enqueue(job.0).await;
    }
    match result {
        Err(e) => match e {
            MemoryStoreError::JobExists { .. } => {
                HttpResponse::build(StatusCode::CONFLICT).body(e.to_string())
            }
            MemoryStoreError::Backing(e) => {
                increment_counter!("errors", "type" => e.error_type(), "queue" => e.queue());
                if e.is_retryable() {
                    HttpResponse::build(StatusCode::TOO_MANY_REQUESTS).body(e.to_string())
                } else {
                    HttpResponse::build(StatusCode::UNPROCESSABLE_ENTITY).body(e.to_string())
                }
            }
            _ => {
                increment_counter!("errors", "type" => e.error_type(), "queue" => e.queue());
                HttpResponse::build(StatusCode::INTERNAL_SERVER_ERROR).body(e.to_string())
            }
        },
        Ok(_) => {
            increment_counter!("enqueued");
            HttpResponse::build(StatusCode::ACCEPTED).finish()
        }
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
    let result;
    {
        let mut lock = data.job_store.lock().await;
        result = lock.borrow_mut().next(&info.queue).await;
    }
    match result {
        Err(e) => match e {
            MemoryStoreError::Backing(e) => {
                increment_counter!("errors", "type" => e.error_type(), "queue" => e.queue());
                if e.is_retryable() {
                    HttpResponse::build(StatusCode::TOO_MANY_REQUESTS).body(e.to_string())
                } else {
                    HttpResponse::build(StatusCode::INTERNAL_SERVER_ERROR).body(e.to_string())
                }
            }
            _ => {
                increment_counter!("errors", "type" => e.error_type(), "queue" => e.queue());
                HttpResponse::build(StatusCode::INTERNAL_SERVER_ERROR).body(e.to_string())
            }
        },
        Ok(job) => match job {
            None => HttpResponse::build(StatusCode::NO_CONTENT).finish(),
            Some(job) => {
                increment_counter!("next");
                HttpResponse::build(StatusCode::OK).json(job)
            }
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
    let state = match state {
        None => None,
        Some(state) => Some(state.0),
    };
    let result;
    {
        let mut lock = data.job_store.lock().await;
        result = lock
            .borrow_mut()
            .touch(&info.queue, &info.job_id, state)
            .await;
    }
    match result {
        Err(e) => {
            increment_counter!("errors", "type" => e.error_type(), "queue" => e.queue());
            match e {
                MemoryStoreError::JobNotFound { .. } => {
                    HttpResponse::build(StatusCode::NOT_FOUND).body(e.to_string())
                }
                MemoryStoreError::Backing(e) => {
                    increment_counter!("errors", "type" => e.error_type(), "queue" => e.queue());
                    if e.is_retryable() {
                        HttpResponse::build(StatusCode::TOO_MANY_REQUESTS).body(e.to_string())
                    } else {
                        HttpResponse::build(StatusCode::UNPROCESSABLE_ENTITY).body(e.to_string())
                    }
                }
                _ => HttpResponse::build(StatusCode::INTERNAL_SERVER_ERROR).body(e.to_string()),
            }
        }
        Ok(_) => {
            increment_counter!("heartbeat");
            HttpResponse::build(StatusCode::ACCEPTED).finish()
        }
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
    let result;
    {
        let mut lock = data.job_store.lock().await;
        result = lock.borrow_mut().complete(&info.queue, &info.job_id).await
    }
    match result {
        Err(e) => {
            increment_counter!("errors", "type" => e.error_type(), "queue" => e.queue());
            match e {
                MemoryStoreError::JobNotFound { .. } => {
                    HttpResponse::build(StatusCode::NOT_FOUND).body(e.to_string())
                }
                MemoryStoreError::Backing(e) => {
                    if e.is_retryable() {
                        HttpResponse::build(StatusCode::TOO_MANY_REQUESTS).body(e.to_string())
                    } else {
                        HttpResponse::build(StatusCode::UNPROCESSABLE_ENTITY).body(e.to_string())
                    }
                }
                _ => HttpResponse::build(StatusCode::INTERNAL_SERVER_ERROR).body(e.to_string()),
            }
        }
        Ok(_) => {
            increment_counter!("complete");
            HttpResponse::build(StatusCode::OK).finish()
        }
    }
}

struct Data<B>
where
    B: Backing + Send + Sync,
{
    job_store: Mutex<MemoryStore<B>>,
}

impl Server {
    /// starts the HTTP server and waits for a shutdown signal before returning.
    #[inline]
    pub async fn run<B>(memory_store: MemoryStore<B>, addr: &str) -> anyhow::Result<()>
    where
        B: Backing + Send + Sync + 'static,
    {
        let store = web::Data::new(Data {
            job_store: Mutex::new(memory_store),
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
                        let mut timeouts = 0;
                        {
                            let mut lock = reaper_store.job_store.lock().await;
                            let mut stream = lock.borrow_mut().reap_timeouts();

                            while let Some(result)= stream.next().await{
                                match result {
                                    Err(e)=>{
                                        error!("error occurred reaping jobs. {}", e.to_string());
                                    },
                                    Ok(job)=>{
                                        timeouts+=1;
                                        warn!("job failed and reached it's max attempts on queue: {} with job id: {}", job.queue, job.id);
                                    }
                                };

                            }
                        }
                        if timeouts > 0 {
                            counter!("timeouts", timeouts);
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
