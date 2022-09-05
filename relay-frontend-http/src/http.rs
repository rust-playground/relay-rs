use actix_web::http::StatusCode;
use actix_web::middleware::Logger;
use actix_web::{web, App, HttpResponse, HttpServer};
use metrics::increment_counter;
use relay_core::{Backend, Error, Job};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use std::sync::Arc;

/// The internal HTTP server representation for Jobs.
pub struct Server;

#[derive(Deserialize)]
struct GetInfo {
    queue: String,
    id: String,
}

#[tracing::instrument(name = "http_get", level = "debug", skip_all)]
async fn get<BE, T>(data: web::Data<Arc<BE>>, info: web::Path<GetInfo>) -> HttpResponse
where
    T: Serialize,
    BE: Backend<T>,
{
    increment_counter!("http_request", "endpoint" => "get", "queue" => info.queue.clone());

    match data.get(&info.queue, &info.id).await {
        Ok(job) => {
            if let Some(job) = job {
                HttpResponse::build(StatusCode::OK).json(job)
            } else {
                HttpResponse::build(StatusCode::NOT_FOUND).finish()
            }
        }
        Err(e) => {
            increment_counter!("errors", "endpoint" => "get", "type" => e.error_type(), "queue" => e.queue());
            match e {
                Error::Backend { .. } => {
                    if e.is_retryable() {
                        HttpResponse::build(StatusCode::TOO_MANY_REQUESTS).body(e.to_string())
                    } else {
                        HttpResponse::build(StatusCode::UNPROCESSABLE_ENTITY).body(e.to_string())
                    }
                }
                _ => HttpResponse::build(StatusCode::INTERNAL_SERVER_ERROR).body(e.to_string()),
            }
        }
    }
}

#[derive(Deserialize)]
struct ExistsInfo {
    queue: String,
    id: String,
}

#[tracing::instrument(name = "http_exists", level = "debug", skip_all)]
async fn exists<BE, T>(data: web::Data<Arc<BE>>, info: web::Path<ExistsInfo>) -> HttpResponse
where
    BE: Backend<T>,
{
    increment_counter!("http_request", "endpoint" => "exists", "queue" => info.queue.clone());

    match data.exists(&info.queue, &info.id).await {
        Ok(exists) => {
            if exists {
                HttpResponse::build(StatusCode::OK).finish()
            } else {
                HttpResponse::build(StatusCode::NOT_FOUND).finish()
            }
        }
        Err(e) => {
            increment_counter!("errors", "endpoint" => "exists", "type" => e.error_type(), "queue" => e.queue());
            match e {
                Error::Backend { .. } => {
                    if e.is_retryable() {
                        HttpResponse::build(StatusCode::TOO_MANY_REQUESTS).body(e.to_string())
                    } else {
                        HttpResponse::build(StatusCode::UNPROCESSABLE_ENTITY).body(e.to_string())
                    }
                }
                _ => HttpResponse::build(StatusCode::INTERNAL_SERVER_ERROR).body(e.to_string()),
            }
        }
    }
}

#[tracing::instrument(name = "http_enqueue", level = "debug", skip_all)]
async fn enqueue<BE, T>(data: web::Data<Arc<BE>>, jobs: web::Json<Vec<Job<T>>>) -> HttpResponse
where
    BE: Backend<T>,
{
    increment_counter!("http_request", "endpoint" => "enqueue");

    if let Err(e) = data.enqueue(&jobs.0).await {
        increment_counter!("errors", "endpoint" => "enqueue", "type" => e.error_type());
        match e {
            Error::Backend { .. } => {
                if e.is_retryable() {
                    HttpResponse::build(StatusCode::TOO_MANY_REQUESTS).body(e.to_string())
                } else {
                    HttpResponse::build(StatusCode::UNPROCESSABLE_ENTITY).body(e.to_string())
                }
            }
            Error::JobExists { .. } => {
                HttpResponse::build(StatusCode::CONFLICT).body(e.to_string())
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
struct NextPathInfo {
    queue: String,
}

#[derive(Deserialize)]
struct NextQueryInfo {
    #[serde(default = "default_num_jobs")]
    num_jobs: u32,
}

const fn default_num_jobs() -> u32 {
    1
}

#[tracing::instrument(name = "http_next", level = "debug", skip_all)]
async fn next<BE, T>(
    data: web::Data<Arc<BE>>,
    path_info: web::Path<NextPathInfo>,
    query_info: web::Query<NextQueryInfo>,
) -> HttpResponse
where
    T: Serialize,
    BE: Backend<T>,
{
    increment_counter!("http_request", "endpoint" => "next", "queue" => path_info.queue.clone());

    match data.next(&path_info.queue, query_info.num_jobs).await {
        Err(e) => {
            increment_counter!("errors", "endpoint" => "next", "type" => e.error_type(), "queue" => e.queue());
            if let Error::Backend { .. } = e {
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
    id: String,
}

#[tracing::instrument(name = "http_heartbeat", level = "debug", skip_all)]
async fn heartbeat<BE, T>(
    data: web::Data<Arc<BE>>,
    info: web::Path<HeartbeatInfo>,
    state: Option<web::Json<T>>,
) -> HttpResponse
where
    BE: Backend<T>,
{
    increment_counter!("http_request", "endpoint" => "heartbeat", "queue" => info.queue.clone());

    let state = match state {
        None => None,
        Some(state) => Some(state.0),
    };
    if let Err(e) = data.heartbeat(&info.queue, &info.id, state).await {
        increment_counter!("errors", "endpoint" => "heartbeat", "type" => e.error_type(), "queue" => e.queue());
        match e {
            Error::JobNotFound { .. } => {
                HttpResponse::build(StatusCode::NOT_FOUND).body(e.to_string())
            }
            Error::Backend { .. } => {
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

#[tracing::instrument(name = "http_reschedule", level = "debug", skip_all)]
async fn reschedule<BE, T>(data: web::Data<Arc<BE>>, job: web::Json<Job<T>>) -> HttpResponse
where
    BE: Backend<T>,
{
    increment_counter!("http_request", "endpoint" => "reschedule", "queue" => job.0.queue.clone());

    if let Err(e) = data.reschedule(&job.0).await {
        increment_counter!("errors", "endpoint" => "enqueued", "type" => e.error_type(), "queue" => e.queue());
        match e {
            Error::JobExists { .. } => {
                HttpResponse::build(StatusCode::INTERNAL_SERVER_ERROR).body(e.to_string())
            }
            Error::Backend { .. } => {
                if e.is_retryable() {
                    HttpResponse::build(StatusCode::TOO_MANY_REQUESTS).body(e.to_string())
                } else {
                    HttpResponse::build(StatusCode::UNPROCESSABLE_ENTITY).body(e.to_string())
                }
            }
            Error::JobNotFound { .. } => {
                HttpResponse::build(StatusCode::NOT_FOUND).body(e.to_string())
            }
        }
    } else {
        HttpResponse::build(StatusCode::ACCEPTED).finish()
    }
}

#[derive(Deserialize)]
struct CompleteInfo {
    queue: String,
    id: String,
}

#[tracing::instrument(name = "http_delete", level = "debug", skip_all)]
async fn delete<BE, T>(data: web::Data<Arc<BE>>, info: web::Path<CompleteInfo>) -> HttpResponse
where
    BE: Backend<T>,
{
    increment_counter!("http_request", "endpoint" => "delete", "queue" => info.queue.clone());

    if let Err(e) = data.delete(&info.queue, &info.id).await {
        increment_counter!("errors", "endpoint" => "delete", "type" => e.error_type(), "queue" => e.queue());
        match e {
            Error::JobNotFound { .. } => {
                HttpResponse::build(StatusCode::NOT_FOUND).body(e.to_string())
            }
            Error::Backend { .. } => {
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

#[allow(clippy::unused_async)]
async fn health() -> HttpResponse {
    HttpResponse::build(StatusCode::OK).finish()
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
    pub async fn run<BE, T>(backend: Arc<BE>, addr: &str) -> anyhow::Result<()>
    where
        T: Serialize + DeserializeOwned + Send + Sync + 'static,
        BE: Backend<T> + Send + Sync + 'static,
    {
        HttpServer::new(move || {
            App::new()
                .app_data(web::Data::new(backend.clone()))
                .wrap(Logger::new("%a %r %s %Dms"))
                .route("/v1/queues/jobs", web::post().to(enqueue::<BE, T>))
                .route("/v1/queues/jobs", web::put().to(reschedule::<BE, T>))
                .route("/v1/queues/{queue}/jobs", web::get().to(next::<BE, T>))
                .route(
                    "/v1/queues/{queue}/jobs/{id}",
                    web::delete().to(delete::<BE, T>),
                )
                .route(
                    "/v1/queues/{queue}/jobs/{id}",
                    web::head().to(exists::<BE, T>),
                )
                .route("/v1/queues/{queue}/jobs/{id}", web::get().to(get::<BE, T>))
                .route(
                    "/v1/queues/{queue}/jobs/{id}",
                    web::patch().to(heartbeat::<BE, T>),
                )
                .route("/health", web::get().to(health))
        })
        .bind(addr)?
        .run()
        .await?;
        Ok(())
    }
}
