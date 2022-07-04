//! # relay-rs API
//!
//! This outlines the HTTP serve that exposes relays functionality.
//!
//! ## API
//!
//! ### `POST /enqueue`
//!
//! Enqueues a Job to be processed.
//!
//! #### Arguments
//! In this case the only arguments are part of the Body payload.
//!
//! | argument    | required | description                                                                                                                                                                            |
//! |-------------|----------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
//! | `id`          | true     | The unique Job Id which is also CAN be used to ensure the Job is a singleton within a Queue.                                                                                           |
//! | `queue`       | true     | Is used to differentiate different job types that can be picked up by job runners.                                                                                                     |
//! | `timeout`     | true     | Denotes the duration, in seconds, after a Job has started processing or since the last heartbeat request occurred before considering the Job failed and being put back into the queue. |
//! | `max_retries` | false    | Determines how many times the Job can be retried, due to timeouts, before being considered.                                                                                            |
//! | `payload`     | false    | The raw JSON payload that the job runner will receive.                                                                                                                                 |
//! | `run_at`      | false    | Schedule/set a Job to be run only at a specific time in the future.                                                                                                                    |
//!
//! #### Request Body
//! ```json
//! {
//!     "id": "1",
//!     "queue": "my-queue",
//!     "timeout": 30,
//!     "max_retries": 0,
//!     "payload": "RAW JSON"
//! }
//! ```
//!
//! ### Response Codes
//!
//! NOTE: The body of the response will have more detail about the specific error.
//!
//! | code  | description                                                                 |
//! |-------|-----------------------------------------------------------------------------|
//! | 202   | Job enqueued and accepted for processing.                                   |
//! | 400   | For a bad/ill-formed request.                                               |
//! | 409   | An conflicting Job already exists with the provided id and queue.           |
//! | 429   | A retryable error occurred. Most likely the backing storage having issues.  |
//! | 422   | A permanent error has occurred.                                             |
//! | 500   | An unknown error has occurred server side.                                  |
//!
//!
//!
//! ### `POST /enqueue/batch`
//!
//! Enqueues multiple `Jobs` to be processed in one call.
//!
//! **NOTE:**
//! That this function will not return an error for conflicts in Job ID, but rather
//! silently drop the record using an `ON CONFLICT DO NOTHING`. If you need to have a
//! Conflict error returned it is recommended to use the `enqueue` function for a single
//! Job instead.
//!
//! #### Arguments
//! In this case the only arguments are part of the Body payload.
//!
//! | argument    | required | description                                                                                                                                                                            |
//! |-------------|----------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
//! | `id`          | true     | The unique Job Id which is also CAN be used to ensure the Job is a singleton within a Queue.                                                                                           |
//! | `queue`       | true     | Is used to differentiate different job types that can be picked up by job runners.                                                                                                     |
//! | `timeout`     | true     | Denotes the duration, in seconds, after a Job has started processing or since the last heartbeat request occurred before considering the Job failed and being put back into the queue. |
//! | `max_retries` | false    | Determines how many times the Job can be retried, due to timeouts, before being considered.                                                                                            |
//! | `payload`     | false    | The raw JSON payload that the job runner will receive.                                                                                                                                 |
//! | `run_at`      | false    | Schedule/set a Job to be run only at a specific time in the future.                                                                                                                    |
//!
//! #### Request Body
//! ```json
//! [
//!     {
//!         "id": "1",
//!         "queue": "my-queue",
//!         "timeout": 30,
//!         "max_retries": 0,
//!         "payload": "RAW JSON"
//!     }
//! ]
//! ```
//!
//! ### Response Codes
//!
//! NOTE: The body of the response will have more detail about the specific error.
//!
//! | code  | description                                                                 |
//! |-------|-----------------------------------------------------------------------------|
//! | 202   | Job enqueued and accepted for processing.                                   |
//! | 400   | For a bad/ill-formed request.                                               |
//! | 429   | A retryable error occurred. Most likely the backing storage having issues.  |
//! | 422   | A permanent error has occurred.                                             |
//! | 500   | An unknown error has occurred server side.                                  |
//!
//!
//!
//! ### `GET /v1/next`
//!
//! Retrieves the next `Job(s)` to be processed based on the provided `queue`.
//!
//! #### Arguments
//! In this case the only arguments are query params.
//!
//! | argument | required | description                                                                    |
//! |----------|----------|--------------------------------------------------------------------------------|
//! | `queue`    | true     | Used to pull the next job from the requested queue.                            |
//! | `num_jobs` | false    | Specifies how many Jobs to pull to process next in one request. Defaults to 1. |
//!
//! #### Response Body
//! Some fields may not be present such as `state` when none exists.
//! ```json
//! [
//!     {
//!         "id": "1",
//!         "queue": "my-queue",
//!         "timeout": 30,
//!         "max_retries": 0,
//!         "payload": "RAW JSON",
//!         "state": "RAW JSON"
//!     }
//! ]
//! ```
//!
//! #### Response Codes
//! NOTE: The body of the response will have more detail about the specific error.
//!
//! | code | description                                                                        |
//! |------|------------------------------------------------------------------------------------|
//! | 200  | Job successfully retrieved.                                                        |
//! | 204  | There is currently no Job in the provided queue to return. Backoff an retry later. |
//! | 429  | A retryable error occurred. Most likely the backing storage having issues.         |
//! | 500  | An unknown error has occurred server side.                                         |
//!
//!
//!
//! ### `PATCH /v1/heartbeat`
//!
//! Updates an in-flight Job incrementing its timestamp and optionally setting some state in case of failure.
//!
//! #### Arguments
//!
//! In this case the only arguments are query params.
//!
//! | argument | required | description                                                      |
//! |----------|----------|------------------------------------------------------------------|
//! | `queue`    | true     | The Queue to apply the heartbeat to.                             |
//! | `job_id`   | true     | The Job ID to apply the heartbeat to within the supplied Queue.  |
//!
//!
//! #### Request Body
//! Any JSON data. This payload is persisted in order to save application state.
//! This is mostly used for long-running jobs to save point-in-time state in order
//! to restart from that state if the Job is retried due to a crash or service interruption.
//!
//! #### Response Codes
//! NOTE: The body of th response will have more detail about the specific error.
//!
//! | code  | description                                                                |
//! |-------|----------------------------------------------------------------------------|
//! | 202   | Heartbeat successfully applied to the Job.                                 |
//! | 429   | A retryable error occurred. Most likely the backing storage having issues. |
//! | 404   | Job was not found for updating.                                            |
//! | 422   | A permanent error has occurred.                                            |
//! | 500   | An unknown error has occurred server side.                                 |
//!
//!
//! ### `POST /reschedule`
//!
//! This endpoint should mainly be used for one-time jobs and scheduled jobs that have
//! the option of being self-perpetuated in combination with the `run_at` field.
//!
//! #### Arguments
//! In this case the only arguments are part of the Body payload.
//!
//! | argument    | required | description                                                                                                                                                                              |
//! |-------------|----------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
//! | `id`          | true     | The unique Job Id which is also CAN be used to ensure the Job is a singleton within a Queue.                                                                                           |
//! | `queue`       | true     | Is used to differentiate different job types that can be picked up by job runners.                                                                                                     |
//! | `timeout`     | true     | Denotes the duration, in seconds, after a Job has started processing or since the last heartbeat request occurred before considering the Job failed and being put back into the queue. |
//! | `max_retries` | false    | Determines how many times the Job can be retried, due to timeouts, before being considered.                                                                                            |
//! | `payload`     | false    | The raw JSON payload that the job runner will receive.                                                                                                                                 |
//! | `state`       | false    | The raw JSON state for the rescheduled Job. If NOT supplied will be reset to nil like a newly enqueued Job.                                                                            |
//! | `run_at`      | false    | Schedule/set a Job to be run only at a specific time in the future.                                                                                                                    |
//!
//! #### Request Body
//! ```json
//! {
//!     "id": "1",
//!     "queue": "my-queue",
//!     "timeout": 30,
//!     "max_retries": 0,
//!     "payload": "RAW JSON"
//! }
//! ```
//!
//! ### Response Codes
//!
//! NOTE: The body of the response will have more detail about the specific error.
//!
//! | code  | description                                                                 |
//! |-------|-----------------------------------------------------------------------------|
//! | 202   | Job enqueued and accepted for processing.                                   |
//! | 400   | For a bad/ill-formed request.                                               |
//! | 404   | Job was not found for updating.                                             |
//! | 429   | A retryable error occurred. Most likely the backing storage having issues.  |
//! | 422   | A permanent error has occurred.                                             |
//! | 500   | An unknown error has occurred server side.                                  |
//!
//!
//!
//! ### `DELETE /v1/complete`
//!
//! Completes a Job by removing it.
//!
//! ### Arguments
//!
//! In this case the only arguments are query params.
//!
//! | argument | required | description                          |
//! |----------|----------|--------------------------------------|
//! | `queue`    | true     | The Queue to remove the `job_id` from. |
//! | `job_id`   | true     | The Job ID to remove from the `queue`. |
//!
//! ### Response Codes
//!
//! NOTE: The body of the response will have more detail about the specific error.
//!
//! | code  | description                                                                 |
//! |-------|-----------------------------------------------------------------------------|
//! | 200   | Job successfully completed.                                                 |
//! | 429   | A retryable error occurred. Most likely the backing storage having issues.  |
//! | 404   | Job was not found for completing.                                           |
//! | 422   | A permanent error has occurred.                                             |
//! | 500   | An unknown error has occurred server side.                                  |

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

#[tracing::instrument(name = "http_enqueue", level = "debug", skip_all)]
async fn enqueue<BE, T>(data: web::Data<Arc<BE>>, job: web::Json<Job<T>>) -> HttpResponse
where
    BE: Backend<T>,
{
    increment_counter!("http_request", "endpoint" => "enqueue", "queue" => job.0.queue.clone());

    if let Err(e) = data.enqueue(&job.0).await {
        increment_counter!("errors", "endpoint" => "enqueue", "type" => e.error_type(), "queue" => e.queue());
        match e {
            Error::JobExists { .. } => {
                HttpResponse::build(StatusCode::CONFLICT).body(e.to_string())
            }
            Error::Backend { .. } => {
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

#[tracing::instrument(name = "http_enqueue_batch", level = "debug", skip_all)]
async fn enqueue_batch<BE, T>(
    data: web::Data<Arc<BE>>,
    jobs: web::Json<Vec<Job<T>>>,
) -> HttpResponse
where
    BE: Backend<T>,
{
    increment_counter!("http_request", "endpoint" => "enqueue_batch");

    if let Err(e) = data.enqueue_batch(&jobs.0).await {
        increment_counter!("errors", "endpoint" => "enqueue_batch", "type" => e.error_type());
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
    } else {
        HttpResponse::build(StatusCode::ACCEPTED).finish()
    }
}

#[derive(Deserialize)]
struct NextInfo {
    queue: String,
    #[serde(default = "default_num_jobs")]
    num_jobs: u32,
}

const fn default_num_jobs() -> u32 {
    1
}

#[tracing::instrument(name = "http_next", level = "debug", skip_all)]
async fn next<BE, T>(data: web::Data<Arc<BE>>, info: web::Query<NextInfo>) -> HttpResponse
where
    T: Serialize,
    BE: Backend<T>,
{
    increment_counter!("http_request", "endpoint" => "next", "queue" => info.queue.clone());

    match data.next(&info.queue, info.num_jobs).await {
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
    job_id: String,
}

#[tracing::instrument(name = "http_heartbeat", level = "debug", skip_all)]
async fn heartbeat<BE, T>(
    data: web::Data<Arc<BE>>,
    info: web::Query<HeartbeatInfo>,
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
    if let Err(e) = data.update(&info.queue, &info.job_id, state).await {
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
    job_id: String,
}

#[tracing::instrument(name = "http_complete", level = "debug", skip_all)]
async fn complete<BE, T>(data: web::Data<Arc<BE>>, info: web::Query<CompleteInfo>) -> HttpResponse
where
    BE: Backend<T>,
{
    increment_counter!("http_request", "endpoint" => "complete", "queue" => info.queue.clone());

    if let Err(e) = data.remove(&info.queue, &info.job_id).await {
        increment_counter!("errors", "endpoint" => "complete", "type" => e.error_type(), "queue" => e.queue());
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
                .route("/enqueue", web::post().to(enqueue::<BE, T>))
                .route("/enqueue/batch", web::post().to(enqueue_batch::<BE, T>))
                .route("/heartbeat", web::patch().to(heartbeat::<BE, T>))
                .route("/reschedule", web::post().to(reschedule::<BE, T>))
                .route("/complete", web::delete().to(complete::<BE, T>))
                .route("/next", web::get().to(next::<BE, T>))
                .route("/health", web::get().to(health))
        })
        .bind(addr)?
        .run()
        .await?;
        Ok(())
    }
}
