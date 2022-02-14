use crate::store::backing::Backing;
use crate::store::{Error, Store};
use crate::Job;
use actix_web::http::StatusCode;
use actix_web::middleware::Logger;
use actix_web::{web, App, HttpResponse, HttpServer};
use metrics::increment_counter;
use serde::Deserialize;

/// The internal HTTP server representation for Jobs.
pub struct Server;

async fn upsert<B>(data: web::Data<Data<B>>, job: web::Json<Job>) -> HttpResponse
where
    B: Backing + Send + Sync + 'static,
{
    increment_counter!("http_request", "endpoint" => "upsert");

    if let Err(e) = data.job_store.upsert(job.0).await {
        increment_counter!("errors", "endpoint" => "upsert", "type" => e.error_type());
        match e {
            Error::InvalidCron(..) | Error::CronExpired => {
                HttpResponse::build(StatusCode::BAD_REQUEST).body(e.to_string())
            }
            Error::Backing(e) => {
                if e.is_retryable() {
                    HttpResponse::build(StatusCode::TOO_MANY_REQUESTS).body(e.to_string())
                } else {
                    HttpResponse::build(StatusCode::UNPROCESSABLE_ENTITY).body(e.to_string())
                }
            }
            Error::NotFound { .. } => {
                HttpResponse::build(StatusCode::INTERNAL_SERVER_ERROR).body(e.to_string())
            }
        }
    } else {
        HttpResponse::build(StatusCode::ACCEPTED).finish()
    }
}

#[derive(Deserialize)]
struct DeleteInfo {
    job_id: String,
}

async fn delete<B>(data: web::Data<Data<B>>, info: web::Query<DeleteInfo>) -> HttpResponse
where
    B: Backing + Send + Sync + 'static,
{
    increment_counter!("http_request", "endpoint" => "delete");

    if let Err(e) = data.job_store.delete(&info.job_id).await {
        increment_counter!("errors", "endpoint" => "delete", "type" => e.error_type());
        match e {
            Error::NotFound { .. } => {
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

#[allow(clippy::unused_async)]
async fn health() -> HttpResponse {
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
    pub async fn run<B>(job_store: Store<B>, addr: &str) -> anyhow::Result<()>
    where
        B: Backing + Send + Sync + 'static,
    {
        let store = web::Data::new(Data { job_store });

        HttpServer::new(move || {
            App::new()
                .app_data(store.clone())
                .wrap(Logger::new("%a %r %s %Dms"))
                .route("/", web::post().to(upsert::<B>))
                .route("/", web::delete().to(delete::<B>))
                .route("/health", web::get().to(health))
        })
        .bind(addr)?
        .run()
        .await?;

        Ok(())
    }
}
