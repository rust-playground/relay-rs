use axum::body::{Body, BoxBody};
use axum::extract::{Path, Query, State};
use axum::http::{Request, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::{delete, get, head, patch, post, put};
use axum::{Json, Router};
use metrics::counter;
use relay_core::{Backend, Error, Job};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use std::future::Future;
use std::sync::Arc;
use std::time::Duration;
use tower_http::trace::TraceLayer;
use tracing::{info, span, Level, Span};
use uuid::Uuid;

/// The internal HTTP server representation for Jobs.
pub struct Server;

#[tracing::instrument(name = "http_get", level = "debug", skip_all)]
async fn get_job<BE, T>(
    State(state): State<Arc<BE>>,
    Path((queue, id)): Path<(String, String)>,
) -> Response
where
    T: Serialize,
    BE: Backend<T, T>,
{
    counter!("http_request", "endpoint" => "get", "queue" => queue.clone()).increment(1);

    match state.get(&queue, &id).await {
        Ok(job) => {
            if let Some(job) = job {
                Json(job).into_response()
            } else {
                StatusCode::NOT_FOUND.into_response()
            }
        }
        Err(e) => {
            counter!("errors", "endpoint" => "get", "type" => e.error_type(), "queue" => e.queue())
                .increment(1);
            match e {
                Error::Backend { .. } => {
                    if e.is_retryable() {
                        (StatusCode::TOO_MANY_REQUESTS, e.to_string()).into_response()
                    } else {
                        (StatusCode::UNPROCESSABLE_ENTITY, e.to_string()).into_response()
                    }
                }
                _ => (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
            }
        }
    }
}

#[tracing::instrument(name = "http_exists", level = "debug", skip_all)]
async fn exists<BE, T>(
    State(state): State<Arc<BE>>,
    Path((queue, id)): Path<(String, String)>,
) -> Response
where
    BE: Backend<T, T>,
{
    counter!("http_request", "endpoint" => "exists", "queue" => queue.clone()).increment(1);

    match state.exists(&queue, &id).await {
        Ok(exists) => {
            if exists {
                StatusCode::OK.into_response()
            } else {
                StatusCode::NOT_FOUND.into_response()
            }
        }
        Err(e) => {
            counter!("errors", "endpoint" => "exists", "type" => e.error_type(), "queue" => e.queue()).increment(1);
            match e {
                Error::Backend { .. } => {
                    if e.is_retryable() {
                        (StatusCode::TOO_MANY_REQUESTS, e.to_string()).into_response()
                    } else {
                        (StatusCode::UNPROCESSABLE_ENTITY, e.to_string()).into_response()
                    }
                }
                _ => (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
            }
        }
    }
}

#[tracing::instrument(name = "http_enqueue", level = "debug", skip_all)]
async fn enqueue<BE, T>(State(state): State<Arc<BE>>, jobs: Json<Vec<Job<T, T>>>) -> Response
where
    BE: Backend<T, T>,
{
    counter!("http_request", "endpoint" => "enqueue").increment(1);

    if let Err(e) = state.enqueue(&jobs.0).await {
        counter!("errors", "endpoint" => "enqueue", "type" => e.error_type()).increment(1);
        match e {
            Error::Backend { .. } => {
                if e.is_retryable() {
                    (StatusCode::TOO_MANY_REQUESTS, e.to_string()).into_response()
                } else {
                    (StatusCode::UNPROCESSABLE_ENTITY, e.to_string()).into_response()
                }
            }
            Error::JobExists { .. } => (StatusCode::CONFLICT, e.to_string()).into_response(),
            Error::JobNotFound { .. } => {
                (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response()
            }
        }
    } else {
        StatusCode::ACCEPTED.into_response()
    }
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
    State(state): State<Arc<BE>>,
    Path(queue): Path<String>,
    params: Query<NextQueryInfo>,
) -> Response
where
    T: Serialize,
    BE: Backend<T, T>,
{
    counter!("http_request", "endpoint" => "next", "queue" => queue.clone()).increment(1);

    match state.next(&queue, params.num_jobs).await {
        Err(e) => {
            counter!("errors", "endpoint" => "next", "type" => e.error_type(), "queue" => e.queue()).increment(1);
            if let Error::Backend { .. } = e {
                if e.is_retryable() {
                    (StatusCode::TOO_MANY_REQUESTS, e.to_string()).into_response()
                } else {
                    (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response()
                }
            } else {
                (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response()
            }
        }
        Ok(job) => match job {
            None => StatusCode::NO_CONTENT.into_response(),
            Some(job) => (StatusCode::OK, Json(job)).into_response(),
        },
    }
}

#[tracing::instrument(name = "http_heartbeat", level = "debug", skip_all)]
async fn heartbeat<BE, T>(
    State(state): State<Arc<BE>>,
    Path((queue, id)): Path<(String, String)>,
    job_state: Option<Json<T>>,
) -> Response
where
    T: DeserializeOwned,
    BE: Backend<T, T>,
{
    counter!("http_request", "endpoint" => "heartbeat", "queue" => queue.clone()).increment(1);

    let job_state = match job_state {
        None => None,
        Some(job_state) => Some(job_state.0),
    };
    if let Err(e) = state.heartbeat(&queue, &id, job_state).await {
        counter!("errors", "endpoint" => "heartbeat", "type" => e.error_type(), "queue" => e.queue()).increment(1);
        match e {
            Error::JobNotFound { .. } => (StatusCode::NOT_FOUND, e.to_string()).into_response(),
            Error::Backend { .. } => {
                if e.is_retryable() {
                    (StatusCode::TOO_MANY_REQUESTS, e.to_string()).into_response()
                } else {
                    (StatusCode::UNPROCESSABLE_ENTITY, e.to_string()).into_response()
                }
            }
            Error::JobExists { .. } => {
                (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response()
            }
        }
    } else {
        StatusCode::ACCEPTED.into_response()
    }
}

#[tracing::instrument(name = "http_reschedule", level = "debug", skip_all)]
async fn reschedule<BE, T>(State(state): State<Arc<BE>>, job: Json<Job<T, T>>) -> Response
where
    BE: Backend<T, T>,
{
    counter!("http_request", "endpoint" => "reschedule", "queue" => job.0.queue.clone())
        .increment(1);

    if let Err(e) = state.reschedule(&job.0).await {
        counter!("errors", "endpoint" => "enqueued", "type" => e.error_type(), "queue" => e.queue()).increment(1);
        match e {
            Error::JobExists { .. } => {
                (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response()
            }
            Error::Backend { .. } => {
                if e.is_retryable() {
                    (StatusCode::TOO_MANY_REQUESTS, e.to_string()).into_response()
                } else {
                    (StatusCode::UNPROCESSABLE_ENTITY, e.to_string()).into_response()
                }
            }
            Error::JobNotFound { .. } => (StatusCode::NOT_FOUND, e.to_string()).into_response(),
        }
    } else {
        StatusCode::ACCEPTED.into_response()
    }
}

#[tracing::instrument(name = "http_delete", level = "debug", skip_all)]
async fn delete_job<BE, T>(
    State(state): State<Arc<BE>>,
    Path((queue, id)): Path<(String, String)>,
) -> Response
where
    BE: Backend<T, T>,
{
    counter!("http_request", "endpoint" => "delete", "queue" => queue.clone()).increment(1);

    if let Err(e) = state.delete(&queue, &id).await {
        counter!("errors", "endpoint" => "delete", "type" => e.error_type(), "queue" => e.queue())
            .increment(1);
        match e {
            Error::JobNotFound { .. } => (StatusCode::NOT_FOUND, e.to_string()).into_response(),
            Error::Backend { .. } => {
                if e.is_retryable() {
                    (StatusCode::TOO_MANY_REQUESTS, e.to_string()).into_response()
                } else {
                    (StatusCode::UNPROCESSABLE_ENTITY, e.to_string()).into_response()
                }
            }
            Error::JobExists { .. } => {
                (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response()
            }
        }
    } else {
        StatusCode::OK.into_response()
    }
}

#[allow(clippy::unused_async)]
async fn health() {}

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
    pub async fn run<BE, T, F>(backend: Arc<BE>, addr: &str, shutdown: F) -> anyhow::Result<()>
    where
        T: Serialize + DeserializeOwned + Send + Sync + 'static,
        BE: Backend<T, T> + Send + Sync + 'static,
        F: Future<Output = ()>,
    {
        let app = Server::init_app(backend);

        axum::Server::bind(&addr.parse().unwrap())
            .serve(app.into_make_service())
            .with_graceful_shutdown(shutdown)
            .await
            .unwrap();
        Ok(())
    }

    pub(crate) fn init_app<BE, T>(backend: Arc<BE>) -> Router
    where
        T: Serialize + DeserializeOwned + Send + Sync + 'static,
        BE: Backend<T, T> + Send + Sync + 'static,
    {
        Router::new()
            .route("/v1/queues/jobs", post(enqueue))
            .route("/v1/queues/jobs", put(reschedule))
            .route("/v1/queues/:queue/jobs", get(next))
            .route("/v1/queues/:queue/jobs/:id", head(exists))
            .route("/v1/queues/:queue/jobs/:id", get(get_job))
            .route("/v1/queues/:queue/jobs/:id", patch(heartbeat))
            .route("/v1/queues/:queue/jobs/:id", delete(delete_job))
            .route("/health", get(health))
            .layer(
                TraceLayer::new_for_http()
                    .make_span_with(|request: &Request<Body>| {
                        let dbg = span!(
                            Level::DEBUG,
                            "request",
                            id = %Uuid::new_v4().to_string(),
                        );
                        span!(
                            parent: &dbg,
                            Level::INFO,
                            "request",
                            method = %request.method(),
                            uri = %request.uri(),
                            version = ?request.version(),
                        )
                    })
                    .on_response(
                        |response: &Response<BoxBody>, latency: Duration, _span: &Span| {
                            info!(
                                target: "response",
                                status = response.status().as_u16(),
                                latency = ?latency,
                            );
                        },
                    ),
            )
            .with_state(backend)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::client::{Builder as ClientBuilder, Client};
    use anyhow::{anyhow, Context};
    use chrono::DurationRound;
    use chrono::Utc;
    use portpicker::pick_unused_port;
    use relay_backend_postgres::PgStore;
    use std::net::{IpAddr, Ipv4Addr, SocketAddr, TcpListener};
    use tokio::task::JoinHandle;
    use uuid::Uuid;

    /// Generates a `SocketAddr` on the IP 0.0.0.0, using a random port.
    pub fn new_random_socket_addr() -> anyhow::Result<SocketAddr> {
        let ip_address = IpAddr::V4(Ipv4Addr::new(0, 0, 0, 0));
        let port = pick_unused_port().ok_or_else(|| anyhow!("No free port was found"))?;
        let addr = SocketAddr::new(ip_address, port);
        Ok(addr)
    }

    async fn init_server() -> anyhow::Result<(JoinHandle<()>, Arc<Client>)> {
        let db_url = std::env::var("DATABASE_URL")?;
        let store = Arc::new(PgStore::default(&db_url).await?);
        let app = Server::init_app(store);

        let socket_address =
            new_random_socket_addr().expect("Cannot create socket address for use");
        let listener = TcpListener::bind(socket_address)
            .with_context(|| "Failed to create TCPListener for TestServer")?;
        let server_address = socket_address.to_string();
        let server = axum::Server::from_tcp(listener)
            .with_context(|| "Failed to create ::axum::Server for TestServer")?
            .serve(app.into_make_service());

        let server_thread = tokio::spawn(async move {
            server.await.expect("Expect server to start serving");
        });

        let url = format!("http://{server_address}");
        let client = ClientBuilder::new(&url).build();
        Ok((server_thread, Arc::new(client)))
    }

    #[tokio::test]
    async fn test_oneshot() -> anyhow::Result<()> {
        let (_srv, client) = init_server().await?;
        let now = Utc::now()
            .duration_trunc(chrono::Duration::milliseconds(1))
            .unwrap();
        let job = Job {
            id: Uuid::new_v4().to_string(),
            queue: Uuid::new_v4().to_string(),
            timeout: 30,
            max_retries: -1,
            payload: (),
            state: None,
            run_at: Some(now),
            updated_at: None,
        };

        client.enqueue(&job).await?;

        let exists = client.exists(&job.queue, &job.id).await?;
        assert!(exists);

        let mut j = client.get::<(), i32>(&job.queue, &job.id).await?;
        assert!(j.updated_at.is_some());
        assert!(j.updated_at.unwrap() >= now);
        // resetting server side set variable before equality check
        j.updated_at = None;
        assert_eq!(job, j);

        let mut jobs = client.poll::<(), i32>(&job.queue, 10).await?;
        assert_eq!(jobs.len(), 1);

        let mut j2 = jobs.pop().unwrap();
        j2.updated_at = None;
        assert_eq!(j2, j);

        client.heartbeat(&job.queue, &job.id, Some(3)).await?;

        let j = client.get::<(), i32>(&job.queue, &job.id).await?;
        assert_eq!(j.state, Some(3));

        client.remove(&job.queue, &job.id).await?;
        Ok(())
    }

    #[tokio::test]
    async fn test_reschedule() -> anyhow::Result<()> {
        let (_srv, client) = init_server().await?;
        let now = Utc::now()
            .duration_trunc(chrono::Duration::milliseconds(1))
            .unwrap();
        let mut job = Job {
            id: Uuid::new_v4().to_string(),
            queue: Uuid::new_v4().to_string(),
            timeout: 30,
            max_retries: -1,
            payload: (),
            state: None,
            run_at: Some(now),
            updated_at: None,
        };

        client.enqueue_batch(&[job.clone()]).await?;

        let mut jobs = client.poll::<(), i32>(&job.queue, 10).await?;
        assert_eq!(jobs.len(), 1);
        job = jobs.pop().unwrap();

        job.run_at = Some(
            Utc::now()
                .duration_trunc(chrono::Duration::milliseconds(1))
                .unwrap(),
        );
        client.reschedule(&job).await?;

        let mut j = client.get::<(), i32>(&job.queue, &job.id).await?;
        assert!(j.updated_at.unwrap() >= job.updated_at.unwrap());
        j.updated_at = None;
        job.updated_at = None;
        assert_eq!(j, job);

        client.remove(&job.queue, &job.id).await?;
        Ok(())
    }
}
