use async_stream::stream;
use async_trait::async_trait;
use chrono::Utc;
use redis::{Client, ErrorKind, RedisError};
use relay::memory_store::backing::{Backing, Error, Result};
use relay::Job;
use serde_json::value::RawValue;
use std::pin::Pin;
use tokio_stream::Stream;

/// Redis backing store
pub struct Store {
    client: Client,
}

impl Store {
    /// Creates a new backing store with default settings for Redis.
    ///
    /// # Errors
    ///
    /// Will return `Err` if connecting to the server fails.
    pub async fn default(uri: &str) -> std::result::Result<Self, RedisError> {
        let client = Client::open(uri)?;
        let mut conn = client.get_async_connection().await?;
        redis::cmd("PING").query_async(&mut conn).await?;
        Ok(Self { client })
    }
}

#[async_trait]
impl Backing for Store {
    #[inline]
    async fn push(&self, job: &Job) -> Result<()> {
        let unique_id = format!("{}-{}", &job.queue, &job.id);

        let mut conn = self
            .client
            .get_async_connection()
            .await
            .map_err(|e| Error::Push {
                job_id: job.id.clone(),
                queue: job.queue.clone(),
                message: e.to_string(),
                is_retryable: is_retryable(&e),
            })?;

        let exists: bool = redis::cmd("EXISTS")
            .arg(&unique_id)
            .query_async(&mut conn)
            .await
            .map_err(|e| Error::Push {
                job_id: job.id.clone(),
                queue: job.queue.clone(),
                message: e.to_string(),
                is_retryable: is_retryable(&e),
            })?;
        if exists {
            return Err(Error::Push {
                job_id: job.id.clone(),
                queue: job.queue.clone(),
                message: "message exists in Redis somehow but not in memory!".to_string(),
                is_retryable: false,
            });
        }

        let data = serde_json::to_string(job).map_err(|e| Error::Push {
            job_id: job.id.clone(),
            queue: job.queue.clone(),
            message: e.to_string(),
            is_retryable: false,
        })?;

        redis::pipe()
            .atomic()
            .cmd("SET")
            .arg(&unique_id)
            .arg(data)
            .ignore()
            .cmd("ZADD")
            .arg("__index__")
            .arg(Utc::now().timestamp_nanos())
            .arg(&unique_id)
            .ignore()
            .query_async(&mut conn)
            .await
            .map_err(|e| Error::Push {
                job_id: job.id.clone(),
                queue: job.queue.clone(),
                message: e.to_string(),
                is_retryable: is_retryable(&e),
            })?;

        Ok(())
    }

    #[inline]
    async fn remove(&self, job: &Job) -> Result<()> {
        let unique_id = format!("{}-{}", &job.queue, &job.id);

        let mut conn = self
            .client
            .get_async_connection()
            .await
            .map_err(|e| Error::Remove {
                job_id: job.id.clone(),
                queue: job.queue.clone(),
                message: e.to_string(),
                is_retryable: is_retryable(&e),
            })?;

        redis::pipe()
            .atomic()
            .cmd("DEL")
            .arg(&unique_id)
            .ignore()
            .cmd("ZREM")
            .arg("__index__")
            .arg(&unique_id)
            .ignore()
            .query_async(&mut conn)
            .await
            .map_err(|e| Error::Remove {
                job_id: job.id.clone(),
                queue: job.queue.clone(),
                message: e.to_string(),
                is_retryable: is_retryable(&e),
            })?;

        Ok(())
    }

    #[inline]
    async fn update(&self, queue: &str, job_id: &str, state: &Option<Box<RawValue>>) -> Result<()> {
        let unique_id = format!("{}-{}", &queue, &job_id);

        let mut conn = self
            .client
            .get_async_connection()
            .await
            .map_err(|e| Error::Update {
                job_id: job_id.to_string(),
                queue: queue.to_string(),
                message: e.to_string(),
                is_retryable: is_retryable(&e),
            })?;

        let data: String = redis::cmd("GET")
            .arg(&unique_id)
            .query_async(&mut conn)
            .await
            .map_err(|e| Error::Update {
                job_id: job_id.to_string(),
                queue: queue.to_string(),
                message: e.to_string(),
                is_retryable: is_retryable(&e),
            })?;

        let mut job: Job = serde_json::from_str(&data).map_err(|e| Error::Update {
            job_id: job_id.to_string(),
            queue: queue.to_string(),
            message: e.to_string(),
            is_retryable: false,
        })?;
        job.state = state.clone();

        let data = serde_json::to_string(&job).map_err(|e| Error::Update {
            job_id: job_id.to_string(),
            queue: queue.to_string(),
            message: e.to_string(),
            is_retryable: false,
        })?;

        redis::cmd("SET")
            .arg(&unique_id)
            .arg(&data)
            .query_async(&mut conn)
            .await
            .map_err(|e| Error::Update {
                job_id: job_id.to_string(),
                queue: queue.to_string(),
                message: e.to_string(),
                is_retryable: is_retryable(&e),
            })?;

        Ok(())
    }

    #[inline]
    fn recover(&'_ self) -> Pin<Box<dyn Stream<Item = Result<Job>> + '_>> {
        Box::pin(stream! {

            let mut conn = self
                .client
                .get_async_connection()
                .await
                .map_err(|e| Error::Recovery {
                    message: e.to_string(),
                    is_retryable: is_retryable(&e),
                })?;

            let unique_ids: Vec<String> =
                    redis::cmd("ZRANGE").arg("__index__").arg(0).arg( -1)
                        .query_async(&mut conn)
                        .await
                        .map_err(|e| Error::Recovery {
                            message: e.to_string(),
                            is_retryable: is_retryable(&e),
                        })?;

            for unique_id in unique_ids {
                let s: String = redis::cmd("GET").arg(&unique_id).query_async(&mut conn).await.map_err(|e| Error::Recovery {
                    message: e.to_string(),
                    is_retryable: is_retryable(&e),
                })?;
                let job: Job = serde_json::from_str(&s).map_err(|e| Error::Recovery {
                    message: e.to_string(),
                    is_retryable: false,
                })?;
                yield Ok(job);
            }
        })
    }
}

#[inline]
fn is_retryable(e: &RedisError) -> bool {
    match e.kind() {
        ErrorKind::BusyLoadingError | ErrorKind::TryAgain => true,
        ErrorKind::IoError => e.is_timeout() || e.is_connection_dropped(),
        _ => false,
    }
}
