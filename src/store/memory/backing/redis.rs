use super::Backing;
use super::Error;
use super::Job;
use async_stream::stream;
use async_trait::async_trait;
use chrono::Utc;
use redis::aio::ConnectionManager;
use redis::{Client, ErrorKind, RedisError};
use serde_json::value::RawValue;
use std::pin::Pin;
use tokio_stream::Stream;

/// Redis backing store
pub struct Store {
    connection_manager: ConnectionManager,
}

impl Store {
    /// Creates a new backing store with default settings for Redis.
    pub async fn default(uri: &str) -> Result<Self, RedisError> {
        let client = Client::open(uri)?;
        let connection_manager = client.get_tokio_connection_manager().await?;
        Ok(Self { connection_manager })
    }
}

#[async_trait]
impl Backing for Store {
    #[inline]
    async fn push(&mut self, job: &Job) -> super::Result<()> {
        let unique_id = format!("{}-{}", &job.queue, &job.id);

        let exists: bool = redis::cmd("EXISTS")
            .arg(&unique_id)
            .query_async(&mut self.connection_manager)
            .await
            .map_err(|e| Error::Push {
                job_id: job.id.clone(),
                queue: job.queue.clone(),
                message: e.to_string(),
                is_retryable: is_retryable(e),
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
            .query_async(&mut self.connection_manager)
            .await
            .map_err(|e| Error::Push {
                job_id: job.id.clone(),
                queue: job.queue.clone(),
                message: e.to_string(),
                is_retryable: is_retryable(e),
            })?;

        Ok(())
    }

    #[inline]
    async fn remove(&mut self, job: &Job) -> super::Result<()> {
        let unique_id = format!("{}-{}", &job.queue, &job.id);

        redis::pipe()
            .atomic()
            .cmd("DEL")
            .arg(&unique_id)
            .ignore()
            .cmd("ZREM")
            .arg("__index__")
            .arg(&unique_id)
            .ignore()
            .query_async(&mut self.connection_manager)
            .await
            .map_err(|e| Error::Remove {
                job_id: job.id.clone(),
                queue: job.queue.clone(),
                message: e.to_string(),
                is_retryable: is_retryable(e),
            })?;

        Ok(())
    }

    #[inline]
    async fn update(
        &mut self,
        queue: &str,
        job_id: &str,
        state: &Option<Box<RawValue>>,
    ) -> super::Result<()> {
        let unique_id = format!("{}-{}", &queue, &job_id);

        let data: String = redis::cmd("GET")
            .arg(&unique_id)
            .query_async(&mut self.connection_manager)
            .await
            .map_err(|e| Error::Update {
                job_id: job_id.to_string(),
                queue: queue.to_string(),
                message: e.to_string(),
                is_retryable: is_retryable(e),
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
            .query_async(&mut self.connection_manager)
            .await
            .map_err(|e| Error::Update {
                job_id: job_id.to_string(),
                queue: queue.to_string(),
                message: e.to_string(),
                is_retryable: is_retryable(e),
            })?;

        Ok(())
    }

    #[inline]
    fn recover(&'_ mut self) -> Pin<Box<dyn Stream<Item = super::Result<Job>> + '_>> {
        Box::pin(stream! {

            let unique_ids: Vec<String> =
                    redis::cmd("ZRANGE").arg("__index__").arg(0 as isize).arg( -1 as isize)
                        .query_async(&mut self.connection_manager)
                        .await
                        .map_err(|e| Error::Recovery {
                            message: e.to_string(),
                            is_retryable: is_retryable(e),
                        })?;

            for unique_id in unique_ids {
                let s: String = redis::cmd("GET").arg(&unique_id).query_async(&mut self.connection_manager).await.map_err(|e| Error::Recovery {
                    message: e.to_string(),
                    is_retryable: is_retryable(e),
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
fn is_retryable(e: RedisError) -> bool {
    match e.kind() {
        ErrorKind::BusyLoadingError | ErrorKind::TryAgain => true,
        ErrorKind::IoError => e.is_timeout() || e.is_connection_dropped(),
        _ => false,
    }
}
