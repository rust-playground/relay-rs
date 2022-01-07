use crate::store::Job;
use async_trait::async_trait;
use serde_json::value::RawValue;
use std::pin::Pin;
use thiserror::Error;
use tokio_stream::Stream;

/// Noop backing store for when no backing store is needed.
pub mod noop;

/// `Redis` backing store
#[cfg(feature = "redis_backing")]
pub mod redis;

/// `SQLite` backing store
#[cfg(feature = "sqlite_backing")]
pub mod sqlite;

/// `DynamoDB` backing store
#[cfg(feature = "dynamodb_backing")]
pub mod dynamodb;

/// `Postgres` backing store
#[cfg(feature = "postgres_backing")]
pub mod postgres;

#[async_trait]
pub trait Backing {
    async fn push(&mut self, job: &Job) -> Result<()>;
    async fn remove(&mut self, job: &Job) -> Result<()>;
    async fn update(
        &mut self,
        queue: &str,
        job_id: &str,
        state: &Option<Box<RawValue>>,
    ) -> Result<()>;
    fn recover(&mut self) -> Pin<Box<dyn Stream<Item = Result<Job>> + '_>>;
}

/// Backing Result type
pub type Result<T> = std::result::Result<T, Error>;

/// Backing errors
#[derive(Error, Debug)]
pub enum Error {
    /// error encountered which attempting to add a Job
    #[error("failed to persist job with id `{job_id}` in queue `{queue}`. {message}")]
    Push {
        job_id: String,
        queue: String,
        message: String,
        is_retryable: bool,
    },

    /// error encountered which attempting to remove a Job
    #[error("failed to remove job with id `{job_id}` in queue `{queue}`. {message}")]
    Remove {
        job_id: String,
        queue: String,
        message: String,
        is_retryable: bool,
    },

    /// error encountered which attempting to update a Jobs state
    #[error("failed to update job state with id `{job_id}` in queue `{queue}`. {message}")]
    Update {
        job_id: String,
        queue: String,
        message: String,
        is_retryable: bool,
    },

    /// error encountered which attempting to recover Jobs
    #[error("failed to recover jobs. {message}")]
    Recovery { message: String, is_retryable: bool },
}

impl Error {
    /// returns if the backing store error is retryable.
    #[inline]
    #[must_use]
    pub fn is_retryable(&self) -> bool {
        match self {
            Error::Push { is_retryable, .. }
            | Error::Remove { is_retryable, .. }
            | Error::Recovery { is_retryable, .. }
            | Error::Update { is_retryable, .. } => *is_retryable,
        }
    }

    /// returns if the backing store error is retryable.
    #[inline]
    #[must_use]
    pub fn queue(&self) -> String {
        match self {
            Error::Push { queue, .. }
            | Error::Remove { queue, .. }
            | Error::Update { queue, .. } => queue.clone(),
            Error::Recovery { .. } => "".to_string(),
        }
    }

    /// returns string interpretation of the error type
    #[inline]
    #[must_use]
    pub fn error_type(&self) -> String {
        match self {
            Error::Push { .. } => "push".to_string(),
            Error::Remove { .. } => "remove".to_string(),
            Error::Recovery { .. } => "recovery".to_string(),
            Error::Update { .. } => "update".to_string(),
        }
    }
}
