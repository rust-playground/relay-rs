use crate::Job;
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use std::pin::Pin;
use thiserror::Error;
use tokio_stream::Stream;

#[async_trait]
pub trait Backing {
    async fn upsert(&self, job: &Job) -> Result<()>;
    async fn touch(&self, job_id: &str, last_run: &DateTime<Utc>) -> Result<()>;
    async fn delete(&self, job_id: &str) -> Result<()>;
    fn recover(&self) -> Pin<Box<dyn Stream<Item = Result<Job>> + '_>>;
}

/// Backing Result type
pub type Result<T> = std::result::Result<T, Error>;

/// Backing errors
#[derive(Error, Debug)]
pub enum Error {
    #[error("failed to save scheduled job with id `{job_id}`. {message}")]
    Create {
        job_id: String,
        message: String,
        is_retryable: bool,
    },

    #[error("failed to upsert scheduled job with id `{job_id}`. {message}")]
    Upsert {
        job_id: String,
        message: String,
        is_retryable: bool,
    },

    #[error("failed to remove scheduled job with id `{job_id}`. {message}")]
    Delete {
        job_id: String,
        message: String,
        is_retryable: bool,
    },

    #[error("failed to set last run for scheduled job with id `{job_id}`. {message}")]
    Touch {
        job_id: String,
        message: String,
        is_retryable: bool,
    },

    #[error("failed to recover scheduled jobs. {message}")]
    Recovery { message: String, is_retryable: bool },

    #[error("Scheduled Job with id {job_id} not found.")]
    NotFound { job_id: String },
}

impl Error {
    /// returns if the backing store error is retryable.
    #[inline]
    #[must_use]
    pub fn is_retryable(&self) -> bool {
        match self {
            Error::Create { is_retryable, .. }
            | Error::Upsert { is_retryable, .. }
            | Error::Delete { is_retryable, .. }
            | Error::Touch { is_retryable, .. }
            | Error::Recovery { is_retryable, .. } => *is_retryable,
            Error::NotFound { .. } => false,
        }
    }

    /// returns string interpretation of the error type
    #[inline]
    #[must_use]
    pub fn error_type(&self) -> String {
        match self {
            Error::Create { .. } => "create".to_string(),
            Error::Upsert { .. } => "update".to_string(),
            Error::Delete { .. } => "delete".to_string(),
            Error::Touch { .. } => "touch".to_string(),
            Error::Recovery { .. } => "recovery".to_string(),
            Error::NotFound { .. } => "not_found".to_string(),
        }
    }
}
