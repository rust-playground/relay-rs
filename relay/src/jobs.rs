use anydate::serde::deserialize::anydate_utc_option;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::value::RawValue;
use thiserror::Error;

/// Job defines all information needed to process a job.
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Job {
    /// The unique Job ID which is also CAN be used to ensure the Job is a singleton.
    pub id: String,

    /// Is used to differentiate different job types that can be picked up by job runners.
    pub queue: String,

    /// Denotes the duration, in seconds, after a Job has started processing or since the last
    /// heartbeat request occurred before considering the Job failed and being put back into the
    /// queue.
    pub timeout: i32,

    /// Determines how many times the Job can be retried, due to timeouts, before being considered
    /// permanently failed.
    #[serde(default)]
    pub max_retries: i32,

    /// The raw JSON payload that the job runner will receive.
    pub payload: Box<RawValue>,

    /// The raw JSON payload that the job runner will receive.
    ///
    /// This state will be ignored when enqueueing a Job and can only be set via a Heartbeat
    /// request.
    #[serde(skip_deserializing)]
    pub state: Option<Box<RawValue>>,

    /// With this you can optionally schedule/set a Job to be run only at a specific time in the
    /// future. This option should mainly be used for one-time jobs and scheduled jobs that have
    /// the option of being self-perpetuated in combination with the reschedule endpoint.
    #[serde(default, deserialize_with = "anydate_utc_option")]
    pub run_at: Option<DateTime<Utc>>,
}

/// The Job Result.
pub type Result<T> = std::result::Result<T, Error>;

/// Job error types.
#[derive(Error, Debug)]
pub enum Error {
    /// indicates a Job with the existing ID and Queue already exists.
    #[error("job with id `{job_id}` already exists in queue `{queue}`.")]
    JobExists { job_id: String, queue: String },

    /// indicates a Job with the existing ID and Queue could not be found.
    #[error("job with id `{job_id}` not found in queue `{queue}`.")]
    JobNotFound { job_id: String, queue: String },

    #[error("Postgres error: {message}")]
    Postgres { message: String, is_retryable: bool },
}

impl Error {
    #[inline]
    #[must_use]
    pub fn queue(&self) -> String {
        match self {
            Error::JobExists { queue, .. } | Error::JobNotFound { queue, .. } => queue.clone(),
            Error::Postgres { .. } => "unknown".to_string(),
        }
    }

    #[inline]
    #[must_use]
    pub fn error_type(&self) -> String {
        match self {
            Error::JobExists { .. } => "job_exists".to_string(),
            Error::JobNotFound { .. } => "job_not_found".to_string(),
            Error::Postgres { .. } => "postgres".to_string(),
        }
    }

    #[inline]
    #[must_use]
    pub fn is_retryable(&self) -> bool {
        match self {
            Error::JobExists { .. } | Error::JobNotFound { .. } => false,
            Error::Postgres { is_retryable, .. } => *is_retryable,
        }
    }
}
