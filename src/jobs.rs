use serde::{Deserialize, Serialize};
use serde_json::value::RawValue;
use thiserror::Error;

pub type Queue = String;
pub type JobId = String;

/// Job defines all information needed to process a job.
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Job {
    /// The unique Job ID which is also CAN be used to ensure the Job is a singleton.
    pub id: JobId,

    /// Is used to differentiate different job types that can be picked up by job runners.
    pub queue: Queue,

    /// Denotes the duration after a Job has started processing or since the last
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
    #[serde(skip_deserializing)]
    pub state: Option<Box<RawValue>>,
}

// mod duration_u64_serde {
//     use serde::{self, Deserialize, Deserializer, Serializer};
//     use std::time::Duration;
//
//     pub fn serialize<S>(d: &Duration, s: S) -> Result<S::Ok, S::Error>
//     where
//         S: Serializer,
//     {
//         s.serialize_u64(d.as_secs())
//     }
//
//     pub fn deserialize<'de, D>(deserializer: D) -> Result<Duration, D::Error>
//     where
//         D: Deserializer<'de>,
//     {
//         let seconds = u64::deserialize(deserializer)?;
//         Ok(Duration::from_secs(seconds))
//     }
// }

/// Memory store Result type.
pub type Result<T> = std::result::Result<T, Error>;

/// Memory store errors.
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
            Error::Postgres { .. } => self.is_retryable(),
        }
    }
}
