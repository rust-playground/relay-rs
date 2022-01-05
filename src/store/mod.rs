use serde::{Deserialize, Serialize};
use serde_json::value::RawValue;
use std::time::Duration;

/// The internal memory store used for storing and serving Jobs.
pub mod memory;

/// The server types which Jobs can be used with.
pub mod server;

type UniqueJob = String;
type Queue = String;
type JobID = String;

/// Job defines all information needed to process a job.
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Job {
    /// The unique Job ID which is also CAN be used to ensure the Job is a singleton.
    pub id: JobID,

    /// Is used to differentiate different job types that can be picked up by job runners.
    pub queue: Queue,

    /// Denotes the duration after a Job has started processing or since the last
    /// heartbeat request occurred before considering the Job failed and being put back into the
    /// queue.
    #[serde(with = "duration_u64_serde")]
    pub timeout: Duration,

    /// Determines how many time the Job can be retried, due to timeouts, before being considered
    /// permanently failed.
    #[serde(default)]
    pub max_retries: u8,

    /// This determines if the Job will live strictly in-memory, or if set to true, persisted to the
    /// configured backing store to withstand restarts or unexpected failure. This will greatly
    /// depends on your Job types and guarantees your systems require.
    #[serde(default)]
    pub persist_data: bool,

    /// The raw JSON payload that the job runner will receive.
    pub payload: Box<RawValue>,

    /// The job state that can be persisted during heartbeat requests for progression based data
    /// used in the event of crash or failure. This is usually reserved for long-running jobs.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub state: Option<Box<RawValue>>,
}

mod duration_u64_serde {
    use serde::{self, Deserialize, Deserializer, Serializer};
    use std::time::Duration;

    pub fn serialize<S>(d: &Duration, s: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        s.serialize_u64(d.as_secs())
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<Duration, D::Error>
    where
        D: Deserializer<'de>,
    {
        let seconds = u64::deserialize(deserializer)?;
        Ok(Duration::from_secs(seconds))
    }
}
