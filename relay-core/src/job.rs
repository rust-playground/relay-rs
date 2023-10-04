use anydate::serde::deserialize::anydate_utc_option;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::value::RawValue;

/// Job defines all information needed to process a job.
#[derive(Default, Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
pub struct Job<P, S> {
    /// The unique Job ID which is also CAN be used to ensure the Job is a singleton.
    pub id: String,

    /// Is used to differentiate different job types that can be picked up by job runners.
    pub queue: String,

    /// Denotes the duration, in seconds, after a Job has started processing or since the last
    /// heartbeat request occurred before considering the Job failed and being put back into the
    /// queue.
    pub timeout: i32,

    /// Determines how many times the Job can be retried, due to timeouts, before being considered
    /// permanently failed. Infinite retries are supported by using a negative number eg. -1
    #[serde(default)]
    pub max_retries: i32,

    /// The raw JSON payload that the job runner will receive.
    pub payload: P,

    /// The raw JSON payload that the job runner will receive.
    pub state: Option<S>,

    /// With this you can optionally schedule/set a Job to be run only at a specific time in the
    /// future. This option should mainly be used for one-time jobs and scheduled jobs that have
    /// the option of being self-perpetuated in combination with the reschedule endpoint.
    #[serde(default, deserialize_with = "anydate_utc_option")]
    pub run_at: Option<DateTime<Utc>>,

    /// This indicates the last time the Job was updated either through enqueue, reschedule or
    /// heartbeat.
    /// This value is for reporting purposes only and will be ignored when enqueuing and rescheduling.
    pub updated_at: Option<DateTime<Utc>>,
}
