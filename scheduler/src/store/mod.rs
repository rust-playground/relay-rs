/// Contains the available backing traits for persisting Scheduled Jobs.
pub mod backing;

use crate::store::backing::Backing;
use crate::Job;
use ahash::RandomState;
use backing::Error as BackingError;
use chrono::Utc;
use cron::error::Error as CronError;
use cron::Schedule;
use metrics::increment_counter;
use reqwest::{Client, StatusCode};
use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;
use thiserror::Error;
use tokio::sync::{oneshot, Mutex};
use tokio::task::JoinHandle;
use tokio::time::sleep;
use tokio_stream::StreamExt;
use tracing::debug;

struct InMemoryJob {
    shutdown_tx: oneshot::Sender<()>,
    handle: JoinHandle<()>,
}

struct InnerStore<B>
where
    B: Backing,
{
    schedules: Mutex<HashMap<String, InMemoryJob, RandomState>>,
    backing: B,
    client: Client,
}

impl<B> InnerStore<B>
where
    B: Backing + Send + Sync,
{
    pub async fn delete(&self, job_id: &str) -> Result<()> {
        let mut lock = self.schedules.lock().await;
        if let Some(sj) = lock.remove(job_id) {
            drop(sj.shutdown_tx);
            if let Err(e) = sj.handle.await {
                debug!("failed to shutdown job {}", e.to_string());
            }
        }
        Ok(())
    }
}

/// The in-memory store & coordinator.
pub struct Store<B>
where
    B: Backing,
{
    inner: Arc<InnerStore<B>>,
}

impl<B> Store<B>
where
    B: Backing + Send + Sync + 'static,
{
    /// Creates a new Store for use.
    ///
    /// # Errors
    ///
    /// Will return `Err` if recovering Scheduled Job from the backing store fails.
    /// to start.
    pub async fn new(backing: B) -> Result<Self> {
        let schedules = Mutex::new(HashMap::default());
        let client = Client::default();
        let inner = Arc::new(InnerStore {
            schedules,
            backing,
            client,
        });

        // recover any data in persistent store
        {
            let mut stream = inner.backing.recover();
            while let Some(result) = stream.next().await {
                let job = result?;
                create_inner(&inner, job, false).await?;
            }
        }

        Ok(Self { inner })
    }

    /// Starts and persists the Scheduled Job to the backing store.
    ///
    /// # Errors
    ///
    /// Will return `Err` if the Scheduled Job fails to be saved in the backing store or fails
    /// to start.
    pub async fn create(&self, job: Job) -> Result<()> {
        create_inner(&self.inner, job, true).await?;
        Ok(())
    }

    /// Updates the Scheduled Job with new setting by first deleting it and then re-creating.
    ///
    /// # Errors
    ///
    /// Will return `Err` if the Scheduled Job fails to be removed or re-created.
    pub async fn upsert(&self, job: Job) -> Result<()> {
        self.delete(&job.id).await?;
        self.create(job).await
    }

    /// Removes the Scheduled Job from memory and the backing store.
    ///
    /// # Errors
    ///
    /// Will return `Err` if the Scheduled Job fails to be removed from the backing store.
    pub async fn delete(&self, job_id: &str) -> Result<()> {
        self.inner.backing.delete(job_id).await?;
        self.inner.delete(job_id).await
    }
}

async fn create_inner<B>(inner: &Arc<InnerStore<B>>, mut job: Job, use_backing: bool) -> Result<()>
where
    B: Backing + Send + Sync + 'static,
{
    let schedule = Schedule::from_str(&job.cron)?;
    let mut next = get_next(&schedule, &job)?;

    if use_backing {
        inner.backing.upsert(&job).await?;
    }
    let job_id = job.id.clone();

    let (shutdown_tx, shutdown_rx) = oneshot::channel();
    let store = inner.clone();

    let handle = tokio::spawn(async move {
        tokio::pin!(shutdown_rx);
        loop {
            tokio::select! {
                _ = (&mut shutdown_rx) => break,
                _ = sleep(next) => {
                    debug!("triggering job {}", &job.id);

                    if let Ok(resp) = store
                        .client
                        .post(job.endpoint.as_str())
                        .json(&job.payload)
                        .send()
                        .await
                    {
                        match resp.status() {
                            StatusCode::SERVICE_UNAVAILABLE
                            | StatusCode::TOO_MANY_REQUESTS
                            | StatusCode::BAD_GATEWAY
                            | StatusCode::GATEWAY_TIMEOUT => {
                                next = Duration::from_secs(1);
                                increment_counter!("retries", "type" => "retryable");
                                continue;
                            }
                            StatusCode::CONFLICT => {
                                if let Some(d) = job.retry_already_running {
                                    next = d;
                                    increment_counter!("retries", "type" => "conflict");
                                    continue;
                                }
                            }
                            _ => {}
                        };
                    } else{
                        // assume retryable error try again in 1 seconds
                        next = Duration::from_secs(1);
                        increment_counter!("retries", "type" => "retryable_unknown");
                        continue;
                    }

                    let now = Utc::now();
                    if store.backing.touch(&job.id, &now).await.is_err() {
                        increment_counter!("errors", "type" => "touch");
                    }
                    job.last_run = Some(now);

                    let now = Utc::now();
                    next = match schedule.after(&now).next() {
                        None => {
                            while store.delete(&job.id).await.is_err() {
                                increment_counter!("errors", "type" => "delete_expired_cron");
                            }
                            break;
                        }
                        Some(dt) => {
                            match (dt - now).to_std() {
                                Ok(d) => d,
                                Err(_) => Duration::default(), // 0 Duration, fire right away
                            }
                        },
                    };
                }
            }
        }
    });

    let in_memory = InMemoryJob {
        shutdown_tx,
        handle,
    };
    let mut lock = inner.schedules.lock().await;
    match lock.entry(job_id) {
        Entry::Occupied(_) => {
            unreachable!()
        }
        Entry::Vacant(v) => {
            v.insert(in_memory);
            Ok(())
        }
    }
}

fn get_next(schedule: &Schedule, job: &Job) -> Result<Duration> {
    let now = Utc::now();

    if job.recovery_check {
        if let Some(last_run) = job.last_run {
            if let Some(prev) = schedule.after(&now).rev().next() {
                if prev >= last_run {
                    return Ok(Duration::default());
                }
            }
        }
    }

    match schedule.after(&now).next() {
        None => Err(Error::CronExpired),
        Some(dt) => {
            match (dt - now).to_std() {
                Ok(d) => Ok(d),
                Err(_) => Ok(Duration::default()), // 0 Duration, fire right away
            }
        }
    }
}

/// Store Result type.
pub type Result<T> = std::result::Result<T, Error>;

/// Store errors.
#[derive(Error, Debug)]
pub enum Error {
    /// indicates a Job with the existing ID could not be found.
    #[error("job with id `{job_id}`.")]
    NotFound { job_id: String },

    /// indicates an issue with the backing store for scheduled Jobs.
    #[error(transparent)]
    Backing(#[from] BackingError),

    /// indicates a Scheduled Job's CRON syntax is invalid.
    #[error(transparent)]
    InvalidCron(#[from] CronError),

    /// indicates the Cron expression will never fire again and so was not accepted.
    #[error("Cron Expression will never trigger again")]
    CronExpired,
}

impl Error {
    #[inline]
    #[must_use]
    pub fn error_type(&self) -> String {
        match self {
            Error::Backing(e) => e.error_type(),
            Error::NotFound { .. } => "not_found".to_string(),
            Error::InvalidCron(_) => "invalid_cron".to_string(),
            Error::CronExpired => "cron_expired".to_string(),
        }
    }
}
