use super::Job;
use super::{JobID, Queue};
use crate::store::memory::backing::noop;
use ahash::RandomState;
use async_stream::stream;
use backing::{Backing, Error as BackingError};
use metrics::counter;
use serde_json::value::RawValue;
use std::collections::hash_map::Entry::{Occupied, Vacant};
use std::collections::{HashMap, HashSet, VecDeque};
use std::ops::DerefMut;
use std::pin::Pin;
use thiserror::Error;
use tokio::sync::{Mutex, RwLock};
use tokio::time::Instant;
use tokio_stream::{Stream, StreamExt};
use tracing::{debug, instrument};

/// Contains the available backing stores for Jobs with `persist_data` = true.
pub mod backing;

// ultimately we're trying to accomplish:
// RWMutex<Queue<Mutex<Jobs>>>>
//
// So the Memory Store would own what portion? ultimately all of it! Make it handle all using interior mutability.
// This also has the advantage of detaching lock scoping from the transfer medium eg. HTTP
//

type Queues = HashMap<Queue, Mutex<QueueState>, RandomState>;

#[derive(Default)]
struct QueueState {
    jobs: HashMap<JobID, StoredJob, RandomState>,
    queued: Queued,
}

#[derive(Default)]
struct Queued {
    jobs: VecDeque<JobID>,
    in_flight: HashSet<JobID, RandomState>,
}

struct StoredJob {
    job: Job,
    heartbeat: Option<Instant>,
    retries: u8,
}

/// The memory store implementation.
pub struct Store<B>
where
    B: Backing,
{
    queues: RwLock<Queues>,
    backing: B,
}

impl<B> Store<B>
where
    B: Backing + Send + Sync,
{
    /// Creates a new memory store for use.
    ///
    /// # Errors
    ///
    /// Will return `Err` if trying to recover any jobs from the backing store fails.
    ///
    #[inline]
    pub async fn new(backing: B) -> Result<Self> {
        let queues = RwLock::new(HashMap::default());

        // reocover any data in persistent store
        {
            let mut noop = noop::Store::default();
            let mut stream = backing.recover();
            while let Some(result) = stream.next().await {
                let job = result?;
                enqueue_in_memory(&mut noop, &queues, job).await?;
            }
        }

        Ok(Self { queues, backing })
    }

    /// Enqueues the provided Job.
    ///
    /// # Errors
    ///
    /// Will return `Err` if the `Job` is not found or there was an issue writing the `Job` to
    /// the backing store fails
    #[inline]
    pub async fn enqueue(&self, job: Job) -> Result<()> {
        enqueue_in_memory(&self.backing, &self.queues, job).await
    }

    /// Resets/Extends the timeout timestamp.
    ///
    /// # Errors
    ///
    /// Will return `Err` if the `Job` is not found or there was an issue updating the `Job` in
    /// the backing store.
    #[inline]
    pub async fn touch(
        &self,
        queue: &str,
        job_id: &str,
        state: Option<Box<RawValue>>,
    ) -> Result<()> {
        let r_lock = self.queues.read().await;
        match r_lock.get(queue) {
            None => Err(Error::JobNotFound {
                job_id: job_id.to_owned(),
                queue: queue.to_owned(),
            }),
            Some(m) => {
                let mut lock = m.lock().await;
                match lock.jobs.get_mut(job_id) {
                    None => Err(Error::JobNotFound {
                        job_id: job_id.to_owned(),
                        queue: queue.to_owned(),
                    }),
                    Some(sj) => {
                        // will only be Some when it's in-flight already.
                        // can't set heartbeat for non in-flight data.
                        if sj.heartbeat.is_some() {
                            if sj.job.persist_data
                                && (state.is_some() || state.is_some() != sj.job.state.is_some())
                            {
                                // update persisted data with state
                                self.backing.update(queue, job_id, &state).await?;
                            }
                            sj.heartbeat = Some(Instant::now());
                            sj.job.state = state;
                        }
                        Ok(())
                    }
                }
            }
        }
    }

    /// Marks the Job as complete and removes the the in-memory and persisted store.
    ///
    /// # Errors
    ///
    /// Will return `Err` if the `Job` is not found or there was an issue removing the `Job` from
    /// the backing store.
    #[inline]
    pub async fn complete(&self, queue: &str, job_id: &str) -> Result<()> {
        let r_lock = self.queues.read().await;
        match r_lock.get(queue) {
            None => Err(Error::JobNotFound {
                job_id: job_id.to_owned(),
                queue: queue.to_owned(),
            }),
            Some(m) => {
                let mut lock = m.lock().await;
                lock.queued.in_flight.remove(job_id);
                if let Some(sj) = lock.jobs.remove(job_id) {
                    if sj.job.persist_data {
                        self.backing.remove(&sj.job).await?;
                    }
                }
                Ok(())
            }
        }
    }

    /// Retrieves the nex available Job or None if there are no Job yet available.
    ///
    /// # Errors
    ///
    /// Will return `Err` if the `Job` is not found.
    #[inline]
    pub async fn next(&self, queue: &str) -> Result<Option<Job>> {
        let r_lock = self.queues.read().await;
        match r_lock.get(queue) {
            None => Ok(None),
            Some(m) => {
                let mut lock = m.lock().await;
                if let Some(job_id) = lock.queued.jobs.pop_front() {
                    lock.queued.in_flight.insert(job_id.clone());

                    let job = match lock.jobs.get_mut(&job_id) {
                        None => {
                            return Err(Error::JobNotFound {
                                job_id,
                                queue: queue.to_owned(),
                            })
                        }
                        Some(sj) => {
                            sj.heartbeat = Some(Instant::now());
                            sj.job.clone()
                        }
                    };
                    Ok(Some(job))
                } else {
                    Ok(None)
                }
            }
        }
    }

    /// Checks all Jobs marked as in-flight for timeouts.
    #[inline]
    pub async fn reap_timeouts(&self) -> Pin<Box<dyn Stream<Item = Result<Job>> + Send + '_>> {
        // muti-pass required for proper order of operations for persisted data
        // - delete from persistent store successfully
        // - remove from in-memory
        let mut queue_job_ids: HashMap<String, Vec<String>, RandomState> = HashMap::default();
        let mut retries = 0;

        let r_lock = self.queues.read().await;

        for v in r_lock.values() {
            let mut lock = v.lock().await;
            let state = lock.deref_mut();

            state
                .queued
                .in_flight
                .retain(|job_id| match state.jobs.get_mut(job_id) {
                    None => false,
                    Some(j) => {
                        if let Some(heartbeat) = j.heartbeat {
                            if heartbeat.elapsed() > j.job.timeout {
                                if j.job.max_retries == 0 || j.retries > j.job.max_retries {
                                    match queue_job_ids.entry(j.job.queue.clone()) {
                                        Occupied(mut o) => o.get_mut().push(j.job.id.clone()),
                                        Vacant(v) => {
                                            let mut job_ids = Vec::new();
                                            job_ids.push(j.job.id.clone());
                                            v.insert(job_ids);
                                        }
                                    };
                                    true
                                } else {
                                    j.retries += 1;
                                    j.heartbeat = None;
                                    state.queued.jobs.push_front(j.job.id.clone());
                                    retries += 1;
                                    false
                                }
                            } else {
                                true
                            }
                        } else {
                            false
                        }
                    }
                });
        }
        if retries > 0 {
            counter!("retries", retries);
        }

        self.reap_timeouts_inner(queue_job_ids)
    }

    fn reap_timeouts_inner(
        &'_ self,
        queue_job_ids: HashMap<String, Vec<String>, RandomState>,
    ) -> Pin<Box<dyn Stream<Item = Result<Job>> + Send + '_>> {
        Box::pin(stream! {
            let r_lock = self.queues.read().await;

            for (queue, job_ids) in queue_job_ids {
                if let Some(m) = r_lock.get(&queue) {
                    let mut lock = m.lock().await;
                    for job_id in job_ids {
                        match lock.jobs.get_mut(&job_id) {
                            None => {
                                lock.queued.in_flight.remove(&job_id);
                                continue;
                            }
                            Some(sj) => {
                                if sj.job.persist_data {
                                    self.backing
                                        .remove(&sj.job)
                                        .await
                                        .map_err(|e| Error::Reaper {
                                            job_id: sj.job.id.clone(),
                                            queue: sj.job.queue.clone(),
                                            message: e.to_string(),
                                        })?;
                                }
                                lock.queued.in_flight.remove(&job_id);
                                // unwrap is safe because to be here we found it in the jobs HashMap already
                                yield Ok(lock.jobs.remove(&job_id).unwrap().job);
                            }
                        };
                    }
                } else {
                    continue;
                }
            }
        })
    }
}

#[inline]
async fn enqueue_in_memory<B>(backing: &B, queues: &RwLock<Queues>, job: Job) -> Result<()>
where
    B: Backing,
{
    let r_lock = queues.read().await;
    match r_lock.get(&job.queue) {
        None => {
            // not found
            drop(r_lock);
            {
                let mut w_lock = queues.write().await;
                match w_lock.entry(job.queue.clone()) {
                    Occupied(_) => {
                        // must hand this event because in between dropping the read lock and acquiring
                        // the write other code could get caught in-between.
                    }
                    Vacant(v) => {
                        v.insert(Mutex::new(QueueState::default()));
                    }
                };
            }
            enqueue_in_memory_inner(
                backing,
                queues
                    .read()
                    .await
                    .get(&job.queue)
                    .unwrap()
                    .lock()
                    .await
                    .deref_mut(),
                job,
            )
            .await
        }
        Some(m) => enqueue_in_memory_inner(backing, m.lock().await.deref_mut(), job).await,
    }
}

#[inline]
#[instrument(level = "debug", skip(backing, queue_state, job), fields(job_id=%job.id))]
async fn enqueue_in_memory_inner<B>(
    backing: &B,
    queue_state: &mut QueueState,
    job: Job,
) -> Result<()>
where
    B: Backing,
{
    if let Vacant(v) = queue_state.jobs.entry(job.id.clone()) {
        debug!("enqueueing job");
        if job.persist_data {
            backing.push(&job).await?;
        }
        queue_state.queued.jobs.push_back(job.id.clone());
        v.insert(StoredJob {
            job,
            heartbeat: None,
            retries: 0,
        });
        Ok(())
    } else {
        Err(Error::JobExists {
            job_id: job.id,
            queue: job.queue,
        })
    }
}

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

    /// indicates an issue with the backing store for persisted Jobs.
    #[error(transparent)]
    Backing(#[from] BackingError),

    /// indicates an issue while attempting to reap Job that have timed out.
    #[error("failed to remove job with id `{job_id}` in queue `{queue}`. {message}")]
    Reaper {
        job_id: String,
        queue: String,
        message: String,
    },
}

impl Error {
    #[inline]
    #[must_use]
    pub fn queue(&self) -> String {
        match self {
            Error::JobExists { queue, .. }
            | Error::JobNotFound { queue, .. }
            | Error::Reaper { queue, .. } => queue.clone(),
            Error::Backing(e) => e.queue(),
        }
    }

    #[inline]
    #[must_use]
    pub fn error_type(&self) -> String {
        match self {
            Error::JobExists { .. } => "job_exists".to_string(),
            Error::JobNotFound { .. } => "job_not_found".to_string(),
            Error::Backing(e) => e.error_type(),
            Error::Reaper { .. } => "reaper".to_string(),
        }
    }
}
