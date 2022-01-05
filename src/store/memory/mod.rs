use super::Job;
use super::{JobID, Queue, UniqueJob};
use crate::store::memory::backing::noop;
use ahash::RandomState;
use async_stream::stream;
use backing::{Backing, Error as BackingError};
use metrics::counter;
use serde_json::value::RawValue;
use std::collections::hash_map::Entry::{Occupied, Vacant};
use std::collections::{HashMap, HashSet, VecDeque};
use std::pin::Pin;
use thiserror::Error;
use tokio::time::Instant;
use tokio_stream::{Stream, StreamExt};
use tracing::{debug, instrument};

/// Contains the available backing stores for Jobs with persist_data = true.
pub mod backing;

struct QueuedJobs {
    jobs: VecDeque<JobID>,
    in_flight: HashSet<UniqueJob, RandomState>,
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
    jobs: HashMap<UniqueJob, StoredJob, RandomState>,
    queue_jobs: HashMap<Queue, QueuedJobs, RandomState>,
    backing: B,
}

impl<B> Store<B>
where
    B: Backing + Send + Sync,
{
    /// Creates a new memory store for use.
    #[inline]
    pub async fn new(mut backing: B) -> Result<Self> {
        let mut jobs: HashMap<UniqueJob, StoredJob, RandomState> = HashMap::default();
        let mut queue_jobs: HashMap<Queue, QueuedJobs, RandomState> = HashMap::default();

        {
            let mut noop = noop::Store::default();
            let mut stream = backing.recover();
            while let Some(result) = stream.next().await {
                let job = result?;
                enqueue_in_memory(&mut noop, job, &mut jobs, &mut queue_jobs).await?;
            }
        }

        Ok(Self {
            jobs,
            queue_jobs,
            backing,
        })
    }

    /// Enqueues the provided Job.
    #[inline]
    pub async fn enqueue(&mut self, job: Job) -> Result<()> {
        enqueue_in_memory(&mut self.backing, job, &mut self.jobs, &mut self.queue_jobs).await
    }

    /// Resets/Extends the timeout timestamp.
    #[inline]
    pub async fn touch(
        &mut self,
        queue: &str,
        job_id: &str,
        state: Option<Box<RawValue>>,
    ) -> Result<()> {
        let unique_id = format!("{}-{}", queue, job_id);

        match self.jobs.get_mut(&unique_id) {
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

    /// Marks the Job as complete and removes the the in-memory and persisted store.
    #[inline]
    pub async fn complete(&mut self, queue: &str, job_id: &str) -> Result<()> {
        let unique_id = format!("{}-{}", queue, job_id);

        let result = match self.queue_jobs.get_mut(queue) {
            None => Err(Error::JobNotFound {
                job_id: job_id.to_owned(),
                queue: queue.to_owned(),
            }),
            Some(j) => {
                if j.in_flight.remove(&unique_id) {
                    Ok(())
                } else {
                    return Err(Error::JobNotFound {
                        job_id: job_id.to_owned(),
                        queue: queue.to_owned(),
                    });
                }
            }
        };
        match self.jobs.remove(&unique_id) {
            None => {}
            Some(sj) => {
                if sj.job.persist_data {
                    self.backing.remove(&sj.job).await?;
                }
            }
        };
        result
    }

    /// Retrieves the nex available Job or None if there are no Job yet available.
    #[inline]
    pub async fn next(&mut self, queue: &str) -> Result<Option<Job>> {
        match self.queue_jobs.get_mut(queue) {
            None => Ok(None),
            Some(queued_jobs) => match queued_jobs.jobs.pop_front() {
                None => Ok(None),
                Some(job_id) => {
                    let unique_id = format!("{}-{}", queue, &job_id);
                    queued_jobs
                        .in_flight
                        .insert(format!("{}-{}", queue, &job_id));

                    let job = match self.jobs.get_mut(&unique_id) {
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
                }
            },
        }
    }

    /// Checks all Jobs marked as in-flight for timeouts.
    #[inline]
    pub fn reap_timeouts(&mut self) -> Pin<Box<dyn Stream<Item = Result<Job>> + Send + '_>> {
        // muti-pass required for proper order of operations for persisted data
        // - delete from persistent store successfully
        // - remove from in-memory
        let mut job_ids = Vec::new();
        let mut retries = 0;

        for (_, v) in self.queue_jobs.iter_mut() {
            v.in_flight
                .retain(|unique_id| match self.jobs.get_mut(unique_id) {
                    None => false,
                    Some(j) => {
                        if j.heartbeat.unwrap().elapsed() > j.job.timeout {
                            if j.job.max_retries == 0 || j.retries > j.job.max_retries {
                                job_ids.push((unique_id.clone(), j.job.queue.clone()));
                                true
                            } else {
                                j.retries += 1;
                                j.heartbeat = None;
                                v.jobs.push_front(j.job.id.clone());
                                retries += 1;
                                false
                            }
                        } else {
                            true
                        }
                    }
                });
        }
        if retries > 0 {
            counter!("retries", retries);
        }

        Box::pin(stream! {
            for (unique_id, queue) in job_ids {
                match self.jobs.get_mut(&unique_id){
                    None=> {
                        if let Some(qj) = self.queue_jobs.get_mut(&queue){
                            qj.in_flight.remove(&unique_id);
                        }
                        continue
                    },
                    Some(sj) =>{
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
                        if let Some(qj) = self.queue_jobs.get_mut(&queue){
                            qj.in_flight.remove(&unique_id);
                        }
                        // unwrap is safe because to be here we found it in the jobs HashMap already
                        yield Ok(self.jobs.remove(&unique_id).unwrap().job);
                    }
                };
            }
        })
    }
}

#[inline]
#[instrument(level = "debug", skip(backing, job, jobs, queue_jobs), fields(job_id=%job.id))]
async fn enqueue_in_memory<B>(
    backing: &mut B,
    job: Job,
    jobs: &mut HashMap<UniqueJob, StoredJob, RandomState>,
    queue_jobs: &mut HashMap<Queue, QueuedJobs, RandomState>,
) -> Result<()>
where
    B: Backing,
{
    let unique_id = format!("{}-{}", &job.queue, &job.id);

    match jobs.entry(unique_id) {
        Occupied(_) => Err(Error::JobExists {
            job_id: job.id,
            queue: job.queue,
        }),
        Vacant(v) => {
            debug!("enqueueing job");
            if job.persist_data {
                backing.push(&job).await?;
            }
            match queue_jobs.entry(job.queue.clone()) {
                Occupied(mut o) => {
                    o.get_mut().jobs.push_back(job.id.clone());
                }
                Vacant(v) => {
                    let mut dv = VecDeque::new();
                    dv.push_back(job.id.clone());
                    v.insert(QueuedJobs {
                        jobs: dv,
                        in_flight: Default::default(),
                    });
                }
            };
            v.insert(StoredJob {
                job,
                heartbeat: None,
                retries: 0,
            });
            Ok(())
        }
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
    pub fn queue(&self) -> String {
        match self {
            Error::JobExists { queue, .. } => queue.clone(),
            Error::JobNotFound { queue, .. } => queue.clone(),
            Error::Backing(e) => e.queue(),
            Error::Reaper { queue, .. } => queue.clone(),
        }
    }

    #[inline]
    pub fn error_type(&self) -> String {
        match self {
            Error::JobExists { .. } => "job_exists".to_string(),
            Error::JobNotFound { .. } => "job_not_found".to_string(),
            Error::Backing(e) => e.error_type(),
            Error::Reaper { .. } => "reaper".to_string(),
        }
    }
}
