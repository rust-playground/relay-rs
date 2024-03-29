use crate::{Job, Result};
use async_trait::async_trait;

/// Backend is the trait definition for any relay backend implementation.
#[async_trait]
pub trait Backend<P, S> {
    async fn enqueue(&self, jobs: &[Job<P, S>]) -> Result<()>;
    async fn get(&self, queue: &str, job_id: &str) -> Result<Option<Job<P, S>>>;
    async fn delete(&self, queue: &str, job_id: &str) -> Result<()>;
    async fn heartbeat(&self, queue: &str, job_id: &str, state: Option<S>) -> Result<()>;
    async fn exists(&self, queue: &str, job_id: &str) -> Result<bool>;
    async fn next(&self, queue: &str, num_jobs: u32) -> Result<Option<Vec<Job<P, S>>>>;
    async fn reschedule(&self, job: &Job<P, S>) -> Result<()>;
    async fn reap(&self, interval_seconds: u64) -> Result<()>;
}
