use crate::{Job, Result};
use async_trait::async_trait;

/// Backend is the trait definition for any relay backend implementation.
#[async_trait]
pub trait Backend<T> {
    async fn enqueue(&self, job: &Job<T>) -> Result<()>;
    async fn enqueue_batch(&self, jobs: &[Job<T>]) -> Result<()>;
    async fn remove(&self, queue: &str, job_id: &str) -> Result<()>;
    async fn next(&self, queue: &str, num_jobs: u32) -> Result<Option<Vec<Job<T>>>>;
    async fn update(&self, queue: &str, job_id: &str, state: Option<T>) -> Result<()>;
    async fn reschedule(&self, job: &Job<T>) -> Result<()>;
    async fn reap(&self, interval_seconds: i64) -> Result<()>;
}
