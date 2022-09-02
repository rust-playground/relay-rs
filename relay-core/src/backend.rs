use crate::{Job, Result};
use async_trait::async_trait;

/// Backend is the trait definition for any relay backend implementation.
#[async_trait]
pub trait Backend<T> {
    async fn create(&self, jobs: &[Job<T>]) -> Result<()>;
    async fn read(&self, queue: &str, job_id: &str) -> Result<Option<Job<T>>>;
    async fn delete(&self, queue: &str, job_id: &str) -> Result<()>;
    async fn heartbeat(&self, queue: &str, job_id: &str, state: Option<T>) -> Result<()>;
    async fn exists(&self, queue: &str, job_id: &str) -> Result<bool>;
    async fn fetch(&self, queue: &str, num_jobs: u32) -> Result<Option<Vec<Job<T>>>>;
    async fn reschedule(&self, job: &Job<T>) -> Result<()>;
    async fn reap(&self, interval_seconds: u64) -> Result<()>;
}
