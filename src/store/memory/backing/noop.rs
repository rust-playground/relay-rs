use super::Backing;
use crate::store::Job;
use async_trait::async_trait;
use serde_json::value::RawValue;
use std::pin::Pin;
use tokio_stream::Stream;

/// Noop store which is a placeholder.
#[derive(Default)]
pub struct Store;

#[async_trait]
impl Backing for Store {
    #[inline]
    async fn push(&mut self, _job: &Job) -> super::Result<()> {
        Ok(())
    }

    #[inline]
    async fn remove(&mut self, _job: &Job) -> super::Result<()> {
        Ok(())
    }

    #[inline]
    async fn update(
        &mut self,
        _queue: &str,
        _job_id: &str,
        _state: &Option<Box<RawValue>>,
    ) -> super::Result<()> {
        Ok(())
    }

    #[inline]
    fn recover(&mut self) -> Pin<Box<dyn Stream<Item = super::Result<Job>> + '_>> {
        Box::pin(tokio_stream::empty())
    }
}
