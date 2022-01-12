use crate::memory_store::backing::Backing;
use crate::memory_store::StoredJob;
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
    async fn push(&self, _job: &StoredJob) -> super::Result<()> {
        Ok(())
    }

    #[inline]
    async fn remove(&self, _job: &StoredJob) -> super::Result<()> {
        Ok(())
    }

    #[inline]
    async fn update(
        &self,
        _queue: &str,
        _job_id: &str,
        _state: &Option<Box<RawValue>>,
        _retries: Option<u8>,
        _in_flight: Option<bool>,
    ) -> super::Result<()> {
        Ok(())
    }

    #[inline]
    fn recover(&self) -> Pin<Box<dyn Stream<Item = super::Result<StoredJob>> + '_>> {
        Box::pin(tokio_stream::empty())
    }
}
