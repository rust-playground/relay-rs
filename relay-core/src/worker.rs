use crate::job::Job;
use async_trait::async_trait;

#[async_trait]
pub trait Worker<C, P, S> {
    async fn run(&self, client: &C, job: Job<P, S>);
}
