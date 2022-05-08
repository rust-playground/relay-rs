use crate::jobs::Job;
use reqwest::{ClientBuilder, StatusCode};
use serde::de::DeserializeOwned;
use serde::Serialize;
use std::error::Error as StdError;
use std::time::Duration;
use thiserror::Error;

pub struct Client {
    base_url: String,
    client: reqwest::Client,
}

impl Client {
    pub fn new(base_url: &str, user_agent: &str) -> std::result::Result<Self, Box<dyn StdError>> {
        Ok(Self {
            base_url: base_url.trim_end_matches('/').to_string(),
            client: ClientBuilder::new()
                .user_agent(user_agent)
                .timeout(Duration::from_secs(15))
                .connect_timeout(Duration::from_secs(5))
                .pool_max_idle_per_host(512)
                .pool_idle_timeout(Duration::from_secs(90))
                .http1_only()
                .build()?,
        })
    }

    pub async fn enqueue<P, S>(&self, payload: Job<P, S>) -> Result<()>
    where
        P: Serialize + DeserializeOwned,
        S: Serialize + DeserializeOwned,
    {
        let url = format!("{}/enqueue", self.base_url);

        match self.client.post(&url).json(&payload).send().await {
            Ok(resp) => {
                if resp.status() == StatusCode::ACCEPTED {
                    Ok(())
                } else {
                    panic!("Unexpected response status: {}", resp.status());
                }
            }
            Err(e) => match e {
                e if e.is_timeout() | e.is_connect() => Err(Error::Retryable(e)),
                e if e.is_status() => match e.status() {
                    Some(status) if status == StatusCode::CONFLICT => {
                        Err(Error::JobAlreadyExists(e))
                    }
                    _ => Err(Error::Request(e)),
                },
                _ => Err(Error::Request(e)),
            },
        }
    }
}

/// The Job action Result.
pub type Result<T> = std::result::Result<T, Error>;

/// Job action error types.
#[derive(Error, Debug)]
pub enum Error {
    #[error(transparent)]
    Retryable(reqwest::Error),

    #[error(transparent)]
    Request(reqwest::Error),

    #[error("Job not found")]
    JobNotFound,

    #[error(transparent)]
    JobAlreadyExists(reqwest::Error),
}

#[cfg(test)]
mod tests {
    use super::*;
    use uuid::Uuid;

    #[tokio::test]
    async fn test_enqueue() -> anyhow::Result<()> {
        let client = Client::new("http://localhost:8080", "RelayClient/test").unwrap();
        let job_id = Uuid::new_v4().to_string();
        let queue = Uuid::new_v4().to_string();
        let job: Job<String, ()> = Job {
            id: job_id.clone(),
            queue: queue.clone(),
            timeout: 30,
            max_retries: 0,
            payload: "test".to_string(),
            state: None,
            run_at: None,
        };
        client.enqueue(job).await?;
        Ok(())
    }
}
