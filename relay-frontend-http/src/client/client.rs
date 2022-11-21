use super::errors::Result;
use crate::client::Error;
use backoff_rs::{Exponential, ExponentialBackoffBuilder};
use percent_encoding::{utf8_percent_encode, NON_ALPHANUMERIC};
use relay_core::Job;
use reqwest::StatusCode;
use serde::de::DeserializeOwned;
use serde::Serialize;
use std::future::Future;
use std::time::Duration;
use tokio::time::sleep;

pub struct ClientBuilder {
    url: String,
    next_backoff: Exponential,
    retry_backoff: Exponential,
    max_retries: Option<usize>,
}

impl ClientBuilder {
    pub fn new(url: &str) -> Self {
        let next_backoff = ExponentialBackoffBuilder::default()
            .interval(Duration::from_millis(200))
            .jitter(Duration::from_millis(25))
            .max(Duration::from_secs(1))
            .build();

        let retry_backoff = ExponentialBackoffBuilder::default()
            .interval(Duration::from_millis(100))
            .jitter(Duration::from_millis(25))
            .max(Duration::from_secs(1))
            .build();

        Self {
            url: url.to_string(),
            next_backoff,
            retry_backoff,
            max_retries: None, // no max retries
        }
    }

    pub fn next_backoff(mut self, backoff: Exponential) -> Self {
        self.next_backoff = backoff;
        self
    }

    pub fn retry_backoff(mut self, backoff: Exponential) -> Self {
        self.retry_backoff = backoff;
        self
    }

    pub fn max_retries(mut self, max_retries: Option<usize>) -> Self {
        self.max_retries = max_retries;
        self
    }

    pub fn build(self) -> Client {
        let client = reqwest::Client::new();
        Client {
            url: self.url,
            client,
            next_backoff: self.next_backoff,
            retry_backoff: self.retry_backoff,
            max_retries: self.max_retries,
        }
    }
}

pub struct Client {
    url: String,
    client: reqwest::Client,
    next_backoff: Exponential,
    retry_backoff: Exponential,
    max_retries: Option<usize>,
}

impl Client {
    pub async fn enqueue<P, S>(&self, job: &Job<P, S>) -> Result<()>
    where
        P: Serialize,
        S: Serialize,
    {
        let url = format!("{}/v1/queues/jobs", self.url);

        self.with_retry(|| async {
            let res = self.client.post(&url).json(&[job]).send().await?;

            match res.status() {
                StatusCode::ACCEPTED => Ok(()),
                _ => {
                    let _ = res.error_for_status()?;
                    panic!("unexpected response code")
                }
            }
        })
        .await
    }

    pub async fn enqueue_batch<P, S>(&self, jobs: &[Job<P, S>]) -> Result<()>
    where
        P: Serialize,
        S: Serialize,
    {
        let url = format!("{}/v1/queues/jobs", self.url);

        self.with_retry(|| async {
            let res = self.client.post(&url).json(jobs).send().await?;

            match res.status() {
                StatusCode::ACCEPTED => Ok(()),
                _ => {
                    let _ = res.error_for_status()?;
                    panic!("unexpected response code")
                }
            }
        })
        .await
    }

    pub async fn remove(&self, queue: &str, job_id: &str) -> Result<()> {
        let queue = url_encode(queue);
        let job_id = url_encode(job_id);
        let url = format!("{}/v1/queues/{queue}/jobs/{job_id}", self.url);

        self.with_retry(|| async {
            let res = self.client.delete(&url).send().await?;

            match res.status() {
                StatusCode::OK => Ok(()),
                _ => {
                    let _ = res.error_for_status()?;
                    panic!("unexpected response code")
                }
            }
        })
        .await
    }

    pub async fn exists(&self, queue: &str, job_id: &str) -> Result<bool> {
        let queue = url_encode(queue);
        let job_id = url_encode(job_id);
        let url = format!("{}/v1/queues/{queue}/jobs/{job_id}", self.url);

        self.with_retry(|| async {
            let res = self.client.head(&url).send().await?;

            match res.status() {
                StatusCode::OK => Ok(true),
                StatusCode::NOT_FOUND => Ok(false),
                _ => {
                    let _ = res.error_for_status()?;
                    panic!("unexpected response code")
                }
            }
        })
        .await
    }

    pub async fn get<P, S>(&self, queue: &str, job_id: &str) -> Result<Job<P, S>>
    where
        P: DeserializeOwned,
        S: DeserializeOwned,
    {
        let queue = url_encode(queue);
        let job_id = url_encode(job_id);
        let url = format!("{}/v1/queues/{queue}/jobs/{job_id}", self.url);

        self.with_retry(|| async {
            let res = self.client.get(&url).send().await?;

            match res.status() {
                StatusCode::OK => Ok(res.json().await?),
                _ => {
                    let _ = res.error_for_status()?;
                    panic!("unexpected response code")
                }
            }
        })
        .await
    }

    pub async fn next<P, S>(&self, queue: &str, num_jobs: usize) -> Result<Vec<Job<P, S>>>
    where
        P: DeserializeOwned,
        S: DeserializeOwned,
    {
        let queue = url_encode(queue);
        let url = format!("{}/v1/queues/{queue}/jobs?num_jobs={num_jobs}", self.url);

        self.with_retry(|| async {
            let res = self.client.get(&url).send().await?;

            match res.status() {
                StatusCode::OK => Ok(res.json().await?),
                StatusCode::NO_CONTENT => Err(Error::Request {
                    status_code: Some(StatusCode::NO_CONTENT),
                    is_retryable: true,
                    is_poll: true,
                }),
                _ => {
                    let _ = res.error_for_status()?;
                    panic!("unexpected response code")
                }
            }
        })
        .await
    }

    pub async fn heartbeat<S>(&self, queue: &str, job_id: &str, state: Option<S>) -> Result<()>
    where
        S: Serialize,
    {
        let queue = url_encode(queue);
        let job_id = url_encode(job_id);
        let url = format!("{}/v1/queues/{queue}/jobs/{job_id}", self.url);

        self.with_retry(|| async {
            let mut req = self.client.patch(&url);

            if let Some(state) = &state {
                req = req.json(state);
            }
            let res = req.send().await?;

            match res.status() {
                StatusCode::ACCEPTED => Ok(()),
                _ => {
                    let _ = res.error_for_status()?;
                    panic!("unexpected response code")
                }
            }
        })
        .await
    }

    pub async fn reschedule<P, S>(&self, job: &Job<P, S>) -> Result<()>
    where
        P: Serialize,
        S: Serialize,
    {
        let url = format!("{}/v1/queues/jobs", self.url);
        self.with_retry(|| async {
            let res = self.client.put(&url).json(&[job]).send().await?;

            match res.status() {
                StatusCode::ACCEPTED => Ok(()),
                _ => {
                    let _ = res.error_for_status()?;
                    panic!("unexpected response code")
                }
            }
        })
        .await
    }

    pub async fn poll<P, S>(&self, queue: &str, num_jobs: usize) -> Result<Vec<Job<P, S>>>
    where
        P: DeserializeOwned,
        S: DeserializeOwned,
    {
        self.with_retry(|| self.next(queue, num_jobs)).await
    }

    async fn with_retry<F, Fut, R>(&self, mut f: F) -> Result<R>
    where
        F: FnMut() -> Fut,
        Fut: Future<Output = Result<R>>,
    {
        let mut attempt = 0;
        loop {
            let result = f().await;
            match result {
                Err(e) => {
                    if e.is_retryable() {
                        if let Some(max_retries) = self.max_retries {
                            if attempt >= max_retries {
                                return Err(e);
                            }
                        }
                        if e.is_poll_retryable() {
                            sleep(self.next_backoff.duration(attempt)).await;
                        } else {
                            sleep(self.retry_backoff.duration(attempt)).await;
                        }
                        attempt += 1;
                        continue;
                    } else {
                        return Err(e);
                    }
                }
                _ => return result,
            };
        }
    }
}

#[inline]
fn url_encode(input: &str) -> String {
    utf8_percent_encode(input, NON_ALPHANUMERIC).to_string()
}
