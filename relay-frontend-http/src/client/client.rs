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

/// A Builder can be used to create a custom `Client`.
pub struct Builder {
    url: String,
    poll_backoff: Exponential,
    retry_backoff: Exponential,
    max_retries: Option<usize>,
    client: reqwest::Client,
}

impl Builder {
    /// Initializes a new Builder with sane defaults to create a custom `Client`
    #[must_use]
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

        let client = reqwest::ClientBuilder::default()
            .timeout(Duration::from_secs(60))
            .connect_timeout(Duration::from_secs(10))
            .tcp_keepalive(Duration::from_secs(60))
            .http2_keep_alive_timeout(Duration::from_secs(60))
            .http2_keep_alive_interval(Duration::from_secs(5))
            .http2_keep_alive_while_idle(true)
            .pool_max_idle_per_host(512)
            .build()
            .expect("valid default HTTP Client configuration");

        Self {
            url: url.to_string(),
            poll_backoff: next_backoff,
            retry_backoff,
            max_retries: None, // no max retries
            client,
        }
    }

    /// Set a custom backoff used when polling for `Job`s.
    #[must_use]
    pub fn poll_backoff(mut self, backoff: Exponential) -> Self {
        self.poll_backoff = backoff;
        self
    }

    /// Set a custom backoff used when retrying temporary errors interacting with the Relay server.
    #[must_use]
    pub fn retry_backoff(mut self, backoff: Exponential) -> Self {
        self.retry_backoff = backoff;
        self
    }

    /// Set the maximum number of retries to perform before considering it a permanent error.
    ///
    /// # Default
    ///
    /// None - retry forever.
    #[must_use]
    pub fn max_retries(mut self, max_retries: Option<usize>) -> Self {
        self.max_retries = max_retries;
        self
    }

    /// Sets a custom HTTP [Client](https://docs.rs/reqwest/latest/reqwest/struct.Client.html#) for use.
    #[must_use]
    pub fn client(mut self, client: reqwest::Client) -> Self {
        self.client = client;
        self
    }

    /// Creates a `Client` which uses the Builders configuration.
    #[must_use]
    pub fn build(self) -> Client {
        Client {
            url: self.url,
            client: self.client,
            poll_backoff: self.poll_backoff,
            retry_backoff: self.retry_backoff,
            max_retries: self.max_retries,
        }
    }
}

/// Relay HTTP Client.
pub struct Client {
    url: String,
    client: reqwest::Client,
    poll_backoff: Exponential,
    retry_backoff: Exponential,
    max_retries: Option<usize>,
}

impl Client {
    /// Enqueues a single `Job`
    ///
    /// # Errors
    ///
    /// Will return `Err` on:
    /// - an unrecoverable network error.
    /// - if the `Job` already exists
    pub async fn enqueue<P, S>(&self, job: &Job<P, S>) -> Result<()>
    where
        P: Serialize,
        S: Serialize,
    {
        let url = format!("{}/v1/queues/jobs", self.url);

        self.with_retry(|| async {
            let res = self.client.post(&url).json(&[job]).send().await?;
            let status_code = res.status();

            if status_code == StatusCode::ACCEPTED {
                Ok(())
            } else {
                res.error_for_status()?;
                Err(Error::Request {
                    status_code: Some(status_code),
                    is_retryable: false,
                    is_poll: false,
                    message: "unexpected HTTP response code".to_string(),
                })
            }
        })
        .await
    }

    /// Enqueues a batch of `Job`s for processing.
    ///
    /// # Note
    ///
    /// If a `Job` included in the batch already exists it will be silently ignored.
    ///
    /// # Errors
    ///
    /// Will return `Err` on an unrecoverable network error.
    pub async fn enqueue_batch<P, S>(&self, jobs: &[Job<P, S>]) -> Result<()>
    where
        P: Serialize,
        S: Serialize,
    {
        let url = format!("{}/v1/queues/jobs", self.url);

        self.with_retry(|| async {
            let res = self.client.post(&url).json(jobs).send().await?;
            let status_code = res.status();

            if status_code == StatusCode::ACCEPTED {
                Ok(())
            } else {
                res.error_for_status()?;
                Err(Error::Request {
                    status_code: Some(status_code),
                    is_retryable: false,
                    is_poll: false,
                    message: "unexpected HTTP response code".to_string(),
                })
            }
        })
        .await
    }

    /// Removes/Completes a `Job`. This should be used when removing or Completing a `Job` since
    /// Relay does not retain `Job` information once it;s no longer needed.
    ///
    /// # Errors
    ///
    /// Will return `Err` on:
    /// - an unrecoverable network error.
    /// - the `Job` doesn't exist.
    pub async fn remove(&self, queue: &str, job_id: &str) -> Result<()> {
        let queue = url_encode(queue);
        let job_id = url_encode(job_id);
        let url = format!("{}/v1/queues/{queue}/jobs/{job_id}", self.url);

        self.with_retry(|| async {
            let res = self.client.delete(&url).send().await?;
            let status_code = res.status();

            if status_code == StatusCode::OK {
                Ok(())
            } else {
                res.error_for_status()?;
                Err(Error::Request {
                    status_code: Some(status_code),
                    is_retryable: false,
                    is_poll: false,
                    message: "unexpected HTTP response code".to_string(),
                })
            }
        })
        .await
    }

    /// Returns if a `Job` exists.
    ///
    /// # Errors
    ///
    /// Will return `Err` on an unrecoverable network error.
    pub async fn exists(&self, queue: &str, job_id: &str) -> Result<bool> {
        let queue = url_encode(queue);
        let job_id = url_encode(job_id);
        let url = format!("{}/v1/queues/{queue}/jobs/{job_id}", self.url);

        self.with_retry(|| async {
            let res = self.client.head(&url).send().await?;

            match res.status() {
                StatusCode::OK => Ok(true),
                StatusCode::NOT_FOUND => Ok(false),
                sc => {
                    res.error_for_status()?;
                    Err(Error::Request {
                        status_code: Some(sc),
                        is_retryable: false,
                        is_poll: false,
                        message: "unexpected HTTP response code".to_string(),
                    })
                }
            }
        })
        .await
    }

    /// Attempts to return the a `Job` in order to report or display it's state.
    ///
    /// # Errors
    ///
    /// Will return `Err` on:
    /// - an unrecoverable network error.
    /// - if the `Job` doesn't exist.
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
            let status_code = res.status();

            if status_code == StatusCode::OK {
                Ok(res.json().await?)
            } else {
                res.error_for_status()?;
                Err(Error::Request {
                    status_code: Some(status_code),
                    is_retryable: false,
                    is_poll: false,
                    message: "unexpected HTTP response code".to_string(),
                })
            }
        })
        .await
    }

    /// Attempts to retrieve the next `Job` for processing.
    ///
    /// # Errors
    ///
    /// Will return `Err` on:
    /// - an unrecoverable network error.
    /// - no `Job`s currently exists.
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
                    message: "no jobs found for processing".to_string(),
                }),
                sc => {
                    res.error_for_status()?;
                    Err(Error::Request {
                        status_code: Some(sc),
                        is_retryable: false,
                        is_poll: false,
                        message: "unexpected HTTP response code".to_string(),
                    })
                }
            }
        })
        .await
    }

    /// Sends a heartbeat request to indicate a `Job` is still processing, resetting the timeout.
    /// Optionally you can update the `Job` state during the same request.
    ///
    /// # Errors
    ///
    /// Will return `Err` on:
    /// - an unrecoverable network error.
    /// - if the `Job` doesn't exist.
    pub async fn heartbeat<S>(&self, queue: &str, job_id: &str, state: Option<S>) -> Result<()>
    where
        S: Serialize,
    {
        let queue = url_encode(queue);
        let job_id = url_encode(job_id);
        let url = format!("{}/v1/queues/{queue}/jobs/{job_id}", self.url);

        self.with_retry(|| async {
            let mut request = self.client.patch(&url);

            if let Some(state) = &state {
                request = request.json(state);
            }
            let res = request.send().await?;
            let status_code = res.status();

            if status_code == StatusCode::ACCEPTED {
                Ok(())
            } else {
                res.error_for_status()?;
                Err(Error::Request {
                    status_code: Some(status_code),
                    is_retryable: false,
                    is_poll: false,
                    message: "unexpected HTTP response code".to_string(),
                })
            }
        })
        .await
    }

    /// Reschedules provided `Job` atomically to run again.
    ///
    /// # Errors
    ///
    /// Will return `Err` on:
    /// - an unrecoverable network error.
    /// - if the `Job` doesn't exists.
    pub async fn reschedule<P, S>(&self, job: &Job<P, S>) -> Result<()>
    where
        P: Serialize,
        S: Serialize,
    {
        let url = format!("{}/v1/queues/jobs", self.url);
        self.with_retry(|| async {
            let res = self.client.put(&url).json(job).send().await?;
            let status_code = res.status();

            if status_code == StatusCode::ACCEPTED {
                Ok(())
            } else {
                res.error_for_status()?;
                Err(Error::Request {
                    status_code: Some(status_code),
                    is_retryable: false,
                    is_poll: false,
                    message: "unexpected HTTP response code".to_string(),
                })
            }
        })
        .await
    }

    /// Polls the Relay server until a `Job` becomes available.
    ///
    /// # Errors
    ///
    /// Will return `Err` on an unrecoverable network error.
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
                            sleep(self.poll_backoff.duration(attempt)).await;
                        } else {
                            sleep(self.retry_backoff.duration(attempt)).await;
                        }
                        attempt += 1;
                        continue;
                    }
                    return Err(e);
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
