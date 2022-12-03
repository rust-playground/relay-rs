use reqwest::StatusCode;
use thiserror::Error;

/// The Job Result.
pub type Result<T> = std::result::Result<T, Error>;

/// Job error types.
#[derive(Error, Debug)]
pub enum Error {
    /// indicates a Job with the existing ID and Queue already exists.
    #[error("Job does not exists")]
    JobExists,

    /// indicates a Job with the existing ID and Queue could not be found.
    #[error("Job not found")]
    JobNotFound,

    #[error("error occurred making request, status_code: `{status_code:?}` is_retryable: `{is_retryable}` error: {message}")]
    Request {
        status_code: Option<StatusCode>,
        is_retryable: bool,
        is_poll: bool,
        message: String,
    },
}

impl Error {
    #[inline]
    #[must_use]
    pub fn is_retryable(&self) -> bool {
        match self {
            Error::Request { is_retryable, .. } => *is_retryable,
            _ => false,
        }
    }

    #[inline]
    #[must_use]
    pub fn is_poll_retryable(&self) -> bool {
        match self {
            Error::Request { is_poll, .. } => *is_poll,
            _ => false,
        }
    }
}

impl From<reqwest::Error> for Error {
    fn from(err: reqwest::Error) -> Self {
        match err.status() {
            Some(StatusCode::NOT_FOUND) => Error::JobNotFound,
            Some(StatusCode::CONFLICT) => Error::JobExists,
            sc @ Some(
                StatusCode::SERVICE_UNAVAILABLE
                | StatusCode::TOO_MANY_REQUESTS
                | StatusCode::BAD_GATEWAY
                | StatusCode::GATEWAY_TIMEOUT
                | StatusCode::REQUEST_TIMEOUT,
            ) => Error::Request {
                status_code: sc,
                is_retryable: true,
                is_poll: false,
                message: sc.unwrap().to_string(),
            },
            sc => Error::Request {
                status_code: sc,
                is_retryable: err.is_timeout(),
                is_poll: false,
                message: err.to_string(),
            },
        }
    }
}
