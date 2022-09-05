//! # relay-rs API
//!
//! This outlines the HTTP serve that exposes relays functionality.
//!
//! ## API
//!
//! ### `POST /v1/queues/jobs`
//!
//! Enqueues Job(s) to be processed. If a single Job is sent you can leverage HTTP 409 to know if
//! the Job exists and was not created. When more that one Job is posted then conflicts of existing
//! Jobs are ignored.
//!
//! #### Arguments
//! In this case the only arguments are part of the Body payload.
//!
//! | argument    | required | description                                                                                                                                                                            |
//! |-------------|----------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
//! | `id`          | true     | The unique Job Id which is also CAN be used to ensure the Job is a singleton within a Queue.                                                                                           |
//! | `queue`       | true     | Is used to differentiate different job types that can be picked up by job runners.                                                                                                     |
//! | `timeout`     | true     | Denotes the duration, in seconds, after a Job has started processing or since the last heartbeat request occurred before considering the Job failed and being put back into the queue. |
//! | `max_retries` | false    | Determines how many times the Job can be retried, due to timeouts, before being considered. Infinite retries are supported by using a negative number eg. -1                           |
//! | `payload`     | false    | The raw JSON payload that the job runner will receive.                                                                                                                                 |
//! | `run_at`      | false    | Schedule/set a Job to be run only at a specific time in the future.                                                                                                                    |
//!
//! #### Request Body
//! ```json
//! [
//!     {
//!         "id": "1",
//!         "queue": "my-queue",
//!         "timeout": 30,
//!         "max_retries": 0,
//!         "payload": "RAW JSON"
//!     }
//! ]
//! ```
//!
//! ### Response Codes
//!
//! NOTE: The body of the response will have more detail about the specific error.
//!
//! | code  | description                                                                                                |
//! |-------|------------------------------------------------------------------------------------------------------------|
//! | 202   | Job enqueued and accepted for processing.                                                                  |
//! | 400   | For a bad/ill-formed request.                                                                              |
//! | 409   | An conflicting Job already exists with the provided id and queue. Reminder only if a single Job is posted. |
//! | 429   | A retryable error occurred. Most likely the backing storage having issues.                                 |
//! | 422   | A permanent error has occurred.                                                                            |
//! | 500   | An unknown error has occurred server side.                                                                 |
//!
//!
//!
//! ### `GET /v1/queues/{queue}/jobs`
//!
//! Retrieves the next `Job(s)` to be processed based on the provided `queue`.
//!
//! #### Arguments
//! In this case the only arguments are path and query params.
//!
//! |  argument  | required | description                                                                    |
//! |------------|----------|--------------------------------------------------------------------------------|
//! | `queue`    | true     | Used to pull the next job from the requested queue.                            |
//! | `num_jobs` | false    | Specifies how many Jobs to pull to process next in one request. Defaults to 1. |
//!
//! #### Response Body
//! Some fields may not be present such as `state` when none exists.
//! ```json
//! [
//!     {
//!         "id": "1",
//!         "queue": "my-queue",
//!         "timeout": 30,
//!         "max_retries": 0,
//!         "payload": "RAW JSON",
//!         "state": "RAW JSON",
//!         "run_at": "2022-09-05T04:37:23Z",
//!         "updated_at": "2022-09-05T04:37:23Z",
//!     }
//! ]
//! ```
//!
//! #### Response Codes
//! NOTE: The body of the response will have more detail about the specific error.
//!
//! | code | description                                                                        |
//! |------|------------------------------------------------------------------------------------|
//! | 200  | Job successfully retrieved.                                                        |
//! | 204  | There is currently no Job in the provided queue to return. Backoff an retry later. |
//! | 429  | A retryable error occurred. Most likely the backing storage having issues.         |
//! | 500  | An unknown error has occurred server side.                                         |
//!
//!
//!
//! ### `PATCH /v1/queues/{queue}/jobs/{id}`
//!
//! Updates an in-flight Job incrementing its timestamp and optionally setting some state in case of failure.
//!
//! #### Arguments
//!
//! In this case the only arguments are path params and JSON body.
//!
//! | argument | required | description                                                      |
//! |----------|----------|------------------------------------------------------------------|
//! | `queue`  | true     | The Queue to apply the heartbeat to.                             |
//! | `id`     | true     | The Job ID to apply the heartbeat to within the supplied Queue.  |
//!
//!
//! #### Request Body
//! Any JSON data. This payload is persisted in order to save application state.
//! This is mostly used for long-running jobs to save point-in-time state in order
//! to restart from that state if the Job is retried due to a crash or service interruption.
//!
//! #### Response Codes
//! NOTE: The body of th response will have more detail about the specific error.
//!
//! | code  | description                                                                |
//! |-------|----------------------------------------------------------------------------|
//! | 202   | Heartbeat successfully applied to the Job.                                 |
//! | 429   | A retryable error occurred. Most likely the backing storage having issues. |
//! | 404   | Job was not found for updating.                                            |
//! | 422   | A permanent error has occurred.                                            |
//! | 500   | An unknown error has occurred server side.                                 |
//!
//!
//! ### `PUT /v1/queues/jobs`
//!
//! This endpoint schedules jobs that have the option of being self-perpetuated in combination
//! with the `run_at` field.
//!
//! #### Arguments
//! In this case the only arguments are part of the Body payload.
//!
//! | argument    | required | description                                                                                                                                                                              |
//! |-------------|----------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
//! | `id`          | true     | The unique Job Id which is also CAN be used to ensure the Job is a singleton within a Queue.                                                                                           |
//! | `queue`       | true     | Is used to differentiate different job types that can be picked up by job runners.                                                                                                     |
//! | `timeout`     | true     | Denotes the duration, in seconds, after a Job has started processing or since the last heartbeat request occurred before considering the Job failed and being put back into the queue. |
//! | `max_retries` | false    | Determines how many times the Job can be retried, due to timeouts, before being considered.                                                                                            |
//! | `payload`     | false    | The raw JSON payload that the job runner will receive.                                                                                                                                 |
//! | `state`       | false    | The raw JSON state for the rescheduled Job. If NOT supplied will be reset to nil like a newly enqueued Job.                                                                            |
//! | `run_at`      | false    | Schedule/set a Job to be run only at a specific time in the future.                                                                                                                    |
//!
//! #### Request Body
//! ```json
//! {
//!     "id": "1",
//!     "queue": "my-queue",
//!     "timeout": 30,
//!     "max_retries": 0,
//!     "payload": "RAW JSON"
//! }
//! ```
//!
//! ### Response Codes
//!
//! NOTE: The body of the response will have more detail about the specific error.
//!
//! | code  | description                                                                 |
//! |-------|-----------------------------------------------------------------------------|
//! | 202   | Job enqueued and accepted for processing.                                   |
//! | 400   | For a bad/ill-formed request.                                               |
//! | 404   | Job was not found for updating.                                             |
//! | 429   | A retryable error occurred. Most likely the backing storage having issues.  |
//! | 422   | A permanent error has occurred.                                             |
//! | 500   | An unknown error has occurred server side.                                  |
//!
//!
//!
//! ### `DELETE /v1/queues/{queues}/jobs/{id}`
//!
//! Completes a Job by removing it.
//!
//! ### Arguments
//!
//! In this case the only arguments are query params.
//!
//! | argument | required | description                            |
//! |----------|----------|----------------------------------------|
//! | `queue`  | true     | The Queue to remove the Job id from.   |
//! | `id`     | true     | The Job ID to remove from the `queue`. |
//!
//! ### Response Codes
//!
//! NOTE: The body of the response will have more detail about the specific error.
//!
//! | code  | description                                                                 |
//! |-------|-----------------------------------------------------------------------------|
//! | 200   | Job successfully completed.                                                 |
//! | 429   | A retryable error occurred. Most likely the backing storage having issues.  |
//! | 404   | Job was not found for completing.                                           |
//! | 422   | A permanent error has occurred.                                             |
//! | 500   | An unknown error has occurred server side.                                  |
//!
//!
//!
//! ### `HEAD /v1/queues/{queue}/jobs/{id}`
//!
//! Using HTTP response codes returns if the Job exists.
//!
//! ### Arguments
//!
//! In this case the only arguments are path params.
//!
//! | argument | required | description                            |
//! |----------|----------|----------------------------------------|
//! | `queue`  | true     | The Queue to remove the Job id from.   |
//! | `id`     | true     | The Job ID to remove from the `queue`. |
//!
//! ### Response Codes
//!
//! NOTE: The body of the response will have more detail about the specific error.
//!
//! | code  | description                                                                |
//! |-------|----------------------------------------------------------------------------|
//! | 200   | Job exists                                                                 |
//! | 429   | A retryable error occurred. Most likely the backing storage having issues. |
//! | 404   | Job was not found and so did not exist.                                    |
//! | 422   | A permanent error has occurred.                                            |
//! | 500   | An unknown error has occurred server side.                                 |
//!
//!
//! ### `GET /v1/queues/{queue}/jobs/{id}`
//!
//! Fetches the Job from the database if it exists.
//!
//! #### Arguments
//! In this case the only arguments are path params.
//!
//! | argument    | required | description                                                                                                                                                                            |
//! |-------------|----------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
//! | `queue`     | true     | Is used to differentiate different job types that can be picked up by job runners.                                                                                                     |
//! | `id`        | true     | The unique Job Id which is also CAN be used to ensure the Job is a singleton within a Queue.                                                                                           |
//!
//! #### Request Body
//! ```json
//! [
//!     {
//!       "id": "1",
//!       "queue": "my-queue",
//!       "timeout": 30,
//!       "max_retries": 0,
//!       "payload": "RAW JSON",
//!       ...
//!     }
//! ]
//! ```
//!
//! ### Response Codes
//!
//! NOTE: The body of the response will have more detail about the specific error.
//!
//! | code | description                                                               |
//! |------|---------------------------------------------------------------------------|
//! | 200  | Job found and in the response body.                                       |
//! | 400  | For a bad/ill-formed request.                                             |
//! | 429  | A retryable error occurred. Most likely the backing storage having issues.|
//! | 422  | A permanent error has occurred.                                           |
//! | 500  | An unknown error has occurred server side.                                |

mod http;

pub use http::Server;
