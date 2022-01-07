# relay-rs

This crate contains a no nonsense ordered job runner with configurable backends for optionally persisted data.

### Features
Optional features:
- [`prometheus_metrics`][]: Enables emitting of Prometheus metrics via a scraping endpoint.
- [`sqlite_backing`][]: Enables an SQLite backed persistent store to handle crashes/restarts..
- [`postgres_backing`][]: Enables a Postgres backed persistent store to handle crashes/restarts.
- [`redis_backing`][]: Enables a Redis backed persistent store to handle crashes/restarts.
- [`dynamodb_backing`][]: Enables an DynamoDB backed persistent store to handle crashes/restarts.

[`prometheus_metrics`]: https://crates.io/crates/metrics-exporter-prometheus
[`sqlite_backing`]: https://crates.io/crates/sqlx
[`postgres_backing`]: https://crates.io/crates/sqlx
[`redis_backing`]: https://crates.io/crates/redis
[`dynamodb_backing`]: https://crates.io/crates/aws-sdk-dynamodb


## API

### `POST /enqueue`

#### Arguments
In this case the only arguments are part of the Body payload.

| argument     | required | description                                                                                                                                                                                                                                          |
|--------------|----------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| id           | true     | The unique Job ID which is also CAN be used to ensure the Job is a singleton.                                                                                                                                                                        |
| queue        | true     | Is used to differentiate different job types that can be picked up by job runners.                                                                                                                                                                   |
| timeout      | true     | Denotes the duration after a Job has started processing or since the last heartbeat request occurred before considering the Job failed and being put back into the queue.                                                                            |
| persist_data | false    | This determines if the Job will live strictly in-memory, or if set to true, persisted to the configured backing store to withstand restarts or unexpected failure. This will greatly depends on your Job types and guarantees your systems require.  |
| max_retries  | false    | Determines how many times the Job can be retried, due to timeouts, before being considered.                                                                                                                                                          |
| payload      | false    | The raw JSON payload that the job runner will receive.                                                                                                                                                                                               |
| state        | false    | The raw JSON job state that can be persisted during heartbeat requests for progression based data used in the event of crash or failure. This is usually reserved for long-running jobs.                                                             |

#### Request Body
```json
{
  "id": "1",
  "queue": "my-queue",
  "timeout": 30,
  "persist_data": true,
  "max_retries": 0,
  "payload": "RAW JSON",
  "state": "RAW JSON"
}
```

### Response Codes

NOTE: The body of the response will have more detail about the specific error.

| code  | description                                                                 |
|-------|-----------------------------------------------------------------------------|
| 202   | Job enqueued and accepted for processing.                                   |
| 409   | An conflicting Job already exists with the provided id and queue.           |
| 429   | A retryable error occurred. Most likely the backing storage having issues.  |
| 422   | A permanent error has occurred.                                             |
| 500   | An unknown error has occurred server side.                                  |



### `GET /v1/next`

#### Arguments
In this case the only arguments are query params.

| argument | required | description                                        |
|----------|----------|----------------------------------------------------|
| queue    | true     | Used to pull the next job from the provided queue. |

#### Response Body
Some of the fiels May not be present such as `state` when none exists.
```json
{
  "id": "1",
  "queue": "my-queue",
  "timeout": 30,
  "persist_data": true,
  "max_retries": 0,
  "payload": "RAW JSON",
  "state": "RAW JSON"
}
```

#### Response Codes
NOTE: The body of the response will have more detail about the specific error.

| code | description                                                                        |
|------|------------------------------------------------------------------------------------|
| 200  | Job successfully retrieved.                                                        |
| 204  | There is currently no Job in the provided queue to return. Backoff an retry later. |
| 429  | A retryable error occurred. Most likely the backing storage having issues.         |
| 500  | An unknown error has occurred server side.                                         |



### `PATCH /v1/heartbeat`

#### Arguments

In this case the only arguments are query params.

| argument | required | description                           |
|----------|----------|---------------------------------------|
| queue    | true     | The Queue to apply the heartbeat to.  |
| job_id   | true     | The Job ID to apply the heartbeat to. |

#### Request Body
Any JSON data. This payload is persisted in order to save application state.
This is mostly used for long-running jobs to save point-in-time state in order
to restart from that state if the Job is retried due to a crash or servie rollout.

#### Response Codes
NOTE: The body of th response will have more detail about the specific error.

| code  | description                                                                |
|-------|----------------------------------------------------------------------------|
| 202   | Heartbeat successfully applied to the Job.                                 |
| 429   | A retryable error occurred. Most likely the backing storage having issues. |
| 404   | Job was not found for updating.                                            |
| 422   | A permanent error has occurred.                                            |
| 500   | An unknown error has occurred server side.                                 |


### `DELETE /v1/complete`

### Arguments

In this case the only arguments are query params.

| argument | required | description                          |
|----------|----------|--------------------------------------|
| queue    | true     | The Queue to remove the job_id from. |
| job_id   | true     | The Job ID to remove from the queue. |

### Response Codes

NOTE: The body of the response will have more detail about the specific error.

| code  | description                                                                 |
|-------|-----------------------------------------------------------------------------|
| 200   | Job successfully completed.                                                 |
| 429   | A retryable error occurred. Most likely the backing storage having issues.  |
| 404   | Job was not found for completing.                                           |
| 422   | A permanent error has occurred.                                             |
| 500   | An unknown error has occurred server side.                                  |

#### License

<sup>
Licensed under either of <a href="LICENSE-APACHE">Apache License, Version
2.0</a> or <a href="LICENSE-MIT">MIT license</a> at your option.
</sup>

<br>

<sub>
Unless you explicitly state otherwise, any contribution intentionally submitted
for inclusion in Proteus by you, as defined in the Apache-2.0 license, shall be
dual licensed as above, without any additional terms or conditions.
</sub>
