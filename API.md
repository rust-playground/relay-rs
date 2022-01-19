# relay-rs API

This outlines the HTTP serve that exposes relays functionality.

## API

### `POST /enqueue`

#### Arguments
In this case the only arguments are part of the Body payload.

| argument     | required | description                                                                                                                                                                            |
|--------------|----------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| id           | true     | The unique Job Id which is also CAN be used to ensure the Job is a singleton within a Queue.                                                                                           |
| queue        | true     | Is used to differentiate different job types that can be picked up by job runners.                                                                                                     |
| timeout      | true     | Denotes the duration, in seconds, after a Job has started processing or since the last heartbeat request occurred before considering the Job failed and being put back into the queue. |
| max_retries  | false    | Determines how many times the Job can be retried, due to timeouts, before being considered.                                                                                            |
| payload      | false    | The raw JSON payload that the job runner will receive.                                                                                                                                 |

#### Request Body
```json
{
  "id": "1",
  "queue": "my-queue",
  "timeout": 30,
  "max_retries": 0,
  "payload": "RAW JSON"
}
```

### Response Codes

NOTE: The body of the response will have more detail about the specific error.

| code  | description                                                                 |
|-------|-----------------------------------------------------------------------------|
| 202   | Job enqueued and accepted for processing.                                   |
| 400   | For a bad/ill-formed request.                                               |
| 409   | An conflicting Job already exists with the provided id and queue.           |
| 429   | A retryable error occurred. Most likely the backing storage having issues.  |
| 422   | A permanent error has occurred.                                             |
| 500   | An unknown error has occurred server side.                                  |



### `GET /v1/next`

#### Arguments
In this case the only arguments are query params.

| argument | required | description                                         |
|----------|----------|-----------------------------------------------------|
| queue    | true     | Used to pull the next job from the requested queue. |

#### Response Body
Some fields may not be present such as `state` when none exists.
```json
{
  "id": "1",
  "queue": "my-queue",
  "timeout": 30,
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

| argument | required | description                                                      |
|----------|----------|------------------------------------------------------------------|
| queue    | true     | The Queue to apply the heartbeat to.                             |
| job_id   | true     | The Job ID to apply the heartbeat to within the supplied Queue.  |


#### Request Body
Any JSON data. This payload is persisted in order to save application state.
This is mostly used for long-running jobs to save point-in-time state in order
to restart from that state if the Job is retried due to a crash or service interruption.

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