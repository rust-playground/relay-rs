# scheduler API

This contains an HTTP server that exposes the scheduler functionality over HTTP.

## API

### `POST /`

This will upsert a Scheduled Job

#### Arguments
In this case the only arguments are part of the Body payload.

| argument              | required | description                                                                                                                                                                                           |
|-----------------------|----------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| id                    | true     | The unique Scheduled Job ID which is also CAN be used to ensure the Job is a singleton.                                                                                                               |
| endpoint              | true     | The Job Runner URL to enqueue jobs to.                                                                                                                                                                |
| cron                  | true     | The CRON schedule to run the Job on. Note: This supports Vixie CRON which support seconds precision.                                                                                                  |
| payload               | true     | The Scheduled Job that will be posted to the Job runner URL provided.                                                                                                                                 |
| retry_already_running | false    | This determines the backoff time in seconds if we should run the Job right after the old one completes or wait for the next Scheduled time if not specified when it's detected to already be running. |
| recovery_check        | false    | Determines if when starting back up/recovering we should check if Jobs should have been fired during the downtime and run it if so.                                                                   |

#### Request Body
```json
{
  "id": "job-scheduler-unique-id",
  "endpoint": "http://localhost:8080/enqueue",
  "cron": "*/10 * * * * * *",
  "payload": {
    "id": "1",
    "queue": "test-queue",
    "timeout": 30,
    "persist_data": true,
    "payload": "say my name"
  },
  "retry_already_running": 3,
  "recovery_check": true
}
```

### Response Codes

NOTE: The body of the response will have more detail about the specific error.

| code | description                                                                |
|------|----------------------------------------------------------------------------|
| 202  | Job enqueued and accepted for processing.                                  |
| 400  | A bad request payload was received.                                        |
| 429  | A retryable error occurred. Most likely the backing storage having issues. |
| 422  | A permanent error has occurred, usually a backing store issue.             |
| 500  | An unknown error has occurred server side.                                 |


### `DELETE /`

### Arguments

In this case the only arguments are query params.

| argument | required | description                                    |
|----------|----------|------------------------------------------------|
| job_id   | true     | The Scheduled Job ID to remove from the queue. |

### Response Codes

NOTE: The body of the response will have more detail about the specific error.

| code  | description                                                                |
|-------|----------------------------------------------------------------------------|
| 200   | Scheduled Job successfully completed.                                      |
| 429   | A retryable error occurred. Most likely the backing storage having issues. |
| 404   | Scheduled Job was not found for deletion.                                  |
| 422   | A permanent error has occurred.                                            |
| 500   | An unknown error has occurred server side.                                 |

