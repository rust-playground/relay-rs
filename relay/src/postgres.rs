#![allow(clippy::cast_possible_truncation)]
use crate::{Error, Job, JobId, Queue, Result};
use chrono::{NaiveDateTime, TimeZone, Utc};
use deadpool_postgres::{HookError, HookErrorCause, Pool, PoolError};
use metrics::counter;
use pg_interval::Interval;
use serde_json::value::RawValue;
use std::io::ErrorKind;
use std::ops::DerefMut;
use std::{io, str::FromStr, time::Duration};
use tokio_postgres::error::SqlState;
use tokio_postgres::row::RowIndex;
use tokio_postgres::types::{Json, ToSql};
use tokio_postgres::Row;
use tokio_stream::{Stream, StreamExt};
use tracing::{debug, warn};

mod embedded {
    use refinery::embed_migrations;
    embed_migrations!("./migrations");
}

/// Postgres backing store
pub struct PgStore {
    pool: Pool,
}

impl PgStore {
    /// Creates a new backing store with preconfigured pool
    ///
    /// # Errors
    ///
    /// Will return `Err` if connecting the server or running migrations fails.
    #[inline]
    pub async fn new_with_pool(pool: Pool) -> std::result::Result<Self, anyhow::Error> {
        // let mut client: &mut PgClient = pool.get().await?.deref_mut().deref_mut();
        let mut client = pool.get().await?;
        embedded::migrations::runner()
            .run_async(client.deref_mut().deref_mut())
            .await?;
        //     sqlx::migrate!("./migrations").run(&pool).await?;
        //
        //     // insert internal records if they don't already exist
        client
            .execute(
                r#"
                INSERT INTO internal_state (id, last_run) VALUES ('reap',$1) ON CONFLICT DO NOTHING
            "#,
                &[&Utc::now().naive_utc()],
            )
            .await?;
        // }

        Ok(Self { pool })
    }

    /// Enqueues a new Job to be processed.
    ///
    /// # Errors
    ///
    /// Will return `Err` if there is any communication issues with the backend Postgres DB.
    pub async fn enqueue(&self, job: &Job) -> Result<()> {
        let now = Utc::now().naive_utc();
        let run_at = if let Some(run_at) = job.run_at {
            run_at.naive_utc()
        } else {
            now
        };

        let client = self.pool.get().await?;

        let stmt = client.prepare_cached(r#"
            INSERT INTO jobs (id, queue, timeout, max_retries, retries_remaining, data, updated_at, created_at, run_at)
            VALUES ($1, $2, $3, $4, $4, $5, $6, $6, $7)"#
        ).await?;

        client
            .query(
                &stmt,
                &[
                    &job.id,
                    &job.queue,
                    &Interval::from_duration(chrono::Duration::seconds(job.timeout as i64)),
                    &job.max_retries,
                    &Json(&job.payload),
                    &now,
                    &run_at,
                ],
            )
            .await
            .map_err(|e| {
                if let Some(&SqlState::UNIQUE_VIOLATION) = e.code() {
                    Error::JobExists {
                        job_id: job.id.clone(),
                        queue: job.queue.clone(),
                    }
                } else {
                    e.into()
                }
            })?;
        Ok(())
    }

    /// Enqueues a batch of Jobs to be processed in a single write transaction.
    ///
    /// NOTE: That this function will not return an error for conflicts in Job ID, but rather
    ///       silently drop the record using an `ON CONFLICT DO NOTHING`. If you need to have a
    ///       Conflict error returned it is recommended to use the `enqueue` function for a single
    ///       Job instead.
    ///
    /// # Errors
    ///
    /// Will return `Err` if there is any communication issues with the backend Postgres DB.
    pub async fn enqueue_batch(&self, jobs: &[Job]) -> Result<()> {
        let mut client = self.pool.get().await?;
        let transaction = client.transaction().await?;

        let stmt = transaction
            .prepare_cached(
                r#"INSERT INTO jobs (
                          id,
                          queue,
                          timeout,
                          max_retries,
                          retries_remaining,
                          data,
                          updated_at,
                          created_at,
                          run_at
                        )
                        VALUES ($1, $2, $3, $4, $4, $5, $6, $6, $7)
                        ON CONFLICT DO NOTHING"#,
            )
            .await?;

        for job in jobs {
            let now = Utc::now().naive_utc();
            let run_at = if let Some(run_at) = job.run_at {
                run_at.naive_utc()
            } else {
                now
            };

            transaction
                .execute(
                    &stmt,
                    &[
                        &job.id,
                        &job.queue,
                        &Interval::from_duration(chrono::Duration::seconds(job.timeout as i64)),
                        &job.max_retries,
                        &Json(&job.payload),
                        &now,
                        &run_at,
                    ],
                )
                .await?;
        }

        transaction.commit().await?;
        Ok(())
    }

    /// Removed the job from the database.
    ///
    /// # Errors
    ///
    /// Will return `Err` if there is any communication issues with the backend Postgres DB.
    pub async fn remove(&self, queue: &Queue, job_id: &JobId) -> Result<()> {
        let client = self.pool.get().await?;
        let stmt = client
            .prepare_cached(r#"DELETE FROM jobs WHERE queue=$1 AND id=$2"#)
            .await?;
        client.execute(&stmt, &[&queue, &job_id]).await?;
        Ok(())
    }

    /// Returns the next Job to be executed in order of insert. FIFO.
    ///
    /// # Errors
    ///
    /// Will return `Err` if there is any communication issues with the backend Postgres DB.
    pub async fn next(&self, queue: &Queue, num_jobs: i64) -> Result<Option<Vec<Job>>> {
        let client = self.pool.get().await?;

        // MUST USE CTE WITH `FOR UPDATE SKIP LOCKED LIMIT` otherwise the Postgres Query Planner
        // CAN optimize the query which will cause MORE updates than the LIMIT specifies within
        // a nested loop.
        // See here for details:
        // https://github.com/feikesteenbergen/demos/blob/19522f66ffb6eb358fe2d532d9bdeae38d4e2a0b/bugs/update_from_correlated.adoc
        let stmt = client
            .prepare_cached(
                r#"
               WITH subquery AS (
                   SELECT
                        id,
                        queue
                   FROM jobs
                   WHERE
                        queue=$1 AND
                        in_flight=false AND
                        run_at <= NOW()
                   ORDER BY run_at ASC
                   FOR UPDATE SKIP LOCKED
                   LIMIT $2
               )
               UPDATE jobs j
               SET in_flight=true,
                   updated_at=NOW(),
                   expires_at=NOW()+timeout
               FROM subquery
               WHERE
                   j.queue=subquery.queue AND
                   j.id=subquery.id
               RETURNING j.id,
                         j.queue,
                         j.timeout,
                         j.max_retries,
                         j.data,
                         j.state,
                         j.run_at
            "#,
            )
            .await?;

        let params: Vec<&(dyn ToSql)> = vec![&queue, &num_jobs];
        let stream = client.query_raw(&stmt, params).await?;

        tokio::pin!(stream);

        let mut jobs = if let Some(size) = stream.size_hint().1 {
            Vec::with_capacity(size)
        } else {
            Vec::new()
        };

        while let Some(row) = stream.next().await {
            let row = row?;

            let job = Job {
                id: row.get(0),
                queue: row.get(1),
                timeout: interval_seconds(row.get::<usize, Interval>(2)),
                max_retries: row.get(3),
                payload: row.get::<usize, Json<Box<RawValue>>>(4).0,
                state: row
                    .get::<usize, Option<Json<Box<RawValue>>>>(5)
                    .map(|state| match state {
                        Json(state) => state,
                    }),
                run_at: Some(Utc.from_utc_datetime(&row.get(6))),
            };
            jobs.push(job);
        }

        if jobs.is_empty() {
            Ok(None)
        } else {
            Ok(Some(jobs))
        }
    }

    /// Updates the existing in-flight job by incrementing it's `updated_at` and option state.
    ///
    /// # Errors
    ///
    /// Will return `Err` if there is any communication issues with the backend Postgres DB or the
    /// Job attempting to be updated cannot be found.
    pub async fn update(
        &self,
        queue: &str,
        job_id: &str,
        state: Option<Box<RawValue>>,
    ) -> Result<()> {
        let client = self.pool.get().await?;
        let stmt = client
            .prepare_cached(
                r#"
               UPDATE jobs
               SET state=$3,
                   updated_at=NOW(),
                   expires_at=NOW()+timeout
               WHERE
                   queue=$1 AND
                   id=$2 AND
                   in_flight=true
            "#,
            )
            .await?;

        let rows_affected = client
            .execute(
                &stmt,
                &[&queue, &job_id, &state.map(|state| Some(Json(state)))],
            )
            .await?;

        if rows_affected == 0 {
            Err(Error::JobNotFound {
                job_id: job_id.to_string(),
                queue: queue.to_string(),
            })
        } else {
            Ok(())
        }
    }

    /// Reschedules the an existing in-flight Job to be run again with the provided new information.
    ///
    /// The Jobs queue and id must match an existing in-flight Job. This is primarily used to
    /// schedule a new/the next run of a singleton Jon. This provides the ability for
    /// self-perpetuating scheduled jobs.
    ///
    /// # Errors
    ///
    /// Will return `Err` if there is any communication issues with the backend Postgres DB.
    pub async fn reschedule(&self, job: &Job) -> Result<()> {
        let now = Utc::now().naive_utc();
        let run_at = if let Some(run_at) = job.run_at {
            run_at.naive_utc()
        } else {
            now
        };

        let client = self.pool.get().await?;
        let stmt = client
            .prepare_cached(
                r#"
                UPDATE jobs
                SET
                    timeout = $3,
                    max_retries = $4,
                    retries_remaining = $4,
                    data = $5,
                    updated_at = $6,
                    created_at = $6,
                    run_at = $7,
                    in_flight = false
                WHERE
                    queue=$1 AND
                    id=$2 AND
                    in_flight=true
                "#,
            )
            .await?;

        let rows_affected = client
            .execute(
                &stmt,
                &[
                    &job.queue,
                    &job.id,
                    &Interval::from_duration(chrono::Duration::seconds(job.timeout as i64)),
                    &job.max_retries,
                    &Json(&job.payload),
                    &now,
                    &run_at,
                ],
            )
            .await?;

        if rows_affected == 0 {
            Err(Error::JobNotFound {
                job_id: job.id.to_string(),
                queue: job.queue.to_string(),
            })
        } else {
            Ok(())
        }
    }

    /// Reset records to be retries and deletes those that have reached their max.
    ///
    /// # Errors
    ///
    /// Will return `Err` if there is any communication issues with the backend Postgres DB.
    pub async fn reap_timeouts(&self, interval_seconds: i64) -> Result<()> {
        let client = self.pool.get().await?;

        let stmt = client
            .prepare_cached(
                r#"
            UPDATE internal_state 
            SET last_run=NOW() 
            WHERE last_run <= NOW() - $1::text::interval"#,
            )
            .await?;

        let rows_affected = client
            .execute(&stmt, &[&format!("{}s", interval_seconds)])
            .await?;

        // another instance has already updated OR time hasn't been hit yet
        if rows_affected == 0 {
            return Ok(());
        }

        debug!("running timeout reaper");

        let stmt = client
            .prepare_cached(
                r#"
               WITH cte_updates AS (
                   UPDATE jobs
                   SET in_flight=false,
                       retries_remaining=retries_remaining-1
                   WHERE
                       in_flight=true AND
                       expires_at < NOW() AND
                       retries_remaining > 0
                   RETURNING queue
               )
               SELECT queue, COUNT(queue)
               FROM cte_updates
               GROUP BY queue
            "#,
            )
            .await?;

        let params: &[i32] = &[];
        let stream = client.query_raw(&stmt, params).await?;
        tokio::pin!(stream);

        while let Some(row) = stream.next().await {
            let row = row?;
            let queue: String = row.get(0);
            let count: i64 = row.get(1);
            counter!("retries", u64::try_from(count).unwrap_or_default(), "queue" => queue);
        }

        let stmt = client
            .prepare_cached(
                r#"
               WITH cte_updates AS (
                   DELETE FROM jobs
                   WHERE
                       in_flight=true AND
                       expires_at < NOW() AND
                       retries_remaining = 0
                   RETURNING queue
               )
               SELECT queue, COUNT(queue)
               FROM cte_updates
               GROUP BY queue
            "#,
            )
            .await?;

        let params: &[i32] = &[];
        let stream = client.query_raw(&stmt, params).await?;
        tokio::pin!(stream);

        while let Some(row) = stream.next().await {
            let row = row?;
            let queue: String = row.get(0);
            let count: i64 = row.get(1);
            warn!(
                "deleted {} records from queue '{}' that reached their max retries",
                count, queue
            );
            counter!("errors", u64::try_from(count).unwrap_or_default(), "queue" => queue);
        }
        Ok(())
    }
}

#[inline]
fn is_retryable(e: tokio_postgres::Error) -> bool {
    match e.code() {
        Some(
            &SqlState::IO_ERROR
            | &SqlState::TOO_MANY_CONNECTIONS
            | &SqlState::LOCK_NOT_AVAILABLE
            | &SqlState::QUERY_CANCELED
            | &SqlState::SYSTEM_ERROR,
        ) => true,
        Some(_) => false,
        None => {
            if let Some(e) = e
                .into_source()
                .as_ref()
                .and_then(|e| e.downcast_ref::<io::Error>())
            {
                match e.kind() {
                    ErrorKind::ConnectionReset
                    | ErrorKind::ConnectionAborted
                    | ErrorKind::NotConnected
                    | ErrorKind::WouldBlock
                    | ErrorKind::TimedOut
                    | ErrorKind::WriteZero
                    | ErrorKind::Interrupted
                    | ErrorKind::UnexpectedEof => true,
                    _ => false,
                }
            } else {
                false
            }
        }
    }
}

#[inline]
fn is_retryable_pool(e: PoolError) -> bool {
    match e {
        PoolError::Timeout(_) => true,
        PoolError::Backend(e) => is_retryable(e),
        PoolError::PreRecycleHook(e)
        | PoolError::PostCreateHook(e)
        | PoolError::PostRecycleHook(e) => match e {
            HookError::Continue(e) => match e {
                Some(HookErrorCause::Backend(e)) => is_retryable(e),
                _ => true,
            },
            HookError::Abort(e) => match e {
                HookErrorCause::Backend(e) => is_retryable(e),
                _ => true,
            },
        },
        PoolError::Closed | PoolError::NoRuntimeSpecified => false,
    }
}

impl From<PoolError> for Error {
    fn from(e: PoolError) -> Self {
        Error::Postgres {
            message: e.to_string(),
            is_retryable: is_retryable_pool(e),
        }
    }
}

impl From<tokio_postgres::Error> for Error {
    fn from(e: tokio_postgres::Error) -> Self {
        Error::Postgres {
            message: e.to_string(),
            is_retryable: is_retryable(e),
        }
    }
}

fn interval_seconds(interval: Interval) -> i32 {
    let month_secs = interval.months * 30 * 24 * 60 * 60;
    let day_secs = interval.days * 24 * 60 * 60;
    let micro_secs = (interval.microseconds / 1_000_000) as i32;

    month_secs + day_secs + micro_secs
}
