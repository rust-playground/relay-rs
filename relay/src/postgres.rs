#![allow(clippy::cast_possible_truncation)]
use crate::{Error, Job, JobId, Queue, Result};
use anyhow::private::kind::TraitKind;
use chrono::{NaiveDateTime, TimeZone, Utc};
use deadpool_postgres::{HookError, HookErrorCause, Pool, PoolError};
use log::LevelFilter;
use metrics::counter;
use serde_json::value::RawValue;
use std::io::ErrorKind;
use std::ops::DerefMut;
use std::{io, str::FromStr, time::Duration};
use tokio_postgres::error::SqlState;
use tokio_postgres::{Client as PgClient, Row};
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
        unimplemented!()
        // let now = Utc::now();
        // let run_at = if let Some(run_at) = job.run_at {
        //     run_at
        // } else {
        //     now
        // };
        //
        // sqlx::query("INSERT INTO jobs (id, queue, timeout, max_retries, retries_remaining, data, updated_at, created_at, run_at) VALUES ($1, $2, $3, $4, $4, $5, $6, $6, $7)")
        //     .bind(&job.id)
        //     .bind(&job.queue)
        //     .bind(PgInterval{
        //         months: 0,
        //         days: 0,
        //         microseconds: i64::from(job.timeout )*1_000_000
        //     }  )
        //     .bind(job.max_retries)
        //     .bind(Json(&job.payload))
        //     .bind(&now)
        //     .bind(&run_at)
        //     .execute(&self.pool)
        //     .await
        //     .map_err(|e| {
        //         if let sqlx::Error::Database(ref db) = e {
        //             if let Some(code) = db.code() {
        //                 // 23505 = unique_violation
        //                 if code == "23505" {
        //                     return Error::JobExists {
        //                         job_id: job.id.clone(),
        //                         queue: job.queue.clone(),
        //                     }
        //                 }
        //             }
        //         }
        //
        //         Error::Postgres {
        //             message: e.to_string(),
        //             is_retryable: is_retryable(e),
        //         }
        //     })?;
        // Ok(())
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
        unimplemented!();
        // let mut transaction = self.pool.begin().await.map_err(|e| Error::Postgres {
        //     message: e.to_string(),
        //     is_retryable: is_retryable(e),
        // })?;
        //
        // for job in jobs {
        //     let now = Utc::now();
        //     let run_at = if let Some(run_at) = job.run_at {
        //         run_at
        //     } else {
        //         now
        //     };
        //
        //     sqlx::query(
        //         r#"INSERT INTO jobs (
        //                   id,
        //                   queue,
        //                   timeout,
        //                   max_retries,
        //                   retries_remaining,
        //                   data,
        //                   updated_at,
        //                   created_at,
        //                   run_at
        //                 )
        //                 VALUES ($1, $2, $3, $4, $4, $5, $6, $6, $7)
        //                 ON CONFLICT DO NOTHING"#,
        //     )
        //     .bind(&job.id)
        //     .bind(&job.queue)
        //     .bind(PgInterval {
        //         months: 0,
        //         days: 0,
        //         microseconds: i64::from(job.timeout) * 1_000_000,
        //     })
        //     .bind(job.max_retries)
        //     .bind(Json(&job.payload))
        //     .bind(&now)
        //     .bind(&run_at)
        //     .execute(&mut transaction)
        //     .await
        //     .map_err(|e| Error::Postgres {
        //         message: e.to_string(),
        //         is_retryable: is_retryable(e),
        //     })?;
        // }
        //
        // transaction.commit().await.map_err(|e| Error::Postgres {
        //     message: e.to_string(),
        //     is_retryable: is_retryable(e),
        // })?;
        //
        // Ok(())
    }

    /// Removed the job from the database.
    ///
    /// # Errors
    ///
    /// Will return `Err` if there is any communication issues with the backend Postgres DB.
    pub async fn remove(&self, queue: &Queue, job_id: &JobId) -> Result<()> {
        unimplemented!();
        // sqlx::query("DELETE FROM jobs WHERE queue=$1 AND id=$2")
        //     .bind(queue)
        //     .bind(job_id)
        //     .execute(&self.pool)
        //     .await
        //     .map_err(|e| Error::Postgres {
        //         message: e.to_string(),
        //         is_retryable: is_retryable(e),
        //     })?;
        //
        // Ok(())
    }

    /// Returns the next Job to be executed in order of insert. FIFO.
    ///
    /// # Errors
    ///
    /// Will return `Err` if there is any communication issues with the backend Postgres DB.
    pub async fn next(&self, queue: &Queue, num_jobs: u32) -> Result<Option<Vec<Job>>> {
        unimplemented!();
        // // MUST USE CTE WITH `FOR UPDATE SKIP LOCKED LIMIT` otherwise the Postgres Query Planner
        // // CAN optimize the query which will cause MORE updates than the LIMIT specifies within
        // // a nested loop.
        // // See here for details:
        // // https://github.com/feikesteenbergen/demos/blob/19522f66ffb6eb358fe2d532d9bdeae38d4e2a0b/bugs/update_from_correlated.adoc
        // let jobs = sqlx::query(
        //     r#"
        //        WITH subquery AS (
        //            SELECT
        //                 id,
        //                 queue
        //            FROM jobs
        //            WHERE
        //                 queue=$1 AND
        //                 in_flight=false AND
        //                 run_at <= NOW()
        //            ORDER BY run_at ASC
        //            FOR UPDATE SKIP LOCKED
        //            LIMIT $2
        //        )
        //        UPDATE jobs j
        //        SET in_flight=true,
        //            updated_at=NOW(),
        //            expires_at=NOW()+timeout
        //        FROM subquery
        //        WHERE
        //            j.queue=subquery.queue AND
        //            j.id=subquery.id
        //        RETURNING j.id,
        //                  j.queue,
        //                  j.timeout,
        //                  j.max_retries,
        //                  j.data,
        //                  j.state,
        //                  j.run_at
        //     "#,
        // )
        // .bind(queue)
        // .bind(num_jobs)
        // .map(|row: PgRow| {
        //     // map the row into a user-defined domain type
        //     let payload: Json<Box<RawValue>> = row.get(4);
        //     let state: Option<Json<Box<RawValue>>> = row.get(5);
        //     let timeout: PgInterval = row.get(2);
        //     let run_at: NaiveDateTime = row.get(6);
        //
        //     Job {
        //         id: row.get(0),
        //         queue: row.get(1),
        //         timeout: (timeout.microseconds / 1_000_000) as i32,
        //         max_retries: row.get(3),
        //         payload: payload.0,
        //         state: state.map(|state| match state {
        //             Json(state) => state,
        //         }),
        //         run_at: Some(Utc.from_utc_datetime(&run_at)),
        //     }
        // })
        // .fetch_all(&self.pool)
        // .await
        // .map_err(|e| Error::Postgres {
        //     message: e.to_string(),
        //     is_retryable: is_retryable(e),
        // })?;
        //
        // if jobs.is_empty() {
        //     Ok(None)
        // } else {
        //     Ok(Some(jobs))
        // }
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
        unimplemented!();
        // let rows_affected = sqlx::query(
        //     r#"
        //        UPDATE jobs
        //        SET state=$3,
        //            updated_at=NOW(),
        //            expires_at=NOW()+timeout
        //        WHERE
        //            queue=$1 AND
        //            id=$2 AND
        //            in_flight=true
        //     "#,
        // )
        // .bind(queue)
        // .bind(job_id)
        // .bind(state.map(|state| Some(Json(state))))
        // .execute(&self.pool)
        // .await
        // .map_err(|e| Error::Postgres {
        //     message: e.to_string(),
        //     is_retryable: is_retryable(e),
        // })?
        // .rows_affected();
        //
        // if rows_affected == 0 {
        //     Err(Error::JobNotFound {
        //         job_id: job_id.to_string(),
        //         queue: queue.to_string(),
        //     })
        // } else {
        //     Ok(())
        // }
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
        unimplemented!();
        // let now = Utc::now();
        // let run_at = if let Some(run_at) = job.run_at {
        //     run_at
        // } else {
        //     now
        // };
        //
        // let rows_affected = sqlx::query(
        //     r#"
        //         UPDATE jobs
        //         SET
        //             timeout = $3,
        //             max_retries = $4,
        //             retries_remaining = $4,
        //             data = $5,
        //             updated_at = $6,
        //             created_at = $6,
        //             run_at = $7,
        //             in_flight = false
        //         WHERE
        //             queue=$1 AND
        //             id=$2 AND
        //             in_flight=true
        //         "#,
        // )
        // .bind(&job.queue)
        // .bind(&job.id)
        // .bind(PgInterval {
        //     months: 0,
        //     days: 0,
        //     microseconds: i64::from(job.timeout) * 1_000_000,
        // })
        // .bind(job.max_retries)
        // .bind(Json(&job.payload))
        // .bind(&now)
        // .bind(&run_at)
        // .execute(&self.pool)
        // .await
        // .map_err(|e| Error::Postgres {
        //     message: e.to_string(),
        //     is_retryable: is_retryable(e),
        // })?
        // .rows_affected();
        //
        // if rows_affected == 0 {
        //     Err(Error::JobNotFound {
        //         job_id: job.id.to_string(),
        //         queue: job.queue.to_string(),
        //     })
        // } else {
        //     Ok(())
        // }
    }

    /// Reset records to be retries and deletes those that have reached their max.
    ///
    /// # Errors
    ///
    /// Will return `Err` if there is any communication issues with the backend Postgres DB.
    pub async fn reap_timeouts(&self, interval_seconds: i64) -> Result<()> {
        let mut client = self.pool.get().await?;

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

        let results: Vec<Row> = client.query(&stmt, &[]).await?;

        for row in results {
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

        let results: Vec<Row> = client.query(&stmt, &[]).await?;

        for row in results {
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
