#![allow(clippy::cast_possible_truncation)]
use crate::{Error, Job, JobId, Queue, Result};
use chrono::Utc;
use log::LevelFilter;
use metrics::counter;
use serde_json::value::RawValue;
use sqlx::postgres::types::PgInterval;
use sqlx::postgres::{PgConnectOptions, PgPoolOptions, PgRow};
use sqlx::types::Json;
use sqlx::{ConnectOptions, Error as SQLXError, Executor, PgPool, Row};
use std::io::ErrorKind;
use std::{str::FromStr, time::Duration};
use tracing::{debug, warn};

/// Postgres backing store
pub struct PgStore {
    pool: PgPool,
}

impl PgStore {
    /// Creates a new backing store with default settings for Postgres.
    ///
    /// # Errors
    ///
    /// Will return `Err` if connecting the server or running migrations fails.
    #[inline]
    pub async fn default(uri: &str) -> std::result::Result<Self, sqlx::error::Error> {
        let options = PgConnectOptions::from_str(uri)?
            .log_statements(LevelFilter::Off)
            .log_slow_statements(LevelFilter::Warn, Duration::from_secs(1))
            .clone();
        Self::new(options).await
    }

    /// Creates a new backing store with advanced options.
    ///
    /// # Errors
    ///
    /// Will return `Err` if connecting the server or running migrations fails.
    #[inline]
    pub async fn new(options: PgConnectOptions) -> std::result::Result<Self, sqlx::error::Error> {
        let pool = PgPoolOptions::new()
            .max_connections(100)
            .min_connections(10)
            .connect_timeout(Duration::from_secs(60))
            .idle_timeout(Duration::from_secs(20))
            .after_connect(|conn| {
                Box::pin(async move {
                    // Insurance as if not at least this isolation mode then some queries are not
                    // transactional safe. Specifically FOR UPDATE SKIP LOCKED.
                    conn.execute("SET default_transaction_isolation TO 'read committed'")
                        .await?;
                    Ok(())
                })
            })
            .connect_with(options)
            .await?;

        Self::new_with_pool(pool).await
    }

    /// Creates a new backing store with preconfigured pool
    ///
    /// # Errors
    ///
    /// Will return `Err` if connecting the server or running migrations fails.
    #[inline]
    pub async fn new_with_pool(pool: PgPool) -> std::result::Result<Self, sqlx::error::Error> {
        {
            sqlx::migrate!("./migrations").run(&pool).await?;

            // insert internal records if they don't already exist
            sqlx::query(
                r#"
                INSERT INTO internal_state (id, last_run) VALUES ($1,$2) ON CONFLICT DO NOTHING
            "#,
            )
            .bind("reap")
            .bind(Utc::now())
            .execute(&pool)
            .await?;
        }

        Ok(Self { pool })
    }

    /// Enqueues a new Job to be processed.
    ///
    /// # Errors
    ///
    /// Will return `Err` if there is any communication issues with the backend Postgres DB.
    pub async fn enqueue(&self, job: &Job) -> Result<()> {
        let now = Utc::now();

        sqlx::query("INSERT INTO jobs (id, queue, timeout, max_retries, retries_remaining, data, updated_at, created_at) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)")
            .bind(&job.id)
            .bind(&job.queue)
            .bind(PgInterval{
                months: 0,
                days: 0,
                microseconds: i64::from(job.timeout )*1_000_000
            }  )
            .bind(job.max_retries)
            .bind(job.max_retries)
            .bind(Json(&job.payload))
            .bind(&now)
            .bind(&now)
            .execute(&self.pool)
            .await
            .map_err(|e| {
                if let sqlx::Error::Database(ref db) = e {
                    if let Some(code) = db.code() {
                        // 23505 = unique_violation
                        if code == "23505" { 
                            return Error::JobExists {
                                job_id: job.id.clone(),
                                queue: job.queue.clone(),
                            }
                        }
                    }
                }

                Error::Postgres {
                    message: e.to_string(),
                    is_retryable: is_retryable(e),
                }
            })?;
        Ok(())
    }

    /// Removed the job from the database.
    ///
    /// # Errors
    ///
    /// Will return `Err` if there is any communication issues with the backend Postgres DB.
    pub async fn remove(&self, queue: &Queue, job_id: &JobId) -> Result<()> {
        sqlx::query("DELETE FROM jobs WHERE queue=$1 AND id=$2")
            .bind(queue)
            .bind(job_id)
            .execute(&self.pool)
            .await
            .map_err(|e| Error::Postgres {
                message: e.to_string(),
                is_retryable: is_retryable(e),
            })?;

        Ok(())
    }

    /// Returns the next Job to be executed in order of insert. FIFO.
    ///
    /// # Errors
    ///
    /// Will return `Err` if there is any communication issues with the backend Postgres DB.
    pub async fn next(&self, queue: &Queue) -> Result<Option<Job>> {
        let job = sqlx::query(
            r#"
               UPDATE jobs j
               SET in_flight=true,
                   updated_at=NOW(),
                   expires_at=NOW()+timeout
               FROM (
                    SELECT
                        id,
                        queue
                   FROM jobs
                   WHERE
                        queue=$1 AND
                        in_flight=false
                   ORDER BY created_at ASC
                   LIMIT 1
                   FOR UPDATE SKIP LOCKED
               ) subquery
               WHERE
                   j.queue=subquery.queue AND
                   j.id=subquery.id
               RETURNING j.id,
                         j.queue,
                         j.timeout,
                         j.max_retries,
                         j.data,
                         j.state
            "#,
        )
        .bind(queue)
        .map(|row: PgRow| {
            // map the row into a user-defined domain type
            let payload: Json<Box<RawValue>> = row.get(4);
            let state: Option<Json<Box<RawValue>>> = row.get(5);
            let timeout: PgInterval = row.get(2);

            Job {
                id: row.get(0),
                queue: row.get(1),
                timeout: (timeout.microseconds / 1_000_000) as i32,
                max_retries: row.get(3),
                payload: payload.0,
                state: state.map(|state| match state {
                    Json(state) => state,
                }),
            }
        })
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| Error::Postgres {
            message: e.to_string(),
            is_retryable: is_retryable(e),
        })?;

        Ok(job)
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
        let rows_affected = sqlx::query(
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
        .bind(queue)
        .bind(job_id)
        .bind(state.map(|state| Some(Json(state))))
        .execute(&self.pool)
        .await
        .map_err(|e| Error::Postgres {
            message: e.to_string(),
            is_retryable: is_retryable(e),
        })?
        .rows_affected();

        if rows_affected == 0 {
            Err(Error::JobNotFound {
                job_id: job_id.to_string(),
                queue: queue.to_string(),
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
        let rows_affected = sqlx::query(
            r#"
            UPDATE internal_state 
            SET last_run=NOW() 
            WHERE last_run <= NOW() - INTERVAL '$1 seconds'"#,
        )
        .bind(interval_seconds)
        .execute(&self.pool)
        .await
        .map_err(|e| Error::Postgres {
            message: e.to_string(),
            is_retryable: is_retryable(e),
        })?
        .rows_affected();

        // another instance has already updated OR time hasn't been hit yet
        if rows_affected == 0 {
            return Ok(());
        }

        debug!("running timeout reaper");

        let results: Vec<(String, i64)> = sqlx::query_as::<_, (String, i64)>(
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
        .fetch_all(&self.pool)
        .await
        .map_err(|e| Error::Postgres {
            message: e.to_string(),
            is_retryable: is_retryable(e),
        })?;

        for (queue, count) in results {
            counter!("retries", u64::try_from(count).unwrap_or_default(), "queue" => queue);
        }

        let results: Vec<(String, i64)> = sqlx::query_as::<_, (String, i64)>(
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
        .fetch_all(&self.pool)
        .await
        .map_err(|e| Error::Postgres {
            message: e.to_string(),
            is_retryable: is_retryable(e),
        })?;

        for (queue, count) in results {
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
fn is_retryable(e: SQLXError) -> bool {
    match e {
        sqlx::Error::Database(ref db) => match db.code() {
            None => false,
            Some(code) => {
                match code.as_ref() {
                    "53300" | "55P03" | "57014" | "58000" | "58030" => {
                        // 53300=too_many_connections
                        // 55P03=lock_not_available
                        // 57014=query_canceled
                        // 58000=system_error
                        // 58030=io_error
                        true
                    }
                    _ => false,
                }
            }
        },
        sqlx::Error::PoolTimedOut => true,
        sqlx::Error::Io(e) => matches!(
            e.kind(),
            ErrorKind::ConnectionReset
                | ErrorKind::ConnectionAborted
                | ErrorKind::NotConnected
                | ErrorKind::WouldBlock
                | ErrorKind::TimedOut
                | ErrorKind::WriteZero
                | ErrorKind::Interrupted
                | ErrorKind::UnexpectedEof
        ),
        _ => false,
    }
}
