#![allow(clippy::cast_possible_truncation)]
use crate::{Error, RawJob, Result};
use chrono::{NaiveDateTime, TimeZone, Utc};
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
            .connect_timeout(Duration::from_secs(5))
            .idle_timeout(Duration::from_secs(60))
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
    #[tracing::instrument(name = "pg_enqueue", level = "debug", skip_all, fields(job_id=%job.id, queue=%job.queue))]
    pub async fn enqueue(&self, job: &RawJob) -> Result<()> {
        let now = Utc::now();
        let run_at = if let Some(run_at) = job.run_at {
            run_at
        } else {
            now
        };

        sqlx::query("INSERT INTO jobs (id, queue, timeout, max_retries, retries_remaining, data, updated_at, created_at, run_at) VALUES ($1, $2, $3, $4, $4, $5, $6, $6, $7)")
            .bind(&job.id)
            .bind(&job.queue)
            .bind(PgInterval{
                months: 0,
                days: 0,
                microseconds: i64::from(job.timeout )*1_000_000
            }  )
            .bind(job.max_retries)
            .bind(Json(&job.payload))
            .bind(&now)
            .bind(&run_at)
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
                e.into()
            })?;
        debug!("enqueued job");
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
    #[tracing::instrument(name = "pg_enqueue_batch", level = "debug", skip_all, fields(jobs = jobs.len()))]
    pub async fn enqueue_batch(&self, jobs: &[RawJob]) -> Result<()> {
        let mut transaction = self.pool.begin().await?;

        for job in jobs {
            let now = Utc::now();
            let run_at = if let Some(run_at) = job.run_at {
                run_at
            } else {
                now
            };

            sqlx::query(
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
            .bind(&job.id)
            .bind(&job.queue)
            .bind(PgInterval {
                months: 0,
                days: 0,
                microseconds: i64::from(job.timeout) * 1_000_000,
            })
            .bind(job.max_retries)
            .bind(Json(&job.payload))
            .bind(&now)
            .bind(&run_at)
            .execute(&mut transaction)
            .await?;
        }

        transaction.commit().await?;
        debug!("enqueued batch");
        Ok(())
    }

    /// Removed the job from the database.
    ///
    /// # Errors
    ///
    /// Will return `Err` if there is any communication issues with the backend Postgres DB.
    #[tracing::instrument(name = "pg_remove", level = "debug", skip_all, fields(job_id=%job_id, queue=%queue))]
    pub async fn remove(&self, queue: &str, job_id: &str) -> Result<()> {
        sqlx::query("DELETE FROM jobs WHERE queue=$1 AND id=$2")
            .bind(queue)
            .bind(job_id)
            .execute(&self.pool)
            .await?;

        debug!("removed job");
        Ok(())
    }

    /// Returns the next Job to be executed in order of insert. FIFO.
    ///
    /// # Errors
    ///
    /// Will return `Err` if there is any communication issues with the backend Postgres DB.
    #[tracing::instrument(name = "pg_next", level = "debug", skip_all, fields(num_jobs=num_jobs, queue=%queue))]
    pub async fn next(&self, queue: &str, num_jobs: u32) -> Result<Option<Vec<RawJob>>> {
        // MUST USE CTE WITH `FOR UPDATE SKIP LOCKED LIMIT` otherwise the Postgres Query Planner
        // CAN optimize the query which will cause MORE updates than the LIMIT specifies within
        // a nested loop.
        // See here for details:
        // https://github.com/feikesteenbergen/demos/blob/19522f66ffb6eb358fe2d532d9bdeae38d4e2a0b/bugs/update_from_correlated.adoc
        let jobs = sqlx::query(
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
        .bind(queue)
        .bind(num_jobs)
        .map(|row: PgRow| {
            // map the row into a user-defined domain type
            let payload: Json<Box<RawValue>> = row.get(4);
            let state: Option<Json<Box<RawValue>>> = row.get(5);
            let timeout: PgInterval = row.get(2);
            let run_at: NaiveDateTime = row.get(6);

            RawJob {
                id: row.get(0),
                queue: row.get(1),
                timeout: (timeout.microseconds / 1_000_000) as i32,
                max_retries: row.get(3),
                payload: payload.0,
                state: state.map(|state| match state {
                    Json(state) => state,
                }),
                run_at: Some(Utc.from_utc_datetime(&run_at)),
            }
        })
        .fetch_all(&self.pool)
        .await?;

        if jobs.is_empty() {
            debug!("fetched no jobs");
            Ok(None)
        } else {
            debug!(fetched_jobs = jobs.len(), "fetched next job(s)");
            Ok(Some(jobs))
        }
    }

    /// Updates the existing in-flight job by incrementing it's `updated_at` and option state.
    ///
    /// # Errors
    ///
    /// Will return `Err` if there is any communication issues with the backend Postgres DB or the
    /// Job attempting to be updated cannot be found.
    #[tracing::instrument(name = "pg_update", level = "debug", skip_all, fields(job_id=%job_id, queue=%queue))]
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
        .await?
        .rows_affected();

        if rows_affected == 0 {
            debug!("job not found");
            Err(Error::JobNotFound {
                job_id: job_id.to_string(),
                queue: queue.to_string(),
            })
        } else {
            debug!("updated job");
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
    #[tracing::instrument(name = "pg_reschedule", level = "debug", skip_all, fields(job_id=%job.id, queue=%job.queue))]
    pub async fn reschedule(&self, job: &RawJob) -> Result<()> {
        let now = Utc::now();
        let run_at = if let Some(run_at) = job.run_at {
            run_at
        } else {
            now
        };

        let rows_affected = sqlx::query(
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
        .bind(&job.queue)
        .bind(&job.id)
        .bind(PgInterval {
            months: 0,
            days: 0,
            microseconds: i64::from(job.timeout) * 1_000_000,
        })
        .bind(job.max_retries)
        .bind(Json(&job.payload))
        .bind(&now)
        .bind(&run_at)
        .execute(&self.pool)
        .await?
        .rows_affected();

        if rows_affected == 0 {
            debug!("job not found");
            Err(Error::JobNotFound {
                job_id: job.id.to_string(),
                queue: job.queue.to_string(),
            })
        } else {
            debug!("rescheduled job");
            Ok(())
        }
    }

    /// Reset records to be retries and deletes those that have reached their max.
    ///
    /// # Errors
    ///
    /// Will return `Err` if there is any communication issues with the backend Postgres DB.
    #[tracing::instrument(name = "pg_reap_timeouts", level = "debug", skip(self))]
    pub async fn reap_timeouts(&self, interval_seconds: i64) -> Result<()> {
        let rows_affected = sqlx::query(
            r#"
            UPDATE internal_state 
            SET last_run=NOW() 
            WHERE last_run <= NOW() - INTERVAL '$1 seconds'"#,
        )
        .bind(interval_seconds)
        .execute(&self.pool)
        .await?
        .rows_affected();

        // another instance has already updated OR time hasn't been hit yet
        if rows_affected == 0 {
            return Ok(());
        }

        debug!("running timeout & delete reaper");

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
        .await?;

        for (queue, count) in results {
            debug!(queue = %queue, count = count, "retrying jobs");
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
        .await?;

        for (queue, count) in results {
            warn!(
                count = count,
                queue = %queue,
                "deleted records from queue that reached their max retries"
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

impl From<sqlx::Error> for Error {
    fn from(e: sqlx::Error) -> Self {
        Error::Postgres {
            message: e.to_string(),
            is_retryable: is_retryable(e),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use uuid::Uuid;

    #[tokio::test]
    async fn test_enqueue_next_complete() -> anyhow::Result<()> {
        let db_url = env!("DATABASE_URL");
        let store = PgStore::default(&db_url).await?;
        let job_id = Uuid::new_v4().to_string();
        let queue = Uuid::new_v4().to_string();
        let job = RawJob {
            id: job_id.clone(),
            queue: queue.clone(),
            timeout: 0,
            max_retries: 3,
            payload: RawValue::from_string("{}".to_string())?,
            state: None,
            run_at: None,
        };
        assert_eq!(store.enqueue(&job).await?, ());

        let next_job = store.next(&queue, 1).await?;
        assert!(next_job.is_some());
        let next_job = next_job.unwrap();
        assert_eq!(next_job.len(), 1);
        assert_eq!(next_job[0].id, job.id);
        assert_eq!(store.remove(&job.queue, &job.id).await?, ());
        Ok(())
    }
}
