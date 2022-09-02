#![allow(clippy::cast_possible_truncation)]
use async_trait::async_trait;
use chrono::{NaiveDateTime, TimeZone, Utc};
use log::LevelFilter;
use metrics::{counter, histogram, increment_counter};
use relay_core::{Backend, Error, Job, Result};
use serde_json::value::RawValue;
use sqlx::postgres::types::PgInterval;
use sqlx::postgres::{PgConnectOptions, PgPoolOptions, PgRow};
use sqlx::types::Json;
use sqlx::{ConnectOptions, Error as SQLXError, Executor, PgPool, Row};
use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::io::ErrorKind;
use std::{str::FromStr, time::Duration};
use tracing::{debug, warn};

/// `RawJob` represents a Relay Job for the Postgres backend.
type RawJob = relay_core::Job<Box<RawValue>>;

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
        Self::new(uri, 10, 100).await
    }

    /// Creates a new backing store with advanced options.
    ///
    /// # Errors
    ///
    /// Will return `Err` if connecting the server or running migrations fails.
    #[inline]
    pub async fn new(
        uri: &str,
        min_connections: u32,
        max_connections: u32,
    ) -> std::result::Result<Self, sqlx::error::Error> {
        let options = PgConnectOptions::from_str(uri)?
            .log_statements(LevelFilter::Off)
            .log_slow_statements(LevelFilter::Warn, Duration::from_secs(1))
            .clone();

        let pool = PgPoolOptions::new()
            .min_connections(min_connections)
            .max_connections(max_connections)
            .acquire_timeout(Duration::from_secs(5))
            .idle_timeout(Duration::from_secs(60))
            .after_connect(|conn, _meta| {
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
}

#[async_trait]
impl Backend<Box<RawValue>> for PgStore {
    /// Returns, if available, the Job in the database with the provided queue and id.
    ///
    /// This would mainly be used to retried state information from the database about a Job.
    ///
    /// # Errors
    ///
    /// Will return `Err` if there is any communication issues with the backend Postgres DB.
    #[tracing::instrument(name = "pg_read", level = "debug", skip_all, fields(job_id=%job_id, queue=%queue))]
    async fn read(&self, queue: &str, job_id: &str) -> Result<Option<Job<Box<RawValue>>>> {
        let job = sqlx::query(
            r#"
               SELECT id,
                      queue,
                      timeout,
                      max_retries,
                      data,
                      state,
                      run_at,
                      updated_at
               FROM jobs
               WHERE
                    queue=$1 AND
                    id=$2
            "#,
        )
        .bind(queue)
        .bind(job_id)
        .map(|row: PgRow| {
            // map the row into a user-defined domain type
            let payload: Json<Box<RawValue>> = row.get(4);
            let state: Option<Json<Box<RawValue>>> = row.get(5);
            let timeout: PgInterval = row.get(2);
            let run_at: NaiveDateTime = row.get(6);
            let updated_at: NaiveDateTime = row.get(7);

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
                updated_at: Some(Utc.from_utc_datetime(&updated_at)),
            }
        })
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| Error::Backend {
            message: e.to_string(),
            is_retryable: is_retryable(e),
        })?;
        increment_counter!("read", "queue" => queue.to_owned());
        debug!("read job");
        Ok(job)
    }

    /// Checks and returns if a Job exists in the database with the provided queue and id.
    ///
    /// # Errors
    ///
    /// Will return `Err` if there is any communication issues with the backend Postgres DB.
    #[tracing::instrument(name = "pg_exists", level = "debug", skip_all, fields(job_id=%job_id, queue=%queue))]
    async fn exists(&self, queue: &str, job_id: &str) -> Result<bool> {
        let exists: bool = sqlx::query(
            r#"
                    SELECT EXISTS (
                        SELECT 1 FROM jobs WHERE
                            queue=$1 AND
                            id=$2
                    )
                "#,
        )
        .bind(queue)
        .bind(job_id)
        .fetch_optional(&self.pool)
        .await
        .map(|row| {
            if let Some(row) = row {
                row.get(0)
            } else {
                false
            }
        })
        .map_err(|e| Error::Backend {
            message: e.to_string(),
            is_retryable: is_retryable(e),
        })?;
        increment_counter!("exists", "queue" => queue.to_owned());
        debug!("exists check job");
        Ok(exists)
    }

    /// Creates a batch of Jobs to be processed in a single write transaction.
    ///
    /// NOTES: If the number of jobs passed is '1' then those will return a `JobExists` error
    ///        identifying the job as already existing.
    ///        If there are more than one jobs this function will not return an error for conflicts
    ///        in Job ID, but rather silently drop the record using an `ON CONFLICT DO NOTHING`.
    ///        If you need to have a Conflict error returned pass a single Job instead.
    ///
    /// # Errors
    ///
    /// Will return `Err` if there is any communication issues with the backend Postgres DB.
    #[tracing::instrument(name = "pg_create", level = "debug", skip_all, fields(jobs = jobs.len()))]
    async fn create(&self, jobs: &[RawJob]) -> Result<()> {
        if jobs.len() == 1 {
            let job = jobs.first().unwrap();
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
                .bind(now)
                .bind(run_at)
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
                    Error::Backend {
                        message: e.to_string(),
                        is_retryable: is_retryable(e),
                    }
                })?;
            increment_counter!("created", "queue" => job.queue.clone());
        } else {
            let mut transaction = self.pool.begin().await.map_err(|e| Error::Backend {
                message: e.to_string(),
                is_retryable: is_retryable(e),
            })?;

            let mut counts = HashMap::new();

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
                .bind(now)
                .bind(run_at)
                .execute(&mut transaction)
                .await
                .map_err(|e| Error::Backend {
                    message: e.to_string(),
                    is_retryable: is_retryable(e),
                })?;
                match counts.entry(job.queue.clone()) {
                    Entry::Occupied(mut o) => *o.get_mut() += 1,
                    Entry::Vacant(v) => {
                        v.insert(1);
                    }
                };
            }

            transaction.commit().await.map_err(|e| Error::Backend {
                message: e.to_string(),
                is_retryable: is_retryable(e),
            })?;

            for (queue, count) in counts {
                counter!("created", count, "queue" => queue);
            }
        }
        debug!("created jobs");
        Ok(())
    }

    /// Deletes the job from the database.
    ///
    /// # Errors
    ///
    /// Will return `Err` if there is any communication issues with the backend Postgres DB.
    #[tracing::instrument(name = "pg_delete", level = "debug", skip_all, fields(job_id=%job_id, queue=%queue))]
    async fn delete(&self, queue: &str, job_id: &str) -> Result<()> {
        let run_at = sqlx::query(
            r#"
                DELETE FROM jobs 
                WHERE 
                    queue=$1 AND 
                    id=$2
                RETURNING run_at
            "#,
        )
        .bind(queue)
        .bind(job_id)
        .fetch_optional(&self.pool)
        .await
        .map(|row| {
            if let Some(row) = row {
                let run_at: NaiveDateTime = row.get(0);
                Some(Utc.from_utc_datetime(&run_at))
            } else {
                None
            }
        })
        .map_err(|e| Error::Backend {
            message: e.to_string(),
            is_retryable: is_retryable(e),
        })?;

        if let Some(run_at) = run_at {
            increment_counter!("deleted", "queue" => queue.to_owned());

            if let Ok(d) = (Utc::now() - run_at).to_std() {
                histogram!("duration", d, "queue" => queue.to_owned(), "type" => "deleted");
            }
            debug!("deleted job");
        }
        Ok(())
    }

    /// Fetches the next available Job(s) to be executed order by `run_at`.
    ///
    /// # Errors
    ///
    /// Will return `Err` if there is any communication issues with the backend Postgres DB.
    #[tracing::instrument(name = "pg_next", level = "debug", skip_all, fields(num_jobs=num_jobs, queue=%queue))]
    async fn next(&self, queue: &str, num_jobs: u32) -> Result<Option<Vec<RawJob>>> {
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
                         j.run_at,
                         j.updated_at
            "#,
        )
        .bind(queue)
        .bind(match i32::try_from(num_jobs) {
            Ok(n) => n,
            Err(_) => i32::MAX,
        })
        .map(|row: PgRow| {
            // map the row into a user-defined domain type
            let payload: Json<Box<RawValue>> = row.get(4);
            let state: Option<Json<Box<RawValue>>> = row.get(5);
            let timeout: PgInterval = row.get(2);
            let run_at: NaiveDateTime = row.get(6);
            let updated_at: NaiveDateTime = row.get(7);

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
                updated_at: Some(Utc.from_utc_datetime(&updated_at)),
            }
        })
        .fetch_all(&self.pool)
        .await
        .map_err(|e| Error::Backend {
            message: e.to_string(),
            is_retryable: is_retryable(e),
        })?;

        if jobs.is_empty() {
            debug!("fetched no jobs");
            Ok(None)
        } else {
            for job in &jobs {
                // using updated_at because this handles:
                // - enqueue -> processing
                // - reschedule -> processing
                // - reaped -> processing
                // This is a possible indicator not enough consumers/processors on the calling side
                // and jobs are backed up processing.
                if let Ok(d) = (Utc::now() - job.updated_at.unwrap()).to_std() {
                    histogram!("latency", d, "queue" => job.queue.clone(), "type" => "to_processing");
                }
            }
            counter!("fetched", jobs.len() as u64, "queue" => queue.to_owned());
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
    #[tracing::instrument(name = "pg_heartbeat", level = "debug", skip_all, fields(job_id=%job_id, queue=%queue))]
    async fn heartbeat(
        &self,
        queue: &str,
        job_id: &str,
        state: Option<Box<RawValue>>,
    ) -> Result<()> {
        let run_at = sqlx::query(
            r#"
               UPDATE jobs
               SET state=$3,
                   updated_at=NOW(),
                   expires_at=NOW()+timeout
               WHERE
                   queue=$1 AND
                   id=$2 AND
                   in_flight=true
               RETURNING (SELECT run_at FROM jobs WHERE queue=$1 AND id=$2 AND in_flight=true)
            "#,
        )
        .bind(queue)
        .bind(job_id)
        .bind(state.map(|state| Some(Json(state))))
        .fetch_optional(&self.pool)
        .await
        .map(|row| {
            if let Some(row) = row {
                let run_at: NaiveDateTime = row.get(0);
                Some(Utc.from_utc_datetime(&run_at))
            } else {
                None
            }
        })
        .map_err(|e| Error::Backend {
            message: e.to_string(),
            is_retryable: is_retryable(e),
        })?;

        if let Some(run_at) = run_at {
            increment_counter!("heartbeat", "queue" => queue.to_owned());

            if let Ok(d) = (Utc::now() - run_at).to_std() {
                histogram!("duration", d, "queue" => queue.to_owned(), "type" => "running");
            }
            debug!("heartbeat job");
            Ok(())
        } else {
            debug!("job not found");
            Err(Error::JobNotFound {
                job_id: job_id.to_string(),
                queue: queue.to_string(),
            })
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
    async fn reschedule(&self, job: &RawJob) -> Result<()> {
        let now = Utc::now();
        let run_at = if let Some(run_at) = job.run_at {
            run_at
        } else {
            now
        };

        let run_at = sqlx::query(
            r#"
                UPDATE jobs
                SET
                    timeout = $3,
                    max_retries = $4,
                    retries_remaining = $4,
                    data = $5,
                    state = $6,
                    updated_at = $7,
                    created_at = $7,
                    run_at = $8,
                    in_flight = false
                WHERE
                    queue=$1 AND
                    id=$2 AND
                    in_flight=true
                RETURNING (SELECT run_at FROM jobs WHERE queue=$1 AND id=$2 AND in_flight=true)
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
        .bind(job.state.as_ref().map(|state| Some(Json(state))))
        .bind(now)
        .bind(run_at)
        .fetch_optional(&self.pool)
        .await
        .map(|row| {
            if let Some(row) = row {
                let run_at: NaiveDateTime = row.get(0);
                Some(Utc.from_utc_datetime(&run_at))
            } else {
                None
            }
        })
        .map_err(|e| Error::Backend {
            message: e.to_string(),
            is_retryable: is_retryable(e),
        })?;

        if let Some(run_at) = run_at {
            increment_counter!("rescheduled", "queue" => job.queue.clone());

            if let Ok(d) = (Utc::now() - run_at).to_std() {
                histogram!("duration", d, "queue" => job.queue.clone(), "type" => "rescheduled");
            }
            debug!("rescheduled job");
            Ok(())
        } else {
            debug!("job not found");
            Err(Error::JobNotFound {
                job_id: job.id.to_string(),
                queue: job.queue.to_string(),
            })
        }
    }

    /// Reset records to be retries and deletes those that have reached their max.
    ///
    /// # Errors
    ///
    /// Will return `Err` if there is any communication issues with the backend Postgres DB.
    #[tracing::instrument(name = "pg_reap_timeouts", level = "debug", skip(self))]
    async fn reap(&self, interval_seconds: u64) -> Result<()> {
        let rows_affected = sqlx::query(
            r#"
            UPDATE internal_state 
            SET last_run=NOW() 
            WHERE last_run <= NOW() - INTERVAL '$1 seconds'"#,
        )
        .bind(match i64::try_from(interval_seconds) {
            Ok(n) => n,
            Err(_) => i64::MAX,
        })
        .execute(&self.pool)
        .await
        .map_err(|e| Error::Backend {
            message: e.to_string(),
            is_retryable: is_retryable(e),
        })?
        .rows_affected();

        // another instance has already updated OR time hasn't been hit yet
        if rows_affected == 0 {
            return Ok(());
        }

        debug!("running timeout & delete reaper");

        let results: Vec<(String, i64)> = sqlx::query_as::<_, (String, i64)>(
            r#"
               WITH cte_max_retries AS (
                    UPDATE jobs
                        SET in_flight=false,
                            retries_remaining=retries_remaining-1
                        WHERE
                            in_flight=true AND
                            expires_at < NOW() AND
                            retries_remaining > 0
                        RETURNING queue
                ),
                cte_no_max_retries AS (
                    UPDATE jobs
                        SET in_flight=false
                        WHERE
                            in_flight=true AND
                            expires_at < NOW() AND
                            retries_remaining < 0
                        RETURNING queue
                )
                SELECT queue, COUNT(queue)
                FROM (
                         SELECT queue
                         FROM cte_max_retries
                         UNION ALL
                         SELECT queue
                         FROM cte_no_max_retries
                     ) as grouped
                GROUP BY queue
            "#,
        )
        .fetch_all(&self.pool)
        .await
        .map_err(|e| Error::Backend {
            message: e.to_string(),
            is_retryable: is_retryable(e),
        })?;

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
        .await
        .map_err(|e| Error::Backend {
            message: e.to_string(),
            is_retryable: is_retryable(e),
        })?;

        for (queue, count) in results {
            warn!(
                count = count,
                queue = %queue,
                "deleted records from queue that reached their max retries"
            );
            counter!("errors", u64::try_from(count).unwrap_or_default(), "queue" => queue, "type" => "max_retries");
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
                // 53300=too_many_connections
                // 55P03=lock_not_available
                // 57014=query_canceled
                // 58000=system_error
                // 58030=io_error
                matches!(
                    code.as_ref(),
                    "53300" | "55P03" | "57014" | "58000" | "58030"
                )
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

#[cfg(test)]
mod tests {
    use super::*;
    use uuid::Uuid;

    #[tokio::test]
    async fn test_enqueue_next_complete() -> anyhow::Result<()> {
        let db_url = std::env::var("DATABASE_URL")?;
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
            updated_at: None,
        };
        store.create(&[job.clone()]).await?;

        let next_job = store.next(&queue, 1).await?;
        assert!(next_job.is_some());
        let next_job = next_job.unwrap();
        assert_eq!(next_job.len(), 1);
        assert_eq!(next_job[0].id, job.id);
        store.delete(&job.queue, &job.id).await?;
        Ok(())
    }

    #[tokio::test]
    async fn test_enqueue_batch_next_complete() -> anyhow::Result<()> {
        let db_url = std::env::var("DATABASE_URL")?;
        let store = PgStore::default(&db_url).await?;
        let job_id = Uuid::new_v4().to_string();
        let queue = Uuid::new_v4().to_string();
        let run_at = Utc.timestamp_millis(Utc::now().timestamp_millis());
        let job = RawJob {
            id: job_id.clone(),
            queue: queue.clone(),
            timeout: 0,
            max_retries: 3,
            payload: RawValue::from_string("{}".to_string())?,
            state: None,
            run_at: Some(run_at),
            updated_at: None,
        };
        store.create(&[job.clone()]).await?;

        let exists = store.exists(&queue, &job_id).await?;
        assert!(exists);

        let db_job = store.read(&queue, &job_id).await?;
        assert!(db_job.is_some());

        let db_job = db_job.unwrap();
        assert_eq!(db_job.id, job.id);
        assert_eq!(db_job.queue, job.queue);
        assert_eq!(db_job.timeout, job.timeout);
        assert_eq!(db_job.max_retries, job.max_retries);
        assert_eq!(db_job.run_at, job.run_at);
        assert_eq!(db_job.payload.to_string(), job.payload.to_string());

        let next_job = store.next(&queue, 1).await?;
        assert!(next_job.is_some());
        let next_job = next_job.unwrap();
        assert_eq!(next_job.len(), 1);
        assert_eq!(next_job[0].id, job_id);
        store.delete(&queue, &job_id).await?;

        let db_job = store.read(&queue, &job_id).await?;
        assert!(db_job.is_none());

        Ok(())
    }

    #[tokio::test]
    async fn test_reschedule() -> anyhow::Result<()> {
        let db_url = std::env::var("DATABASE_URL")?;
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
            updated_at: None,
        };
        store.create(&[job.clone()]).await?;

        let next_job = store.next(&queue, 1).await?;
        assert!(next_job.is_some());
        let next_job = next_job.unwrap();
        assert_eq!(next_job.len(), 1);
        assert_eq!(next_job[0].id, job.id);

        store.reschedule(&job).await?;

        let next_job = store.next(&queue, 1).await?;
        assert!(next_job.is_some());
        let next_job = next_job.unwrap();
        assert_eq!(next_job.len(), 1);
        assert_eq!(next_job[0].id, job.id);

        store.delete(&job.queue, &job.id).await?;
        Ok(())
    }

    #[tokio::test]
    async fn test_reschedule_future() -> anyhow::Result<()> {
        let db_url = std::env::var("DATABASE_URL")?;
        let store = PgStore::default(&db_url).await?;
        let job_id = Uuid::new_v4().to_string();
        let queue = Uuid::new_v4().to_string();
        let mut job = RawJob {
            id: job_id.clone(),
            queue: queue.clone(),
            timeout: 0,
            max_retries: 3,
            payload: RawValue::from_string("{}".to_string())?,
            state: None,
            run_at: None,
            updated_at: None,
        };
        store.create(&[job.clone()]).await?;

        let next_job = store.next(&queue, 1).await?;
        assert!(next_job.is_some());
        let next_job = next_job.unwrap();
        assert_eq!(next_job.len(), 1);
        assert_eq!(next_job[0].id, job.id);

        job.run_at = Some(Utc::now() + chrono::Duration::seconds(60 * 60));
        store.reschedule(&job).await?;

        let next_job = store.next(&queue, 1).await?;
        assert!(next_job.is_none());

        store.delete(&job.queue, &job.id).await?;
        Ok(())
    }
}
