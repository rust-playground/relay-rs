#![allow(clippy::cast_possible_truncation)]
use async_trait::async_trait;
use chrono::{NaiveDateTime, TimeZone, Utc};
use deadpool_postgres::{
    ClientWrapper, GenericClient, Hook, HookError, HookErrorCause, Manager, ManagerConfig, Pool,
    PoolError, RecyclingMethod,
};
use log::LevelFilter;
use metrics::{counter, histogram, increment_counter};
use pg_interval::Interval;
use relay_core::{Backend, Error, Job, Result};
use serde_json::value::RawValue;
use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::io;
use std::io::ErrorKind;
use std::{str::FromStr, time::Duration};
use tokio_postgres::error::SqlState;
use tokio_postgres::types::{Json, ToSql};
use tokio_postgres::{Config as PostgresConfig, NoTls};
use tokio_postgres_migration::Migration;
use tokio_stream::{Stream, StreamExt};
use tracing::{debug, warn};

const MIGRATIONS_UP: [(&str, &str); 1] = [(
    "1678464484380_initialize.sql",
    include_str!("../migrations/1678464484380_initialize.sql"),
)];

/// `RawJob` represents a Relay Job for the Postgres backend.
type RawJob = Job<Box<RawValue>, Box<RawValue>>;

/// Postgres backing store
pub struct PgStore {
    pool: Pool,
}

impl PgStore {
    /// Creates a new backing store with default settings for Postgres.
    ///
    /// # Errors
    ///
    /// Will return `Err` if connecting the server or running migrations fails.
    #[inline]
    pub async fn default(uri: &str) -> std::result::Result<Self, anyhow::Error> {
        Self::new(uri, 10).await
    }

    /// Creates a new backing store with advanced options.
    ///
    /// # Errors
    ///
    /// Will return `Err` if connecting the server or running migrations fails.
    #[inline]
    pub async fn new(
        uri: &str,
        max_connections: usize,
    ) -> std::result::Result<Self, anyhow::Error> {
        let mut pg_config = PostgresConfig::from_str(uri)?;
        if pg_config.get_connect_timeout().is_none() {
            pg_config.connect_timeout(Duration::from_secs(5));
        }
        if pg_config.get_application_name().is_none() {
            pg_config.application_name("relay");
        }

        let mgr = Manager::from_config(
            pg_config,
            NoTls,
            ManagerConfig {
                recycling_method: RecyclingMethod::Fast,
            },
        );
        let pool = Pool::builder(mgr)
            .max_size(max_connections)
            .post_create(Hook::async_fn(|client: &mut ClientWrapper, _| {
                Box::pin(async move {
                    client
                        .simple_query("SET default_transaction_isolation TO 'read committed'")
                        .await
                        .map_err(|e| HookError::Abort(HookErrorCause::Backend(e)))?;
                    Ok(())
                })
            }))
            .build()?;

        Self::new_with_pool(pool).await
    }

    /// Creates a new backing store with preconfigured pool
    ///
    /// # Errors
    ///
    /// Will return `Err` if connecting the server or running migrations fails.
    #[inline]
    pub async fn new_with_pool(pool: Pool) -> std::result::Result<Self, anyhow::Error> {
        let mut client = pool.get().await?;

        let migration = Migration::new("_relay_rs_migrations".to_string());
        migration.up(&mut **client, &MIGRATIONS_UP).await.unwrap();

        client
            .execute(
                r#"
                INSERT INTO internal_state (id, last_run) VALUES ('reap',$1) ON CONFLICT DO NOTHING
            "#,
                &[&Utc::now().naive_utc()],
            )
            .await?;

        Ok(Self { pool })
    }
}

#[async_trait]
impl Backend<Box<RawValue>, Box<RawValue>> for PgStore {
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
    #[tracing::instrument(name = "pg_enqueue", level = "debug", skip_all, fields(jobs = jobs.len()))]
    async fn enqueue(&self, jobs: &[RawJob]) -> Result<()> {
        if jobs.len() == 1 {
            let job = jobs.first().unwrap();
            let now = Utc::now().naive_utc();
            let run_at = if let Some(run_at) = job.run_at {
                run_at.naive_utc()
            } else {
                now
            };

            let client = self.pool.get().await.map_err(|e| Error::Backend {
                message: e.to_string(),
                is_retryable: is_retryable_pool(e),
            })?;

            let stmt = client
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
                        VALUES ($1, $2, $3, $4, $4, $5, $6, $6, $7)"#,
                )
                .await
                .map_err(|e| Error::Backend {
                    message: e.to_string(),
                    is_retryable: is_retryable(e),
                })?;

            client
                .execute(
                    &stmt,
                    &[
                        &job.id,
                        &job.queue,
                        &Interval::from_duration(chrono::Duration::seconds(i64::from(job.timeout))),
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
                        Error::Backend {
                            message: e.to_string(),
                            is_retryable: is_retryable(e),
                        }
                    }
                })?;
            increment_counter!("enqueued", "queue" => job.queue.clone());
        } else {
            let mut counts = HashMap::new();
            let mut client = self.pool.get().await.map_err(|e| Error::Backend {
                message: e.to_string(),
                is_retryable: is_retryable_pool(e),
            })?;
            let transaction = client.transaction().await.map_err(|e| Error::Backend {
                message: e.to_string(),
                is_retryable: is_retryable(e),
            })?;

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
                .await
                .map_err(|e| Error::Backend {
                    message: e.to_string(),
                    is_retryable: is_retryable(e),
                })?;

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
                            &Interval::from_duration(chrono::Duration::seconds(i64::from(
                                job.timeout,
                            ))),
                            &job.max_retries,
                            &Json(&job.payload),
                            &now,
                            &run_at,
                        ],
                    )
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
                counter!("enqueued", count, "queue" => queue);
            }
        }
        debug!("enqueued jobs");
        Ok(())
    }

    /// Returns, if available, the Job in the database with the provided queue and id.
    ///
    /// This would mainly be used to retried state information from the database about a Job.
    ///
    /// # Errors
    ///
    /// Will return `Err` if there is any communication issues with the backend Postgres DB.
    #[tracing::instrument(name = "pg_get", level = "debug", skip_all, fields(job_id=%job_id, queue=%queue))]
    async fn get(&self, queue: &str, job_id: &str) -> Result<Option<RawJob>> {
        unimplemented!()
        // let job = sqlx::query(
        //     r#"
        //        SELECT id,
        //               queue,
        //               timeout,
        //               max_retries,
        //               data,
        //               state,
        //               run_at,
        //               updated_at
        //        FROM jobs
        //        WHERE
        //             queue=$1 AND
        //             id=$2
        //     "#,
        // )
        // .bind(queue)
        // .bind(job_id)
        // .map(|row: PgRow| {
        //     // map the row into a user-defined domain type
        //     let payload: Json<Box<RawValue>> = row.get(4);
        //     let state: Option<Json<Box<RawValue>>> = row.get(5);
        //     let timeout: PgInterval = row.get(2);
        //     let run_at: NaiveDateTime = row.get(6);
        //     let updated_at: NaiveDateTime = row.get(7);
        //
        //     RawJob {
        //         id: row.get(0),
        //         queue: row.get(1),
        //         timeout: (timeout.microseconds / 1_000_000) as i32,
        //         max_retries: row.get(3),
        //         payload: payload.0,
        //         state: state.map(|state| match state {
        //             Json(state) => state,
        //         }),
        //         run_at: Some(Utc.from_utc_datetime(&run_at)),
        //         updated_at: Some(Utc.from_utc_datetime(&updated_at)),
        //     }
        // })
        // .fetch_optional(&self.pool)
        // .await
        // .map_err(|e| Error::Backend {
        //     message: e.to_string(),
        //     is_retryable: is_retryable(e),
        // })?;
        // increment_counter!("get", "queue" => queue.to_owned());
        // debug!("got job");
        // Ok(job)
    }

    /// Deletes the job from the database.
    ///
    /// # Errors
    ///
    /// Will return `Err` if there is any communication issues with the backend Postgres DB.
    #[tracing::instrument(name = "pg_delete", level = "debug", skip_all, fields(job_id=%job_id, queue=%queue))]
    async fn delete(&self, queue: &str, job_id: &str) -> Result<()> {
        unimplemented!()
        // let run_at = sqlx::query(
        //     r#"
        //         DELETE FROM jobs
        //         WHERE
        //             queue=$1 AND
        //             id=$2
        //         RETURNING run_at
        //     "#,
        // )
        // .bind(queue)
        // .bind(job_id)
        // .fetch_optional(&self.pool)
        // .await
        // .map(|row| {
        //     if let Some(row) = row {
        //         let run_at: NaiveDateTime = row.get(0);
        //         Some(Utc.from_utc_datetime(&run_at))
        //     } else {
        //         None
        //     }
        // })
        // .map_err(|e| Error::Backend {
        //     message: e.to_string(),
        //     is_retryable: is_retryable(e),
        // })?;
        //
        // if let Some(run_at) = run_at {
        //     increment_counter!("deleted", "queue" => queue.to_owned());
        //
        //     if let Ok(d) = (Utc::now() - run_at).to_std() {
        //         histogram!("duration", d, "queue" => queue.to_owned(), "type" => "deleted");
        //     }
        //     debug!("deleted job");
        // }
        // Ok(())
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
        unimplemented!()
        // let run_at = sqlx::query(
        //     r#"
        //        UPDATE jobs
        //        SET state=$3,
        //            updated_at=NOW(),
        //            expires_at=NOW()+timeout
        //        WHERE
        //            queue=$1 AND
        //            id=$2 AND
        //            in_flight=true
        //        RETURNING (SELECT run_at FROM jobs WHERE queue=$1 AND id=$2 AND in_flight=true)
        //     "#,
        // )
        // .bind(queue)
        // .bind(job_id)
        // .bind(state.map(|state| Some(Json(state))))
        // .fetch_optional(&self.pool)
        // .await
        // .map(|row| {
        //     if let Some(row) = row {
        //         let run_at: NaiveDateTime = row.get(0);
        //         Some(Utc.from_utc_datetime(&run_at))
        //     } else {
        //         None
        //     }
        // })
        // .map_err(|e| Error::Backend {
        //     message: e.to_string(),
        //     is_retryable: is_retryable(e),
        // })?;
        //
        // if let Some(run_at) = run_at {
        //     increment_counter!("heartbeat", "queue" => queue.to_owned());
        //
        //     if let Ok(d) = (Utc::now() - run_at).to_std() {
        //         histogram!("duration", d, "queue" => queue.to_owned(), "type" => "running");
        //     }
        //     debug!("heartbeat job");
        //     Ok(())
        // } else {
        //     debug!("job not found");
        //     Err(Error::JobNotFound {
        //         job_id: job_id.to_string(),
        //         queue: queue.to_string(),
        //     })
        // }
    }

    /// Checks and returns if a Job exists in the database with the provided queue and id.
    ///
    /// # Errors
    ///
    /// Will return `Err` if there is any communication issues with the backend Postgres DB.
    #[tracing::instrument(name = "pg_exists", level = "debug", skip_all, fields(job_id=%job_id, queue=%queue))]
    async fn exists(&self, queue: &str, job_id: &str) -> Result<bool> {
        unimplemented!()
        // let exists: bool = sqlx::query(
        //     r#"
        //             SELECT EXISTS (
        //                 SELECT 1 FROM jobs WHERE
        //                     queue=$1 AND
        //                     id=$2
        //             )
        //         "#,
        // )
        // .bind(queue)
        // .bind(job_id)
        // .fetch_optional(&self.pool)
        // .await
        // .map(|row| {
        //     if let Some(row) = row {
        //         row.get(0)
        //     } else {
        //         false
        //     }
        // })
        // .map_err(|e| Error::Backend {
        //     message: e.to_string(),
        //     is_retryable: is_retryable(e),
        // })?;
        // increment_counter!("exists", "queue" => queue.to_owned());
        // debug!("exists check job");
        // Ok(exists)
    }

    /// Fetches the next available Job(s) to be executed order by `run_at`.
    ///
    /// # Errors
    ///
    /// Will return `Err` if there is any communication issues with the backend Postgres DB.
    #[tracing::instrument(name = "pg_next", level = "debug", skip_all, fields(num_jobs=num_jobs, queue=%queue))]
    async fn next(&self, queue: &str, num_jobs: u32) -> Result<Option<Vec<RawJob>>> {
        unimplemented!()
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
        //                  j.run_at,
        //                  j.updated_at
        //     "#,
        // )
        // .bind(queue)
        // .bind(match i32::try_from(num_jobs) {
        //     Ok(n) => n,
        //     Err(_) => i32::MAX,
        // })
        // .map(|row: PgRow| {
        //     // map the row into a user-defined domain type
        //     let payload: Json<Box<RawValue>> = row.get(4);
        //     let state: Option<Json<Box<RawValue>>> = row.get(5);
        //     let timeout: PgInterval = row.get(2);
        //     let run_at: NaiveDateTime = row.get(6);
        //     let updated_at: NaiveDateTime = row.get(7);
        //
        //     RawJob {
        //         id: row.get(0),
        //         queue: row.get(1),
        //         timeout: (timeout.microseconds / 1_000_000) as i32,
        //         max_retries: row.get(3),
        //         payload: payload.0,
        //         state: state.map(|state| match state {
        //             Json(state) => state,
        //         }),
        //         run_at: Some(Utc.from_utc_datetime(&run_at)),
        //         updated_at: Some(Utc.from_utc_datetime(&updated_at)),
        //     }
        // })
        // .fetch_all(&self.pool)
        // .await
        // .map_err(|e| Error::Backend {
        //     message: e.to_string(),
        //     is_retryable: is_retryable(e),
        // })?;
        //
        // if jobs.is_empty() {
        //     debug!("fetched no jobs");
        //     Ok(None)
        // } else {
        //     for job in &jobs {
        //         // using updated_at because this handles:
        //         // - enqueue -> processing
        //         // - reschedule -> processing
        //         // - reaped -> processing
        //         // This is a possible indicator not enough consumers/processors on the calling side
        //         // and jobs are backed up processing.
        //         if let Ok(d) = (Utc::now() - job.updated_at.unwrap()).to_std() {
        //             histogram!("latency", d, "queue" => job.queue.clone(), "type" => "to_processing");
        //         }
        //     }
        //     counter!("fetched", jobs.len() as u64, "queue" => queue.to_owned());
        //     debug!(fetched_jobs = jobs.len(), "fetched next job(s)");
        //     Ok(Some(jobs))
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
    #[tracing::instrument(name = "pg_reschedule", level = "debug", skip_all, fields(job_id=%job.id, queue=%job.queue))]
    async fn reschedule(&self, job: &RawJob) -> Result<()> {
        unimplemented!()
        // let now = Utc::now();
        // let run_at = if let Some(run_at) = job.run_at {
        //     run_at
        // } else {
        //     now
        // };
        //
        // let run_at = sqlx::query(
        //     r#"
        //         UPDATE jobs
        //         SET
        //             timeout = $3,
        //             max_retries = $4,
        //             retries_remaining = $4,
        //             data = $5,
        //             state = $6,
        //             updated_at = $7,
        //             created_at = $7,
        //             run_at = $8,
        //             in_flight = false
        //         WHERE
        //             queue=$1 AND
        //             id=$2 AND
        //             in_flight=true
        //         RETURNING (SELECT run_at FROM jobs WHERE queue=$1 AND id=$2 AND in_flight=true)
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
        // .bind(job.state.as_ref().map(|state| Some(Json(state))))
        // .bind(now)
        // .bind(run_at)
        // .fetch_optional(&self.pool)
        // .await
        // .map(|row| {
        //     if let Some(row) = row {
        //         let run_at: NaiveDateTime = row.get(0);
        //         Some(Utc.from_utc_datetime(&run_at))
        //     } else {
        //         None
        //     }
        // })
        // .map_err(|e| Error::Backend {
        //     message: e.to_string(),
        //     is_retryable: is_retryable(e),
        // })?;
        //
        // if let Some(run_at) = run_at {
        //     increment_counter!("rescheduled", "queue" => job.queue.clone());
        //
        //     if let Ok(d) = (Utc::now() - run_at).to_std() {
        //         histogram!("duration", d, "queue" => job.queue.clone(), "type" => "rescheduled");
        //     }
        //     debug!("rescheduled job");
        //     Ok(())
        // } else {
        //     debug!("job not found");
        //     Err(Error::JobNotFound {
        //         job_id: job.id.to_string(),
        //         queue: job.queue.to_string(),
        //     })
        // }
    }

    /// Reset records to be retries and deletes those that have reached their max.
    ///
    /// # Errors
    ///
    /// Will return `Err` if there is any communication issues with the backend Postgres DB.
    #[tracing::instrument(name = "pg_reap_timeouts", level = "debug", skip(self))]
    async fn reap(&self, interval_seconds: u64) -> Result<()> {
        let client = self.pool.get().await.map_err(|e| Error::Backend {
            message: e.to_string(),
            is_retryable: is_retryable_pool(e),
        })?;

        let stmt = client
            .prepare_cached(
                r#"
            UPDATE internal_state 
            SET last_run=NOW() 
            WHERE last_run <= NOW() - $1::text::interval"#,
            )
            .await
            .map_err(|e| Error::Backend {
                message: e.to_string(),
                is_retryable: is_retryable(e),
            })?;

        let rows_affected = client
            .execute(
                &stmt,
                &[&format!(
                    "{}s",
                    match i64::try_from(interval_seconds) {
                        Ok(n) => n,
                        Err(_) => i64::MAX,
                    }
                )],
            )
            .await
            .map_err(|e| Error::Backend {
                message: e.to_string(),
                is_retryable: is_retryable(e),
            })?;

        // another instance has already updated OR time hasn't been hit yet
        if rows_affected == 0 {
            return Ok(());
        }

        debug!("running timeout & delete reaper");

        let stmt = client
            .prepare_cached(
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
            .await
            .map_err(|e| Error::Backend {
                message: e.to_string(),
                is_retryable: is_retryable(e),
            })?;

        let stream = client
            .query_raw(&stmt, &[] as &[i32])
            .await
            .map_err(|e| Error::Backend {
                message: e.to_string(),
                is_retryable: is_retryable(e),
            })?;
        tokio::pin!(stream);

        while let Some(row) = stream.next().await {
            let row = row.map_err(|e| Error::Backend {
                message: e.to_string(),
                is_retryable: is_retryable(e),
            })?;
            let queue: String = row.get(0);
            let count: i64 = row.get(1);
            debug!(queue = %queue, count = count, "retrying jobs");
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
            .await
            .map_err(|e| Error::Backend {
                message: e.to_string(),
                is_retryable: is_retryable(e),
            })?;

        let stream = client
            .query_raw(&stmt, &[] as &[i32])
            .await
            .map_err(|e| Error::Backend {
                message: e.to_string(),
                is_retryable: is_retryable(e),
            })?;
        tokio::pin!(stream);

        while let Some(row) = stream.next().await {
            let row = row.map_err(|e| Error::Backend {
                message: e.to_string(),
                is_retryable: is_retryable(e),
            })?;
            let queue: String = row.get(0);
            let count: i64 = row.get(1);
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
fn is_retryable(e: tokio_postgres::Error) -> bool {
    match e.code() {
        Some(
            &(SqlState::IO_ERROR
            | SqlState::TOO_MANY_CONNECTIONS
            | SqlState::LOCK_NOT_AVAILABLE
            | SqlState::QUERY_CANCELED
            | SqlState::SYSTEM_ERROR),
        ) => true,
        Some(_) => false,
        None => {
            if let Some(e) = e
                .into_source()
                .as_ref()
                .and_then(|e| e.downcast_ref::<io::Error>())
            {
                matches!(
                    e.kind(),
                    ErrorKind::ConnectionReset
                        | ErrorKind::ConnectionAborted
                        | ErrorKind::NotConnected
                        | ErrorKind::WouldBlock
                        | ErrorKind::TimedOut
                        | ErrorKind::WriteZero
                        | ErrorKind::Interrupted
                        | ErrorKind::UnexpectedEof
                )
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

// impl From<PoolError> for Error {
//     fn from(e: PoolError) -> Self {
//         Error::Postgres {
//             message: e.to_string(),
//             is_retryable: is_retryable_pool(e),
//         }
//     }
// }
//
// impl From<tokio_postgres::Error> for Error {
//     fn from(e: tokio_postgres::Error) -> Self {
//         Error::Postgres {
//             message: e.to_string(),
//             is_retryable: is_retryable(e),
//         }
//     }
// }

fn interval_seconds(interval: Interval) -> i32 {
    let month_secs = interval.months * 30 * 24 * 60 * 60;
    let day_secs = interval.days * 24 * 60 * 60;
    let micro_secs = (interval.microseconds / 1_000_000) as i32;

    month_secs + day_secs + micro_secs
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::DurationRound;
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
        store.enqueue(&[job.clone()]).await?;

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
        let run_at = Utc::now()
            .duration_trunc(chrono::Duration::milliseconds(1))
            .unwrap();
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
        store.enqueue(&[job.clone()]).await?;

        let exists = store.exists(&queue, &job_id).await?;
        assert!(exists);

        let db_job = store.get(&queue, &job_id).await?;
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

        let db_job = store.get(&queue, &job_id).await?;
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
        store.enqueue(&[job.clone()]).await?;

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
        store.enqueue(&[job.clone()]).await?;

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
