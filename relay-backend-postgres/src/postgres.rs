#![allow(clippy::cast_possible_truncation)]
use crate::migrations::{run_migrations, Migration};
use async_trait::async_trait;
use chrono::{TimeZone, Utc};
use deadpool_postgres::{
    ClientWrapper, GenericClient, Hook, HookError, HookErrorCause, Manager, ManagerConfig, Pool,
    PoolError, RecyclingMethod,
};
use metrics::{counter, histogram};
use pg_interval::Interval;
use relay_core::{Backend, Error, Job, Result};
use rustls::client::{ServerCertVerified, ServerCertVerifier, WebPkiVerifier};
use rustls::{Certificate, OwnedTrustAnchor, RootCertStore, ServerName};
use serde_json::value::RawValue;
use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::io;
use std::io::ErrorKind;
use std::sync::Arc;
use std::time::SystemTime;
use std::{str::FromStr, time::Duration};
use tokio_postgres::error::SqlState;
use tokio_postgres::types::{Json, ToSql};
use tokio_postgres::{Config as PostgresConfig, Row};
use tokio_stream::{Stream, StreamExt};
use tracing::{debug, warn};

const MIGRATIONS: [Migration; 1] = [Migration::new(
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
        // Attempt to parse out the ssl mode before config parsing
        // library currently does not support verify-ca nor verify-full and will error.
        let mut uri = uri.to_string();
        let mut accept_invalid_certs = true;
        let mut accept_invalid_hostnames = true;

        if uri.contains("sslmode=verify-ca") {
            accept_invalid_certs = false;
            // so the Config doesn't fail to parse.
            uri = uri.replace("sslmode=verify-ca", "sslmode=required");
        } else if uri.contains("sslmode=verify-full") {
            accept_invalid_certs = false;
            accept_invalid_hostnames = false;
            // so the Config doesn't fail to parse.
            uri = uri.replace("sslmode=verify-full", "sslmode=required");
        }

        let mut pg_config = PostgresConfig::from_str(&uri)?;
        if pg_config.get_connect_timeout().is_none() {
            pg_config.connect_timeout(Duration::from_secs(5));
        }
        if pg_config.get_application_name().is_none() {
            pg_config.application_name("relay");
        }

        let tls_config_defaults = rustls::ClientConfig::builder().with_safe_defaults();

        let tls_config = if accept_invalid_certs {
            tls_config_defaults
                .with_custom_certificate_verifier(Arc::new(AcceptAllTlsVerifier))
                .with_no_client_auth()
        } else {
            let mut cert_store = RootCertStore::empty();
            cert_store.add_trust_anchors(webpki_roots::TLS_SERVER_ROOTS.0.iter().map(|ta| {
                OwnedTrustAnchor::from_subject_spki_name_constraints(
                    ta.subject,
                    ta.spki,
                    ta.name_constraints,
                )
            }));

            if accept_invalid_hostnames {
                let verifier = WebPkiVerifier::new(cert_store, None);
                tls_config_defaults
                    .with_custom_certificate_verifier(Arc::new(NoHostnameTlsVerifier { verifier }))
                    .with_no_client_auth()
            } else {
                tls_config_defaults
                    .with_root_certificates(cert_store)
                    .with_no_client_auth()
            }
        };

        let tls = tokio_postgres_rustls::MakeRustlsConnect::new(tls_config);

        let mgr = Manager::from_config(
            pg_config,
            tls,
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
        {
            let mut client = pool.get().await?;
            run_migrations("_relay_rs_migrations", &mut client, &MIGRATIONS).await?;
        }
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
                    r"INSERT INTO jobs (
                          id,
                          queue,
                          timeout,
                          max_retries,
                          retries_remaining,
                          data,
                          state,
                          updated_at,
                          created_at,
                          run_at
                        )
                        VALUES ($1, $2, $3, $4, $4, $5, $6, $7, $7, $8)",
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
                        &job.state.as_ref().map(|state| Some(Json(state))),
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
            counter!("enqueued", "queue" => job.queue.clone()).increment(1);
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
                    r"INSERT INTO jobs (
                          id,
                          queue,
                          timeout,
                          max_retries,
                          retries_remaining,
                          data,
                          state,
                          updated_at,
                          created_at,
                          run_at
                        )
                        VALUES ($1, $2, $3, $4, $4, $5, $6, $7, $7, $8)
                        ON CONFLICT DO NOTHING",
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
                            &job.state.as_ref().map(|state| Some(Json(state))),
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
                counter!("enqueued", "queue" => queue).increment(count);
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
        let client = self.pool.get().await.map_err(|e| Error::Backend {
            message: e.to_string(),
            is_retryable: is_retryable_pool(e),
        })?;

        let stmt = client
            .prepare_cached(
                r"
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
            ",
            )
            .await
            .map_err(|e| Error::Backend {
                message: e.to_string(),
                is_retryable: is_retryable(e),
            })?;

        let row = client
            .query_opt(&stmt, &[&queue, &job_id])
            .await
            .map_err(|e| Error::Backend {
                message: e.to_string(),
                is_retryable: is_retryable(e),
            })?;

        let job = row.as_ref().map(row_to_job);

        counter!("get", "queue" => queue.to_owned()).increment(1);
        debug!("got job");
        Ok(job)
    }

    /// Deletes the job from the database.
    ///
    /// # Errors
    ///
    /// Will return `Err` if there is any communication issues with the backend Postgres DB.
    #[tracing::instrument(name = "pg_delete", level = "debug", skip_all, fields(job_id=%job_id, queue=%queue))]
    async fn delete(&self, queue: &str, job_id: &str) -> Result<()> {
        let client = self.pool.get().await.map_err(|e| Error::Backend {
            message: e.to_string(),
            is_retryable: is_retryable_pool(e),
        })?;
        let stmt = client
            .prepare_cached(
                r"
                DELETE FROM jobs
                WHERE
                    queue=$1 AND
                    id=$2
                RETURNING run_at
            ",
            )
            .await
            .map_err(|e| Error::Backend {
                message: e.to_string(),
                is_retryable: is_retryable(e),
            })?;
        let row = client
            .query_opt(&stmt, &[&queue, &job_id])
            .await
            .map_err(|e| Error::Backend {
                message: e.to_string(),
                is_retryable: is_retryable(e),
            })?;

        if let Some(row) = row {
            let run_at = Utc.from_utc_datetime(&row.get(0));

            counter!("deleted", "queue" => queue.to_owned()).increment(1);

            if let Ok(d) = (Utc::now() - run_at).to_std() {
                histogram!("duration",  "queue" => queue.to_owned(), "type" => "deleted").record(d);
            }
            debug!("deleted job");
        }
        Ok(())
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
        let client = self.pool.get().await.map_err(|e| Error::Backend {
            message: e.to_string(),
            is_retryable: is_retryable_pool(e),
        })?;

        let stmt = client
            .prepare_cached(
                r"
               UPDATE jobs
               SET state=$3,
                   updated_at=NOW(),
                   expires_at=NOW()+timeout
               WHERE
                   queue=$1 AND
                   id=$2 AND
                   in_flight=true
               RETURNING (SELECT run_at FROM jobs WHERE queue=$1 AND id=$2 AND in_flight=true)
            ",
            )
            .await
            .map_err(|e| Error::Backend {
                message: e.to_string(),
                is_retryable: is_retryable(e),
            })?;

        let row = client
            .query_opt(
                &stmt,
                &[&queue, &job_id, &state.map(|state| Some(Json(state)))],
            )
            .await
            .map_err(|e| Error::Backend {
                message: e.to_string(),
                is_retryable: is_retryable(e),
            })?;

        if let Some(row) = row {
            let run_at = Utc.from_utc_datetime(&row.get(0));

            counter!("heartbeat", "queue" => queue.to_owned()).increment(1);

            if let Ok(d) = (Utc::now() - run_at).to_std() {
                histogram!("duration",  "queue" => queue.to_owned(), "type" => "running").record(d);
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

    /// Checks and returns if a Job exists in the database with the provided queue and id.
    ///
    /// # Errors
    ///
    /// Will return `Err` if there is any communication issues with the backend Postgres DB.
    #[tracing::instrument(name = "pg_exists", level = "debug", skip_all, fields(job_id=%job_id, queue=%queue))]
    async fn exists(&self, queue: &str, job_id: &str) -> Result<bool> {
        let client = self.pool.get().await.map_err(|e| Error::Backend {
            message: e.to_string(),
            is_retryable: is_retryable_pool(e),
        })?;

        let stmt = client
            .prepare_cached(
                r"
                    SELECT EXISTS (
                        SELECT 1 FROM jobs WHERE
                            queue=$1 AND
                            id=$2
                    )
                ",
            )
            .await
            .map_err(|e| Error::Backend {
                message: e.to_string(),
                is_retryable: is_retryable(e),
            })?;

        let exists: bool = client
            .query_one(&stmt, &[&queue, &job_id])
            .await
            .map_err(|e| Error::Backend {
                message: e.to_string(),
                is_retryable: is_retryable(e),
            })?
            .get(0);

        counter!("exists", "queue" => queue.to_owned()).increment(1);
        debug!("exists check job");
        Ok(exists)
    }

    /// Fetches the next available Job(s) to be executed order by `run_at`.
    ///
    /// # Errors
    ///
    /// Will return `Err` if there is any communication issues with the backend Postgres DB.
    #[tracing::instrument(name = "pg_next", level = "debug", skip_all, fields(num_jobs=num_jobs, queue=%queue))]
    async fn next(&self, queue: &str, num_jobs: u32) -> Result<Option<Vec<RawJob>>> {
        let client = self.pool.get().await.map_err(|e| Error::Backend {
            message: e.to_string(),
            is_retryable: is_retryable_pool(e),
        })?;

        // MUST USE CTE WITH `FOR UPDATE SKIP LOCKED LIMIT` otherwise the Postgres Query Planner
        // CAN optimize the query which will cause MORE updates than the LIMIT specifies within
        // a nested loop.
        // See here for details:
        // https://github.com/feikesteenbergen/demos/blob/19522f66ffb6eb358fe2d532d9bdeae38d4e2a0b/bugs/update_from_correlated.adoc
        let stmt = client
            .prepare_cached(
                r"
               WITH subquery AS (
                   SELECT
                        id,
                        queue,
                        updated_at
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
                         j.updated_at,
                         subquery.updated_at as subquery_updated_at
            ",
            )
            .await
            .map_err(|e| Error::Backend {
                message: e.to_string(),
                is_retryable: is_retryable(e),
            })?;

        let now = Utc::now();
        let limit = i64::from(num_jobs);
        let params: Vec<&(dyn ToSql + Sync)> = vec![&queue, &limit];
        let stream = client
            .query_raw(&stmt, params)
            .await
            .map_err(|e| Error::Backend {
                message: e.to_string(),
                is_retryable: is_retryable(e),
            })?;

        tokio::pin!(stream);

        // on purpose NOT using num_jobs as the capacity to avoid the potential attack vector of
        // someone exhausting all memory by sending a large number even if there aren't that many
        // records in the database.
        let mut jobs = if let Some(size) = stream.size_hint().1 {
            Vec::with_capacity(size)
        } else {
            Vec::new()
        };

        while let Some(row) = stream.next().await {
            let row = row.map_err(|e| Error::Backend {
                message: e.to_string(),
                is_retryable: is_retryable(e),
            })?;
            let j = row_to_job(&row);

            let updated_at = Utc.from_utc_datetime(&row.get(8));
            let to_processing = if j.run_at.is_none() || updated_at > j.run_at.unwrap() {
                updated_at
            } else {
                j.run_at.unwrap()
            };
            // using updated_at because this handles:
            // - enqueue -> processing
            // - reschedule -> processing
            // - reaped -> processing
            // This is a possible indicator not enough consumers/processors on the calling side
            // and jobs are backed up processing.
            if let Ok(d) = (now - to_processing).to_std() {
                histogram!("latency",  "queue" => j.queue.clone(), "type" => "to_processing")
                    .record(d);
            }

            jobs.push(j);
        }

        if jobs.is_empty() {
            debug!("fetched no jobs");
            Ok(None)
        } else {
            counter!("fetched", "queue" => queue.to_owned()).increment(jobs.len() as u64);
            debug!(fetched_jobs = jobs.len(), "fetched next job(s)");
            Ok(Some(jobs))
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
                r"
                UPDATE jobs
                SET
                    timeout = $3,
                    max_retries = $4,
                    retries_remaining = $4,
                    data = $5,
                    state = $6,
                    updated_at = $7,
                    run_at = $8,
                    in_flight = false
                WHERE
                    queue=$1 AND
                    id=$2 AND
                    in_flight=true
                RETURNING (SELECT run_at FROM jobs WHERE queue=$1 AND id=$2 AND in_flight=true)
                ",
            )
            .await
            .map_err(|e| Error::Backend {
                message: e.to_string(),
                is_retryable: is_retryable(e),
            })?;

        let row = client
            .query_opt(
                &stmt,
                &[
                    &job.queue,
                    &job.id,
                    &Interval::from_duration(chrono::Duration::seconds(i64::from(job.timeout))),
                    &job.max_retries,
                    &Json(&job.payload),
                    &job.state.as_ref().map(|state| Some(Json(state))),
                    &now,
                    &run_at,
                ],
            )
            .await
            .map_err(|e| Error::Backend {
                message: e.to_string(),
                is_retryable: is_retryable(e),
            })?;

        if let Some(row) = row {
            let run_at = Utc.from_utc_datetime(&row.get(0));

            counter!("rescheduled", "queue" => job.queue.clone()).increment(1);

            if let Ok(d) = (Utc::now() - run_at).to_std() {
                histogram!("duration",  "queue" => job.queue.clone(), "type" => "rescheduled")
                    .record(d);
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
        let client = self.pool.get().await.map_err(|e| Error::Backend {
            message: e.to_string(),
            is_retryable: is_retryable_pool(e),
        })?;

        let stmt = client
            .prepare_cached(
                r"
            UPDATE internal_state
            SET last_run=NOW()
            WHERE last_run <= NOW() - $1::text::interval",
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
                r"
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
            ",
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
            counter!("retries", "queue" => queue)
                .increment(u64::try_from(count).unwrap_or_default());
        }

        let stmt = client
            .prepare_cached(
                r"
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
            ",
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
            counter!("errors", "queue" => queue, "type" => "max_retries")
                .increment(u64::try_from(count).unwrap_or_default());
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

#[inline]
fn row_to_job(row: &Row) -> RawJob {
    RawJob {
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
        updated_at: Some(Utc.from_utc_datetime(&row.get(7))),
    }
}

fn interval_seconds(interval: Interval) -> i32 {
    let month_secs = interval.months * 30 * 24 * 60 * 60;
    let day_secs = interval.days * 24 * 60 * 60;
    let micro_secs = (interval.microseconds / 1_000_000) as i32;

    month_secs + day_secs + micro_secs
}

struct AcceptAllTlsVerifier;

impl ServerCertVerifier for AcceptAllTlsVerifier {
    fn verify_server_cert(
        &self,
        _end_entity: &Certificate,
        _intermediates: &[Certificate],
        _server_name: &ServerName,
        _scts: &mut dyn Iterator<Item = &[u8]>,
        _ocsp_response: &[u8],
        _now: SystemTime,
    ) -> std::result::Result<ServerCertVerified, rustls::Error> {
        Ok(ServerCertVerified::assertion())
    }
}

pub struct NoHostnameTlsVerifier {
    verifier: WebPkiVerifier,
}

impl ServerCertVerifier for NoHostnameTlsVerifier {
    fn verify_server_cert(
        &self,
        end_entity: &Certificate,
        intermediates: &[Certificate],
        server_name: &ServerName,
        scts: &mut dyn Iterator<Item = &[u8]>,
        ocsp_response: &[u8],
        now: SystemTime,
    ) -> std::result::Result<ServerCertVerified, rustls::Error> {
        match self.verifier.verify_server_cert(
            end_entity,
            intermediates,
            server_name,
            scts,
            ocsp_response,
            now,
        ) {
            Err(rustls::Error::InvalidCertificate(rustls::CertificateError::NotValidForName)) => {
                Ok(ServerCertVerified::assertion())
            }
            res => res,
        }
    }
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
