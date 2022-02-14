use crate::store::backing::{Backing, Error, Result};
use crate::Job;
use async_stream::stream;
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use log::LevelFilter;
use sqlx::postgres::{PgConnectOptions, PgPoolOptions};
use sqlx::types::Json;
use sqlx::{ConnectOptions, Error as SQLXError, PgPool, Row};
use std::io::ErrorKind;
use std::pin::Pin;
use std::{str::FromStr, time::Duration};
use tokio_stream::{Stream, StreamExt};
use tracing::error;

/// Postgres backing store
pub struct PgStore {
    pool: PgPool,
}

impl PgStore {
    /// Creates a new Postgres store with default settings for Postgres.
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

    /// Creates a new Postgres store with advanced options.
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
            .connect_with(options)
            .await?;

        Self::new_with_pool(pool).await
    }

    /// Creates a new Postgres store with preconfigured pool
    ///
    /// # Errors
    ///
    /// Will return `Err` if connecting the server or running migrations fails.
    #[inline]
    pub async fn new_with_pool(pool: PgPool) -> std::result::Result<Self, sqlx::error::Error> {
        {
            sqlx::migrate!("./migrations").run(&pool).await?;
        }
        Ok(Self { pool })
    }
}

#[async_trait]
impl Backing for PgStore {
    async fn upsert(&self, job: &Job) -> Result<()> {
        sqlx::query("INSERT INTO scheduled_jobs (id, data, last_run) VALUES ($1, $2, $3) ON CONFLICT (id) DO UPDATE SET data=excluded.data, last_run=excluded.last_run")
            .bind(&job.id)
            .bind(Json(job))
            .bind(&job.last_run)
            .execute(&self.pool)
            .await
            .map_err(|e| Error::Upsert {
                job_id: job.id.clone(),
                message: e.to_string(),
                is_retryable: is_retryable(e),
            })?;
        Ok(())
    }

    async fn touch(&self, job_id: &str, last_run: &DateTime<Utc>) -> Result<()> {
        sqlx::query("UPDATE scheduled_jobs SET last_run=$2 WHERE id=$1")
            .bind(job_id)
            .bind(last_run)
            .execute(&self.pool)
            .await
            .map_err(|e| Error::Touch {
                job_id: job_id.to_string(),
                message: e.to_string(),
                is_retryable: is_retryable(e),
            })?;

        Ok(())
    }

    async fn delete(&self, job_id: &str) -> Result<()> {
        sqlx::query("DELETE FROM scheduled_jobs WHERE id=$1")
            .bind(job_id)
            .execute(&self.pool)
            .await
            .map_err(|e| Error::Delete {
                job_id: job_id.to_string(),
                message: e.to_string(),
                is_retryable: is_retryable(e),
            })?;
        Ok(())
    }

    #[inline]
    fn recover(&'_ self) -> Pin<Box<dyn Stream<Item = Result<Job>> + '_>> {
        Box::pin(stream! {
            let mut cursor = sqlx::query("SELECT data FROM scheduled_jobs")
                .fetch(&self.pool);

            while let Some(result) = cursor
                .next()
                .await
            {
                match result {
                    Err(e) => {
                        yield Err(Error::Recovery {
                            message: e.to_string(),
                            is_retryable: is_retryable(e),
                        });
                    }
                    Ok(row) => {
                        let job: Json<Job> = match row.try_get(0){
                            Ok(v)=>v,
                            Err(e) => {
                                error!("failed to recover data: {}", e.to_string());
                                continue
                            }
                        };
                        yield Ok(job.0);
                    }
                };
            };
        })
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
