use async_stream::stream;
use async_trait::async_trait;
use chrono::Utc;
use log::LevelFilter;
use relay::memory_store::backing::{Backing, Error, Result};
use relay::memory_store::StoredJob;
use serde_json::value::RawValue;
use sqlx::postgres::PgConnectOptions;
use sqlx::types::Json;
use sqlx::{ConnectOptions, Error as SQLXError, PgPool, Pool, Postgres, Row};
use std::io::ErrorKind;
use std::pin::Pin;
use std::{str::FromStr, time::Duration};
use tokio_stream::{Stream, StreamExt};
use tracing::error;

/// Postgres backing store
pub struct Store {
    pool: Pool<Postgres>,
}

impl Store {
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
        let pool = PgPool::connect_with(options).await?;
        let mut conn = pool.acquire().await?;
        sqlx::migrate!("./migrations").run(&mut conn).await?;

        Ok(Self { pool })
    }
}

#[async_trait]
impl Backing for Store {
    #[inline]
    async fn push(&self, stored: &StoredJob) -> Result<()> {
        let now = Utc::now();

        let mut conn = self.pool.acquire().await.map_err(|e| Error::Push {
            job_id: stored.job.id.clone(),
            queue: stored.job.queue.clone(),
            message: e.to_string(),
            is_retryable: is_retryable(e),
        })?;

        sqlx::query("INSERT INTO jobs (id, data, retries, in_flight, created_at) VALUES ($1, $2, $3, $4, $5)")
            .bind(&format!("{}-{}", &stored.job.queue, &stored.job.id))
            .bind(Json(&stored))
            .bind(i32::from(stored.retries))
            .bind(stored.in_flight)
            .bind(&now)
            .execute(&mut conn)
            .await
            .map_err(|e| Error::Push {
                job_id: stored.job.id.clone(),
                queue: stored.job.queue.clone(),
                message: e.to_string(),
                is_retryable: is_retryable(e),
            })?;
        Ok(())
    }

    #[inline]
    async fn remove(&self, stored: &StoredJob) -> Result<()> {
        let mut conn = self.pool.acquire().await.map_err(|e| Error::Remove {
            job_id: stored.job.id.clone(),
            queue: stored.job.queue.clone(),
            message: e.to_string(),
            is_retryable: is_retryable(e),
        })?;

        sqlx::query("DELETE FROM jobs WHERE id=$1")
            .bind(format!("{}-{}", &stored.job.queue, &stored.job.id))
            .execute(&mut conn)
            .await
            .map_err(|e| Error::Remove {
                job_id: stored.job.id.clone(),
                queue: stored.job.queue.clone(),
                message: e.to_string(),
                is_retryable: is_retryable(e),
            })?;
        Ok(())
    }

    #[inline]
    async fn update(
        &self,
        queue: &str,
        job_id: &str,
        state: &Option<Box<RawValue>>,
        retries: Option<u8>,
        in_flight: Option<bool>,
    ) -> Result<()> {
        let unique_id = format!("{}-{}", &queue, &job_id);

        let mut conn = self.pool.acquire().await.map_err(|e| Error::Update {
            job_id: job_id.to_string(),
            queue: queue.to_string(),
            message: e.to_string(),
            is_retryable: is_retryable(e),
        })?;

        let state = if let Some(state) = state {
            Some(serde_json::to_vec(&state).map_err(|e| Error::Update {
                job_id: job_id.to_string(),
                queue: queue.to_string(),
                message: e.to_string(),
                is_retryable: false,
            })?)
        } else {
            None
        };

        let query = match (state, retries, in_flight) {
            (Some(state), Some(retries), Some(in_flight)) => {
                sqlx::query("UPDATE jobs SET state=$2,retries=$3,in_flight=$4 WHERE id=$1")
                    .bind(&unique_id)
                    .bind(Json(state))
                    .bind(i32::from(retries))
                    .bind(in_flight)
            }
            (Some(state), Some(retries), None) => {
                sqlx::query("UPDATE jobs SET state=$2,retries=$3 WHERE id=$1")
                    .bind(&unique_id)
                    .bind(Json(state))
                    .bind(i32::from(retries))
            }
            (Some(state), None, Some(in_flight)) => {
                sqlx::query("UPDATE jobs SET state=$2,in_flight=$3 WHERE id=$1")
                    .bind(&unique_id)
                    .bind(Json(state))
                    .bind(in_flight)
            }
            (Some(state), None, None) => sqlx::query("UPDATE jobs SET state=$2 WHERE id=$1")
                .bind(&unique_id)
                .bind(Json(state)),
            (None, Some(retries), Some(in_flight)) => {
                sqlx::query("UPDATE jobs SET retries=$2,in_flight=$3 WHERE id=$1")
                    .bind(&unique_id)
                    .bind(i32::from(retries))
                    .bind(in_flight)
            }
            (None, Some(retries), None) => sqlx::query("UPDATE jobs SET retries=$2 WHERE id=$1")
                .bind(&unique_id)
                .bind(i32::from(retries)),
            (None, None, Some(in_flight)) => {
                sqlx::query("UPDATE jobs SET in_flight=$2 WHERE id=$1")
                    .bind(&unique_id)
                    .bind(in_flight)
            }
            (None, None, None) => return Ok(()),
        };

        query.execute(&mut conn).await.map_err(|e| Error::Update {
            job_id: job_id.to_string(),
            queue: queue.to_string(),
            message: e.to_string(),
            is_retryable: is_retryable(e),
        })?;

        Ok(())
    }

    #[inline]
    fn recover(&'_ self) -> Pin<Box<dyn Stream<Item = Result<StoredJob>> + '_>> {
        Box::pin(stream! {
            let mut conn = self.pool.acquire().await.map_err(|e| Error::Recovery {
                message: e.to_string(),
                is_retryable: is_retryable(e),
            })?;

            let mut cursor = sqlx::query("SELECT data, state, retries, in_flight FROM jobs ORDER BY created_at ASC")
                .fetch(&mut conn);

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
                        let mut job: Json<StoredJob> = match row.try_get(0){
                            Ok(v)=>v,
                            Err(e) => {
                                error!("failed to recover data: {}", e.to_string());
                                continue
                            }
                        };
                        let state: Option<Json<Box<RawValue>>> = match row.try_get(1){
                            Ok(v)=>v,
                            Err(e) => {
                                error!("failed to recover state data: {}", e.to_string());
                                continue
                            }
                        };
                        if let Some(state) = state {
                            job.state = Some(state.0)
                        }
                        let retries: i32 = row.get(2);
                        let in_flight: bool = row.get(3);

                        job.retries = match u8::try_from(retries) {
                            Ok(i) => i,
                            Err(_) => 0,
                        };
                        job.in_flight = in_flight;
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
