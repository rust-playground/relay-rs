use async_stream::stream;
use async_trait::async_trait;
use chrono::Utc;
use log::LevelFilter;
use relay::memory_store::backing::{Backing, Error, Result};
use relay::memory_store::StoredJob;
use serde_json::value::RawValue;
use sqlx::sqlite::SqliteConnectOptions;
use sqlx::{ConnectOptions, Error as SQLXError, Pool, Row, Sqlite, SqlitePool};
use std::io::ErrorKind;
use std::pin::Pin;
use std::{str::FromStr, time::Duration};
use tokio_stream::{Stream, StreamExt};
use tracing::error;

/// `SQLite` backing store
pub struct Store {
    pool: Pool<Sqlite>,
}

impl Store {
    /// Creates a new backing store with default settings for `SQLite`.
    ///
    /// # Errors
    ///
    /// Will return `Err` if connecting to the `SQLite` database fails or migrations fail to run.
    #[inline]
    pub async fn default(uri: &str) -> std::result::Result<Self, sqlx::error::Error> {
        let options = SqliteConnectOptions::from_str(uri)?
            .create_if_missing(true)
            .log_statements(LevelFilter::Off)
            .log_slow_statements(LevelFilter::Warn, Duration::from_secs(1))
            .clone();
        Self::new(options).await
    }

    /// Creates a new backing store with advanced options.
    ///
    /// # Errors
    ///
    /// Will return `Err` if connecting to the `SQLite` database fails or migrations fail to run.
    #[inline]
    pub async fn new(
        options: SqliteConnectOptions,
    ) -> std::result::Result<Self, sqlx::error::Error> {
        let pool = SqlitePool::connect_with(options).await?;
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

        let blob = serde_json::to_vec(&stored).map_err(|e| Error::Push {
            job_id: stored.job.id.clone(),
            queue: stored.job.queue.clone(),
            message: e.to_string(),
            is_retryable: false,
        })?;

        let state = match &stored.state {
            None => None,
            Some(state) => {
                let state = serde_json::to_vec(&state).map_err(|e| Error::Push {
                    job_id: stored.job.id.clone(),
                    queue: stored.job.queue.clone(),
                    message: e.to_string(),
                    is_retryable: false,
                })?;
                Some(state)
            }
        };

        let mut conn = self.pool.acquire().await.map_err(|e| Error::Push {
            job_id: stored.job.id.clone(),
            queue: stored.job.queue.clone(),
            message: e.to_string(),
            is_retryable: is_retryable(e),
        })?;

        sqlx::query("INSERT INTO jobs (id, data, state, retries, in_flight, created_at) VALUES (?1, ?2, ?3, ?4, ?5, ?6)")
            .bind(&format!("{}-{}", &stored.job.queue, &stored.job.id))
            .bind(&blob)
            .bind(&state)
            .bind(&stored.retries)
            .bind(&stored.in_flight)
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

        sqlx::query("DELETE FROM jobs WHERE id=?1")
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
                sqlx::query("UPDATE jobs SET state=?2,retries=?3,in_flight=?4 WHERE id=?1")
                    .bind(&unique_id)
                    .bind(state)
                    .bind(retries)
                    .bind(in_flight)
            }
            (Some(state), Some(retries), None) => {
                sqlx::query("UPDATE jobs SET state=?2,retries=?3 WHERE id=?1")
                    .bind(&unique_id)
                    .bind(state)
                    .bind(retries)
            }
            (Some(state), None, Some(in_flight)) => {
                sqlx::query("UPDATE jobs SET state=?2,in_flight=?3 WHERE id=?1")
                    .bind(&unique_id)
                    .bind(state)
                    .bind(in_flight)
            }
            (Some(state), None, None) => sqlx::query("UPDATE jobs SET state=?2 WHERE id=?1")
                .bind(&unique_id)
                .bind(state),
            (None, Some(retries), Some(in_flight)) => {
                sqlx::query("UPDATE jobs SET retries=?2,in_flight=?3 WHERE id=?1")
                    .bind(&unique_id)
                    .bind(retries)
                    .bind(in_flight)
            }
            (None, Some(retries), None) => sqlx::query("UPDATE jobs SET retries=?2 WHERE id=?1")
                .bind(&unique_id)
                .bind(retries),
            (None, None, Some(in_flight)) => {
                sqlx::query("UPDATE jobs SET in_flight=?2 WHERE id=?1")
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
                        let blob: Vec<u8> = row.get(0);
                        let state: Option<Vec<u8>> = row.get(1);
                        let retries: u8 = row.get(2);
                        let in_flight: bool = row.get(3);
                        let mut job: StoredJob = match serde_json::from_slice(&blob) {
                            Ok(v) => v,
                            Err(e) => {
                                error!("failed to recover data: {}", e.to_string());
                                continue
                            }
                        };
                        if let Some(state) = state {
                            job.state = match serde_json::from_slice(&state) {
                                Ok(v)=>v,
                                Err(e) => {
                                    error!("failed to recover state data: {}", e.to_string());
                                    continue
                                }
                            };
                        }
                        job.retries = retries;
                        job.in_flight = in_flight;
                        yield Ok(job);
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
                    "5" | "10" | "261" | "773" | "513" | "769" | "3338" | "2314" | "266"
                    | "778" => {
                        // 5=SQLITE_BUSY
                        // 10=SQLITE_IOERR
                        // 261=SQLITE_BUSY_RECOVERY
                        // 773=SQLITE_BUSY_TIMEOUT
                        // 513=SQLITE_ERROR_RETRY
                        // 769=SQLITE_ERROR_SNAPSHOT
                        // 3338=SQLITE_IOERR_ACCESS
                        // 2314=SQLITE_IOERR_UNLOCK
                        // 266=SQLITE_IOERR_READ
                        // 778=SQLITE_IOERR_WRITE
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
