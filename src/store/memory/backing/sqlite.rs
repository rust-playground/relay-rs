use super::Backing;
use super::Error;
use super::Job;
use async_stream::stream;
use async_trait::async_trait;
use chrono::Utc;
use log::LevelFilter;
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
    pub async fn default(uri: &str) -> Result<Self, sqlx::error::Error> {
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
    pub async fn new(options: SqliteConnectOptions) -> Result<Self, sqlx::error::Error> {
        let pool = SqlitePool::connect_with(options).await?;
        let mut conn = pool.acquire().await?;
        sqlx::migrate!("./migrations/sqlite").run(&mut conn).await?;
        Ok(Self { pool })
    }
}

#[async_trait]
impl Backing for Store {
    #[inline]
    async fn push(&self, job: &Job) -> super::Result<()> {
        let now = Utc::now();

        let blob = serde_json::to_vec(&job).map_err(|e| Error::Push {
            job_id: job.id.clone(),
            queue: job.queue.clone(),
            message: e.to_string(),
            is_retryable: false,
        })?;

        let state = match &job.state {
            None => None,
            Some(state) => {
                let state = serde_json::to_vec(&state).map_err(|e| Error::Push {
                    job_id: job.id.clone(),
                    queue: job.queue.clone(),
                    message: e.to_string(),
                    is_retryable: false,
                })?;
                Some(state)
            }
        };

        let mut conn = self.pool.acquire().await.map_err(|e| Error::Push {
            job_id: job.id.clone(),
            queue: job.queue.clone(),
            message: e.to_string(),
            is_retryable: is_retryable(e),
        })?;

        sqlx::query("INSERT INTO jobs (id, data, state, created_at) VALUES (?1, ?2, ?3, ?4)")
            .bind(&format!("{}-{}", &job.queue, &job.id))
            .bind(&blob)
            .bind(&state)
            .bind(&now)
            .execute(&mut conn)
            .await
            .map_err(|e| Error::Push {
                job_id: job.id.clone(),
                queue: job.queue.clone(),
                message: e.to_string(),
                is_retryable: is_retryable(e),
            })?;
        Ok(())
    }

    #[inline]
    async fn remove(&self, job: &Job) -> super::Result<()> {
        let mut conn = self.pool.acquire().await.map_err(|e| Error::Remove {
            job_id: job.id.clone(),
            queue: job.queue.clone(),
            message: e.to_string(),
            is_retryable: is_retryable(e),
        })?;

        sqlx::query("DELETE FROM jobs WHERE id=?1")
            .bind(format!("{}-{}", &job.queue, &job.id))
            .execute(&mut conn)
            .await
            .map_err(|e| Error::Remove {
                job_id: job.id.clone(),
                queue: job.queue.clone(),
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
    ) -> super::Result<()> {
        let unique_id = format!("{}-{}", &queue, &job_id);

        let mut conn = self.pool.acquire().await.map_err(|e| Error::Update {
            job_id: job_id.to_string(),
            queue: queue.to_string(),
            message: e.to_string(),
            is_retryable: is_retryable(e),
        })?;

        match state {
            None => {
                sqlx::query("UPDATE jobs SET state=null WHERE id=?1")
                    .bind(&unique_id)
                    .execute(&mut conn)
                    .await
                    .map_err(|e| Error::Update {
                        job_id: job_id.to_string(),
                        queue: queue.to_string(),
                        message: e.to_string(),
                        is_retryable: is_retryable(e),
                    })?;
            }
            Some(state) => {
                let state = serde_json::to_vec(&state).map_err(|e| Error::Update {
                    job_id: job_id.to_string(),
                    queue: queue.to_string(),
                    message: e.to_string(),
                    is_retryable: false,
                })?;
                sqlx::query("UPDATE jobs SET state=?2 WHERE id=?1")
                    .bind(&unique_id)
                    .bind(state)
                    .execute(&mut conn)
                    .await
                    .map_err(|e| Error::Update {
                        job_id: job_id.to_string(),
                        queue: queue.to_string(),
                        message: e.to_string(),
                        is_retryable: is_retryable(e),
                    })?;
            }
        }

        Ok(())
    }

    #[inline]
    fn recover(&'_ self) -> Pin<Box<dyn Stream<Item = super::Result<Job>> + '_>> {
        Box::pin(stream! {
            let mut conn = self.pool.acquire().await.map_err(|e| Error::Recovery {
                message: e.to_string(),
                is_retryable: is_retryable(e),
            })?;

            let mut cursor = sqlx::query("SELECT data, state FROM jobs ORDER BY created_at ASC")
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
                        let mut job: Job = match serde_json::from_slice(&blob) {
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
