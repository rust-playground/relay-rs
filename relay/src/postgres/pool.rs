use async_trait::async_trait;
use deadpool::managed::{Hook, HookError, HookErrorCause, Manager, RecycleResult};
use deadpool::Runtime;
use log::LevelFilter;
use sqlx::postgres::PgConnectOptions;
use sqlx::{ConnectOptions, Connection, Error as SqlxError, Executor, PgConnection};
use std::str::FromStr;
use std::time::Duration;

type Pool = deadpool::managed::Pool<PgPool>;

pub struct PgPool {
    options: PgConnectOptions,
}

impl PgPool {
    pub fn default(uri: &str, max_size: usize) -> Result<Pool, sqlx::error::Error> {
        let options = PgConnectOptions::from_str(uri)?
            .log_statements(LevelFilter::Off)
            .log_slow_statements(LevelFilter::Warn, Duration::from_secs(1))
            .clone();
        Self::new(options, max_size)
    }

    pub fn new(options: PgConnectOptions, max_size: usize) -> Result<Pool, sqlx::error::Error> {
        let pool = Pool::builder(Self { options })
            .runtime(Runtime::Tokio1)
            .max_size(max_size)
            .create_timeout(Some(Duration::from_secs(60)))
            .recycle_timeout(Some(Duration::from_secs(60)))
            .post_create(Hook::async_fn(|conn: &mut PgConnection, _| {
                Box::pin(async move {
                    conn.execute("SET default_transaction_isolation TO 'read committed'")
                        .await
                        .map_err(|e| HookError::Abort(HookErrorCause::Backend(e)))?;
                    Ok(())
                })
            }))
            .build()
            .unwrap();
        Ok(pool)
    }
}

#[async_trait]
impl Manager for PgPool {
    type Type = PgConnection;
    type Error = SqlxError;

    async fn create(&self) -> Result<PgConnection, SqlxError> {
        PgConnection::connect_with(&self.options).await
    }
    async fn recycle(&self, obj: &mut PgConnection) -> RecycleResult<SqlxError> {
        Ok(obj.ping().await?)
    }
}
