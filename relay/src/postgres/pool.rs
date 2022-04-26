use async_trait::async_trait;
use deadpool::managed::{Hook, HookError, HookErrorCause, Manager, RecycleResult};
use deadpool::Runtime;
use log::LevelFilter;
use sqlx::postgres::PgConnectOptions;
use sqlx::{ConnectOptions, Connection, Error as SqlxError, Executor, PgConnection};
use std::str::FromStr;
use std::time::Duration;

/// Pool represents the managed DB connection pool
type Pool = deadpool::managed::Pool<Connections>;

/// Is the managed DB connection pool
pub struct Connections {
    options: PgConnectOptions,
}

impl Connections {
    /// Creates a new connection pool with default options
    ///
    /// # Panics
    ///
    /// Will panic on invalid Pool configuration. Since this is not tunable by the user it should be
    /// infallible.
    ///
    /// # Errors
    ///
    /// Will return an `Err` if the pool cannot be created.
    ///
    pub fn default(uri: &str, max_size: usize) -> Result<Pool, sqlx::error::Error> {
        let options = PgConnectOptions::from_str(uri)?
            .log_statements(LevelFilter::Off)
            .log_slow_statements(LevelFilter::Warn, Duration::from_secs(1))
            .clone();
        Self::new(options, max_size)
    }

    /// Creates a new connection pool with a set of options
    ///
    /// # Panics
    ///
    /// Will panic on invalid Pool configuration. Since this is not tunable by the user it should be
    /// infallible.
    ///
    /// # Errors
    ///
    /// Will panic on invalid Pool configuration. Since this is not tunable by the user it should be
    /// infallible.
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
impl Manager for Connections {
    type Type = PgConnection;
    type Error = SqlxError;

    async fn create(&self) -> Result<PgConnection, SqlxError> {
        PgConnection::connect_with(&self.options).await
    }
    async fn recycle(&self, obj: &mut PgConnection) -> RecycleResult<SqlxError> {
        Ok(obj.ping().await?)
    }
}
