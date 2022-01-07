//! # `relay_rs`
//!
//! This crate contains a no nonsense job runner with configurable backends for optionally persisted data.
//!
//! Any significant changes to `relay_rs` are documented in
//! the [`CHANGELOG.md`](https://github.com/rust-playground/relay_rs/blob/main/CHANGELOG.md) file.
//!
//!
//! ### Features
//!
//! Optional features:
//!
//! - [`prometheus_metrics`][]: Enables emitting of Prometheus metrics via a scraping endpoint.
//! - [`sqlite_backing`][]: Enables an `SQLite` backed persistent store to handle crashes/restarts..
//! - [`postgres_backing`][]: Enables an `Postgres` backed persistent store to handle crashes/restarts.
//! - [`redis_backing`][]: Enables an `Redis` backed persistent store to handle crashes/restarts.
//! - [`dynamodb_backing`][]: Enables an `DynamoDB` backed persistent store to handle crashes/restarts.
//!
//! [`prometheus_metrics`]: https://crates.io/crates/metrics-exporter-prometheus
//! [`sqlite_backing`]: https://crates.io/crates/sqlx
//! [`postgres_backing`]: https://crates.io/crates/sqlx
//! [`redis_backing`]: https://crates.io/crates/redis
//! [`dynamodb_backing`]: https://crates.io/crates/aws-sdk-dynamodb
//!

/// The storage and server implementations.
pub mod store;
