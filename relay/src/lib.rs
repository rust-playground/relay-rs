//! # relay-rs
//!
//! This contains a no nonsense, horizontally scalable, ordered job runner backed by Postgres.
//!
//! ### Features
//! Optional features:
//! - [`metrics-prometheus`][]: Enables emitting of Prometheus metrics via a scraping endpoint.
//!
//! [`metrics-prometheus`]: https://crates.io/crates/metrics-exporter-prometheus
//!

mod jobs;

/// Contains the `HTTP` server exposing the relay functionality.
pub mod http;
/// Postgres backing store for relay functionality.
pub mod postgres;
pub use jobs::{Error, RawJob, Result};
