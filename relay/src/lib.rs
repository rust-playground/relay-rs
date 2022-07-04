//! # relay-rs
//!
//! This contains a no nonsense, horizontally scalable, ordered job runner backed by Postgres.
//!
//! ### Features
//! Optional features:
//! - [`frontend-http`][]: Enables the HTTP frontend (default).
//! - [`backend-postgres`][]: Enables the Postgres backend (default).
//! - [`metrics-prometheus`][]: Enables emitting of Prometheus metrics via a scraping endpoint.
//!
//! [`frontend-http`]: `relay_frontend_http`
//! [`backend-postgres`]: `relay_backend_postgres`
//! [`metrics-prometheus`]: https://crates.io/crates/metrics-exporter-prometheus
//!
