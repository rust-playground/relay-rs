#![allow(clippy::module_name_repetitions)]
#![allow(clippy::module_inception)]
mod client;
mod consumer;
mod errors;

pub use client::{Builder, Client};
pub use consumer::{Builder as ConsumerBuilder, Consumer};
pub use errors::{Error, Result};
