# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.14.4] - 2023-12-14
### Fixed
- `to_processing` metric when job rescheduled.

## [0.14.3] - 2023-12-13
### Fixed
- version not incremented in Cargo.toml.

## [0.14.2] - 2023-12-13
### Fixed
- `to_processing` metric using wrong `updated_at`.

### Added
- Deprecation notice to README.

## [0.14.1] - 2023-06-02
### Fixed
- created_at being updated when rescheduling a Job, only updated_at should/needs to be for metrics reporting.

## [0.14.0] - 2023-05-21
### Security
- Updated deps with various fixes and optimizations.

## [0.13.0] - 2023-04-24
### Added
- Ability to set state upon Job creation.

### Changed
- Updated all dependencies to latest versions.
- Signal shutdown now uses tracing logs.
- Remove ansi lof printing.

## [0.12.1] - 2023-03-21
### Fixed
- Added back TLS support accidentally removed in Tokio Postgres changeover.

## [0.12.0] - 2023-03-18
### Changed
- SQLX -> Tokio Postgres + Deadpool. Better performance and allows for finer grained control.
- Actix Web -> Axum.
- Switched to internal migrations runner with proper locking semantics.

## [0.11.0] - 2023-01-03
### Added
- Building of arm64 docker image.
- Add labels to image eg. To indicate proper license.

### Changed
- Workspace QOL improvements to use top level Cargo.toml.
- Updated dependencies.
- Updated README to add Rust HTTP Client link.

## [0.10.0] - 2022-10-29
### Added
- HTTP Client.
- HTTP Consumer abstraction.
- Worker trait to be used by HTTP Consumer and future frontend consumers.
- Additional documentation.

### Changed
- Updated dependencies to latest versions.
- Updated CI build file dependencies.

### Fixed
- Generic params for Job struct to two separate as intended.
- CI build because of openssl and pkg-config issues.

## [0.9.0] - 2022-09-05
### Added
- Added new exists and get HTTP endpoint to be able to check for a Job's existence or fetch it.

### Changed
- HTTP endpoints to be more sane + RESTful.
- Metrics name.

## [0.8.0] - 2022-09-02
### Added
- Added per queue runtime & health metrics.

## [0.7.0] - 2022-07-04
### Changed
- Refactored code for more modular approach. This should allow for more frontends and backend in the future.

### Removed
- CRON scheduler. With self-perpetuating jobs this is far less useful and anyone can easily implement on their own.

### Added
- Ability to schedule Jobs with infinite retries by setting max_retries to a negative number eg. -1

## [0.6.1] - 2022-05-18
### Fixed
- Reschedule metric.

## [0.6.0] - 2022-05-13
### Changed
- Reschedule to accept setting/unsetting of state.

## [0.5.3] - 2022-05-07
### Fixed
- Fixed recursive function call in is_retryable().
- Updated dependencies with security/issue fixes.

### Added
- Caching in CI for faster test & builds.
- More debug level tracing.

### Changed
- Default connection and idle timeouts to 5 and 60 seconds respectively.
- Cleaned up error handling with impl From.
- Renamed Job to RawJob for future client with Job with generic types.

### Removed
- JobId and Queue alias types.

## [0.5.2] - 2022-04-22
### Fixed
- Next update query when under very specific circumstances the Postgres query planner can do the wrong thing. See here for a detailed example https://github.com/feikesteenbergen/demos/blob/19522f66ffb6eb358fe2d532d9bdeae38d4e2a0b/bugs/update_from_correlated.adoc

### Added
- Requirements section to README.

## [0.5.1] - 2022-04-22
### Added
- Automatic docker image build and push.

## [0.5.0] - 2022-04-22
### Fixed
- Updated job schema to add uuid. This fixes a locking issue when grabbing new items using IN rather than a SELECT FROM.

## [0.4.0] - 2022-03-14
### Added
- Update deps.

## [0.3.0] - 2022-03-05
### Added
- `/enqueue/batch` endpoint for efficient batch/bulk creation of Jobs.
- Refactored `/next` endpoint to accept an optional number of Jobs to return for fetching a batch of Jobs.

## [0.2.0] - 2022-02-27
### Added
- Future Job support using new `run_at` Job field.
- Reschedule endpoint allowing the Job Runner to manage a unique/singleton Job rescheduling itself.

[Unreleased]: https://github.com/rust-playground/relay-rs/compare/v0.14.4...HEAD
[0.14.4]: https://github.com/rust-playground/relay-rs/compare/v0.14.3...v0.14.4
[0.14.3]: https://github.com/rust-playground/relay-rs/compare/v0.14.2...v0.14.3
[0.14.2]: https://github.com/rust-playground/relay-rs/compare/v0.14.2...v0.14.2
[0.14.1]: https://github.com/rust-playground/relay-rs/compare/v0.14.0...v0.14.1
[0.14.0]: https://github.com/rust-playground/relay-rs/compare/v0.13.0...v0.14.0
[0.13.0]: https://github.com/rust-playground/relay-rs/compare/v0.12.1...v0.13.0
[0.12.1]: https://github.com/rust-playground/relay-rs/compare/v0.12.0...v0.12.1
[0.12.0]: https://github.com/rust-playground/relay-rs/compare/v0.11.0...v0.12.0
[0.11.0]: https://github.com/rust-playground/relay-rs/compare/v0.10.0...v0.11.0
[0.10.0]: https://github.com/rust-playground/relay-rs/compare/v0.9.0...v0.10.0
[0.9.0]: https://github.com/rust-playground/relay-rs/compare/v0.8.0...v0.9.0
[0.8.0]: https://github.com/rust-playground/relay-rs/compare/v0.7.0...v0.8.0
[0.7.0]: https://github.com/rust-playground/relay-rs/compare/v0.6.1...v0.7.0
[0.6.1]: https://github.com/rust-playground/relay-rs/compare/v0.6.0...v0.6.1
[0.6.0]: https://github.com/rust-playground/relay-rs/compare/v0.5.3...v0.6.0
[0.5.3]: https://github.com/rust-playground/relay-rs/compare/v0.5.2...v0.5.3
[0.5.2]: https://github.com/rust-playground/relay-rs/compare/v0.5.1...v0.5.2
[0.5.1]: https://github.com/rust-playground/relay-rs/compare/v0.5.0...v0.5.1
[0.5.0]: https://github.com/rust-playground/relay-rs/compare/v0.4.0...v0.5.0
[0.4.0]: https://github.com/rust-playground/relay-rs/compare/v0.3.0...v0.4.0
[0.3.0]: https://github.com/rust-playground/relay-rs/compare/55f4ffca5f12ebce195d6b53cf2d2f92c9036614...v0.3.0
[0.2.0]: https://github.com/rust-playground/relay-rs/commit/55f4ffca5f12ebce195d6b53cf2d2f92c9036614
