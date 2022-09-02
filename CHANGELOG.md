# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

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

[Unreleased]: https://github.com/rust-playground/relay-rs/compare/v0.8.0...HEAD
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