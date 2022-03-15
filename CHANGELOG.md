# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

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

[Unreleased]: https://github.com/rust-playground/relay-rs/compare/v0.4.0...HEAD
[0.4.0]: https://github.com/rust-playground/relay-rs/compare/v0.3.0...v0.4.0
[0.3.0]: https://github.com/rust-playground/relay-rs/compare/55f4ffca5f12ebce195d6b53cf2d2f92c9036614...v0.3.0
[0.2.0]: https://github.com/rust-playground/relay-rs/commit/55f4ffca5f12ebce195d6b53cf2d2f92c9036614