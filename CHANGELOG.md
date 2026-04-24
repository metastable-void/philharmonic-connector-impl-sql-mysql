# Changelog

All notable changes to this crate are documented in this file.

The format is based on
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and
this crate adheres to
[Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.1.0] - 2026-04-24

- First functional release of `sql_mysql` implementing the
  `sql_query` connector capability.
- Added typed JSON response mapping for MySQL result sets,
  including precision-preserving decimal handling and
  unsigned-integer overflow protection.
- Added unit tests and Docker-gated integration tests against
  `mysql:8.0` via testcontainers.
