# philharmonic-connector-impl-sql-mysql

`sql_mysql` implements Philharmonic's `sql_query` capability for
MySQL-family databases (MySQL 8, MariaDB 10.5+, Aurora MySQL, TiDB)
via `sqlx` (`mysql` + `runtime-tokio-rustls`). It executes
parameterized `?`-placeholder queries, enforces per-config caps for
`timeout_ms` and `max_rows`, returns typed JSON rows plus ordered
column metadata, and maps upstream/database failures into
`ImplementationError` categories defined by the connector wire
protocol.

## Contributing

This crate is developed as a submodule of the Philharmonic
workspace. Workspace-wide development conventions — git workflow,
script wrappers, Rust code rules, versioning, terminology — live
in the workspace meta-repo at
[metastable-void/philharmonic-workspace](https://github.com/metastable-void/philharmonic-workspace),
authoritatively in its
[`CONTRIBUTING.md`](https://github.com/metastable-void/philharmonic-workspace/blob/main/CONTRIBUTING.md).

SPDX-License-Identifier: Apache-2.0 OR MPL-2.0
