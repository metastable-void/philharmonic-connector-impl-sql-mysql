//! MySQL-family SQL connector implementation for Philharmonic.
//!
//! `sql_mysql` implements the connector `sql_query` capability using
//! `sqlx` with the MySQL driver (`mysql`, `runtime-tokio-rustls`).
//! Requests are deserialized from script-provided JSON, executed as
//! parameterized SQL (no interpolation), and returned in the wire shape
//! defined in `docs/design/08-connector-architecture.md`.
//!
//! MySQL-specific behavior:
//!
//! - Unsigned integer columns are range-checked against `i64` for JSON
//!   number compatibility; values above `i64::MAX` return `upstream_error`.
//! - `DECIMAL` / `NUMERIC` are always surfaced as JSON strings to
//!   preserve precision.
//! - `DATETIME` values are zone-naive local times; `TIMESTAMP` values
//!   are UTC-normalized and serialized with a `Z` suffix.
//! - MySQL has no SQL `ARRAY` type; array request params are rejected as
//!   `invalid_request`.

mod config;
mod error;
mod execute;
mod request;
mod response;
mod types;

pub use crate::config::{PreparedConfig, SqlMysqlConfig};
pub use crate::request::SqlQueryRequest;
pub use crate::response::{Column, SqlQueryResponse};
pub use philharmonic_connector_impl_api::{
    ConnectorCallContext, Implementation, ImplementationError, JsonValue, async_trait,
};

use crate::error::Error;

const NAME: &str = "sql_mysql";

/// `sql_query` implementation for MySQL-family databases.
#[derive(Clone, Debug, Default)]
pub struct SqlMysql;

impl SqlMysql {
    /// Builds a new stateless implementation instance.
    pub fn new() -> Self {
        Self
    }
}

#[async_trait]
impl Implementation for SqlMysql {
    fn name(&self) -> &str {
        NAME
    }

    async fn execute(
        &self,
        config: &JsonValue,
        request: &JsonValue,
        _ctx: &ConnectorCallContext,
    ) -> Result<JsonValue, ImplementationError> {
        let config: SqlMysqlConfig = serde_json::from_value(config.clone())
            .map_err(|e| Error::InvalidConfig(e.to_string()))
            .map_err(ImplementationError::from)?;
        let prepared = config.prepare().map_err(ImplementationError::from)?;

        let request: SqlQueryRequest = serde_json::from_value(request.clone())
            .map_err(|e| Error::InvalidRequest(e.to_string()))
            .map_err(ImplementationError::from)?;

        let response = execute::execute_sql_query(&prepared, &request)
            .await
            .map_err(ImplementationError::from)?;

        serde_json::to_value(response)
            .map_err(|e| Error::Internal(format!("failed to serialize sql response: {e}")))
            .map_err(ImplementationError::from)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn name_matches_wire_impl() {
        let implm = SqlMysql::new();
        assert_eq!(implm.name(), "sql_mysql");
    }
}
