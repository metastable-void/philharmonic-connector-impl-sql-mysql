//! Response model for `sql_query` on `sql_mysql`.

use philharmonic_connector_impl_api::JsonValue;
use serde::{Deserialize, Serialize};

/// Query response payload.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SqlQueryResponse {
    /// Result rows as object-per-row.
    pub rows: Vec<serde_json::Map<String, JsonValue>>,
    /// Number of rows returned (or affected for DML).
    pub row_count: u64,
    /// Ordered column metadata for result-set introspection.
    pub columns: Vec<Column>,
    /// Whether rows were clipped by `max_rows`.
    pub truncated: bool,
}

/// Result column metadata entry.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Column {
    /// Column label from the query result.
    pub name: String,
    /// Backend SQL type name (normalized to lowercase).
    pub sql_type: String,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn column_order_is_preserved() {
        let response = SqlQueryResponse {
            rows: vec![],
            row_count: 0,
            columns: vec![
                Column {
                    name: "id".to_owned(),
                    sql_type: "bigint".to_owned(),
                },
                Column {
                    name: "name".to_owned(),
                    sql_type: "varchar".to_owned(),
                },
            ],
            truncated: false,
        };

        assert_eq!(response.columns[0].name, "id");
        assert_eq!(response.columns[1].name, "name");
    }
}
