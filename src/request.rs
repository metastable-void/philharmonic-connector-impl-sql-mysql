//! Request model for `sql_query` on `sql_mysql`.

use philharmonic_connector_impl_api::JsonValue;
use serde::{Deserialize, Serialize};

/// One SQL query request.
#[derive(Debug, Clone, PartialEq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct SqlQueryRequest {
    /// SQL text with `?` positional placeholders.
    pub sql: String,
    /// Positional parameter values.
    #[serde(default)]
    pub params: Vec<JsonValue>,
    /// Optional per-request row cap; clamped to config cap.
    #[serde(default)]
    pub max_rows: Option<usize>,
    /// Optional per-request timeout; clamped to config cap.
    #[serde(default)]
    pub timeout_ms: Option<u64>,
}

impl SqlQueryRequest {
    /// Effective row cap: request override only if lower than config cap.
    pub fn effective_max_rows(&self, config_cap: usize) -> usize {
        self.max_rows
            .map_or(config_cap, |value| value.min(config_cap))
    }

    /// Effective timeout cap: request override only if lower than config cap.
    pub fn effective_timeout_ms(&self, config_cap: u64) -> u64 {
        self.timeout_ms
            .map_or(config_cap, |value| value.min(config_cap))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn deserialize_sql_request_shape() {
        let value = json!({
            "sql": "SELECT ?",
            "params": ["abc"],
            "max_rows": 50,
            "timeout_ms": 2000
        });

        let req = serde_json::from_value::<SqlQueryRequest>(value).unwrap();
        assert_eq!(req.sql, "SELECT ?");
        assert_eq!(req.params, vec![json!("abc")]);
        assert_eq!(req.max_rows, Some(50));
        assert_eq!(req.timeout_ms, Some(2000));
    }

    #[test]
    fn deserialize_rejects_unknown_fields() {
        let value = json!({
            "sql": "SELECT 1",
            "params": [],
            "unknown": true
        });

        let err = serde_json::from_value::<SqlQueryRequest>(value).unwrap_err();
        assert!(err.to_string().contains("unknown field"));
    }

    #[test]
    fn clamping_is_downward_only() {
        let req = SqlQueryRequest {
            sql: "SELECT 1".to_owned(),
            params: vec![],
            max_rows: Some(9999),
            timeout_ms: Some(25_000),
        };

        assert_eq!(req.effective_max_rows(100), 100);
        assert_eq!(req.effective_timeout_ms(2000), 2000);

        let req = SqlQueryRequest {
            max_rows: Some(25),
            timeout_ms: Some(500),
            ..req
        };

        assert_eq!(req.effective_max_rows(100), 25);
        assert_eq!(req.effective_timeout_ms(2000), 500);
    }
}
