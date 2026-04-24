//! Configuration model for `sql_mysql`.

use crate::error::{Error, Result};
use serde::Deserialize;
use sqlx::{MySqlPool, mysql::MySqlPoolOptions};
use std::time::Duration;

const DEFAULT_MAX_CONNECTIONS: u32 = 10;
const DEFAULT_TIMEOUT_MS: u64 = 30_000;
const DEFAULT_MAX_ROWS: usize = 10_000;

/// Top-level connector config for `sql_mysql`.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SqlMysqlConfig {
    /// MySQL-family SQLx connection URL (`mysql://` or `mariadb://`).
    pub connection_url: String,
    /// Maximum pool size for this tenant config.
    #[serde(default = "default_max_connections")]
    pub max_connections: u32,
    /// Default query timeout cap in milliseconds.
    #[serde(default = "default_timeout_ms")]
    pub default_timeout_ms: u64,
    /// Default row-return cap for result sets.
    #[serde(default = "default_max_rows")]
    pub default_max_rows: usize,
}

fn default_max_connections() -> u32 {
    DEFAULT_MAX_CONNECTIONS
}

fn default_timeout_ms() -> u64 {
    DEFAULT_TIMEOUT_MS
}

fn default_max_rows() -> usize {
    DEFAULT_MAX_ROWS
}

/// Runtime-ready configuration with a prepared SQLx pool.
#[derive(Clone, Debug)]
pub struct PreparedConfig {
    /// MySQL connection pool used for query execution.
    pub pool: MySqlPool,
    /// Effective request timeout cap.
    pub default_timeout_ms: u64,
    /// Effective max row cap.
    pub default_max_rows: usize,
}

impl SqlMysqlConfig {
    /// Validates config and builds a lazily-connecting pool.
    pub(crate) fn prepare(&self) -> Result<PreparedConfig> {
        if self.connection_url.trim().is_empty() {
            return Err(Error::InvalidConfig(
                "connection_url must not be empty".to_owned(),
            ));
        }

        validate_connection_url_scheme(&self.connection_url)?;

        if self.max_connections == 0 {
            return Err(Error::InvalidConfig(
                "max_connections must be at least 1".to_owned(),
            ));
        }

        if self.default_timeout_ms == 0 {
            return Err(Error::InvalidConfig(
                "default_timeout_ms must be at least 1".to_owned(),
            ));
        }

        if self.default_max_rows == 0 {
            return Err(Error::InvalidConfig(
                "default_max_rows must be at least 1".to_owned(),
            ));
        }

        let pool = MySqlPoolOptions::new()
            .max_connections(self.max_connections)
            .acquire_timeout(Duration::from_secs(1))
            .connect_lazy(&self.connection_url)
            .map_err(|e| {
                Error::InvalidConfig(format!("invalid MySQL connection_url for sqlx: {e}"))
            })?;

        Ok(PreparedConfig {
            pool,
            default_timeout_ms: self.default_timeout_ms,
            default_max_rows: self.default_max_rows,
        })
    }
}

fn validate_connection_url_scheme(connection_url: &str) -> Result<()> {
    let Some((scheme, _rest)) = connection_url.split_once("://") else {
        return Err(Error::InvalidConfig(
            "connection_url must start with mysql:// or mariadb://".to_owned(),
        ));
    };

    let scheme = scheme.to_ascii_lowercase();
    if scheme != "mysql" && scheme != "mariadb" {
        return Err(Error::InvalidConfig(
            "connection_url must start with mysql:// or mariadb://".to_owned(),
        ));
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn deserialize_rejects_unknown_fields() {
        let value = json!({
            "connection_url": "mysql://root@127.0.0.1:3306/test",
            "max_connections": 4,
            "default_timeout_ms": 1000,
            "default_max_rows": 100,
            "extra": true
        });

        let err = serde_json::from_value::<SqlMysqlConfig>(value).unwrap_err();
        assert!(err.to_string().contains("unknown field"));
    }

    #[test]
    fn defaults_apply_when_optional_fields_missing() {
        let value = json!({
            "connection_url": "mysql://root@127.0.0.1:3306/test"
        });

        let cfg = serde_json::from_value::<SqlMysqlConfig>(value).unwrap();
        assert_eq!(cfg.max_connections, DEFAULT_MAX_CONNECTIONS);
        assert_eq!(cfg.default_timeout_ms, DEFAULT_TIMEOUT_MS);
        assert_eq!(cfg.default_max_rows, DEFAULT_MAX_ROWS);
    }

    #[tokio::test]
    async fn accepts_mysql_and_mariadb_schemes() {
        let mysql = SqlMysqlConfig {
            connection_url: "mysql://root@127.0.0.1:3306/test".to_owned(),
            max_connections: 4,
            default_timeout_ms: 1000,
            default_max_rows: 100,
        };
        mysql.prepare().unwrap();

        let mariadb = SqlMysqlConfig {
            connection_url: "mariadb://root@127.0.0.1:3306/test".to_owned(),
            max_connections: 4,
            default_timeout_ms: 1000,
            default_max_rows: 100,
        };
        mariadb.prepare().unwrap();
    }

    #[test]
    fn rejects_non_mysql_scheme() {
        let cfg = SqlMysqlConfig {
            connection_url: "postgres://u:p@127.0.0.1/db".to_owned(),
            max_connections: 4,
            default_timeout_ms: 1000,
            default_max_rows: 100,
        };

        let err = cfg.prepare().unwrap_err();
        assert!(matches!(err, Error::InvalidConfig(_)));
        assert!(err.to_string().contains("mysql:// or mariadb://"));
    }
}
