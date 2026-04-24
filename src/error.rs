//! Internal error model for `sql_mysql`.

use philharmonic_connector_impl_api::ImplementationError;

pub(crate) type Result<T> = std::result::Result<T, Error>;

/// Internal failure variants used before wire mapping.
#[derive(Debug, thiserror::Error, Clone, PartialEq)]
pub(crate) enum Error {
    #[error("{0}")]
    InvalidConfig(String),

    #[error("{0}")]
    InvalidRequest(String),

    #[error("{0}")]
    InvalidSql(String),

    #[error("parameter count mismatch: expected {expected}, got {actual}")]
    ParameterMismatch { expected: usize, actual: usize },

    #[error("upstream database error: {0}")]
    UpstreamDb(String),

    #[error("upstream timeout")]
    UpstreamTimeout,

    #[error("{0}")]
    UpstreamUnreachable(String),

    #[error("integer overflow for column `{column}`: unsigned value {value} exceeds i64::MAX")]
    IntegerOverflow { column: String, value: String },

    #[error("{0}")]
    Internal(String),
}

impl Error {
    /// Classifies SQLx errors into connector wire-level categories.
    pub(crate) fn from_sqlx(err: sqlx::Error) -> Self {
        match err {
            sqlx::Error::Database(db_err) => classify_database_error(&*db_err),
            sqlx::Error::Io(io_err) => Error::UpstreamUnreachable(io_err.to_string()),
            sqlx::Error::PoolTimedOut | sqlx::Error::PoolClosed => {
                Error::UpstreamUnreachable("database connection pool unavailable".to_owned())
            }
            sqlx::Error::Protocol(detail) => Error::UpstreamUnreachable(detail),
            sqlx::Error::Tls(detail) => Error::UpstreamUnreachable(detail.to_string()),
            sqlx::Error::RowNotFound
            | sqlx::Error::TypeNotFound { .. }
            | sqlx::Error::ColumnIndexOutOfBounds { .. }
            | sqlx::Error::ColumnNotFound(_)
            | sqlx::Error::ColumnDecode { .. }
            | sqlx::Error::Decode(_) => Error::Internal(err.to_string()),
            _ => Error::Internal(err.to_string()),
        }
    }
}

fn classify_database_error(db_err: &dyn sqlx::error::DatabaseError) -> Error {
    if is_invalid_sql_error(db_err) {
        return Error::InvalidSql(db_err.message().to_owned());
    }
    Error::UpstreamDb(db_err.message().to_owned())
}

fn is_invalid_sql_error(db_err: &dyn sqlx::error::DatabaseError) -> bool {
    if let Some(code) = db_err.code() {
        let code = code.as_ref();
        if code.starts_with("42") || matches!(code, "1064" | "1054" | "1146" | "1149") {
            return true;
        }
    }

    if let Some(mysql_err) = db_err.try_downcast_ref::<sqlx::mysql::MySqlDatabaseError>()
        && matches!(mysql_err.number(), 1064 | 1054 | 1146 | 1149)
    {
        return true;
    }

    false
}

impl From<Error> for ImplementationError {
    fn from(value: Error) -> Self {
        match value {
            Error::InvalidConfig(detail) => ImplementationError::InvalidConfig { detail },
            Error::InvalidRequest(detail) => ImplementationError::InvalidRequest { detail },
            Error::InvalidSql(detail) => ImplementationError::InvalidRequest { detail },
            Error::ParameterMismatch { expected, actual } => ImplementationError::InvalidRequest {
                detail: format!("parameter count mismatch: expected {expected}, got {actual}"),
            },
            Error::UpstreamDb(message) => ImplementationError::UpstreamError {
                status: 500,
                body: message,
            },
            Error::UpstreamTimeout => ImplementationError::UpstreamTimeout,
            Error::UpstreamUnreachable(detail) => {
                ImplementationError::UpstreamUnreachable { detail }
            }
            Error::IntegerOverflow { column, value } => ImplementationError::UpstreamError {
                status: 500,
                body: format!(
                    "integer overflow for column `{column}`: unsigned value {value} exceeds i64::MAX"
                ),
            },
            Error::Internal(detail) => ImplementationError::Internal { detail },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_internal_variant_maps_to_wire() {
        assert_eq!(
            ImplementationError::from(Error::InvalidConfig("bad cfg".to_owned())),
            ImplementationError::InvalidConfig {
                detail: "bad cfg".to_owned()
            }
        );

        assert_eq!(
            ImplementationError::from(Error::InvalidRequest("bad req".to_owned())),
            ImplementationError::InvalidRequest {
                detail: "bad req".to_owned()
            }
        );

        assert_eq!(
            ImplementationError::from(Error::InvalidSql("syntax".to_owned())),
            ImplementationError::InvalidRequest {
                detail: "syntax".to_owned()
            }
        );

        assert_eq!(
            ImplementationError::from(Error::ParameterMismatch {
                expected: 2,
                actual: 1,
            }),
            ImplementationError::InvalidRequest {
                detail: "parameter count mismatch: expected 2, got 1".to_owned(),
            }
        );

        assert_eq!(
            ImplementationError::from(Error::UpstreamDb("duplicate".to_owned())),
            ImplementationError::UpstreamError {
                status: 500,
                body: "duplicate".to_owned()
            }
        );

        assert_eq!(
            ImplementationError::from(Error::UpstreamTimeout),
            ImplementationError::UpstreamTimeout
        );

        assert_eq!(
            ImplementationError::from(Error::UpstreamUnreachable("io".to_owned())),
            ImplementationError::UpstreamUnreachable {
                detail: "io".to_owned()
            }
        );

        let overflow = ImplementationError::from(Error::IntegerOverflow {
            column: "id".to_owned(),
            value: "18446744073709551615".to_owned(),
        });
        let ImplementationError::UpstreamError { status, body } = overflow else {
            panic!("expected upstream error mapping");
        };
        assert_eq!(status, 500);
        assert!(body.contains("integer overflow for column `id`"));

        assert_eq!(
            ImplementationError::from(Error::Internal("boom".to_owned())),
            ImplementationError::Internal {
                detail: "boom".to_owned()
            }
        );
    }
}
