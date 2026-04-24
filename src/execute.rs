//! Query execution core for `sql_mysql`.

use crate::{
    config::PreparedConfig,
    error::{Error, Result},
    request::SqlQueryRequest,
    response::{Column, SqlQueryResponse},
    types,
};
use futures_util::StreamExt;
use philharmonic_connector_impl_api::JsonValue;
use sqlx::{Column as SqlxColumn, Executor, MySql, TypeInfo, mysql::MySqlArguments, query::Query};
use std::time::Duration;

pub(crate) async fn execute_sql_query(
    config: &PreparedConfig,
    request: &SqlQueryRequest,
) -> Result<SqlQueryResponse> {
    if request.sql.trim().is_empty() {
        return Err(Error::InvalidRequest("sql must not be empty".to_owned()));
    }

    let expected = count_mysql_placeholders(&request.sql);
    let actual = request.params.len();
    if expected != actual {
        return Err(Error::ParameterMismatch { expected, actual });
    }

    let timeout_ms = request.effective_timeout_ms(config.default_timeout_ms);
    let max_rows = request.effective_max_rows(config.default_max_rows);

    let run = async {
        let describe = config
            .pool
            .describe(&request.sql)
            .await
            .map_err(Error::from_sqlx)?;

        let columns = describe
            .columns()
            .iter()
            .map(|column| Column {
                name: column.name().to_owned(),
                sql_type: types::normalize_sql_type(column.type_info().name()),
            })
            .collect::<Vec<_>>();

        if columns.is_empty() {
            let result = bind_params(sqlx::query(&request.sql), &request.params)?
                .execute(&config.pool)
                .await
                .map_err(Error::from_sqlx)?;

            return Ok(SqlQueryResponse {
                rows: Vec::new(),
                row_count: result.rows_affected(),
                columns,
                truncated: false,
            });
        }

        let mut rows = Vec::new();
        let mut truncated = false;
        let take_limit = max_rows
            .checked_add(1)
            .ok_or_else(|| Error::Internal("max_rows overflow".to_owned()))?;

        let mut stream = bind_params(sqlx::query(&request.sql), &request.params)?
            .fetch(&config.pool)
            .take(take_limit);

        while let Some(next_row) = stream.next().await {
            let row = next_row.map_err(Error::from_sqlx)?;
            if rows.len() == max_rows {
                truncated = true;
                break;
            }
            rows.push(types::mysql_row_to_json(&row)?);
        }

        let row_count = u64::try_from(rows.len())
            .map_err(|_| Error::Internal("row count conversion overflow".to_owned()))?;

        Ok(SqlQueryResponse {
            rows,
            row_count,
            columns,
            truncated,
        })
    };

    match tokio::time::timeout(Duration::from_millis(timeout_ms), run).await {
        Ok(result) => result,
        Err(_) => Err(Error::UpstreamTimeout),
    }
}

fn bind_params<'q>(
    mut query: Query<'q, MySql, MySqlArguments>,
    params: &[JsonValue],
) -> Result<Query<'q, MySql, MySqlArguments>> {
    for value in params {
        query = match value {
            JsonValue::Null => query.bind(Option::<String>::None),
            JsonValue::Bool(flag) => query.bind(*flag),
            JsonValue::Number(number) => {
                if let Some(value) = number.as_i64() {
                    query.bind(value)
                } else if let Some(value) = number.as_u64() {
                    query.bind(value)
                } else if let Some(value) = number.as_f64() {
                    query.bind(value)
                } else {
                    return Err(Error::InvalidRequest(format!(
                        "unsupported JSON numeric parameter `{number}`"
                    )));
                }
            }
            JsonValue::String(text) => query.bind(text.clone()),
            JsonValue::Array(_) => {
                return Err(Error::InvalidRequest(
                    "mysql parameters do not support top-level JSON arrays".to_owned(),
                ));
            }
            JsonValue::Object(_) => query.bind(sqlx::types::Json(value.clone())),
        };
    }

    Ok(query)
}

fn count_mysql_placeholders(sql: &str) -> usize {
    #[derive(Clone, Copy, Debug, PartialEq, Eq)]
    enum State {
        Normal,
        SingleQuote,
        DoubleQuote,
        Backtick,
        LineComment,
        BlockComment,
    }

    let bytes = sql.as_bytes();
    let mut idx = 0usize;
    let mut count = 0usize;
    let mut state = State::Normal;

    while idx < bytes.len() {
        let byte = bytes[idx];

        match state {
            State::Normal => {
                if byte == b'?' {
                    count += 1;
                    idx += 1;
                    continue;
                }
                if byte == b'\'' {
                    state = State::SingleQuote;
                    idx += 1;
                    continue;
                }
                if byte == b'"' {
                    state = State::DoubleQuote;
                    idx += 1;
                    continue;
                }
                if byte == b'`' {
                    state = State::Backtick;
                    idx += 1;
                    continue;
                }
                if byte == b'#' {
                    state = State::LineComment;
                    idx += 1;
                    continue;
                }
                if byte == b'-' && idx + 1 < bytes.len() && bytes[idx + 1] == b'-' {
                    state = State::LineComment;
                    idx += 2;
                    continue;
                }
                if byte == b'/' && idx + 1 < bytes.len() && bytes[idx + 1] == b'*' {
                    state = State::BlockComment;
                    idx += 2;
                    continue;
                }
                idx += 1;
            }
            State::SingleQuote => {
                if byte == b'\\' && idx + 1 < bytes.len() {
                    idx += 2;
                    continue;
                }
                if byte == b'\'' {
                    if idx + 1 < bytes.len() && bytes[idx + 1] == b'\'' {
                        idx += 2;
                        continue;
                    }
                    state = State::Normal;
                }
                idx += 1;
            }
            State::DoubleQuote => {
                if byte == b'\\' && idx + 1 < bytes.len() {
                    idx += 2;
                    continue;
                }
                if byte == b'"' {
                    if idx + 1 < bytes.len() && bytes[idx + 1] == b'"' {
                        idx += 2;
                        continue;
                    }
                    state = State::Normal;
                }
                idx += 1;
            }
            State::Backtick => {
                if byte == b'`' {
                    state = State::Normal;
                }
                idx += 1;
            }
            State::LineComment => {
                if byte == b'\n' {
                    state = State::Normal;
                }
                idx += 1;
            }
            State::BlockComment => {
                if byte == b'*' && idx + 1 < bytes.len() && bytes[idx + 1] == b'/' {
                    state = State::Normal;
                    idx += 2;
                    continue;
                }
                idx += 1;
            }
        }
    }

    count
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn placeholder_count_ignores_strings_identifiers_and_comments() {
        let sql = "SELECT '?', \"?\", `?`, col FROM t -- ?\nWHERE a = ? AND b = /* ? */ ? # ?\n";
        assert_eq!(count_mysql_placeholders(sql), 2);
    }

    #[test]
    fn bind_rejects_top_level_array_param() {
        let query = sqlx::query("SELECT ?");
        let err = match bind_params(query, &[json!([1, 2, 3])]) {
            Ok(_) => panic!("array params must be rejected for mysql"),
            Err(err) => err,
        };
        assert!(matches!(err, Error::InvalidRequest(_)));
        assert!(
            err.to_string()
                .contains("do not support top-level JSON arrays")
        );
    }
}
