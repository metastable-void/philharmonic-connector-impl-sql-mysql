//! SQL-to-JSON conversion for MySQL rows.
//!
//! `DATETIME` and `TIMESTAMP` are intentionally formatted differently:
//! MySQL `DATETIME` is zone-naive local time and is emitted without a
//! timezone suffix, while `TIMESTAMP` is UTC-normalized and emitted with
//! `Z`.

use crate::error::{Error, Result};
use base64::Engine;
use chrono::{NaiveDate, NaiveDateTime, NaiveTime};
use philharmonic_connector_impl_api::JsonValue;
use serde_json::Number;
use sqlx::{Column, Row, TypeInfo, ValueRef, mysql::MySqlRow};

#[derive(Debug, Clone, PartialEq)]
enum DecodedValue {
    Null,
    Signed(i64),
    Unsigned(u64),
    Float(f64),
    Decimal(String),
    Bool(bool),
    Text(String),
    Bytes(Vec<u8>),
    Date(NaiveDate),
    Time(NaiveTime),
    DateTime(NaiveDateTime),
    Timestamp(NaiveDateTime),
    Json(JsonValue),
}

pub(crate) fn mysql_row_to_json(row: &MySqlRow) -> Result<serde_json::Map<String, JsonValue>> {
    let mut out = serde_json::Map::with_capacity(row.columns().len());

    for (idx, column) in row.columns().iter().enumerate() {
        let name = column.name().to_owned();
        let sql_type = normalize_sql_type(column.type_info().name());
        let decoded = decode_cell(row, idx, &sql_type)?;
        let json_value = json_from_decoded(&name, &sql_type, decoded)?;
        out.insert(name, json_value);
    }

    Ok(out)
}

pub(crate) fn normalize_sql_type(raw: &str) -> String {
    raw.trim().to_ascii_lowercase()
}

fn decode_cell(row: &MySqlRow, idx: usize, sql_type: &str) -> Result<DecodedValue> {
    let raw = row.try_get_raw(idx).map_err(Error::from_sqlx)?;
    if raw.is_null() {
        return Ok(DecodedValue::Null);
    }

    if is_unsigned_integer_type(sql_type) {
        return row
            .try_get::<u64, _>(idx)
            .map(DecodedValue::Unsigned)
            .map_err(Error::from_sqlx);
    }

    if is_signed_integer_type(sql_type) {
        return row
            .try_get::<i64, _>(idx)
            .map(DecodedValue::Signed)
            .map_err(Error::from_sqlx);
    }

    if is_decimal_type(sql_type) {
        return row
            .try_get_unchecked::<String, _>(idx)
            .map(DecodedValue::Decimal)
            .map_err(Error::from_sqlx);
    }

    if is_bool_type(sql_type) {
        return row
            .try_get::<bool, _>(idx)
            .map(DecodedValue::Bool)
            .map_err(Error::from_sqlx);
    }

    if is_float_type(sql_type) {
        return row
            .try_get::<f64, _>(idx)
            .map(DecodedValue::Float)
            .map_err(Error::from_sqlx);
    }

    if is_json_type(sql_type) {
        return row
            .try_get::<sqlx::types::Json<JsonValue>, _>(idx)
            .map(|json| DecodedValue::Json(json.0))
            .map_err(Error::from_sqlx);
    }

    if is_binary_type(sql_type) {
        return row
            .try_get::<Vec<u8>, _>(idx)
            .map(DecodedValue::Bytes)
            .map_err(Error::from_sqlx);
    }

    if is_date_type(sql_type) {
        return row
            .try_get::<NaiveDate, _>(idx)
            .map(DecodedValue::Date)
            .map_err(Error::from_sqlx);
    }

    if is_time_type(sql_type) {
        return row
            .try_get::<NaiveTime, _>(idx)
            .map(DecodedValue::Time)
            .map_err(Error::from_sqlx);
    }

    if is_datetime_type(sql_type) {
        return row
            .try_get::<NaiveDateTime, _>(idx)
            .map(DecodedValue::DateTime)
            .map_err(Error::from_sqlx);
    }

    if is_timestamp_type(sql_type) {
        return row
            .try_get_unchecked::<NaiveDateTime, _>(idx)
            .map(DecodedValue::Timestamp)
            .map_err(Error::from_sqlx);
    }

    if is_text_type(sql_type) {
        return row
            .try_get::<String, _>(idx)
            .map(DecodedValue::Text)
            .map_err(Error::from_sqlx);
    }

    if let Ok(json) = row.try_get::<sqlx::types::Json<JsonValue>, _>(idx) {
        return Ok(DecodedValue::Json(json.0));
    }

    if let Ok(text) = row.try_get::<String, _>(idx) {
        return Ok(DecodedValue::Text(text));
    }

    if let Ok(bytes) = row.try_get::<Vec<u8>, _>(idx) {
        return Ok(DecodedValue::Bytes(bytes));
    }

    Err(Error::Internal(format!(
        "unsupported MySQL column type `{sql_type}` at index {idx}"
    )))
}

fn json_from_decoded(column: &str, _sql_type: &str, value: DecodedValue) -> Result<JsonValue> {
    match value {
        DecodedValue::Null => Ok(JsonValue::Null),
        DecodedValue::Signed(value) => Ok(JsonValue::Number(Number::from(value))),
        DecodedValue::Unsigned(value) => unsigned_to_json(column, value),
        DecodedValue::Float(value) => {
            let number = Number::from_f64(value).ok_or_else(|| {
                Error::Internal(format!("non-finite float for column `{column}`"))
            })?;
            Ok(JsonValue::Number(number))
        }
        DecodedValue::Decimal(value) => Ok(JsonValue::String(value)),
        DecodedValue::Bool(value) => Ok(JsonValue::Bool(value)),
        DecodedValue::Text(value) => Ok(JsonValue::String(value)),
        DecodedValue::Bytes(value) => Ok(JsonValue::String(
            base64::engine::general_purpose::STANDARD.encode(value),
        )),
        DecodedValue::Date(value) => Ok(JsonValue::String(value.format("%Y-%m-%d").to_string())),
        DecodedValue::Time(value) => Ok(JsonValue::String(format_naive_time(value))),
        DecodedValue::DateTime(value) => Ok(JsonValue::String(format_naive_datetime(value))),
        DecodedValue::Timestamp(value) => Ok(JsonValue::String(format!(
            "{}Z",
            format_naive_datetime(value)
        ))),
        DecodedValue::Json(value) => Ok(value),
    }
}

fn format_naive_datetime(value: NaiveDateTime) -> String {
    value.format("%Y-%m-%dT%H:%M:%S%.f").to_string()
}

fn format_naive_time(value: NaiveTime) -> String {
    value.format("%H:%M:%S%.f").to_string()
}

fn unsigned_to_json(column: &str, value: u64) -> Result<JsonValue> {
    if value > i64::MAX as u64 {
        return Err(Error::IntegerOverflow {
            column: column.to_owned(),
            value: value.to_string(),
        });
    }

    Ok(JsonValue::Number(Number::from(value as i64)))
}

fn is_unsigned_integer_type(sql_type: &str) -> bool {
    sql_type.contains("unsigned")
        && (sql_type.contains("tinyint")
            || sql_type.contains("smallint")
            || sql_type.contains("mediumint")
            || sql_type == "int unsigned"
            || sql_type == "integer unsigned"
            || sql_type.contains("bigint"))
}

fn is_signed_integer_type(sql_type: &str) -> bool {
    matches!(
        sql_type,
        "tinyint" | "smallint" | "mediumint" | "int" | "integer" | "bigint"
    )
}

fn is_decimal_type(sql_type: &str) -> bool {
    matches!(sql_type, "decimal" | "numeric" | "newdecimal")
}

fn is_bool_type(sql_type: &str) -> bool {
    matches!(sql_type, "bool" | "boolean")
}

fn is_float_type(sql_type: &str) -> bool {
    matches!(sql_type, "float" | "double" | "real")
}

fn is_text_type(sql_type: &str) -> bool {
    matches!(
        sql_type,
        "char" | "varchar" | "text" | "tinytext" | "mediumtext" | "longtext" | "enum" | "set"
    )
}

fn is_binary_type(sql_type: &str) -> bool {
    matches!(
        sql_type,
        "binary" | "varbinary" | "blob" | "tinyblob" | "mediumblob" | "longblob"
    )
}

fn is_date_type(sql_type: &str) -> bool {
    sql_type == "date"
}

fn is_time_type(sql_type: &str) -> bool {
    sql_type == "time"
}

fn is_datetime_type(sql_type: &str) -> bool {
    sql_type == "datetime"
}

fn is_timestamp_type(sql_type: &str) -> bool {
    sql_type == "timestamp"
}

fn is_json_type(sql_type: &str) -> bool {
    sql_type == "json"
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::{NaiveDate, NaiveDateTime, NaiveTime};
    use serde_json::json;

    #[test]
    fn unsigned_boundary_at_i64_max_is_number() {
        let value = unsigned_to_json("u", i64::MAX as u64).unwrap();
        assert_eq!(value, json!(i64::MAX));
    }

    #[test]
    fn unsigned_above_i64_max_returns_overflow_error() {
        let err = unsigned_to_json("u", i64::MAX as u64 + 1).unwrap_err();
        assert!(matches!(err, Error::IntegerOverflow { .. }));
    }

    #[test]
    fn decimal_maps_to_string() {
        let value = json_from_decoded(
            "amount",
            "decimal",
            DecodedValue::Decimal("123.4500".to_owned()),
        )
        .unwrap();
        assert_eq!(value, json!("123.4500"));
    }

    #[test]
    fn datetime_and_timestamp_zone_semantics_differ() {
        let dt = NaiveDateTime::parse_from_str("2026-04-24 12:34:56", "%Y-%m-%d %H:%M:%S").unwrap();
        let datetime = json_from_decoded("dt", "datetime", DecodedValue::DateTime(dt)).unwrap();
        let timestamp = json_from_decoded("ts", "timestamp", DecodedValue::Timestamp(dt)).unwrap();

        assert_eq!(datetime, json!("2026-04-24T12:34:56"));
        assert_eq!(timestamp, json!("2026-04-24T12:34:56Z"));
    }

    #[test]
    fn no_mysql_array_type_and_null_maps_to_json_null() {
        let null_value = json_from_decoded("nullable", "varchar", DecodedValue::Null).unwrap();
        assert_eq!(null_value, JsonValue::Null);
    }

    #[test]
    fn scalar_mappings_cover_core_types() {
        let signed = json_from_decoded("i", "bigint", DecodedValue::Signed(-7)).unwrap();
        let float = json_from_decoded("f", "double", DecodedValue::Float(3.25)).unwrap();
        let boolean = json_from_decoded("b", "boolean", DecodedValue::Bool(true)).unwrap();
        let text =
            json_from_decoded("t", "varchar", DecodedValue::Text("hello".to_owned())).unwrap();
        let bytes =
            json_from_decoded("bytes", "blob", DecodedValue::Bytes(b"hello".to_vec())).unwrap();
        let date = json_from_decoded(
            "d",
            "date",
            DecodedValue::Date(NaiveDate::from_ymd_opt(2026, 4, 24).unwrap()),
        )
        .unwrap();
        let time = json_from_decoded(
            "tm",
            "time",
            DecodedValue::Time(NaiveTime::from_hms_opt(12, 34, 56).unwrap()),
        )
        .unwrap();
        let json_value =
            json_from_decoded("j", "json", DecodedValue::Json(json!({"k": "v"}))).unwrap();

        assert_eq!(signed, json!(-7));
        assert_eq!(float, json!(3.25));
        assert_eq!(boolean, json!(true));
        assert_eq!(text, json!("hello"));
        assert_eq!(bytes, json!("aGVsbG8="));
        assert_eq!(date, json!("2026-04-24"));
        assert_eq!(time, json!("12:34:56"));
        assert_eq!(json_value, json!({"k": "v"}));
    }
}
