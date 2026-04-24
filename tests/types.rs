mod common;

use common::{TestResult, execute, setup};
use serde_json::json;

/// Requires Docker with `mysql:8.0`; ignored by default so non-Docker CI stays green.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires Docker (mysql:8.0 testcontainer)"]
#[serial_test::file_serial(docker)]
async fn sql_to_json_type_mapping_round_trip() -> TestResult<()> {
    let harness = setup().await?;

    sqlx::query(
        "CREATE TABLE type_samples (
            signed_i BIGINT NOT NULL,
            unsigned_i BIGINT UNSIGNED NOT NULL,
            float_v DOUBLE NOT NULL,
            decimal_v DECIMAL(12,4) NOT NULL,
            text_v VARCHAR(64) NOT NULL,
            blob_v BLOB NOT NULL,
            date_v DATE NOT NULL,
            time_v TIME NOT NULL,
            datetime_v DATETIME NOT NULL,
            timestamp_v TIMESTAMP NOT NULL,
            json_v JSON NOT NULL,
            null_v VARCHAR(16) NULL
        )",
    )
    .execute(&harness.pool)
    .await?;

    sqlx::query(
        "INSERT INTO type_samples
         (signed_i, unsigned_i, float_v, decimal_v, text_v, blob_v, date_v, time_v, datetime_v, timestamp_v, json_v, null_v)
         VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, JSON_OBJECT('k', 'v'), ?)",
    )
    .bind(-7_i64)
    .bind(42_u64)
    .bind(3.25_f64)
    .bind("123.4500")
    .bind("hello")
    .bind(Vec::from("hello".as_bytes()))
    .bind("2026-04-24")
    .bind("12:34:56")
    .bind("2026-04-24 12:34:56")
    .bind("2026-04-24 12:34:56")
    .bind(Option::<String>::None)
    .execute(&harness.pool)
    .await?;

    let response = execute(
        &harness,
        json!({
            "sql": "SELECT signed_i, unsigned_i, float_v, decimal_v, text_v, blob_v, date_v, time_v, datetime_v, timestamp_v, json_v, null_v FROM type_samples",
            "params": []
        }),
    )
    .await?;

    let row = &response["rows"][0];
    assert_eq!(row["signed_i"], json!(-7));
    assert_eq!(row["unsigned_i"], json!(42));
    assert_eq!(row["float_v"], json!(3.25));
    assert_eq!(row["decimal_v"], json!("123.4500"));
    assert_eq!(row["text_v"], json!("hello"));
    assert_eq!(row["blob_v"], json!("aGVsbG8="));
    assert_eq!(row["date_v"], json!("2026-04-24"));
    assert_eq!(row["time_v"], json!("12:34:56"));
    assert_eq!(row["datetime_v"], json!("2026-04-24T12:34:56"));
    assert_eq!(row["timestamp_v"], json!("2026-04-24T12:34:56Z"));
    assert_eq!(row["json_v"], json!({"k": "v"}));
    assert_eq!(row["null_v"], json!(null));

    Ok(())
}

#[test]
fn test_file_present() {
    assert_eq!(2 + 2, 4);
}
