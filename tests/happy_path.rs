mod common;

use common::{TestResult, execute, setup};
use philharmonic_connector_impl_sql_mysql::{Implementation, ImplementationError};
use serde_json::json;

/// Requires Docker with `mysql:8.0`; ignored by default so non-Docker CI stays green.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires Docker (mysql:8.0 testcontainer)"]
#[serial_test::file_serial(docker)]
async fn select_insert_update_delete_empty_and_truncation() -> TestResult<()> {
    let harness = setup().await?;

    sqlx::query(
        "CREATE TABLE users (
            id BIGINT PRIMARY KEY AUTO_INCREMENT,
            tenant VARCHAR(64) NOT NULL,
            score INT NOT NULL,
            active BOOLEAN NOT NULL,
            created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
        )",
    )
    .execute(&harness.pool)
    .await?;

    let insert_request = json!({
        "sql": "INSERT INTO users (tenant, score, active) VALUES (?, ?, ?), (?, ?, ?), (?, ?, ?)",
        "params": ["t_a", 10, true, "t_b", 20, false, "t_c", 30, true]
    });
    let inserted = execute(&harness, insert_request).await?;
    assert_eq!(inserted["row_count"], json!(3));
    assert_eq!(inserted["rows"], json!([]));
    assert_eq!(inserted["columns"], json!([]));
    assert_eq!(inserted["truncated"], json!(false));

    let select_request = json!({
        "sql": "SELECT tenant, score FROM users ORDER BY score ASC",
        "params": []
    });
    let selected = execute(&harness, select_request).await?;
    assert_eq!(selected["row_count"], json!(3));
    assert_eq!(selected["truncated"], json!(false));
    assert_eq!(selected["rows"][0]["tenant"], json!("t_a"));
    assert_eq!(selected["rows"][1]["score"], json!(20));

    let update_request = json!({
        "sql": "UPDATE users SET score = ? WHERE tenant = ?",
        "params": [99, "t_b"]
    });
    let updated = execute(&harness, update_request).await?;
    assert_eq!(updated["row_count"], json!(1));

    let delete_request = json!({
        "sql": "DELETE FROM users WHERE tenant = ?",
        "params": ["t_c"]
    });
    let deleted = execute(&harness, delete_request).await?;
    assert_eq!(deleted["row_count"], json!(1));

    let empty_request = json!({
        "sql": "SELECT tenant FROM users WHERE tenant = ?",
        "params": ["does-not-exist"]
    });
    let empty = execute(&harness, empty_request).await?;
    assert_eq!(empty["rows"], json!([]));
    assert_eq!(empty["row_count"], json!(0));
    assert_eq!(
        empty["columns"],
        json!([
            {"name": "tenant", "sql_type": "varchar"}
        ])
    );

    let truncated_request = json!({
        "sql": "SELECT tenant FROM users ORDER BY tenant ASC",
        "params": [],
        "max_rows": 1
    });
    let truncated = execute(&harness, truncated_request).await?;
    assert_eq!(truncated["rows"].as_array().unwrap().len(), 1);
    assert_eq!(truncated["row_count"], json!(1));
    assert_eq!(truncated["truncated"], json!(true));

    Ok(())
}

/// Requires Docker with `mysql:8.0`; ignored by default so non-Docker CI stays green.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires Docker (mysql:8.0 testcontainer)"]
#[serial_test::file_serial(docker)]
async fn request_clamp_respects_config_caps() -> TestResult<()> {
    let harness = setup().await?;

    sqlx::query("CREATE TABLE clamp_check (id BIGINT PRIMARY KEY AUTO_INCREMENT)")
        .execute(&harness.pool)
        .await?;
    for _ in 0..5 {
        sqlx::query("INSERT INTO clamp_check () VALUES ()")
            .execute(&harness.pool)
            .await?;
    }

    let response = harness
        .connector
        .execute(
            &serde_json::json!({
                "connection_url": harness.config["connection_url"],
                "max_connections": 4,
                "default_timeout_ms": 100,
                "default_max_rows": 2
            }),
            &json!({
                "sql": "SELECT id FROM clamp_check ORDER BY id ASC",
                "params": [],
                "timeout_ms": 5_000,
                "max_rows": 50
            }),
            &harness.ctx,
        )
        .await;

    let response = response.map_err(|e| format!("unexpected error: {e:?}"))?;
    assert_eq!(response["rows"].as_array().unwrap().len(), 2);
    assert_eq!(response["truncated"], json!(true));

    Ok(())
}

#[test]
fn implementation_error_type_is_accessible() {
    let _ = ImplementationError::UpstreamTimeout;
}
