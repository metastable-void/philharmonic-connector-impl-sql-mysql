mod common;

use common::{TestResult, execute, setup};
use philharmonic_connector_impl_sql_mysql::{
    ConnectorCallContext, Implementation, ImplementationError, SqlMysql,
};
use serde_json::json;

/// Requires Docker with `mysql:8.0`; ignored by default so non-Docker CI stays green.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires Docker (mysql:8.0 testcontainer)"]
#[serial_test::file_serial(docker)]
async fn db_backed_error_cases_map_correctly() -> TestResult<()> {
    let harness = setup().await?;

    let syntax_err = execute(
        &harness,
        json!({
            "sql": "SELEC 1",
            "params": []
        }),
    )
    .await
    .unwrap_err();
    assert!(matches!(
        syntax_err,
        ImplementationError::InvalidRequest { .. }
    ));

    sqlx::query("CREATE TABLE unique_values (v VARCHAR(64) PRIMARY KEY)")
        .execute(&harness.pool)
        .await?;

    execute(
        &harness,
        json!({
            "sql": "INSERT INTO unique_values (v) VALUES (?)",
            "params": ["dup"]
        }),
    )
    .await
    .unwrap();

    let constraint_err = execute(
        &harness,
        json!({
            "sql": "INSERT INTO unique_values (v) VALUES (?)",
            "params": ["dup"]
        }),
    )
    .await
    .unwrap_err();
    let ImplementationError::UpstreamError {
        status: constraint_status,
        body: constraint_body,
    } = constraint_err
    else {
        panic!("expected upstream error for constraint violation");
    };
    assert_eq!(constraint_status, 500);
    assert!(!constraint_body.is_empty());

    let timeout_err = execute(
        &harness,
        json!({
            "sql": "SELECT SLEEP(?)",
            "params": [2],
            "timeout_ms": 50
        }),
    )
    .await
    .unwrap_err();
    assert_eq!(timeout_err, ImplementationError::UpstreamTimeout);

    let overflow_err = execute(
        &harness,
        json!({
            "sql": "SELECT CAST(18446744073709551615 AS UNSIGNED) AS too_big",
            "params": []
        }),
    )
    .await
    .unwrap_err();
    let ImplementationError::UpstreamError {
        status: overflow_status,
        body: overflow_body,
    } = overflow_err
    else {
        panic!("expected upstream error for unsigned overflow");
    };
    assert_eq!(overflow_status, 500);
    assert!(overflow_body.contains("integer overflow"));

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn parameter_mismatch_maps_to_invalid_request() {
    let connector = SqlMysql::new();
    let config = json!({
        "connection_url": "mysql://root@127.0.0.1:65535/not_reachable",
        "max_connections": 1,
        "default_timeout_ms": 5_000,
        "default_max_rows": 10
    });
    let request = json!({
        "sql": "SELECT ? + ?",
        "params": [1]
    });
    let ctx = ConnectorCallContext {
        tenant_id: philharmonic_connector_common::Uuid::nil(),
        instance_id: philharmonic_connector_common::Uuid::nil(),
        step_seq: 0,
        config_uuid: philharmonic_connector_common::Uuid::nil(),
        issued_at: philharmonic_connector_common::UnixMillis(0),
        expires_at: philharmonic_connector_common::UnixMillis(1),
    };

    let err = connector
        .execute(&config, &request, &ctx)
        .await
        .unwrap_err();
    let ImplementationError::InvalidRequest { detail } = err else {
        panic!("expected invalid request");
    };
    assert!(detail.contains("parameter count mismatch"));
}

#[tokio::test(flavor = "multi_thread")]
async fn connection_refused_maps_to_upstream_unreachable() {
    let connector = SqlMysql::new();
    let config = json!({
        "connection_url": "mysql://root@127.0.0.1:65535/not_reachable",
        "max_connections": 1,
        "default_timeout_ms": 5_000,
        "default_max_rows": 10
    });
    let request = json!({
        "sql": "SELECT 1",
        "params": []
    });
    let ctx = ConnectorCallContext {
        tenant_id: philharmonic_connector_common::Uuid::nil(),
        instance_id: philharmonic_connector_common::Uuid::nil(),
        step_seq: 0,
        config_uuid: philharmonic_connector_common::Uuid::nil(),
        issued_at: philharmonic_connector_common::UnixMillis(0),
        expires_at: philharmonic_connector_common::UnixMillis(1),
    };

    let err = connector
        .execute(&config, &request, &ctx)
        .await
        .unwrap_err();
    assert!(matches!(
        err,
        ImplementationError::UpstreamUnreachable { .. }
    ));
}
