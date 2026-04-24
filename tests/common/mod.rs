use philharmonic_connector_common::{UnixMillis, Uuid};
use philharmonic_connector_impl_sql_mysql::{
    ConnectorCallContext, Implementation, ImplementationError, JsonValue, SqlMysql,
};
use sqlx::{MySqlPool, mysql::MySqlPoolOptions};
use std::{sync::OnceLock, time::Duration};
use testcontainers::{
    ContainerAsync, GenericImage, ImageExt, core::IntoContainerPort, runners::AsyncRunner,
};
use tokio::sync::{Mutex, MutexGuard};

pub type TestResult<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;
type ContainerHandle = ContainerAsync<GenericImage>;

pub struct Harness {
    #[allow(dead_code)]
    serial_guard: MutexGuard<'static, ()>,
    #[allow(dead_code)]
    pub container: ContainerHandle,
    #[allow(dead_code)]
    pub pool: MySqlPool,
    pub connector: SqlMysql,
    pub config: JsonValue,
    pub ctx: ConnectorCallContext,
}

static TEST_MUTEX: OnceLock<Mutex<()>> = OnceLock::new();

fn test_mutex() -> &'static Mutex<()> {
    TEST_MUTEX.get_or_init(|| Mutex::new(()))
}

fn context() -> ConnectorCallContext {
    ConnectorCallContext {
        tenant_id: Uuid::nil(),
        instance_id: Uuid::nil(),
        step_seq: 0,
        config_uuid: Uuid::nil(),
        issued_at: UnixMillis(0),
        expires_at: UnixMillis(1),
    }
}

pub async fn setup() -> TestResult<Harness> {
    let guard = test_mutex().lock().await;

    let image = GenericImage::new("mysql", "8.0")
        .with_exposed_port(3306.tcp())
        .with_env_var("MYSQL_ROOT_PASSWORD", "rootpass")
        .with_env_var("MYSQL_DATABASE", "philharmonic_test")
        .with_startup_timeout(Duration::from_secs(180));

    let container = image.start().await?;

    let host = container.get_host().await?;
    let port = container.get_host_port_ipv4(3306.tcp()).await?;
    let connection_url = format!("mysql://root:rootpass@{host}:{port}/philharmonic_test");

    let pool = connect_with_retry(&connection_url).await?;

    sqlx::query("SET time_zone = '+00:00'")
        .execute(&pool)
        .await?;

    let config = serde_json::json!({
        "connection_url": connection_url,
        "max_connections": 4,
        "default_timeout_ms": 10_000,
        "default_max_rows": 100,
    });

    Ok(Harness {
        serial_guard: guard,
        container,
        pool,
        connector: SqlMysql::new(),
        config,
        ctx: context(),
    })
}

async fn connect_with_retry(connection_url: &str) -> TestResult<MySqlPool> {
    let mut last_error = String::new();

    for _ in 0..60 {
        match MySqlPoolOptions::new()
            .max_connections(4)
            .acquire_timeout(Duration::from_secs(5))
            .connect(connection_url)
            .await
        {
            Ok(pool) => return Ok(pool),
            Err(err) => {
                last_error = err.to_string();
                tokio::time::sleep(Duration::from_millis(500)).await;
            }
        }
    }

    Err(format!("failed to connect to mysql testcontainer: {last_error}").into())
}

pub async fn execute(
    harness: &Harness,
    request: JsonValue,
) -> Result<JsonValue, ImplementationError> {
    harness
        .connector
        .execute(&harness.config, &request, &harness.ctx)
        .await
}
