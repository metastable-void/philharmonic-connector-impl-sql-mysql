use dockerlet::{Container, GenericImage, IntoContainerPort, WaitFor};
use philharmonic_connector_common::{UnixMillis, Uuid};
use philharmonic_connector_impl_sql_mysql::{
    ConnectorCallContext, Implementation, ImplementationError, JsonValue, SqlMysql,
};
use sqlx::{MySqlPool, mysql::MySqlPoolOptions};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;
use tokio::sync::OnceCell;

pub type TestResult<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;

/// Warm shared MySQL container; same pattern as
/// philharmonic-store-sqlx-mysql/tests/integration.rs (D23 round
/// 01 follow-up). Per-test isolation by unique database name.
static SHARED_MYSQL: OnceCell<SharedMysql> = OnceCell::const_new();

struct SharedMysql {
    _container: Container,
    /// `mysql://root:rootpass@{host}:{port}` (no path component).
    base_url: String,
}

async fn shared_mysql() -> &'static SharedMysql {
    SHARED_MYSQL
        .get_or_init(|| async {
            let container = GenericImage::new("mysql", "8.0")
                .with_exposed_port(3306.tcp())
                .with_env_var("MYSQL_ROOT_PASSWORD", "rootpass")
                .with_wait_for(WaitFor::message_on_stderr("ready for connections"))
                .with_startup_timeout(Duration::from_secs(180))
                .start()
                .await
                .expect("start shared MySQL container");
            let host = container.get_host().await.expect("container host");
            let port = container
                .get_host_port_ipv4(3306.tcp())
                .await
                .expect("container port");
            SharedMysql {
                _container: container,
                base_url: format!("mysql://root:rootpass@{host}:{port}"),
            }
        })
        .await
}

fn unique_db_name() -> String {
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let n = COUNTER.fetch_add(1, Ordering::Relaxed);
    format!("dl_t_{}_{n}", std::process::id())
}

pub struct Harness {
    db_name: String,
    #[allow(dead_code)]
    pub pool: MySqlPool,
    pub connector: SqlMysql,
    pub config: JsonValue,
    pub ctx: ConnectorCallContext,
}

impl Drop for Harness {
    fn drop(&mut self) {
        let db_name = std::mem::take(&mut self.db_name);
        let base_url = SHARED_MYSQL
            .get()
            .map(|s| s.base_url.clone())
            .unwrap_or_default();
        std::thread::spawn(move || {
            let runtime = match tokio::runtime::Builder::new_current_thread()
                .enable_io()
                .enable_time()
                .build()
            {
                Ok(rt) => rt,
                Err(_) => return,
            };
            runtime.block_on(async move {
                if let Ok(pool) = MySqlPool::connect(&format!("{base_url}/mysql")).await {
                    let _ = sqlx::query(&format!("DROP DATABASE IF EXISTS `{db_name}`"))
                        .execute(&pool)
                        .await;
                }
            });
        });
    }
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
    let shared = shared_mysql().await;
    let db_name = unique_db_name();

    let admin = connect_with_retry(&format!("{}/mysql", shared.base_url)).await?;
    sqlx::query(&format!("CREATE DATABASE `{db_name}`"))
        .execute(&admin)
        .await?;
    drop(admin);

    let connection_url = format!("{}/{db_name}", shared.base_url);
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
        db_name,
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
