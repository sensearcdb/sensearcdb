//! SenseArcDB 单机版主入口（Open Core 装配层）。
//!
//! 启动序列：
//! 1. 加载配置 + 初始化日志 + 部署安全检查链（`startup::bootstrap`）
//! 2. 启动 gRPC 5201 WriteService / 5202 QueryService / 5206 SnapshotService
//! 3. 构建 REST 路由并启动 axum HTTP 服务器（5100）
//! 4. 优雅关闭（SIGTERM / Ctrl+C → 冻结 MemTable + 落盘 snapshot.bin）
//!
//! 与完整版差异：
//! - 无 5300 集群 gRPC（Cluster/Shard/RestoreAdmin）
//! - 无 5203 CollectorAdmin、无 5205 AI 推理聚合
//! - 无控制面鉴权断言（`control_plane_auth::verify_*`）
//! - gRPC 认证降级为单机 interceptor（`x-tenant-id`，永不 401）

mod grpc_tenant;
mod routes;
mod startup;

use sensearcdb_common::constants::{
    DEFAULT_GRPC_QUERY_PORT, DEFAULT_GRPC_SNAPSHOT_PORT, DEFAULT_GRPC_WRITE_PORT, DEFAULT_REST_PORT,
};

/// SenseArcDB 服务器主函数。
///
/// # 启动参数
///
/// - 配置文件路径通过 `SENSEARCDB_CONFIG` 环境变量指定，默认 `"config.toml"`。
/// - 监听地址通过 `SENSEARCDB_BIND_ADDR` 环境变量指定，默认 `"0.0.0.0:5100"`。
#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // 1. 启动子系统（配置加载 → 日志初始化 → 安全检查链 → 各子系统启动）。
    let config_path =
        std::env::var("SENSEARCDB_CONFIG").unwrap_or_else(|_| "config.toml".to_string());
    let app_state = startup::bootstrap(&config_path).await?;

    tracing::info!("SenseArcDB starting...");

    // 2. 启动 gRPC 5201 WriteService / 5202 QueryService。
    spawn_ingest_grpc_servers(&app_state);

    // 3. 启动 gRPC 5206 SnapshotService（Get/Batch/List/Subscribe）。
    spawn_snapshot_grpc_server(&app_state);

    // 4. 构建 REST 路由。
    let app = routes::build_router(&app_state);

    // 5. 启动 HTTP 服务器（§6.11 端口规划：REST 默认 5100）。
    let bind_addr = std::env::var("SENSEARCDB_BIND_ADDR")
        .unwrap_or_else(|_| format!("0.0.0.0:{}", DEFAULT_REST_PORT));
    let listener = tokio::net::TcpListener::bind(&bind_addr).await?;

    tracing::info!(bind_addr = %bind_addr, "SenseArcDB HTTP server listening");

    axum::serve(
        listener,
        app.into_make_service_with_connect_info::<std::net::SocketAddr>(),
    )
    .with_graceful_shutdown(shutdown_signal(app_state))
    .await?;

    tracing::info!("SenseArcDB shutdown complete");
    Ok(())
}

/// 启动 gRPC 5201 WriteService / 5202 QueryService（单机直连 Engine）。
///
/// 单机版无集群/分片，写入恒走 `EngineDirectWriter`；认证降级为单机 interceptor。
fn spawn_ingest_grpc_servers(app_state: &startup::AppState) {
    use tonic::transport::Server;

    use sensearcdb_ingest::grpc::query_service::GrpcQueryService;
    use sensearcdb_ingest::grpc::write_service::GrpcWriteService;
    use sensearcdb_ingest::proto::{
        query_service_server::QueryServiceServer, write_service_server::WriteServiceServer,
    };
    use sensearcdb_ingest::EngineDirectWriter;

    // 单机认证降级：读 x-tenant-id → AuthContext，永不 401。
    let auth_interceptor = grpc_tenant::build_single_node_interceptor();
    let tenant_resolver = grpc_tenant::build_single_node_tenant_resolver();

    // ─── 5201 WriteService ────────────────────────────────────────────────
    let write_writer: std::sync::Arc<dyn sensearcdb_ingest::EngineWriter> =
        std::sync::Arc::new(EngineDirectWriter::new(app_state.engine.clone()));
    // table_name → table_id 解析：注入 SchemaMirror 桥接（GAP2-ING-02）。
    let write_resolver: std::sync::Arc<dyn sensearcdb_ingest::rest::write::TableNameResolver> =
        std::sync::Arc::new(startup::SchemaTableResolver::new(
            app_state.schema_mirror.clone(),
        ));
    let write_service = WriteServiceServer::with_interceptor(
        GrpcWriteService::new(write_writer)
            .with_resolver(write_resolver)
            .with_tenant_resolver(tenant_resolver.clone()),
        auth_interceptor.clone(),
    );
    let write_addr = format!("0.0.0.0:{}", DEFAULT_GRPC_WRITE_PORT);
    tokio::spawn(async move {
        match write_addr.parse::<std::net::SocketAddr>() {
            Ok(addr) => {
                if let Err(e) = Server::builder()
                    .add_service(write_service)
                    .serve(addr)
                    .await
                {
                    tracing::error!(error = %e, "gRPC 5201 WriteService server failed");
                }
            }
            Err(e) => tracing::error!(error = %e, "invalid gRPC 5201 bind address"),
        }
    });
    tracing::info!(port = DEFAULT_GRPC_WRITE_PORT, "gRPC 5201 WriteService server started");

    // ─── 5202 QueryService ────────────────────────────────────────────────
    let query_service = QueryServiceServer::with_interceptor(
        GrpcQueryService::new(app_state.engine.clone())
            .with_tenant_resolver(tenant_resolver.clone()),
        auth_interceptor.clone(),
    );
    let query_addr = format!("0.0.0.0:{}", DEFAULT_GRPC_QUERY_PORT);
    tokio::spawn(async move {
        match query_addr.parse::<std::net::SocketAddr>() {
            Ok(addr) => {
                if let Err(e) = Server::builder()
                    .add_service(query_service)
                    .serve(addr)
                    .await
                {
                    tracing::error!(error = %e, "gRPC 5202 QueryService server failed");
                }
            }
            Err(e) => tracing::error!(error = %e, "invalid gRPC 5202 bind address"),
        }
    });
    tracing::info!(port = DEFAULT_GRPC_QUERY_PORT, "gRPC 5202 QueryService server started");
}

/// 启动 gRPC 5206 快照数据面服务（Get/Batch/List/Subscribe）。
///
/// 单机版：认证降级为单机 interceptor，列级可见性 resolver 传 `None`（全列可见）。
fn spawn_snapshot_grpc_server(app_state: &startup::AppState) {
    use tonic::transport::Server;

    use sensearcdb_snapshot::api::grpc::SnapshotGrpcService;
    use sensearcdb_snapshot::snapshot_proto::snapshot_service_server::SnapshotServiceServer;

    let auth_interceptor = grpc_tenant::build_single_node_interceptor();
    let tenant_resolver = grpc_tenant::build_single_node_tenant_resolver();

    let service = SnapshotServiceServer::with_interceptor(
        SnapshotGrpcService::new(
            app_state.snapshot_store.clone(),
            app_state.snapshot_subscriptions.clone(),
        )
        .with_tenant_resolver(tenant_resolver),
        auth_interceptor,
    );

    let addr = format!("0.0.0.0:{}", DEFAULT_GRPC_SNAPSHOT_PORT);
    tokio::spawn(async move {
        match addr.parse::<std::net::SocketAddr>() {
            Ok(addr) => {
                if let Err(e) = Server::builder().add_service(service).serve(addr).await {
                    tracing::error!(error = %e, "gRPC 5206 SnapshotService server failed");
                }
            }
            Err(e) => tracing::error!(error = %e, "invalid gRPC 5206 bind address"),
        }
    });
    tracing::info!(
        port = DEFAULT_GRPC_SNAPSHOT_PORT,
        "gRPC 5206 SnapshotService server started (Get/Batch/List/Subscribe)"
    );
}

/// 优雅关闭信号处理。
///
/// 监听 SIGINT (Ctrl+C) 与 SIGTERM，触发引擎冻结 + snapshot.bin 落盘。
async fn shutdown_signal(app_state: startup::AppState) {
    let ctrl_c = async {
        tokio::signal::ctrl_c()
            .await
            .expect("failed to install Ctrl+C handler");
        tracing::info!("received Ctrl+C, shutting down gracefully...");
    };

    #[cfg(unix)]
    let terminate = async {
        tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .expect("failed to install SIGTERM handler")
            .recv()
            .await;
        tracing::info!("received SIGTERM, shutting down gracefully...");
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => {},
        _ = terminate => {},
    }

    // 优雅关闭：冻结 MemTable 并刷新 WAL。
    if let Err(e) = app_state.engine.shutdown().await {
        tracing::error!(error = %e, "engine shutdown error");
    }

    // 优雅关闭：强制落盘一次 snapshot.bin。
    if let Err(e) = app_state.snapshot_checkpointer.flush_once() {
        tracing::error!(error = %e, "snapshot checkpointer flush error");
    }

    tracing::info!("graceful shutdown complete");
}
