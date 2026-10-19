//! SenseArcDB 单机版 REST API 路由拼接（Open Core 装配层）。
//!
//! 保留的公开路由（与完整版一致的路径前缀）：
//!
//! | 路径前缀               | 来源                          |
//! |------------------------|-------------------------------|
//! | `/metrics`             | 本模块（Prometheus 指标导出）   |
//! | `/health*`             | sensearcdb-ingest REST         |
//! | `/api/v1/schema/*`     | sensearcdb-schema REST 端点    |
//! | `/api/v1/write/*`      | sensearcdb-ingest REST 端点    |
//! | `/api/v1/query/*`      | sensearcdb-ingest REST 端点    |
//! | `/api/v1/cq/*`         | sensearcdb-cq REST 端点        |
//! | `/api/v1/stream/*`     | sensearcdb-stream REST 端点    |
//! | `/api/v1/derived-tags*`| sensearcdb-derived REST 端点   |
//!
//! 删除的闭源路由：lifecycle / security（auth/role/api-key/user/asset-grant/audit）/
//! ws-ticket / tenant_rest / event / ws_gateway / alert / notification / cluster /
//! shard / AI 路由 / console。

use std::sync::Arc;

use axum::http::{HeaderMap, StatusCode};
use axum::response::IntoResponse;
use axum::Router;
use prometheus::{Encoder, TextEncoder};

use sensearcdb_common::route_registry::RouteRegistryExt;
use sensearcdb_common::{SenseArcError, TenantId};

use crate::startup::{AppState, PartitionRoutingWriter, SchemaTableResolver};

/// 构建完整的 axum 路由（单机版）。
pub fn build_router(state: &AppState) -> Router {
    // 清空运行时挂载登记集合，使本次构建的真实挂载成为单一真相源。
    sensearcdb_common::route_registry::clear();

    // 单机版租户解析器：读 `X-Tenant-Id` header → parse → 缺省 DEFAULT，恒 Ok(Some)。
    let tenant_resolver: Arc<
        dyn Fn(&HeaderMap) -> Result<Option<TenantId>, SenseArcError> + Send + Sync,
    > = Arc::new(|headers: &HeaderMap| {
        let tenant = headers
            .get("x-tenant-id")
            .and_then(|v| v.to_str().ok())
            .and_then(|s| s.parse::<u32>().ok())
            .map(TenantId)
            .unwrap_or(TenantId::DEFAULT);
        Ok(Some(tenant))
    });

    // ─── Schema REST 路由 ────────────────────────────────────────────────
    let schema_router = sensearcdb_schema::api::rest::create_router(
        state.schema_mgr.clone(),
        tenant_resolver.clone(),
    );

    // ─── Ingest REST 路由 ────────────────────────────────────────────────
    // GAP2-PAR-01: 写入路径接入分区路由（PartitionRoutingWriter）。
    let ingest_writer: Arc<dyn sensearcdb_ingest::EngineWriter> = Arc::new(
        PartitionRoutingWriter::new(state.engine.clone(), state.partition_manager.clone()),
    );
    // GAP2-ING-02: 注入 SchemaMirror 完成 table_name→table_id 与 is_writable 校验。
    let table_resolver: Option<Arc<dyn sensearcdb_ingest::rest::write::TableNameResolver>> =
        Some(Arc::new(SchemaTableResolver::new(state.schema_mirror.clone())));

    // Readiness 探针：WAL 可写 + MemTable 可服务（单机无 Leader 判定）。
    let engine_for_probe = state.engine.clone();
    let readiness_probe: sensearcdb_ingest::rest::health::ReadinessProbe = Arc::new(move || {
        use sensearcdb_ingest::rest::health::ReadinessState;
        if !engine_for_probe.wal.is_writable() {
            return ReadinessState::not_ready("wal not writable");
        }
        if !engine_for_probe.memtable.is_ready() {
            return ReadinessState::not_ready("memtable not ready");
        }
        ReadinessState::ready()
    });

    let ingest_router = sensearcdb_ingest::rest::router::build_router(
        ingest_writer,
        false, // 单机无认证中间件
        false, // rate_limit 默认关闭
        table_resolver,
        Some(tenant_resolver.clone()),
        readiness_probe,
    );

    // ─── CQ REST 路由 ────────────────────────────────────────────────────
    let cq_router = if let Some(ref cq) = state.cq_engine {
        sensearcdb_cq::api::rest::create_router(
            cq.clone(),
            state.schema_mirror.clone(),
            tenant_resolver.clone(),
        )
    } else {
        Router::new()
    };

    // ─── Stream REST 路由 ────────────────────────────────────────────────
    let stream_catalog: Arc<dyn sensearcdb_stream::StreamCatalog> =
        Arc::new(crate::startup::StreamCatalogAdapter::new(
            state.schema_mirror.clone(),
            state.schema_mgr.clone(),
        ));
    let stream_router = sensearcdb_stream::api::rest::create_router(
        state.stream_engine.clone(),
        stream_catalog,
        tenant_resolver.clone(),
    );

    // ─── Derived REST 路由 ───────────────────────────────────────────────
    let derived_router = match state.derived_engine_arc.as_ref() {
        Some(engine) => sensearcdb_derived::api::rest::router(engine.clone(), tenant_resolver.clone()),
        None => Router::new(),
    };

    Router::new()
        .register_route("/metrics", sensearcdb_common::route!(get(metrics_handler)))
        .merge(schema_router)
        .merge(ingest_router)
        .merge(cq_router)
        .merge(stream_router)
        .merge(derived_router)
}

/// Prometheus 指标导出（`/metrics`）。
async fn metrics_handler() -> impl IntoResponse {
    let mut buffer = Vec::new();
    let encoder = TextEncoder::new();
    let metric_families = prometheus::gather();
    if let Err(e) = encoder.encode(&metric_families, &mut buffer) {
        tracing::error!(error = %e, "failed to encode prometheus metrics");
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            "metrics encoding error".to_string(),
        );
    }
    match String::from_utf8(buffer) {
        Ok(text) => (StatusCode::OK, text),
        Err(e) => {
            tracing::error!(error = %e, "metrics buffer is not valid utf-8");
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                "metrics encoding error".to_string(),
            )
        }
    }
}
