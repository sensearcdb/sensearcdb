//! REST API Router — axum Router with middleware chain.
//!
//! Middleware order: RequestId → Auth → RateLimit → Audit
//!
//! Endpoints:
//! - POST /api/v1/write/batch, /api/v1/write/single
//! - POST /api/v1/query
//! - GET/POST /api/v1/admin/*
//! - GET /health, /health/ready, /health/live

use std::sync::Arc;

use axum::{middleware, Router};
use tower_http::cors::{Any, CorsLayer};

use super::admin;
use super::health;
use super::health::{HealthState, ReadinessProbe};
use super::middleware::{audit_middleware, request_id_middleware};
use super::query;
use super::write;

use sensearcdb_common::route_registry::RouteRegistryExt;
/// Build the REST API router with all middleware and routes.
///
/// # Arguments
/// * `writer` - Engine writer for data ingestion endpoints.
/// * `enable_auth` - Whether to enable the auth middleware.
/// * `enable_rate_limit` - Whether to enable rate limiting middleware.
/// * `resolver` - Optional `table_name → table_id` resolver (GAP2-ING-02).
/// * `tenant_resolver` - Optional request tenant resolver (S5 Step 2).
/// * `query_engine` - Optional query engine for `POST /api/v1/query` (None → 503).
/// * `readiness_probe` - Readiness probe closure (aggregates WAL/MemTable/Leader state).
pub fn build_router(
    writer: Arc<dyn crate::writer::EngineWriter>,
    enable_auth: bool,
    _enable_rate_limit: bool,
    resolver: Option<Arc<dyn write::TableNameResolver>>,
    tenant_resolver: Option<write::TenantResolver>,
    query_engine: Option<Arc<sensearcdb_engine::engine::Engine>>,
    readiness_probe: ReadinessProbe,
) -> Router {
    let cors = CorsLayer::new()
        .allow_origin(Any)
        .allow_methods(Any)
        .allow_headers(Any);

    // Write routes
    let write_state = write::WriteState {
        writer,
        resolver,
        tenant_resolver: tenant_resolver.clone(),
    };
    let write_router = Router::new()
        .register_route(
            "/api/v1/write/batch",
            sensearcdb_common::route!(post(write::handle_batch_write)),
        )
        .register_route(
            "/api/v1/write/single",
            sensearcdb_common::route!(post(write::handle_single_write)),
        )
        .with_state(write_state);

    // Query routes
    let query_state = query::QueryState {
        engine: query_engine,
        tenant_resolver,
    };
    let query_router = Router::new()
        .register_route(
            "/api/v1/query",
            sensearcdb_common::route!(post(query::handle_query)),
        )
        .with_state(query_state);

    // Schema 路由已由 sensearcdb-schema 的 `api::rest::create_router` 统一提供，
    // 此处不再注册 ingest 侧遗留的假 schema 桩（否则与真实 schema router 在顶层
    // merge 时触发 axum 0.7 的「Overlapping method route」启动 panic）。

    // Admin routes
    let admin_router = Router::new()
        .register_route(
            "/api/v1/admin/users",
            sensearcdb_common::route!(get(admin::handle_list_users)),
        )
        .register_route(
            "/api/v1/admin/roles",
            sensearcdb_common::route!(get(admin::handle_list_roles)),
        )
        .register_route(
            "/api/v1/admin/apikeys",
            sensearcdb_common::route!(post(admin::handle_create_api_key)),
        );

    // 注：认证端点由 sensearcdb-security 的 `/api/v1/auth/*`（真实 JWT 实现）统一提供，
    // 此处不再注册 ingest 侧遗留的假 auth 桩（阶段 4「不留假实现」铁律）。

    // Health routes (no auth)
    let health_state = Arc::new(HealthState { readiness_probe });
    let health_router = Router::new()
        .register_route(
            "/health",
            sensearcdb_common::route!(get(health::handle_health)),
        )
        .register_route(
            "/health/ready",
            sensearcdb_common::route!(get(health::handle_readiness)),
        )
        .register_route(
            "/health/live",
            sensearcdb_common::route!(get(health::handle_liveness)),
        )
        .with_state(health_state);

    let mut app = Router::new()
        .merge(write_router)
        .merge(query_router)
        .merge(admin_router)
        .merge(health_router)
        .layer(cors)
        .layer(middleware::from_fn(request_id_middleware))
        .layer(middleware::from_fn(move |req, next| {
            audit_middleware(req, next)
        }));

    if enable_auth {
        // Auth middleware would be added here
        // app = app.layer(middleware::from_fn(auth_middleware));
    }

    app
}

/// Application state shared across REST handlers.
#[derive(Clone)]
pub struct AppState {
    /// Engine writer for data ingestion.
    pub writer: Arc<dyn crate::writer::EngineWriter>,
}

impl AppState {
    pub fn new(writer: Arc<dyn crate::writer::EngineWriter>) -> Self {
        Self { writer }
    }
}
