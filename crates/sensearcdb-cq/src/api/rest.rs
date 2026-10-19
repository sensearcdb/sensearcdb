//! CQ 引擎管理 REST 端点。
//!
//! 提供 CQ 定义和状态的 HTTP 管理接口。
//!
//! ## 端点
//!
//! - `GET  /api/v1/cq/definitions`   — 列出所有 CQ 定义
//! - `GET  /api/v1/cq/definitions/:table_id` — 获取指定表的 CQ 定义
//! - `POST /api/v1/cq/definitions/:table_id/enable`  — 启用指定表的 CQ
//! - `POST /api/v1/cq/definitions/:table_id/disable` — 禁用指定表的 CQ
//! - `GET  /api/v1/cq/windows/:table_id`     — 获取窗口状态
//! - `GET  /api/v1/cq/status`                — CQ 引擎状态概览

use axum::{
    extract::{Path, State},
    http::{HeaderMap, StatusCode},
    response::IntoResponse,
    Json, Router,
};
use sensearcdb_common::route_registry::RouteRegistryExt;
use sensearcdb_common::{SenseArcError, TenantId};
use sensearcdb_schema::SchemaMirror;
use serde::Serialize;
use std::sync::Arc;
use tokio::sync::RwLock;

use crate::definition::CqDefinition;
use crate::engine::CqEngine;

/// 请求租户解析器（bin 层注入，避免 cq crate 反向依赖 bin 的 principal 提取）。
pub type TenantResolver =
    Arc<dyn Fn(&HeaderMap) -> Result<Option<TenantId>, SenseArcError> + Send + Sync>;

// ─── 响应类型 ───

/// 标准 API 响应包装。
#[derive(Debug, Serialize)]
pub struct ApiResponse<T: Serialize> {
    pub code: u32,
    pub message: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub data: Option<T>,
}

impl<T: Serialize> ApiResponse<T> {
    pub fn success(data: T) -> Self {
        Self {
            code: 0,
            message: "ok".into(),
            data: Some(data),
        }
    }

    pub fn error(code: u32, message: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
            data: None,
        }
    }
}

// ─── DTO 类型 ───

/// CQ 定义摘要（用于列表响应）。
#[derive(Debug, Serialize)]
pub struct CqDefinitionSummary {
    pub name: String,
    pub source_table_id: u32,
    pub target_table_id: u32,
    pub granularity: String,
    pub interval_ms: i64,
    pub column_count: usize,
    pub status: String,
    pub last_executed_window_end: i64,
    pub consecutive_failures: u32,
}

impl From<&CqDefinition> for CqDefinitionSummary {
    fn from(def: &CqDefinition) -> Self {
        Self {
            name: def.name.clone(),
            source_table_id: def.source_table_id,
            target_table_id: def.target_table_id,
            granularity: def.granularity.name().to_string(),
            interval_ms: def.granularity.interval_ms(),
            column_count: def.source_columns.len(),
            status: format!("{:?}", def.status),
            last_executed_window_end: def.last_executed_window_end,
            consecutive_failures: def.consecutive_failures,
        }
    }
}

/// CQ 引擎状态概览。
#[derive(Debug, Serialize)]
pub struct CqStatusResponse {
    pub running: bool,
    pub definitions_count: usize,
    pub recalculation_queue_len: usize,
    pub window_states_count: usize,
    pub enabled: bool,
}

/// CQ 窗口状态摘要。
#[derive(Debug, Serialize)]
pub struct CqWindowSummary {
    pub source_table_id: u32,
    pub granularity: String,
    pub window_end: i64,
    pub window_start: i64,
    pub status: String,
    pub dirty: bool,
    pub calculation_count: u32,
}

// ─── 共享状态 ───

/// REST handler 的共享状态。
pub struct CqApiState {
    pub engine: Arc<RwLock<CqEngine>>,
    /// Schema 镜像（CQ 列表按 `source_table_id` 反查 tenant）。
    pub mirror: Arc<SchemaMirror>,
    /// 请求租户解析器（读侧列表隔离）。
    pub tenant_resolver: TenantResolver,
}

// ─── 路由 ───

/// 创建 CQ REST 路由。
pub fn create_router(
    engine: Arc<RwLock<CqEngine>>,
    mirror: Arc<SchemaMirror>,
    tenant_resolver: TenantResolver,
) -> Router {
    let state = Arc::new(CqApiState {
        engine,
        mirror,
        tenant_resolver,
    });

    Router::new()
        .register_route(
            "/api/v1/cq/status",
            sensearcdb_common::route!(get(get_status)),
        )
        .register_route(
            "/api/v1/cq/definitions",
            sensearcdb_common::route!(get(list_definitions)),
        )
        .register_route(
            "/api/v1/cq/definitions/:table_id",
            sensearcdb_common::route!(get(get_table_definitions)),
        )
        .register_route(
            "/api/v1/cq/definitions/:table_id/enable",
            sensearcdb_common::route!(post(enable_cq)),
        )
        .register_route(
            "/api/v1/cq/definitions/:table_id/disable",
            sensearcdb_common::route!(post(disable_cq)),
        )
        .register_route(
            "/api/v1/cq/windows/:table_id",
            sensearcdb_common::route!(get(get_window_states)),
        )
        .with_state(state)
}

// ─── Handler ───

/// 解析「生效租户」用于读侧列表/计数（D7）：`Err` 原样回传、`None` 空列表防枚举。
fn resolve_list_tenant(
    resolver: &TenantResolver,
    headers: &HeaderMap,
) -> Result<Option<TenantId>, axum::response::Response> {
    match resolver(headers) {
        Ok(v) => Ok(v),
        Err(e) => {
            let status =
                StatusCode::from_u16(e.http_status()).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR);
            Err((
                status,
                Json(ApiResponse::<serde_json::Value>::error(
                    e.code(),
                    e.to_string(),
                )),
            )
                .into_response())
        }
    }
}

/// CQ 定义归属租户 = `mirror.get_table(source_table_id).tenant_id`（A2 反查）。
fn cq_def_tenant(state: &CqApiState, def: &CqDefinition) -> Option<TenantId> {
    state
        .mirror
        .get_table(def.source_table_id)
        .map(|t| t.tenant_id)
}

/// 点查「不存在」统一响应（与「真的没有定义」同语义，防枚举）。
fn cq_not_found(table_id: u32) -> axum::response::Response {
    (
        StatusCode::NOT_FOUND,
        Json(ApiResponse::<Vec<CqDefinitionSummary>>::error(
            40401,
            format!("no CQ definitions for table {}", table_id),
        )),
    )
        .into_response()
}

/// enable/disable 端点「不存在」统一响应（与「真的没有定义」同 body，防枚举）。
fn cq_mutation_not_found(table_id: u32) -> axum::response::Response {
    (
        StatusCode::NOT_FOUND,
        Json(ApiResponse::<String>::error(
            40401,
            format!("no CQ definitions for table {}", table_id),
        )),
    )
        .into_response()
}

/// windows 端点「不存在」统一响应（与「真的没有定义」同 body，防枚举）。
fn cq_windows_not_found(table_id: u32) -> axum::response::Response {
    (
        StatusCode::NOT_FOUND,
        Json(ApiResponse::<Vec<CqWindowSummary>>::error(
            40401,
            format!("no CQ definitions for table {}", table_id),
        )),
    )
        .into_response()
}

/// 表归属租户 = `mirror.get_table(table_id).tenant_id`（A2 mirror 反查；miss 返回 None）。
fn cq_table_tenant(state: &CqApiState, table_id: u32) -> Option<TenantId> {
    state.mirror.get_table(table_id).map(|t| t.tenant_id)
}

/// GET /api/v1/cq/status
async fn get_status(State(state): State<Arc<CqApiState>>, headers: HeaderMap) -> impl IntoResponse {
    let tenant = match resolve_list_tenant(&state.tenant_resolver, &headers) {
        Ok(t) => t,
        Err(resp) => return resp,
    };

    let engine = state.engine.read().await;
    let defs = engine.get_definitions();
    // D7：`None` → 空计数；`Some(t)` → 仅归属 t 的定义计数（mirror miss 跳过）。
    let visible_defs = match tenant {
        None => 0usize,
        Some(t) => defs
            .iter()
            .filter(|d| cq_def_tenant(&state, d) == Some(t))
            .count(),
    };
    let status = CqStatusResponse {
        running: engine.is_running(),
        definitions_count: visible_defs,
        recalculation_queue_len: engine.recalculation_queue_len(),
        window_states_count: visible_defs * 4, // 粗略估计
        enabled: true,
    };
    Json(ApiResponse::success(status)).into_response()
}

/// GET /api/v1/cq/definitions
async fn list_definitions(
    State(state): State<Arc<CqApiState>>,
    headers: HeaderMap,
) -> impl IntoResponse {
    let tenant = match resolve_list_tenant(&state.tenant_resolver, &headers) {
        Ok(t) => t,
        Err(resp) => return resp,
    };

    let engine = state.engine.read().await;
    let defs = engine.get_definitions();
    // D7：`None` → 空列表；`Some(t)` → 仅归属 t 的定义（mirror miss 跳过，源表已删）。
    let summaries: Vec<CqDefinitionSummary> = match tenant {
        None => Vec::new(),
        Some(t) => defs
            .iter()
            .filter(|d| cq_def_tenant(&state, d) == Some(t))
            .map(|d| d.into())
            .collect(),
    };
    Json(ApiResponse::success(summaries)).into_response()
}

/// GET /api/v1/cq/definitions/:table_id
async fn get_table_definitions(
    State(state): State<Arc<CqApiState>>,
    Path(table_id): Path<u32>,
    headers: HeaderMap,
) -> impl IntoResponse {
    // 点查防枚举（对齐 D7「None→不存在」）：起手解析生效租户，`Err` 原样回传。
    let tenant = match resolve_list_tenant(&state.tenant_resolver, &headers) {
        Ok(t) => t,
        Err(resp) => return resp,
    };
    // 平台主体无 override（None）→ 404「不存在」，防枚举。
    let Some(tenant) = tenant else {
        return cq_not_found(table_id);
    };

    // 该表归属租户 = `mirror.get_table(table_id).tenant_id`（A2 mirror 反查）；
    // 跨租户 / 表不存在（mirror miss）→ 404「不存在」，防按 table_id 枚举探测。
    let table_tenant = state.mirror.get_table(table_id).map(|t| t.tenant_id);
    if table_tenant != Some(tenant) {
        return cq_not_found(table_id);
    }

    let engine = state.engine.read().await;
    let defs = engine.get_definitions_for_table(table_id);
    if defs.is_empty() {
        return cq_not_found(table_id);
    }
    let summaries: Vec<CqDefinitionSummary> = defs.iter().map(|d| d.into()).collect();
    (StatusCode::OK, Json(ApiResponse::success(summaries))).into_response()
}

/// POST /api/v1/cq/definitions/:table_id/enable
async fn enable_cq(
    State(state): State<Arc<CqApiState>>,
    Path(table_id): Path<u32>,
    headers: HeaderMap,
) -> impl IntoResponse {
    // 点查防枚举（对齐 D7「None→不存在」）：起手解析生效租户，`Err` 原样回传。
    let tenant = match resolve_list_tenant(&state.tenant_resolver, &headers) {
        Ok(t) => t,
        Err(resp) => return resp,
    };
    // 平台主体无 override（None）→ 404「不存在」，防按 table_id 枚举探测。
    let Some(tenant) = tenant else {
        return cq_mutation_not_found(table_id);
    };
    // 该表归属租户 = `mirror.get_table(table_id).tenant_id`；跨租户 / 表不存在（miss）→ 404。
    if cq_table_tenant(&state, table_id) != Some(tenant) {
        return cq_mutation_not_found(table_id);
    }

    let engine = state.engine.read().await;
    let updated = engine.set_definitions_enabled(table_id, true);
    if updated == 0 {
        return cq_mutation_not_found(table_id);
    }
    (
        StatusCode::OK,
        Json(ApiResponse::success(format!(
            "CQ definitions for table {} enabled ({} definitions)",
            table_id, updated
        ))),
    )
        .into_response()
}

/// POST /api/v1/cq/definitions/:table_id/disable
async fn disable_cq(
    State(state): State<Arc<CqApiState>>,
    Path(table_id): Path<u32>,
    headers: HeaderMap,
) -> impl IntoResponse {
    // 点查防枚举（对齐 D7「None→不存在」）：起手解析生效租户，`Err` 原样回传。
    let tenant = match resolve_list_tenant(&state.tenant_resolver, &headers) {
        Ok(t) => t,
        Err(resp) => return resp,
    };
    // 平台主体无 override（None）→ 404「不存在」，防按 table_id 枚举探测。
    let Some(tenant) = tenant else {
        return cq_mutation_not_found(table_id);
    };
    // 该表归属租户 = `mirror.get_table(table_id).tenant_id`；跨租户 / 表不存在（miss）→ 404。
    if cq_table_tenant(&state, table_id) != Some(tenant) {
        return cq_mutation_not_found(table_id);
    }

    let engine = state.engine.read().await;
    let updated = engine.set_definitions_enabled(table_id, false);
    if updated == 0 {
        return cq_mutation_not_found(table_id);
    }
    (
        StatusCode::OK,
        Json(ApiResponse::success(format!(
            "CQ definitions for table {} disabled ({} definitions)",
            table_id, updated
        ))),
    )
        .into_response()
}

/// GET /api/v1/cq/windows/:table_id
async fn get_window_states(
    State(state): State<Arc<CqApiState>>,
    Path(table_id): Path<u32>,
    headers: HeaderMap,
) -> impl IntoResponse {
    // 点查防枚举（对齐 D7「None→不存在」）：起手解析生效租户，`Err` 原样回传。
    let tenant = match resolve_list_tenant(&state.tenant_resolver, &headers) {
        Ok(t) => t,
        Err(resp) => return resp,
    };
    // 平台主体无 override（None）→ 404「不存在」，防按 table_id 枚举探测。
    let Some(tenant) = tenant else {
        return cq_windows_not_found(table_id);
    };
    // 该表归属租户 = `mirror.get_table(table_id).tenant_id`；跨租户 / 表不存在（miss）→ 404。
    if cq_table_tenant(&state, table_id) != Some(tenant) {
        return cq_windows_not_found(table_id);
    }

    let engine = state.engine.read().await;
    let defs = engine.get_definitions_for_table(table_id);

    if defs.is_empty() {
        return cq_windows_not_found(table_id);
    }

    let mut windows: Vec<CqWindowSummary> = Vec::new();
    for def in &defs {
        let key = crate::window::CqWindowKey::new(
            def.source_table_id,
            def.granularity,
            def.last_executed_window_end,
        );
        if let Some(state) = engine.get_window_state(&key) {
            windows.push(CqWindowSummary {
                source_table_id: def.source_table_id,
                granularity: def.granularity.name().to_string(),
                window_end: key.window_end,
                window_start: key.window_start(),
                status: format!("{:?}", state.status),
                dirty: state.dirty,
                calculation_count: state.calculation_count,
            });
        }
    }

    (StatusCode::OK, Json(ApiResponse::success(windows))).into_response()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::{CqQueryExecutor, CqWriteTarget};
    use sensearcdb_common::{
        AppConfig, ConfigHandle, DataType, Durability, TimestampNanos, TypedValue,
    };
    use sensearcdb_schema::mirror::{ColumnDef, TableInfo, TableType};
    use sensearcdb_schema::Catalog;

    #[test]
    fn test_api_response_success() {
        let resp = ApiResponse::success("hello");
        assert_eq!(resp.code, 0);
        assert_eq!(resp.message, "ok");
        assert_eq!(resp.data, Some("hello"));
    }

    #[test]
    fn test_api_response_error() {
        let resp = ApiResponse::<String>::error(40401, "not found");
        assert_eq!(resp.code, 40401);
        assert_eq!(resp.message, "not found");
        assert!(resp.data.is_none());
    }

    // ─── 点查 tenant 过滤（问题 2 修复）handler 级测试 ──────────────────────

    struct MockExec;
    impl CqQueryExecutor for MockExec {
        fn query_raw_with_ts(
            &self,
            _table_id: u32,
            _column_ids: &[u16],
            _time_start: i64,
            _time_end: i64,
        ) -> Result<(Vec<Vec<Option<TypedValue>>>, Vec<i64>), SenseArcError> {
            Ok((vec![], vec![]))
        }
    }

    struct MockTarget;
    impl CqWriteTarget for MockTarget {
        fn write_batch(
            &self,
            _table_id: u32,
            _timestamp: i64,
            _column_values: &[(u16, TypedValue)],
        ) -> Result<(), SenseArcError> {
            Ok(())
        }
    }

    fn make_mirror_with_table(table_id: u32, tenant: TenantId) -> Arc<SchemaMirror> {
        let mirror = Arc::new(SchemaMirror::new());
        mirror.insert_table(TableInfo {
            table_id,
            tenant_id: tenant,
            table_name: format!("src_{}", table_id),
            schema_id: 0,
            schema_version: 1,
            table_group: None,
            table_type: TableType::Raw,
            durability: Durability::Group,
            description: None,
            enabled: true,
            columns: vec![ColumnDef {
                column_id: 0,
                name: "value".to_string(),
                data_type: DataType::F64,
                precision: 0.0,
                nullable: true,
                unit: None,
                description: None,
                deadband: None,
                enum_values: None,
                deleted_at: None,
                range_low: 0.0,
                range_high: 0.0,
                instrument_range_low: None,
                instrument_range_high: None,
                display_digits: 2,
            }],
            deleted_at: None,
            created_at: TimestampNanos(0),
            updated_at: TimestampNanos(0),
        });
        mirror
    }

    /// 构造 `CqApiState`：mirror 内源表 99 归属 tenant=7，engine 预注册表 99 的一条 CQ 定义，
    /// resolver 固定返回 `request_tenant`。
    async fn make_state(request_tenant: Option<TenantId>) -> Arc<CqApiState> {
        let dir = tempfile::tempdir().unwrap();
        let db = dir.path().join("config.db");
        let catalog = Arc::new(Catalog::open(&db).unwrap());

        let mut engine = CqEngine::new(
            Arc::new(MockExec),
            Arc::new(MockTarget),
            ConfigHandle::new(AppConfig::default()),
            None,
            catalog,
        );
        // 注册源表 99 的一条 CQ 定义（4 粒度），使同租户点查能返回 200。
        engine.on_table_created(99, &[100, 101, 102, 103], &[(0u16, DataType::F64)]);

        let mirror = make_mirror_with_table(99, TenantId(7));
        let resolver: TenantResolver = Arc::new(move |_headers: &HeaderMap| Ok(request_tenant));
        Arc::new(CqApiState {
            engine: Arc::new(RwLock::new(engine)),
            mirror,
            tenant_resolver: resolver,
        })
    }

    async fn body_json(resp: axum::response::Response) -> serde_json::Value {
        let bytes = axum::body::to_bytes(resp.into_body(), usize::MAX)
            .await
            .expect("read response body");
        serde_json::from_slice(&bytes).expect("response body must be JSON")
    }

    /// 同租户点查：请求 tenant=7，源表 99 归属 tenant=7 → 200 返回 CQ 定义。
    #[tokio::test]
    async fn test_get_table_definitions_same_tenant_returns_ok() {
        let state = make_state(Some(TenantId(7))).await;

        let resp = get_table_definitions(State(state), Path(99u32), HeaderMap::new())
            .await
            .into_response();
        assert_eq!(resp.status(), StatusCode::OK);
        let body = body_json(resp).await;
        let data = body["data"].as_array().expect("data must be an array");
        assert!(!data.is_empty(), "同租户点查应返回 CQ 定义");
    }

    /// 跨租户点查（问题 2 重点）：请求 tenant=8，源表 99 归属 tenant=7 → 404「不存在」，防枚举。
    #[tokio::test]
    async fn test_get_table_definitions_cross_tenant_returns_not_found() {
        let state = make_state(Some(TenantId(8))).await;

        let resp = get_table_definitions(State(state), Path(99u32), HeaderMap::new())
            .await
            .into_response();
        assert_eq!(resp.status(), StatusCode::NOT_FOUND);
        let body = body_json(resp).await;
        assert_eq!(body["code"], 40401);
        assert!(body["data"].is_null(), "跨租户点查不得泄露 CQ 定义");
    }

    /// 平台主体无 override（None）→ 404「不存在」，防枚举。
    #[tokio::test]
    async fn test_get_table_definitions_none_tenant_returns_not_found() {
        let state = make_state(None).await;

        let resp = get_table_definitions(State(state), Path(99u32), HeaderMap::new())
            .await
            .into_response();
        assert_eq!(resp.status(), StatusCode::NOT_FOUND);
        let body = body_json(resp).await;
        assert_eq!(body["code"], 40401);
        assert!(body["data"].is_null());
    }

    // ─── enable/disable/windows tenant 过滤（问题 3 补完）handler 级测试 ────

    /// 同租户 enable：请求 tenant=7，源表 99 归属 tenant=7 → 200。
    #[tokio::test]
    async fn test_enable_cq_same_tenant_returns_ok() {
        let state = make_state(Some(TenantId(7))).await;

        let resp = enable_cq(State(state), Path(99u32), HeaderMap::new())
            .await
            .into_response();
        assert_eq!(resp.status(), StatusCode::OK);
        let body = body_json(resp).await;
        assert_eq!(body["code"], 0);
    }

    /// 跨租户 enable：请求 tenant=8，源表 99 归属 tenant=7 → 404「不存在」，防枚举。
    #[tokio::test]
    async fn test_enable_cq_cross_tenant_returns_not_found() {
        let state = make_state(Some(TenantId(8))).await;

        let resp = enable_cq(State(state), Path(99u32), HeaderMap::new())
            .await
            .into_response();
        assert_eq!(resp.status(), StatusCode::NOT_FOUND);
        let body = body_json(resp).await;
        assert_eq!(body["code"], 40401);
        assert!(
            body["data"].is_null(),
            "跨租户 enable 不得泄露 CQ 定义存在性"
        );
    }

    /// 同租户 disable：请求 tenant=7，源表 99 归属 tenant=7 → 200。
    #[tokio::test]
    async fn test_disable_cq_same_tenant_returns_ok() {
        let state = make_state(Some(TenantId(7))).await;

        let resp = disable_cq(State(state), Path(99u32), HeaderMap::new())
            .await
            .into_response();
        assert_eq!(resp.status(), StatusCode::OK);
        let body = body_json(resp).await;
        assert_eq!(body["code"], 0);
    }

    /// 跨租户 disable：请求 tenant=8，源表 99 归属 tenant=7 → 404「不存在」，防枚举。
    #[tokio::test]
    async fn test_disable_cq_cross_tenant_returns_not_found() {
        let state = make_state(Some(TenantId(8))).await;

        let resp = disable_cq(State(state), Path(99u32), HeaderMap::new())
            .await
            .into_response();
        assert_eq!(resp.status(), StatusCode::NOT_FOUND);
        let body = body_json(resp).await;
        assert_eq!(body["code"], 40401);
        assert!(
            body["data"].is_null(),
            "跨租户 disable 不得泄露 CQ 定义存在性"
        );
    }

    /// 同租户 windows：请求 tenant=7，源表 99 归属 tenant=7 → 200（窗口列表可为空）。
    #[tokio::test]
    async fn test_get_window_states_same_tenant_returns_ok() {
        let state = make_state(Some(TenantId(7))).await;

        let resp = get_window_states(State(state), Path(99u32), HeaderMap::new())
            .await
            .into_response();
        assert_eq!(resp.status(), StatusCode::OK);
        let body = body_json(resp).await;
        assert_eq!(body["code"], 0);
    }

    /// 跨租户 windows：请求 tenant=8，源表 99 归属 tenant=7 → 404「不存在」，防枚举。
    #[tokio::test]
    async fn test_get_window_states_cross_tenant_returns_not_found() {
        let state = make_state(Some(TenantId(8))).await;

        let resp = get_window_states(State(state), Path(99u32), HeaderMap::new())
            .await
            .into_response();
        assert_eq!(resp.status(), StatusCode::NOT_FOUND);
        let body = body_json(resp).await;
        assert_eq!(body["code"], 40401);
        assert!(body["data"].is_null(), "跨租户 windows 不得泄露窗口状态");
    }
}
