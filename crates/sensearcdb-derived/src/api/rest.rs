//! REST API endpoints for the derived tag engine.
//!
//! # Endpoints
//!
//! | Method   | Path                                    | Description                  |
//! |----------|-----------------------------------------|------------------------------|
//! | `POST`   | `/api/v1/derived-tags`                  | Register a derived tag       |
//! | `DELETE` | `/api/v1/derived-tags/:name`            | Unregister a derived tag     |
//! | `GET`    | `/api/v1/derived-tags/:name`            | Get a derived tag definition |
//! | `GET`    | `/api/v1/derived-tags`                  | List all derived tags        |
//! | `POST`   | `/api/v1/derived-tags/validate`         | Validate an expression       |
//! | `GET`    | `/api/v1/derived-tags/:name/dependents` | Get dependents of a tag      |
//! | `GET`    | `/api/v1/derived-tags/graph`            | Get dependency graph DOT     |

use axum::extract::{Path, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::IntoResponse;
use axum::{Json, Router};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;

use crate::deadband::DeadbandConfig;
use crate::engine::DerivedTagEngine;
use crate::fallback::FallbackPolicy;

use sensearcdb_common::route_registry::RouteRegistryExt;
use sensearcdb_common::{SenseArcError, TenantId};

/// 请求租户解析器（bin 层注入，避免 derived crate 反向依赖 bin 的 principal 提取）。
pub type TenantResolver =
    Arc<dyn Fn(&HeaderMap) -> Result<Option<TenantId>, SenseArcError> + Send + Sync>;
// ─── Request/Response types ───

/// Request to register a derived tag.
#[derive(Debug, Deserialize)]
pub struct RegisterTagRequest {
    /// Target table ID.
    pub table_id: u32,
    /// Derived tag name.
    pub tag_name: String,
    /// Expression string (e.g. "`sensor_a` + `sensor_b` * 2").
    pub expression: String,
    /// Fallback policy (json): {"Strict": null} | {"Lenient": {"last_good_max_age_secs": 300}} | {"Coalesce": {"defaults": {"tag": 0.0}}}
    #[serde(default)]
    pub fallback_policy: FallbackPolicyDef,
    /// Deadband configuration.
    #[serde(default)]
    pub deadband: DeadbandConfigDef,
}

/// JSON representation of fallback policy.
#[derive(Debug, Clone, Deserialize, Default)]
#[serde(tag = "type")]
pub enum FallbackPolicyDef {
    #[default]
    #[serde(rename = "Strict")]
    Strict,
    #[serde(rename = "Lenient")]
    Lenient { last_good_max_age_secs: u32 },
    #[serde(rename = "Coalesce")]
    Coalesce {
        #[serde(default)]
        defaults: HashMap<String, f64>,
    },
}

impl From<FallbackPolicyDef> for FallbackPolicy {
    fn from(def: FallbackPolicyDef) -> Self {
        match def {
            FallbackPolicyDef::Strict => FallbackPolicy::Strict,
            FallbackPolicyDef::Lenient {
                last_good_max_age_secs,
            } => FallbackPolicy::Lenient {
                last_good_max_age_secs,
            },
            FallbackPolicyDef::Coalesce { defaults } => FallbackPolicy::Coalesce { defaults },
        }
    }
}

/// JSON representation of deadband config.
#[derive(Debug, Clone, Deserialize, Default)]
pub struct DeadbandConfigDef {
    #[serde(default)]
    pub abs: f64,
    #[serde(default)]
    pub rel: f64,
    #[serde(default)]
    pub max_time_secs: u32,
}

impl From<DeadbandConfigDef> for DeadbandConfig {
    fn from(def: DeadbandConfigDef) -> Self {
        DeadbandConfig {
            abs: def.abs,
            rel: def.rel,
            max_time_secs: def.max_time_secs,
        }
    }
}

/// Request to validate an expression.
#[derive(Debug, Deserialize)]
pub struct ValidateExpressionRequest {
    pub expression: String,
    #[serde(default = "default_max_depth")]
    pub max_depth: usize,
}

fn default_max_depth() -> usize {
    32
}

/// Validation result.
#[derive(Debug, Serialize)]
pub struct ValidateExpressionResponse {
    pub valid: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub source_tags: Option<Vec<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ast_display: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

/// Derived tag info response.
#[derive(Debug, Serialize)]
pub struct DerivedTagInfo {
    pub tag_name: String,
    pub table_id: u32,
    pub source_tags: Vec<String>,
    pub expression_hash: u64,
    pub state: String,
}

/// Standard error response.
#[derive(Debug, Serialize)]
pub struct ErrorResponse {
    pub code: String,
    pub message: String,
}

// ─── Application State ───

/// Shared application state for REST handlers.
#[derive(Clone)]
pub struct AppState {
    pub engine: Arc<DerivedTagEngine>,
    /// 请求租户解析器（读侧 `(tenant, name)` 寻址 + 列表隔离）。
    pub tenant_resolver: TenantResolver,
}

// ─── Router ───

/// Build the REST router for derived tag management.
pub fn router(engine: Arc<DerivedTagEngine>, tenant_resolver: TenantResolver) -> Router {
    let state = AppState {
        engine,
        tenant_resolver,
    };

    Router::new()
        .register_route(
            "/api/v1/derived-tags",
            sensearcdb_common::route!(post(register_tag), get(list_tags)),
        )
        .register_route(
            "/api/v1/derived-tags/:name",
            sensearcdb_common::route!(get(get_tag), delete(unregister_tag)),
        )
        .register_route(
            "/api/v1/derived-tags/validate",
            sensearcdb_common::route!(post(validate_expression)),
        )
        .register_route(
            "/api/v1/derived-tags/:name/dependents",
            sensearcdb_common::route!(get(get_dependents)),
        )
        .register_route(
            "/api/v1/derived-tags/graph",
            sensearcdb_common::route!(get(get_graph)),
        )
        .with_state(state)
}

// ─── Handlers ───

/// 解析「生效租户」（D7 读侧 + D4/D6 同步写路径授权共用）：
/// `Err` 原样回传、`None` 表示平台主体无 override（读侧列表/图回空、点查/写路径回 404 防枚举）。
fn resolve_effective_tenant(
    resolver: &TenantResolver,
    headers: &HeaderMap,
) -> Result<Option<TenantId>, (StatusCode, Json<ErrorResponse>)> {
    match resolver(headers) {
        Ok(v) => Ok(v),
        Err(e) => {
            let status =
                StatusCode::from_u16(e.http_status()).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR);
            Err((
                status,
                Json(ErrorResponse {
                    code: e.code().to_string(),
                    message: e.to_string(),
                }),
            ))
        }
    }
}

/// 读侧列表/寻址沿用「生效租户」解析（D7）。
fn resolve_list_tenant(
    resolver: &TenantResolver,
    headers: &HeaderMap,
) -> Result<Option<TenantId>, (StatusCode, Json<ErrorResponse>)> {
    resolve_effective_tenant(resolver, headers)
}

/// 点查/写路径「不存在」统一响应（与「真的没有该 tag」同 body，防枚举）。
fn tag_not_found(name: &str) -> axum::response::Response {
    (
        StatusCode::NOT_FOUND,
        Json(ErrorResponse {
            code: "DERIVED_TAG_NOT_FOUND".into(),
            message: format!("derived tag '{}' not found", name),
        }),
    )
        .into_response()
}

/// POST /api/v1/derived-tags — Register a new derived tag.
async fn register_tag(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(req): Json<RegisterTagRequest>,
) -> impl IntoResponse {
    // 同步注册入口的跨租户写授权（D4/D6）：起手解析生效租户。
    // `Err` 原样回传；`None`（平台主体无 override）→ 404「不存在」防枚举。
    let tenant = match resolve_effective_tenant(&state.tenant_resolver, &headers) {
        Ok(t) => t,
        Err(resp) => return resp.into_response(),
    };
    let Some(tenant) = tenant else {
        return tag_not_found(&req.tag_name);
    };

    // 目标表归属 tenant 必须 == 请求 tenant；不匹配（跨租户 / 表不存在）→ 404，
    // 与「不存在」同 body 防 table_id 枚举。注册后的异步求值写回仍按 D4/D6
    // 目标表归属反查 tenant（不受此同步注册入口授权层影响）。
    if state.engine.table_tenant(req.table_id) != Some(tenant) {
        return tag_not_found(&req.tag_name);
    }

    match state.engine.register_tag(
        req.table_id,
        &req.tag_name,
        &req.expression,
        req.fallback_policy.into(),
        req.deadband.into(),
    ) {
        Ok(()) => (
            StatusCode::CREATED,
            Json(serde_json::json!({
                "status": "created",
                "tag_name": req.tag_name
            })),
        )
            .into_response(),
        Err(e) => (
            StatusCode::BAD_REQUEST,
            Json(ErrorResponse {
                code: e.code().to_string(),
                message: format!("{}", e),
            }),
        )
            .into_response(),
    }
}

/// DELETE /api/v1/derived-tags/:name — Unregister a derived tag（租户作用域）.
async fn unregister_tag(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(name): Path<String>,
) -> impl IntoResponse {
    let tenant = match resolve_list_tenant(&state.tenant_resolver, &headers) {
        Ok(t) => t,
        Err(resp) => return resp.into_response(),
    };
    let Some(tenant) = tenant else {
        return (
            StatusCode::NOT_FOUND,
            Json(ErrorResponse {
                code: "DERIVED_TAG_NOT_FOUND".into(),
                message: format!("derived tag '{}' not found", name),
            }),
        )
            .into_response();
    };

    match state.engine.unregister_tag(tenant, &name) {
        Ok(cascade) => (
            StatusCode::OK,
            Json(serde_json::json!({
                "status": "deleted",
                "tag_name": name,
                "cascade_disabled": cascade
            })),
        )
            .into_response(),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ErrorResponse {
                code: e.code().to_string(),
                message: format!("{}", e),
            }),
        )
            .into_response(),
    }
}

/// GET /api/v1/derived-tags/:name — Get a derived tag's definition（租户作用域）.
async fn get_tag(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(name): Path<String>,
) -> impl IntoResponse {
    let tenant = match resolve_list_tenant(&state.tenant_resolver, &headers) {
        Ok(t) => t,
        Err(resp) => return resp.into_response(),
    };
    let Some(tenant) = tenant else {
        return (
            StatusCode::NOT_FOUND,
            Json(ErrorResponse {
                code: "DERIVED_TAG_NOT_FOUND".into(),
                message: format!("derived tag '{}' not found", name),
            }),
        )
            .into_response();
    };

    let graph = state.engine.dep_graph().read();
    match graph.get(tenant, &name) {
        Some(node) => (
            StatusCode::OK,
            Json(DerivedTagInfo {
                tag_name: node.name.clone(),
                table_id: node.table_id,
                source_tags: node.source_tags.clone(),
                expression_hash: node.expression_hash,
                state: format!("{:?}", node.state),
            }),
        )
            .into_response(),
        None => (
            StatusCode::NOT_FOUND,
            Json(ErrorResponse {
                code: "DERIVED_TAG_NOT_FOUND".into(),
                message: format!("derived tag '{}' not found", name),
            }),
        )
            .into_response(),
    }
}

/// GET /api/v1/derived-tags — List all registered derived tags（租户作用域）.
async fn list_tags(State(state): State<AppState>, headers: HeaderMap) -> impl IntoResponse {
    let tenant = match resolve_list_tenant(&state.tenant_resolver, &headers) {
        Ok(t) => t,
        Err(resp) => return resp.into_response(),
    };
    // D7：`None` → 空列表防枚举。
    let Some(tenant) = tenant else {
        return (
            StatusCode::OK,
            Json(serde_json::json!({ "tags": Vec::<DerivedTagInfo>::new() })),
        )
            .into_response();
    };

    let graph = state.engine.dep_graph().read();
    let tags: Vec<DerivedTagInfo> = graph
        .node_names_for_tenant(tenant)
        .into_iter()
        .filter_map(|name| {
            graph.get(tenant, name).map(|node| DerivedTagInfo {
                tag_name: node.name.clone(),
                table_id: node.table_id,
                source_tags: node.source_tags.clone(),
                expression_hash: node.expression_hash,
                state: format!("{:?}", node.state),
            })
        })
        .collect();

    (StatusCode::OK, Json(serde_json::json!({ "tags": tags }))).into_response()
}

/// POST /api/v1/derived-tags/validate — Validate an expression without registering.
async fn validate_expression(
    State(_state): State<AppState>,
    Json(req): Json<ValidateExpressionRequest>,
) -> impl IntoResponse {
    match DerivedTagEngine::validate_expression(&req.expression, req.max_depth) {
        Ok((ast, source_tags)) => (
            StatusCode::OK,
            Json(ValidateExpressionResponse {
                valid: true,
                source_tags: Some(source_tags),
                ast_display: Some(format!("{}", ast)),
                error: None,
            }),
        )
            .into_response(),
        Err(e) => (
            StatusCode::BAD_REQUEST,
            Json(ValidateExpressionResponse {
                valid: false,
                source_tags: None,
                ast_display: None,
                error: Some(format!("{}", e)),
            }),
        )
            .into_response(),
    }
}

/// GET /api/v1/derived-tags/:name/dependents — Get tags that depend on this tag（租户作用域）.
async fn get_dependents(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(name): Path<String>,
) -> impl IntoResponse {
    let tenant = match resolve_list_tenant(&state.tenant_resolver, &headers) {
        Ok(t) => t,
        Err(resp) => return resp.into_response(),
    };
    // D7：`None` → 空 dependents 防枚举。
    let Some(tenant) = tenant else {
        return (
            StatusCode::OK,
            Json(serde_json::json!({
                "tag": name,
                "dependents": Vec::<String>::new()
            })),
        )
            .into_response();
    };

    let graph = state.engine.dep_graph().read();
    let tags = graph.get_transitive_derived_tags(&[(tenant, name.clone())]);
    let deps: Vec<String> = tags.into_iter().map(|k| k.1).collect();

    (
        StatusCode::OK,
        Json(serde_json::json!({
            "tag": name,
            "dependents": deps
        })),
    )
        .into_response()
}

/// GET /api/v1/derived-tags/graph — Get the dependency graph in DOT format（租户作用域）.
async fn get_graph(State(state): State<AppState>, headers: HeaderMap) -> impl IntoResponse {
    let tenant = match resolve_list_tenant(&state.tenant_resolver, &headers) {
        Ok(t) => t,
        Err(resp) => return resp.into_response(),
    };

    let graph = state.engine.dep_graph().read();
    let mut dot = String::from("digraph derived_tags {\n");
    dot.push_str("  rankdir=LR;\n");

    // D7：`None` → 空图防枚举。
    if let Some(tenant) = tenant {
        for name in graph.node_names_for_tenant(tenant) {
            if let Some(node) = graph.get(tenant, name) {
                dot.push_str(&format!(
                    "  \"{}\" [label=\"{}\", shape=box];\n",
                    node.name, node.name
                ));
                for source in &node.source_tags {
                    dot.push_str(&format!("  \"{}\" -> \"{}\";\n", source, node.name));
                }
            }
        }
    }
    dot.push_str("}\n");

    (
        StatusCode::OK,
        [(axum::http::header::CONTENT_TYPE, "text/vnd.graphviz")],
        dot,
    )
        .into_response()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::{DerivedTagEngine, SchemaAccess, SnapshotRead, WriteSink};
    use sensearcdb_common::{AppConfig, ColumnRef, ConfigHandle};

    /// 测试用快照读取器（`register_tag` 不读取快照，仅需实现 trait）。
    struct MockSnapshot;

    impl SnapshotRead for MockSnapshot {
        fn batch_get_columns(&self, _columns: &[ColumnRef]) -> HashMap<ColumnRef, (f64, u8, i64)> {
            HashMap::new()
        }
    }

    /// 测试用写回槽（`register_tag` 不写回，仅需实现 trait）。
    struct MockWriter;

    impl WriteSink for MockWriter {
        fn write_batch(&self, _entries: &[(u32, String, f64, u8, i64)]) {}
    }

    /// 测试用 schema：反查 table_id → tenant，并解析源 tag 名 → [`ColumnRef`]。
    struct TestSchema {
        table_tenants: HashMap<u32, TenantId>,
        tables: HashMap<String, u32>,
    }

    impl SchemaAccess for TestSchema {
        fn table_tenant(&self, table_id: u32) -> Option<TenantId> {
            self.table_tenants.get(&table_id).copied()
        }

        fn resolve_column_ref(
            &self,
            _tenant_id: TenantId,
            table_name: &str,
            column_name: Option<&str>,
        ) -> Option<ColumnRef> {
            let table_id = *self.tables.get(table_name)?;
            match column_name {
                None => Some(ColumnRef::single(table_id)),
                Some(_) => None,
            }
        }
    }

    /// 构造 [`AppState`]：目标表 `target_table_id` 归属 `target_tenant`，
    /// resolver 固定返回 `request_tenant`；源表 `sensor_a` 可解析。
    fn make_state(
        request_tenant: Option<TenantId>,
        target_table_id: u32,
        target_tenant: TenantId,
    ) -> AppState {
        let snapshot: Arc<dyn SnapshotRead> = Arc::new(MockSnapshot);
        let writer: Arc<dyn WriteSink> = Arc::new(MockWriter);

        let mut table_tenants = HashMap::new();
        table_tenants.insert(target_table_id, target_tenant);
        let mut tables = HashMap::new();
        tables.insert("sensor_a".to_string(), target_table_id);
        let schema: Arc<dyn SchemaAccess> = Arc::new(TestSchema {
            table_tenants,
            tables,
        });

        let config = ConfigHandle::new(AppConfig::default());
        let engine = Arc::new(DerivedTagEngine::new(
            snapshot, writer, schema, config, None,
        ));

        let tenant_resolver: TenantResolver =
            Arc::new(move |_headers: &HeaderMap| Ok(request_tenant));

        AppState {
            engine,
            tenant_resolver,
        }
    }

    async fn body_json(resp: axum::response::Response) -> serde_json::Value {
        let bytes = axum::body::to_bytes(resp.into_body(), usize::MAX)
            .await
            .expect("read response body");
        serde_json::from_slice(&bytes).expect("response body must be JSON")
    }

    fn req(table_id: u32, tag_name: &str) -> RegisterTagRequest {
        RegisterTagRequest {
            table_id,
            tag_name: tag_name.to_string(),
            expression: "sensor_a".to_string(),
            fallback_policy: FallbackPolicyDef::Strict,
            deadband: DeadbandConfigDef::default(),
        }
    }

    /// 同租户 register_tag：请求 tenant == 目标表归属 → 201 创建成功。
    #[tokio::test]
    async fn test_register_tag_same_tenant_created() {
        let state = make_state(Some(TenantId(7)), 42, TenantId(7));

        let resp = register_tag(State(state), HeaderMap::new(), Json(req(42, "derived_a")))
            .await
            .into_response();
        assert_eq!(resp.status(), StatusCode::CREATED);
        let body = body_json(resp).await;
        assert_eq!(body["status"], "created");
        assert_eq!(body["tag_name"], "derived_a");
    }

    /// 跨租户 register_tag：请求 tenant=8，目标表 42 归属 tenant=7 → 404「不存在」，防枚举。
    #[tokio::test]
    async fn test_register_tag_cross_tenant_not_found() {
        let state = make_state(Some(TenantId(8)), 42, TenantId(7));

        let resp = register_tag(State(state), HeaderMap::new(), Json(req(42, "derived_a")))
            .await
            .into_response();
        assert_eq!(resp.status(), StatusCode::NOT_FOUND);
        let body = body_json(resp).await;
        assert_eq!(body["code"], "DERIVED_TAG_NOT_FOUND");
        assert!(
            body["message"].as_str().is_some(),
            "跨租户写拒绝应返回与「不存在」同 body，防 table_id 枚举"
        );
    }

    /// 平台主体无 override（None）→ 404「不存在」，防按 table_id 枚举。
    #[tokio::test]
    async fn test_register_tag_none_tenant_not_found() {
        let state = make_state(None, 42, TenantId(7));

        let resp = register_tag(State(state), HeaderMap::new(), Json(req(42, "derived_a")))
            .await
            .into_response();
        assert_eq!(resp.status(), StatusCode::NOT_FOUND);
        let body = body_json(resp).await;
        assert_eq!(body["code"], "DERIVED_TAG_NOT_FOUND");
    }
}
