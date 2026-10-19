//! 流计算引擎管理 REST 端点。
//!
//! 提供流计算任务的管理 HTTP 接口。
//!
//! ## 端点
//!
//! - `GET    /api/v1/stream/status`       — 引擎状态概览
//! - `GET    /api/v1/stream/tasks`        — 列出所有任务
//! - `GET    /api/v1/stream/tasks/:id`    — 获取指定任务详情
//! - `POST   /api/v1/stream/tasks`        — 创建任务 (SQL body)
//! - `DELETE /api/v1/stream/tasks/:id`    — 删除任务
//! - `POST   /api/v1/stream/tasks/:id/pause`  — 暂停任务
//! - `POST   /api/v1/stream/tasks/:id/resume` — 恢复任务

use axum::{
    extract::{Path, State},
    http::{HeaderMap, StatusCode},
    response::IntoResponse,
    Json, Router,
};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tokio::sync::RwLock as TokioRwLock;

use sensearcdb_common::route_registry::RouteRegistryExt;
use sensearcdb_common::DataType;
use sensearcdb_common::{SenseArcError, TenantId};

use crate::engine::StreamComputeEngine;
use crate::sql_parser::{parse_stream_sql, StreamSqlStatement};
use crate::task::{PartitionBySpec, ProjectionColumn, ProjectionSpec, TargetColumnDef, WindowSpec};
use crate::{stream_result_type, StreamCatalog};

/// 请求租户解析器（bin 层注入，避免 stream crate 反向依赖 bin 的 principal 提取）。
///
/// `Err` 回传「非平台主体 override / API Key override → 403」「未知 slug → 404」，
/// 由 handler 映射为 HTTP 状态，而非静默回退 `TenantId::DEFAULT`。
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

/// 创建任务的请求体。
#[derive(Debug, Deserialize)]
pub struct CreateStreamTaskRequest {
    /// SQL 语句：CREATE STREAM COMPUTE ...
    pub sql: String,
}

/// 创建任务的响应。
#[derive(Debug, Serialize)]
pub struct CreateStreamTaskResponse {
    pub task_id: String,
    pub name: String,
    pub message: String,
}

/// 流计算任务摘要。
#[derive(Debug, Serialize)]
pub struct StreamTaskSummary {
    pub task_id: String,
    pub name: String,
    pub source_table_id: u32,
    pub target_table_id: u32,
    pub window_type: String,
    pub window_spec: String,
    pub status: String,
    pub partition_by: Option<String>,
    pub column_count: usize,
    pub created_at: i64,
}

/// 引擎状态响应。
#[derive(Debug, Serialize)]
pub struct StreamStatusResponse {
    pub running: bool,
    pub total_tasks: usize,
    pub running_tasks: usize,
    pub paused_tasks: usize,
    pub error_tasks: usize,
    pub total_active_partitions: usize,
    pub config_enabled: bool,
    pub dropped_events: u64,
}

/// 前端 `StreamTask`（console/src/api/index.ts:389-404）。
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct FrontendStreamTask {
    pub id: String,
    pub name: String,
    pub source: String,
    pub sink: String,
    pub status: String,
    /// 后端无吞吐统计（诚实降级：null，前端需后续补统计）。
    #[serde(skip_serializing_if = "Option::is_none")]
    pub throughput_pts_per_sec: Option<f64>,
    /// 后端无滞后统计（诚实降级：null）。
    #[serde(skip_serializing_if = "Option::is_none")]
    pub lag_ms: Option<f64>,
    /// 后端无 (re)start 时间戳（诚实降级：null）。
    #[serde(skip_serializing_if = "Option::is_none")]
    pub started_at: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error_message: Option<String>,
}

/// 前端 `PagedResult<StreamTask>`。
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct FrontendStreamTaskList {
    pub items: Vec<FrontendStreamTask>,
    pub total: usize,
    pub page: usize,
    pub page_size: usize,
}

/// 将后端 `TaskStatus` 映射为前端 status（running/stopped/failed）。
fn task_status_to_frontend(status: &crate::task::TaskStatus) -> (&'static str, Option<String>) {
    match status {
        crate::task::TaskStatus::Running => ("running", None),
        crate::task::TaskStatus::Paused | crate::task::TaskStatus::Stopped => ("stopped", None),
        crate::task::TaskStatus::Error { reason } => ("failed", Some(reason.clone())),
    }
}

// ─── 共享状态 ───

/// REST handler 共享状态。
pub struct StreamApiState {
    pub engine: Arc<TokioRwLock<StreamComputeEngine>>,
    /// Catalog 解析器（必选，用于 SQL 中表名/列名 → ID 映射）
    pub catalog: Arc<dyn StreamCatalog>,
    /// 请求租户解析器（写路径 name→id 解析 + 读侧列表隔离）
    pub tenant_resolver: TenantResolver,
}

// ─── 路由 ───

/// 创建流计算 REST 路由。
pub fn create_router(
    engine: Arc<TokioRwLock<StreamComputeEngine>>,
    catalog: Arc<dyn StreamCatalog>,
    tenant_resolver: TenantResolver,
) -> Router {
    let state = Arc::new(StreamApiState {
        engine,
        catalog,
        tenant_resolver,
    });

    Router::new()
        .register_route(
            "/api/v1/stream/status",
            sensearcdb_common::route!(get(get_status)),
        )
        .register_route(
            "/api/v1/stream/tasks",
            sensearcdb_common::route!(get(list_tasks), post(create_task)),
        )
        .register_route(
            "/api/v1/stream/tasks/:task_id",
            sensearcdb_common::route!(get(get_task), delete(delete_task)),
        )
        .register_route(
            "/api/v1/stream/tasks/:task_id/pause",
            sensearcdb_common::route!(post(pause_task)),
        )
        .register_route(
            "/api/v1/stream/tasks/:task_id/resume",
            sensearcdb_common::route!(post(resume_task)),
        )
        // 前端契约别名：`/api/v1/stream-tasks*`（console/src/api/index.ts:1271-1291）。
        // start → resume 语义，stop → pause 语义。
        .register_route(
            "/api/v1/stream-tasks",
            sensearcdb_common::route!(get(list_stream_tasks)),
        )
        .register_route(
            "/api/v1/stream-tasks/:task_id/start",
            sensearcdb_common::route!(post(start_stream_task)),
        )
        .register_route(
            "/api/v1/stream-tasks/:task_id/stop",
            sensearcdb_common::route!(post(stop_stream_task)),
        )
        .with_state(state)
}

// ─── Handlers ───

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

/// 点查「不存在」统一响应（与「真的没有该任务」同语义，防枚举）。
fn task_not_found(task_id: uuid::Uuid) -> axum::response::Response {
    (
        StatusCode::NOT_FOUND,
        Json(ApiResponse::<StreamTaskSummary>::error(
            40401,
            format!("stream task not found: {}", task_id),
        )),
    )
        .into_response()
}

/// 变更类端点（delete/pause/resume/start/stop）「不存在」统一响应，
/// 与引擎内部 `stream task not found` 错误 body 完全一致（防枚举）。
fn task_mutation_not_found(task_id: uuid::Uuid) -> axum::response::Response {
    (
        StatusCode::NOT_FOUND,
        Json(ApiResponse::<String>::error(
            40401,
            format!("stream task not found: {}", task_id),
        )),
    )
        .into_response()
}

/// GET /api/v1/stream/status
async fn get_status(
    State(state): State<Arc<StreamApiState>>,
    headers: HeaderMap,
) -> impl IntoResponse {
    let tenant = match resolve_list_tenant(&state.tenant_resolver, &headers) {
        Ok(t) => t,
        Err(resp) => return resp,
    };

    let engine = state.engine.read().await;
    // D7：`None` → 空计数；`Some(t)` → 仅归属 t 的任务计数。
    let summary = match tenant {
        None => {
            let base = engine.status_summary();
            crate::engine::StreamEngineStatus {
                running: base.running,
                total_tasks: 0,
                running_tasks: 0,
                paused_tasks: 0,
                error_tasks: 0,
                total_active_partitions: 0,
                config_enabled: base.config_enabled,
                dropped_events: base.dropped_events,
            }
        }
        Some(t) => engine.status_summary_for_tenant(t),
    };

    let resp = StreamStatusResponse {
        running: summary.running,
        total_tasks: summary.total_tasks,
        running_tasks: summary.running_tasks,
        paused_tasks: summary.paused_tasks,
        error_tasks: summary.error_tasks,
        total_active_partitions: summary.total_active_partitions,
        config_enabled: summary.config_enabled,
        dropped_events: summary.dropped_events,
    };

    Json(ApiResponse::success(resp)).into_response()
}

/// GET /api/v1/stream/tasks
async fn list_tasks(
    State(state): State<Arc<StreamApiState>>,
    headers: HeaderMap,
) -> impl IntoResponse {
    let tenant = match resolve_list_tenant(&state.tenant_resolver, &headers) {
        Ok(t) => t,
        Err(resp) => return resp,
    };

    let engine = state.engine.read().await;
    // D7：`None` → 空列表；`Some(t)` → 仅归属 t 的任务（A3 `StreamTask.tenant_id`）。
    let tasks = match tenant {
        None => Vec::new(),
        Some(t) => engine.get_tasks_for_tenant(t),
    };

    let summaries: Vec<StreamTaskSummary> = tasks
        .iter()
        .map(|task| {
            let window_spec_str = match &task.window_spec {
                WindowSpec::Tumbling { interval_ms } => {
                    format!("Tumbling({}ms)", interval_ms)
                }
                WindowSpec::Hopping {
                    interval_ms,
                    step_ms,
                } => {
                    format!("Hopping(size={}ms, step={}ms)", interval_ms, step_ms)
                }
                WindowSpec::Session { gap_ms } => {
                    format!("Session(gap={}ms)", gap_ms)
                }
            };

            StreamTaskSummary {
                task_id: task.id.to_string(),
                name: task.name.clone(),
                source_table_id: task.source_table_id,
                target_table_id: task.target_table_id,
                window_type: task.window_spec.type_name().to_string(),
                window_spec: window_spec_str,
                status: format!("{:?}", task.status),
                partition_by: task
                    .partition_by
                    .as_ref()
                    .map(|p| p.source_column_name.clone()),
                column_count: task.projection.columns.len(),
                created_at: task.created_at,
            }
        })
        .collect();

    Json(ApiResponse::success(summaries)).into_response()
}

/// GET /api/v1/stream/tasks/:task_id
async fn get_task(
    State(state): State<Arc<StreamApiState>>,
    Path(task_id_str): Path<String>,
    headers: HeaderMap,
) -> impl IntoResponse {
    let task_id = match uuid::Uuid::parse_str(&task_id_str) {
        Ok(id) => id,
        Err(_) => {
            return (
                StatusCode::BAD_REQUEST,
                Json(ApiResponse::<StreamTaskSummary>::error(
                    40001,
                    format!("invalid task_id: {}", task_id_str),
                )),
            )
                .into_response();
        }
    };

    // 点查防枚举（对齐 D7「None→不存在」）：起手解析生效租户，`Err` 原样回传。
    let tenant = match resolve_list_tenant(&state.tenant_resolver, &headers) {
        Ok(t) => t,
        Err(resp) => return resp,
    };
    // 平台主体无 override（None）→ 404「不存在」，防枚举。
    let Some(tenant) = tenant else {
        return task_not_found(task_id);
    };

    let engine = state.engine.read().await;
    match engine.get_task(task_id) {
        // A3：按 `StreamTask.tenant_id` 比对；跨租户 → 404「不存在」（与「真的没有」同语义）。
        Some(task) if task.tenant_id == tenant => {
            let window_spec_str = match &task.window_spec {
                WindowSpec::Tumbling { interval_ms } => {
                    format!("Tumbling({}ms)", interval_ms)
                }
                WindowSpec::Hopping {
                    interval_ms,
                    step_ms,
                } => {
                    format!("Hopping(size={}ms, step={}ms)", interval_ms, step_ms)
                }
                WindowSpec::Session { gap_ms } => {
                    format!("Session(gap={}ms)", gap_ms)
                }
            };

            let summary = StreamTaskSummary {
                task_id: task.id.to_string(),
                name: task.name.clone(),
                source_table_id: task.source_table_id,
                target_table_id: task.target_table_id,
                window_type: task.window_spec.type_name().to_string(),
                window_spec: window_spec_str,
                status: format!("{:?}", task.status),
                partition_by: task
                    .partition_by
                    .as_ref()
                    .map(|p| p.source_column_name.clone()),
                column_count: task.projection.columns.len(),
                created_at: task.created_at,
            };

            (StatusCode::OK, Json(ApiResponse::success(summary))).into_response()
        }
        Some(_) | None => task_not_found(task_id),
    }
}

/// POST /api/v1/stream/tasks
async fn create_task(
    State(state): State<Arc<StreamApiState>>,
    headers: HeaderMap,
    Json(body): Json<CreateStreamTaskRequest>,
) -> impl IntoResponse {
    // 起手解析「生效租户」：name→id 解析需租户作用域（D2 例外：请求上下文）。
    let tenant_id = match (state.tenant_resolver)(&headers) {
        Ok(Some(t)) => t,
        Ok(None) => TenantId::DEFAULT,
        Err(e) => {
            let status =
                StatusCode::from_u16(e.http_status()).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR);
            return (
                status,
                Json(ApiResponse::<CreateStreamTaskResponse>::error(
                    e.code(),
                    e.to_string(),
                )),
            );
        }
    };

    // 解析 SQL
    let stmt = match parse_stream_sql(&body.sql) {
        Ok(s) => s,
        Err(e) => {
            return (
                StatusCode::BAD_REQUEST,
                Json(ApiResponse::<CreateStreamTaskResponse>::error(
                    40001,
                    format!("SQL parse error: {}", e),
                )),
            );
        }
    };

    match stmt {
        StreamSqlStatement::CreateStream {
            name,
            source_table: source_table_name,
            target_table,
            window_spec,
            aggregations,
            partition_by,
            ..
        } => {
            let catalog = &state.catalog;

            // 1. 解析源表名 → source_table_id（真解析，失败显式报错）
            let source_table_id: u32 = match catalog.resolve_table(tenant_id, &source_table_name) {
                Some(id) => id,
                None => {
                    return (
                        StatusCode::BAD_REQUEST,
                        Json(ApiResponse::<CreateStreamTaskResponse>::error(
                            40001,
                            format!("source table not found: {}", source_table_name),
                        )),
                    );
                }
            };

            // 2. 解析聚合列 → source_column_id + 真实结果类型
            let mut projection_columns = Vec::with_capacity(aggregations.len());
            let mut target_schema = Vec::with_capacity(aggregations.len());
            let mut target_columns: Vec<(String, DataType)> =
                Vec::with_capacity(aggregations.len());

            for (i, agg) in aggregations.iter().enumerate() {
                let source_column: u16 =
                    match catalog.resolve_column(source_table_id, &agg.source_column_name) {
                        Some(cid) => cid,
                        None => {
                            return (
                                StatusCode::BAD_REQUEST,
                                Json(ApiResponse::<CreateStreamTaskResponse>::error(
                                    40001,
                                    format!("source column not found: {}", agg.source_column_name),
                                )),
                            );
                        }
                    };
                let source_type = catalog
                    .resolve_column_type(source_table_id, &agg.source_column_name)
                    .unwrap_or(DataType::F64);
                let result_type = stream_result_type(agg.function, source_type);

                projection_columns.push(ProjectionColumn::Aggregate {
                    function: agg.function,
                    source_column,
                    alias: agg.alias.clone(),
                });
                target_schema.push(TargetColumnDef {
                    // 0-based：与写回路径 EngineStreamSink 的位置映射（`i as u16`）对齐，
                    // 消除 off-by-one（聚合结果列按投影位序 0 起始落目标表列）。
                    column_id: i as u16,
                    column_name: agg.alias.clone(),
                    data_type: result_type,
                });
                target_columns.push((agg.alias.clone(), result_type));
            }

            // 3. 目标表名：优先用户指定 `INTO <target_table>`，否则 `{name}_stream`
            let target_table_name = target_table.unwrap_or_else(|| format!("{}_stream", name));

            // 4. 自动创建目标表（真实列类型）
            let target_table_id: u32 = match catalog.auto_create_target_table(
                tenant_id,
                &target_table_name,
                &target_columns,
            ) {
                Ok(id) => id,
                Err(e) => {
                    return (
                        StatusCode::INTERNAL_SERVER_ERROR,
                        Json(ApiResponse::<CreateStreamTaskResponse>::error(
                            50001,
                            format!("failed to create target table: {}", e),
                        )),
                    );
                }
            };

            let projection = ProjectionSpec {
                columns: projection_columns,
                target_schema,
            };

            // 5. 解析分区键列 → column_id（真解析，失败显式报错）
            let partition_spec = match partition_by {
                Some(col_name) => match catalog.resolve_column(source_table_id, &col_name) {
                    Some(cid) => Some(PartitionBySpec {
                        source_column: cid,
                        source_column_name: col_name,
                    }),
                    None => {
                        return (
                            StatusCode::BAD_REQUEST,
                            Json(ApiResponse::<CreateStreamTaskResponse>::error(
                                40001,
                                format!("partition column not found: {}", col_name),
                            )),
                        );
                    }
                },
                None => None,
            };

            let mut engine = state.engine.write().await;
            match engine
                .create_task(
                    name.clone(),
                    tenant_id,
                    source_table_id,
                    target_table_id,
                    window_spec,
                    projection,
                    partition_spec,
                )
                .await
            {
                Ok(task_id) => {
                    let resp = CreateStreamTaskResponse {
                        task_id: task_id.to_string(),
                        name,
                        message: "stream task created successfully".into(),
                    };
                    (StatusCode::CREATED, Json(ApiResponse::success(resp)))
                }
                Err(e) => (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(ApiResponse::<CreateStreamTaskResponse>::error(
                        50001,
                        format!("failed to create stream task: {}", e),
                    )),
                ),
            }
        }
        _ => (
            StatusCode::BAD_REQUEST,
            Json(ApiResponse::<CreateStreamTaskResponse>::error(
                40001,
                "SQL must be CREATE STREAM COMPUTE ...".to_string(),
            )),
        ),
    }
}

/// DELETE /api/v1/stream/tasks/:task_id
async fn delete_task(
    State(state): State<Arc<StreamApiState>>,
    Path(task_id_str): Path<String>,
    headers: HeaderMap,
) -> impl IntoResponse {
    let task_id = match uuid::Uuid::parse_str(&task_id_str) {
        Ok(id) => id,
        Err(_) => {
            return (
                StatusCode::BAD_REQUEST,
                Json(ApiResponse::<String>::error(
                    40001,
                    format!("invalid task_id: {}", task_id_str),
                )),
            )
                .into_response();
        }
    };

    // 点查防枚举（对齐 D7「None→不存在」）：起手解析生效租户，`Err` 原样回传。
    let tenant = match resolve_list_tenant(&state.tenant_resolver, &headers) {
        Ok(t) => t,
        Err(resp) => return resp,
    };
    // 平台主体无 override（None）→ 404「不存在」，防按 task_id 枚举探测。
    let Some(tenant) = tenant else {
        return task_mutation_not_found(task_id);
    };

    let mut engine = state.engine.write().await;
    // A3：按 `StreamTask.tenant_id` 比对；跨租户 / 不存在 → 404（与「真的没有」同语义）。
    match engine.get_task(task_id) {
        Some(task) if task.tenant_id == tenant => match engine.drop_task(task_id).await {
            Ok(()) => (
                StatusCode::OK,
                Json(ApiResponse::success(format!(
                    "stream task {} deleted",
                    task_id
                ))),
            )
                .into_response(),
            Err(e) => (
                StatusCode::NOT_FOUND,
                Json(ApiResponse::<String>::error(40401, format!("{}", e))),
            )
                .into_response(),
        },
        Some(_) | None => task_mutation_not_found(task_id),
    }
}

/// POST /api/v1/stream/tasks/:task_id/pause
async fn pause_task(
    State(state): State<Arc<StreamApiState>>,
    Path(task_id_str): Path<String>,
    headers: HeaderMap,
) -> impl IntoResponse {
    let task_id = match uuid::Uuid::parse_str(&task_id_str) {
        Ok(id) => id,
        Err(_) => {
            return (
                StatusCode::BAD_REQUEST,
                Json(ApiResponse::<String>::error(
                    40001,
                    format!("invalid task_id: {}", task_id_str),
                )),
            )
                .into_response();
        }
    };

    // 点查防枚举（对齐 D7「None→不存在」）：起手解析生效租户，`Err` 原样回传。
    let tenant = match resolve_list_tenant(&state.tenant_resolver, &headers) {
        Ok(t) => t,
        Err(resp) => return resp,
    };
    // 平台主体无 override（None）→ 404「不存在」，防按 task_id 枚举探测。
    let Some(tenant) = tenant else {
        return task_mutation_not_found(task_id);
    };

    let mut engine = state.engine.write().await;
    // A3：按 `StreamTask.tenant_id` 比对；跨租户 / 不存在 → 404（与「真的没有」同语义）。
    match engine.get_task(task_id) {
        Some(task) if task.tenant_id == tenant => match engine.pause_task(task_id).await {
            Ok(()) => (
                StatusCode::OK,
                Json(ApiResponse::success(format!(
                    "stream task {} paused",
                    task_id
                ))),
            )
                .into_response(),
            Err(e) => (
                StatusCode::NOT_FOUND,
                Json(ApiResponse::<String>::error(40401, format!("{}", e))),
            )
                .into_response(),
        },
        Some(_) | None => task_mutation_not_found(task_id),
    }
}

/// POST /api/v1/stream/tasks/:task_id/resume
async fn resume_task(
    State(state): State<Arc<StreamApiState>>,
    Path(task_id_str): Path<String>,
    headers: HeaderMap,
) -> impl IntoResponse {
    let task_id = match uuid::Uuid::parse_str(&task_id_str) {
        Ok(id) => id,
        Err(_) => {
            return (
                StatusCode::BAD_REQUEST,
                Json(ApiResponse::<String>::error(
                    40001,
                    format!("invalid task_id: {}", task_id_str),
                )),
            )
                .into_response();
        }
    };

    // 点查防枚举（对齐 D7「None→不存在」）：起手解析生效租户，`Err` 原样回传。
    let tenant = match resolve_list_tenant(&state.tenant_resolver, &headers) {
        Ok(t) => t,
        Err(resp) => return resp,
    };
    // 平台主体无 override（None）→ 404「不存在」，防按 task_id 枚举探测。
    let Some(tenant) = tenant else {
        return task_mutation_not_found(task_id);
    };

    let mut engine = state.engine.write().await;
    // A3：按 `StreamTask.tenant_id` 比对；跨租户 / 不存在 → 404（与「真的没有」同语义）。
    match engine.get_task(task_id) {
        Some(task) if task.tenant_id == tenant => match engine.resume_task(task_id).await {
            Ok(()) => (
                StatusCode::OK,
                Json(ApiResponse::success(format!(
                    "stream task {} resumed",
                    task_id
                ))),
            )
                .into_response(),
            Err(e) => (
                StatusCode::NOT_FOUND,
                Json(ApiResponse::<String>::error(40401, format!("{}", e))),
            )
                .into_response(),
        },
        Some(_) | None => task_mutation_not_found(task_id),
    }
}

/// GET /api/v1/stream-tasks —— 前端 `PagedResult<StreamTask>` 形状。
async fn list_stream_tasks(
    State(state): State<Arc<StreamApiState>>,
    headers: HeaderMap,
) -> impl IntoResponse {
    let tenant = match resolve_list_tenant(&state.tenant_resolver, &headers) {
        Ok(t) => t,
        Err(resp) => return resp,
    };

    let engine = state.engine.read().await;
    // D7：`None` → 空列表；`Some(t)` → 仅归属 t 的任务。
    let tasks = match tenant {
        None => Vec::new(),
        Some(t) => engine.get_tasks_for_tenant(t),
    };

    let items: Vec<FrontendStreamTask> = tasks
        .iter()
        .map(|task| {
            let (status, error_message) = task_status_to_frontend(&task.status);
            let source = state
                .catalog
                .table_name(task.source_table_id)
                .unwrap_or_else(|| task.source_table_id.to_string());
            let sink = state
                .catalog
                .table_name(task.target_table_id)
                .unwrap_or_else(|| task.target_table_id.to_string());

            FrontendStreamTask {
                id: task.id.to_string(),
                name: task.name.clone(),
                source,
                sink,
                status: status.to_string(),
                throughput_pts_per_sec: None,
                lag_ms: None,
                started_at: None,
                error_message,
            }
        })
        .collect();

    let total = items.len();
    (
        StatusCode::OK,
        Json(FrontendStreamTaskList {
            items,
            total,
            page: 1,
            page_size: total.max(1),
        }),
    )
        .into_response()
}

/// POST /api/v1/stream-tasks/:task_id/start —— 复用 resume 语义。
async fn start_stream_task(
    State(state): State<Arc<StreamApiState>>,
    Path(task_id_str): Path<String>,
    headers: HeaderMap,
) -> impl IntoResponse {
    let task_id = match uuid::Uuid::parse_str(&task_id_str) {
        Ok(id) => id,
        Err(_) => {
            return (
                StatusCode::BAD_REQUEST,
                Json(ApiResponse::<String>::error(
                    40001,
                    format!("invalid task_id: {}", task_id_str),
                )),
            )
                .into_response();
        }
    };

    // 点查防枚举（对齐 D7「None→不存在」）：起手解析生效租户，`Err` 原样回传。
    let tenant = match resolve_list_tenant(&state.tenant_resolver, &headers) {
        Ok(t) => t,
        Err(resp) => return resp,
    };
    // 平台主体无 override（None）→ 404「不存在」，防按 task_id 枚举探测。
    let Some(tenant) = tenant else {
        return task_mutation_not_found(task_id);
    };

    let mut engine = state.engine.write().await;
    // A3：按 `StreamTask.tenant_id` 比对；跨租户 / 不存在 → 404（与「真的没有」同语义）。
    match engine.get_task(task_id) {
        Some(task) if task.tenant_id == tenant => match engine.resume_task(task_id).await {
            Ok(()) => (
                StatusCode::OK,
                Json(ApiResponse::success(format!(
                    "stream task {} started",
                    task_id
                ))),
            )
                .into_response(),
            Err(e) => (
                StatusCode::NOT_FOUND,
                Json(ApiResponse::<String>::error(40401, format!("{}", e))),
            )
                .into_response(),
        },
        Some(_) | None => task_mutation_not_found(task_id),
    }
}

/// POST /api/v1/stream-tasks/:task_id/stop —— 复用 pause 语义。
async fn stop_stream_task(
    State(state): State<Arc<StreamApiState>>,
    Path(task_id_str): Path<String>,
    headers: HeaderMap,
) -> impl IntoResponse {
    let task_id = match uuid::Uuid::parse_str(&task_id_str) {
        Ok(id) => id,
        Err(_) => {
            return (
                StatusCode::BAD_REQUEST,
                Json(ApiResponse::<String>::error(
                    40001,
                    format!("invalid task_id: {}", task_id_str),
                )),
            )
                .into_response();
        }
    };

    // 点查防枚举（对齐 D7「None→不存在」）：起手解析生效租户，`Err` 原样回传。
    let tenant = match resolve_list_tenant(&state.tenant_resolver, &headers) {
        Ok(t) => t,
        Err(resp) => return resp,
    };
    // 平台主体无 override（None）→ 404「不存在」，防按 task_id 枚举探测。
    let Some(tenant) = tenant else {
        return task_mutation_not_found(task_id);
    };

    let mut engine = state.engine.write().await;
    // A3：按 `StreamTask.tenant_id` 比对；跨租户 / 不存在 → 404（与「真的没有」同语义）。
    match engine.get_task(task_id) {
        Some(task) if task.tenant_id == tenant => match engine.pause_task(task_id).await {
            Ok(()) => (
                StatusCode::OK,
                Json(ApiResponse::success(format!(
                    "stream task {} stopped",
                    task_id
                ))),
            )
                .into_response(),
            Err(e) => (
                StatusCode::NOT_FOUND,
                Json(ApiResponse::<String>::error(40401, format!("{}", e))),
            )
                .into_response(),
        },
        Some(_) | None => task_mutation_not_found(task_id),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::task::{ProjectionColumn, ProjectionSpec, TargetColumnDef, WindowSpec};
    use sensearcdb_common::{AppConfig, ConfigHandle};

    #[test]
    fn test_api_response_success() {
        let resp = ApiResponse::success("hello");
        assert_eq!(resp.code, 0);
        assert_eq!(resp.data, Some("hello"));
    }

    #[test]
    fn test_api_response_error() {
        let resp = ApiResponse::<String>::error(40401, "not found");
        assert_eq!(resp.code, 40401);
        assert!(resp.data.is_none());
    }

    // ─── 读侧隔离 handler 级测试（D7 防枚举分支）──────────────────────────

    struct NoopSink;
    impl crate::StreamSink for NoopSink {
        fn write_batch(
            &self,
            _target_table_id: u32,
            _rows: &[(i64, Vec<sensearcdb_common::TypedValue>)],
        ) -> Result<(), String> {
            Ok(())
        }
    }

    struct NoopCatalog;
    impl crate::StreamCatalog for NoopCatalog {
        fn resolve_table(&self, _tenant_id: TenantId, _table_name: &str) -> Option<u32> {
            None
        }
        fn resolve_column(&self, _table_id: u32, _column_name: &str) -> Option<u16> {
            None
        }
        fn resolve_column_type(&self, _table_id: u32, _column_name: &str) -> Option<DataType> {
            None
        }
        fn auto_create_target_table(
            &self,
            _tenant_id: TenantId,
            _table_name: &str,
            _columns: &[(String, DataType)],
        ) -> Result<u32, String> {
            Ok(0)
        }
        fn table_exists(&self, _table_id: u32) -> bool {
            false
        }
    }

    fn make_test_config() -> ConfigHandle {
        let mut cfg = AppConfig::default();
        cfg.stream.enabled = true;
        cfg.stream.channel_capacity = 100;
        cfg.stream.max_partitions_per_task = 100;
        ConfigHandle::new(cfg)
    }

    fn make_test_projection() -> ProjectionSpec {
        ProjectionSpec {
            columns: vec![ProjectionColumn::Aggregate {
                function: sensearcdb_common::AggFunction::Avg,
                source_column: 1,
                alias: "avg_val".into(),
            }],
            target_schema: vec![TargetColumnDef {
                column_id: 1,
                column_name: "avg_val".into(),
                data_type: DataType::F64,
            }],
        }
    }

    /// 构造 `StreamApiState`：engine 内预置一条 tenant=1 的任务，resolver 固定返回 `tenant`。
    async fn make_state(tenant: Option<TenantId>) -> (Arc<StreamApiState>, uuid::Uuid) {
        let path =
            std::env::temp_dir().join(format!("test_stream_rest_{}.db", uuid::Uuid::new_v4()));
        let store = Arc::new(crate::StreamTaskStore::open(&path).unwrap());
        let sink: Arc<dyn crate::StreamSink> = Arc::new(NoopSink);
        let mut engine = StreamComputeEngine::new(make_test_config(), sink, store);

        let task_id = engine
            .create_task(
                "task_a".into(),
                TenantId(1),
                10, // source_table_id
                20, // target_table_id
                WindowSpec::Tumbling {
                    interval_ms: 10_000,
                },
                make_test_projection(),
                None,
            )
            .await
            .unwrap();

        let resolver: TenantResolver = Arc::new(move |_headers: &HeaderMap| Ok(tenant));
        let state = Arc::new(StreamApiState {
            engine: Arc::new(TokioRwLock::new(engine)),
            catalog: Arc::new(NoopCatalog),
            tenant_resolver: resolver,
        });
        (state, task_id)
    }

    async fn body_json(resp: axum::response::Response) -> serde_json::Value {
        let bytes = axum::body::to_bytes(resp.into_body(), usize::MAX)
            .await
            .expect("read response body");
        serde_json::from_slice(&bytes).expect("response body must be JSON")
    }

    /// D7「None→空」：租户解析器返回 `None`（平台主体无 override）→ 列表返回空，防枚举。
    #[tokio::test]
    async fn test_list_tasks_none_tenant_returns_empty() {
        let (state, _task_id) = make_state(None).await;

        let resp = list_tasks(State(state), HeaderMap::new())
            .await
            .into_response();
        assert_eq!(resp.status(), StatusCode::OK);
        let body = body_json(resp).await;
        let data = body["data"].as_array().expect("data must be an array");
        assert!(data.is_empty(), "None 租户必须返回空列表，防枚举");
    }

    /// 点查同租户：租户解析器返回 tenant=1，任务 tenant=1 → 200 + 正常任务详情。
    #[tokio::test]
    async fn test_get_task_same_tenant_returns_ok() {
        let (state, task_id) = make_state(Some(TenantId(1))).await;

        let resp = get_task(State(state), Path(task_id.to_string()), HeaderMap::new())
            .await
            .into_response();
        assert_eq!(resp.status(), StatusCode::OK);
        let body = body_json(resp).await;
        assert_eq!(body["data"]["task_id"], task_id.to_string());
        assert_eq!(body["data"]["name"], "task_a");
    }

    /// 点查跨租户（问题 2 修复）：租户解析器返回 tenant=2，任务 tenant=1 → 404「不存在」。
    #[tokio::test]
    async fn test_get_task_cross_tenant_returns_not_found() {
        let (state, task_id) = make_state(Some(TenantId(2))).await;

        let resp = get_task(State(state), Path(task_id.to_string()), HeaderMap::new())
            .await
            .into_response();
        assert_eq!(resp.status(), StatusCode::NOT_FOUND);
        let body = body_json(resp).await;
        assert_eq!(body["code"], 40401);
        assert!(body["data"].is_null(), "跨租户点查不得泄露任务详情");
    }

    /// 点查 None 租户（平台主体无 override）→ 404「不存在」，防枚举。
    #[tokio::test]
    async fn test_get_task_none_tenant_returns_not_found() {
        let (state, task_id) = make_state(None).await;

        let resp = get_task(State(state), Path(task_id.to_string()), HeaderMap::new())
            .await
            .into_response();
        assert_eq!(resp.status(), StatusCode::NOT_FOUND);
        let body = body_json(resp).await;
        assert_eq!(body["code"], 40401);
        assert!(body["data"].is_null(), "None 租户点查不得泄露任务详情");
    }

    // ─── 变更类端点 tenant 过滤（问题 3 补完）handler 级测试 ──────────────
    // 任务归属 tenant=1；resolver 返回 tenant=2 为跨租户，tenant=1 为同租户。

    /// 同租户 delete：请求 tenant=1，任务 tenant=1 → 200。
    #[tokio::test]
    async fn test_delete_task_same_tenant_returns_ok() {
        let (state, task_id) = make_state(Some(TenantId(1))).await;

        let resp = delete_task(State(state), Path(task_id.to_string()), HeaderMap::new())
            .await
            .into_response();
        assert_eq!(resp.status(), StatusCode::OK);
        let body = body_json(resp).await;
        assert_eq!(body["code"], 0);
    }

    /// 跨租户 delete：请求 tenant=2，任务 tenant=1 → 404「不存在」，防枚举。
    #[tokio::test]
    async fn test_delete_task_cross_tenant_returns_not_found() {
        let (state, task_id) = make_state(Some(TenantId(2))).await;

        let resp = delete_task(State(state), Path(task_id.to_string()), HeaderMap::new())
            .await
            .into_response();
        assert_eq!(resp.status(), StatusCode::NOT_FOUND);
        let body = body_json(resp).await;
        assert_eq!(body["code"], 40401);
        assert!(body["data"].is_null(), "跨租户 delete 不得泄露任务存在性");
    }

    /// 同租户 pause：请求 tenant=1，任务 tenant=1 → 200。
    #[tokio::test]
    async fn test_pause_task_same_tenant_returns_ok() {
        let (state, task_id) = make_state(Some(TenantId(1))).await;

        let resp = pause_task(State(state), Path(task_id.to_string()), HeaderMap::new())
            .await
            .into_response();
        assert_eq!(resp.status(), StatusCode::OK);
        let body = body_json(resp).await;
        assert_eq!(body["code"], 0);
    }

    /// 跨租户 pause：请求 tenant=2，任务 tenant=1 → 404「不存在」，防枚举。
    #[tokio::test]
    async fn test_pause_task_cross_tenant_returns_not_found() {
        let (state, task_id) = make_state(Some(TenantId(2))).await;

        let resp = pause_task(State(state), Path(task_id.to_string()), HeaderMap::new())
            .await
            .into_response();
        assert_eq!(resp.status(), StatusCode::NOT_FOUND);
        let body = body_json(resp).await;
        assert_eq!(body["code"], 40401);
        assert!(body["data"].is_null(), "跨租户 pause 不得泄露任务存在性");
    }

    /// 同租户 resume：请求 tenant=1，任务 tenant=1 → 200。
    #[tokio::test]
    async fn test_resume_task_same_tenant_returns_ok() {
        let (state, task_id) = make_state(Some(TenantId(1))).await;

        let resp = resume_task(State(state), Path(task_id.to_string()), HeaderMap::new())
            .await
            .into_response();
        assert_eq!(resp.status(), StatusCode::OK);
        let body = body_json(resp).await;
        assert_eq!(body["code"], 0);
    }

    /// 跨租户 resume：请求 tenant=2，任务 tenant=1 → 404「不存在」，防枚举。
    #[tokio::test]
    async fn test_resume_task_cross_tenant_returns_not_found() {
        let (state, task_id) = make_state(Some(TenantId(2))).await;

        let resp = resume_task(State(state), Path(task_id.to_string()), HeaderMap::new())
            .await
            .into_response();
        assert_eq!(resp.status(), StatusCode::NOT_FOUND);
        let body = body_json(resp).await;
        assert_eq!(body["code"], 40401);
        assert!(body["data"].is_null(), "跨租户 resume 不得泄露任务存在性");
    }

    /// 同租户 start：请求 tenant=1，任务 tenant=1 → 200。
    #[tokio::test]
    async fn test_start_stream_task_same_tenant_returns_ok() {
        let (state, task_id) = make_state(Some(TenantId(1))).await;

        let resp = start_stream_task(State(state), Path(task_id.to_string()), HeaderMap::new())
            .await
            .into_response();
        assert_eq!(resp.status(), StatusCode::OK);
        let body = body_json(resp).await;
        assert_eq!(body["code"], 0);
    }

    /// 跨租户 start：请求 tenant=2，任务 tenant=1 → 404「不存在」，防枚举。
    #[tokio::test]
    async fn test_start_stream_task_cross_tenant_returns_not_found() {
        let (state, task_id) = make_state(Some(TenantId(2))).await;

        let resp = start_stream_task(State(state), Path(task_id.to_string()), HeaderMap::new())
            .await
            .into_response();
        assert_eq!(resp.status(), StatusCode::NOT_FOUND);
        let body = body_json(resp).await;
        assert_eq!(body["code"], 40401);
        assert!(body["data"].is_null(), "跨租户 start 不得泄露任务存在性");
    }

    /// 同租户 stop：请求 tenant=1，任务 tenant=1 → 200。
    #[tokio::test]
    async fn test_stop_stream_task_same_tenant_returns_ok() {
        let (state, task_id) = make_state(Some(TenantId(1))).await;

        let resp = stop_stream_task(State(state), Path(task_id.to_string()), HeaderMap::new())
            .await
            .into_response();
        assert_eq!(resp.status(), StatusCode::OK);
        let body = body_json(resp).await;
        assert_eq!(body["code"], 0);
    }

    /// 跨租户 stop：请求 tenant=2，任务 tenant=1 → 404「不存在」，防枚举。
    #[tokio::test]
    async fn test_stop_stream_task_cross_tenant_returns_not_found() {
        let (state, task_id) = make_state(Some(TenantId(2))).await;

        let resp = stop_stream_task(State(state), Path(task_id.to_string()), HeaderMap::new())
            .await
            .into_response();
        assert_eq!(resp.status(), StatusCode::NOT_FOUND);
        let body = body_json(resp).await;
        assert_eq!(body["code"], 40401);
        assert!(body["data"].is_null(), "跨租户 stop 不得泄露任务存在性");
    }
}
