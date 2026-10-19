//! Schema REST API 端点。
//!
//! 基于 axum 框架，路径前缀 `/api/v1/schema`。
//!
//! # 端点列表
//! - `POST   /api/v1/schema/tables`              创建表
//! - `GET    /api/v1/schema/tables`              列出所有表
//! - `GET    /api/v1/schema/tables/:id`          获取单表
//! - `PUT    /api/v1/schema/tables/:id`          修改表结构
//! - `DELETE /api/v1/schema/tables/:id`          删除表
//! - `POST   /api/v1/schema/tables/:id/columns`  添加列
//! - `PUT    /api/v1/schema/tables/:id/columns/:name`  修改列
//! - `DELETE /api/v1/schema/tables/:id/columns/:name`  删除列

use crate::manager::SchemaManager;
use crate::mirror::{ColumnDef, DeadbandConfig, DeadbandType, TableInfo, TableType};
use axum::{
    extract::{Path, State},
    http::{HeaderMap, StatusCode},
    response::IntoResponse,
    Json, Router,
};
use sensearcdb_common::route_registry::RouteRegistryExt;
use sensearcdb_common::{DataType, Durability, SenseArcError, TenantId, TimestampNanos};
use serde::{Deserialize, Serialize};
use std::sync::Arc;

/// 请求租户解析器（bin 层注入，避免 schema crate 反向依赖 bin 的 principal 提取）。
///
/// `Err` 用于回传「非平台主体 override / API Key override → 403」「未知 slug → 404」，
/// 由调用方映射为 HTTP 状态，而非静默回退 `TenantId::DEFAULT`。
pub type TenantResolver =
    Arc<dyn Fn(&HeaderMap) -> Result<Option<TenantId>, SenseArcError> + Send + Sync>;

/// 共享 Schema API 状态（`SchemaManager` + 租户解析器）。
pub struct SchemaApiState {
    pub mgr: Arc<SchemaManager>,
    pub tenant_resolver: TenantResolver,
}

/// 共享的 SchemaManager 状态（axum 提取类型）。
pub type AppState = Arc<SchemaApiState>;

/// 构建 Schema REST 路由（默认 create_table 直写 SchemaManager）。
pub fn create_router(state: Arc<SchemaManager>, tenant_resolver: TenantResolver) -> Router {
    create_router_with(state, tenant_resolver, create_table)
}

/// 构建 Schema REST 路由，支持注入自定义 create_table handler（T5b S7）。
///
/// shard 模式下 main 二进制注入「走 MetaShard 两阶段」的 handler，与默认直写
/// 共享同一 `POST /api/v1/schema/tables` 路径（单一路由、无 merge 冲突，避免
/// axum 0.7 对同 method+path 重复注册的 panic）。
///
/// # Arguments
/// - `state`: 共享的 `SchemaManager` 状态（读端点 + 默认 create 都用它）。
/// - `tenant_resolver`: bin 层注入的请求租户解析器。
/// - `create_handler`: `POST /api/v1/schema/tables` 的 handler 函数（而非预构建
///   `MethodRouter`），内部经 `route!` 宏与 `GET list_tables` 同源派生方法，
///   关闭 `RouteSpec::explicit` 的手工写方法窄口。
pub fn create_router_with<H, T>(
    state: Arc<SchemaManager>,
    tenant_resolver: TenantResolver,
    create_handler: H,
) -> Router
where
    H: axum::handler::Handler<T, AppState>,
    T: 'static,
{
    let api_state = Arc::new(SchemaApiState {
        mgr: state,
        tenant_resolver,
    });
    Router::new()
        .register_route(
            "/api/v1/schema/tables",
            sensearcdb_common::route!(post(create_handler), get(list_tables)),
        )
        .register_route(
            "/api/v1/schema/tables/:table_id",
            sensearcdb_common::route!(get(get_table), put(alter_table), delete(drop_table)),
        )
        .register_route(
            "/api/v1/schema/tables/:table_id/columns",
            sensearcdb_common::route!(post(add_column)),
        )
        .register_route(
            "/api/v1/schema/tables/:table_id/columns/:column_name",
            sensearcdb_common::route!(put(alter_column), delete(drop_column)),
        )
        // 前端契约别名：`/api/v1/schemas*`（console/src/api/index.ts:1188-1215）。
        .register_route(
            "/api/v1/schemas",
            sensearcdb_common::route!(get(list_schemas), post(create_schema)),
        )
        .register_route(
            "/api/v1/schemas/:id",
            sensearcdb_common::route!(get(get_schema), put(update_schema), delete(delete_schema)),
        )
        .with_state(api_state)
}

// ─── Request / Response types ─────────────────────────────────────────────

#[derive(Debug, Deserialize)]
struct CreateTableRequest {
    table_name: String,
    #[serde(default)]
    columns: Vec<ColumnDefRequest>,
    #[serde(default)]
    table_group: Option<String>,
    #[serde(default = "default_table_type")]
    table_type: String,
    #[serde(default = "default_durability")]
    durability: String,
    #[serde(default)]
    description: Option<String>,
    #[serde(default = "default_true")]
    enabled: bool,
}

fn default_table_type() -> String {
    "RAW".to_string()
}
fn default_durability() -> String {
    "group".to_string()
}
fn default_true() -> bool {
    true
}

#[derive(Debug, Deserialize)]
struct ColumnDefRequest {
    name: String,
    data_type: String,
    #[serde(default)]
    precision: f64,
    #[serde(default)]
    nullable: bool,
    #[serde(default)]
    unit: Option<String>,
    #[serde(default)]
    description: Option<String>,
    #[serde(default)]
    deadband: Option<DeadbandRequest>,
    #[serde(default)]
    enum_values: Option<Vec<String>>,
    #[serde(default)]
    range_low: f64,
    #[serde(default = "default_range_high")]
    range_high: f64,
    #[serde(default)]
    instrument_range_low: Option<f64>,
    #[serde(default)]
    instrument_range_high: Option<f64>,
    #[serde(default = "default_display_digits")]
    display_digits: u16,
}

fn default_range_high() -> f64 {
    100.0
}
fn default_display_digits() -> u16 {
    2
}

#[derive(Debug, Deserialize)]
struct DeadbandRequest {
    #[serde(default = "default_deadband_type")]
    deadband_type: String,
    value: f64,
}

fn default_deadband_type() -> String {
    "NONE".to_string()
}

#[derive(Debug, Deserialize)]
struct AlterTableRequest {
    #[serde(default)]
    columns: Vec<ColumnDefRequest>,
}

#[derive(Debug, Deserialize)]
struct AlterColumnRequest {
    #[serde(default)]
    name: Option<String>,
    #[serde(default)]
    data_type: Option<String>,
    #[serde(default)]
    precision: Option<f64>,
    #[serde(default)]
    nullable: Option<bool>,
    #[serde(default)]
    unit: Option<Option<String>>,
    #[serde(default)]
    description: Option<Option<String>>,
    #[serde(default)]
    deadband: Option<Option<DeadbandRequest>>,
    #[serde(default)]
    range_low: Option<f64>,
    #[serde(default)]
    range_high: Option<f64>,
    #[serde(default)]
    display_digits: Option<u16>,
    #[serde(default)]
    instrument_range_low: Option<Option<f64>>,
    #[serde(default)]
    instrument_range_high: Option<Option<f64>>,
}

#[derive(Debug, Serialize)]
struct TableResponse {
    table_id: u32,
    table_name: String,
    schema_id: u32,
    schema_version: u32,
    table_group: Option<String>,
    table_type: String,
    durability: String,
    description: Option<String>,
    enabled: bool,
    columns: Vec<ColumnResponse>,
    deleted_at: Option<i64>,
    created_at: i64,
    updated_at: i64,
}

#[derive(Debug, Serialize)]
struct ColumnResponse {
    column_id: u16,
    name: String,
    data_type: String,
    precision: f64,
    nullable: bool,
    unit: Option<String>,
    description: Option<String>,
    deadband: Option<DeadbandResponse>,
    enum_values: Option<Vec<String>>,
    range_low: f64,
    range_high: f64,
    instrument_range_low: Option<f64>,
    instrument_range_high: Option<f64>,
    display_digits: u16,
}

#[derive(Debug, Serialize)]
struct DeadbandResponse {
    deadband_type: String,
    value: f64,
}

#[derive(Debug, Serialize)]
struct ErrorResponse {
    code: u32,
    message: String,
}

#[derive(Debug, Serialize)]
struct ListTablesResponse {
    tables: Vec<TableResponse>,
}

#[derive(Debug, Serialize)]
struct DropTableResponse {
    table_id: u32,
    deleted: bool,
}

// ─── 前端契约对齐 DTO（`/api/v1/schemas*`，console/src/api/index.ts）──────

/// 前端 `SchemaField`（index.ts:311-318）。
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct FrontendSchemaField {
    name: String,
    data_type: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    unit: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    description: Option<String>,
    nullable: bool,
}

/// 前端 `SchemaDefinition`（index.ts:320-328）。
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct FrontendSchemaDefinition {
    id: String,
    name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    description: Option<String>,
    fields: Vec<FrontendSchemaField>,
    created_at: i64,
    updated_at: i64,
}

/// 前端 `PagedResult<SchemaDefinition>`（index.ts:130-136）。
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct FrontendSchemaList {
    items: Vec<FrontendSchemaDefinition>,
    total: usize,
    page: usize,
    page_size: usize,
}

/// 前端 `SchemaPayload`（index.ts:330-335）——创建/更新请求。
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct CreateSchemaRequest {
    name: String,
    #[serde(default)]
    description: Option<String>,
    #[serde(default)]
    fields: Vec<SchemaFieldRequest>,
}

/// 前端 `SchemaField`（请求侧）。
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct SchemaFieldRequest {
    name: String,
    data_type: String,
    #[serde(default)]
    unit: Option<String>,
    #[serde(default)]
    description: Option<String>,
    #[serde(default)]
    nullable: bool,
}

/// 前端 `Partial<SchemaPayload>`——更新请求（字段均可选）。
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct UpdateSchemaRequest {
    #[serde(default)]
    name: Option<String>,
    #[serde(default)]
    description: Option<String>,
    #[serde(default)]
    fields: Option<Vec<SchemaFieldRequest>>,
}

// ─── Handlers ─────────────────────────────────────────────────────────────

async fn create_table(
    State(api): State<AppState>,
    headers: HeaderMap,
    Json(req): Json<CreateTableRequest>,
) -> Result<impl IntoResponse, AppError> {
    let mgr = &api.mgr;
    let tenant_id = match (api.tenant_resolver)(&headers) {
        Ok(Some(t)) => t,
        Ok(None) => TenantId::DEFAULT,
        Err(e) => return Err(AppError(e)),
    };
    let columns: Vec<ColumnDef> = req
        .columns
        .iter()
        .enumerate()
        .map(|(i, c)| column_request_to_def(i as u16, c))
        .collect();

    let table_type = TableType::from_str(&req.table_type).unwrap_or(TableType::Raw);
    let durability = Durability::from_str(&req.durability);

    let info = TableInfo {
        table_id: 0,
        tenant_id,
        table_name: req.table_name,
        schema_id: 0,
        schema_version: 1,
        table_group: req.table_group,
        table_type,
        durability,
        description: req.description,
        enabled: req.enabled,
        columns,
        deleted_at: None,
        created_at: now_nanos(),
        updated_at: now_nanos(),
    };

    let created = mgr.create_table(info).await?;
    Ok((StatusCode::CREATED, Json(table_info_to_response(&created))))
}

async fn list_tables(State(api): State<AppState>) -> Result<impl IntoResponse, AppError> {
    let mgr = &api.mgr;
    let tables: Vec<TableResponse> = mgr
        .list_tables()
        .iter()
        .map(table_info_to_response)
        .collect();
    Ok(Json(ListTablesResponse { tables }))
}

async fn get_table(
    State(api): State<AppState>,
    Path(table_id): Path<u32>,
) -> Result<impl IntoResponse, AppError> {
    let mgr = &api.mgr;
    let info = mgr
        .get_table(table_id)
        .ok_or(SenseArcError::TableNotFound(table_id))?;
    Ok(Json(table_info_to_response(&info)))
}

async fn alter_table(
    State(api): State<AppState>,
    Path(table_id): Path<u32>,
    Json(req): Json<AlterTableRequest>,
) -> Result<impl IntoResponse, AppError> {
    let mgr = &api.mgr;
    let new_columns: Vec<ColumnDef> = req
        .columns
        .iter()
        .enumerate()
        .map(|(i, c)| column_request_to_def(i as u16, c))
        .collect();

    let updated = mgr.alter_table(table_id, new_columns).await?;
    Ok(Json(table_info_to_response(&updated)))
}

async fn drop_table(
    State(api): State<AppState>,
    Path(table_id): Path<u32>,
) -> Result<impl IntoResponse, AppError> {
    let mgr = &api.mgr;
    mgr.drop_table(table_id).await?;
    Ok(Json(DropTableResponse {
        table_id,
        deleted: true,
    }))
}

async fn add_column(
    State(api): State<AppState>,
    Path(table_id): Path<u32>,
    Json(req): Json<ColumnDefRequest>,
) -> Result<impl IntoResponse, AppError> {
    let mgr = &api.mgr;
    let col = column_request_to_def(0, &req);
    let added = mgr.add_column(table_id, col).await?;
    Ok((StatusCode::CREATED, Json(column_to_response(&added))))
}

async fn alter_column(
    State(api): State<AppState>,
    Path((table_id, column_name)): Path<(u32, String)>,
    Json(req): Json<AlterColumnRequest>,
) -> Result<impl IntoResponse, AppError> {
    let mgr = &api.mgr;
    let existing = mgr
        .get_table(table_id)
        .ok_or(SenseArcError::TableNotFound(table_id))?;

    let old_col = existing
        .columns
        .iter()
        .find(|c| c.name == column_name && c.deleted_at.is_none())
        .ok_or_else(|| SenseArcError::ColumnNotFound(0))?;

    let new_name = req.name.unwrap_or_else(|| old_col.name.clone());
    let data_type = req
        .data_type
        .map(|s| parse_data_type(&s))
        .transpose()?
        .unwrap_or(old_col.data_type);
    let deadband = req
        .deadband
        .map(|opt| {
            opt.map(|d| DeadbandConfig {
                deadband_type: DeadbandType::from_str(&d.deadband_type)
                    .unwrap_or(DeadbandType::None),
                value: d.value,
            })
        })
        .unwrap_or_else(|| old_col.deadband.clone());

    let new_col = ColumnDef {
        column_id: old_col.column_id,
        name: new_name,
        data_type,
        precision: req.precision.unwrap_or(old_col.precision),
        nullable: req.nullable.unwrap_or(old_col.nullable),
        unit: req.unit.unwrap_or_else(|| old_col.unit.clone()),
        description: req
            .description
            .unwrap_or_else(|| old_col.description.clone()),
        deadband,
        enum_values: old_col.enum_values.clone(),
        deleted_at: None,
        range_low: req.range_low.unwrap_or(old_col.range_low),
        range_high: req.range_high.unwrap_or(old_col.range_high),
        instrument_range_low: req
            .instrument_range_low
            .unwrap_or(old_col.instrument_range_low),
        instrument_range_high: req
            .instrument_range_high
            .unwrap_or(old_col.instrument_range_high),
        display_digits: req.display_digits.unwrap_or(old_col.display_digits),
    };

    mgr.alter_column(table_id, &column_name, new_col).await?;
    Ok(Json(serde_json::json!({ "status": "ok" })))
}

async fn drop_column(
    State(api): State<AppState>,
    Path((table_id, column_name)): Path<(u32, String)>,
) -> Result<impl IntoResponse, AppError> {
    let mgr = &api.mgr;
    mgr.drop_column(table_id, &column_name).await?;
    Ok(Json(serde_json::json!({ "status": "ok" })))
}

// ─── 前端契约别名 handlers（`/api/v1/schemas*`）───────────────────────────
//
// 把 table → SchemaDefinition、column → SchemaField 映射为前端形状。
// 说明：SchemaManager 无 rename/description 更新能力，PUT 仅支持替换 `fields`
// （name/description 变更属 B 类，本轮不硬造）。

/// GET /api/v1/schemas —— 列出全部 schema（前端 `PagedResult<SchemaDefinition>`）。
async fn list_schemas(State(api): State<AppState>) -> Result<impl IntoResponse, AppError> {
    let mgr = &api.mgr;
    let items: Vec<FrontendSchemaDefinition> = mgr
        .list_tables()
        .iter()
        .map(table_info_to_schema_definition)
        .collect();
    let total = items.len();
    Ok(Json(FrontendSchemaList {
        items,
        total,
        page: 1,
        page_size: total.max(1),
    }))
}

/// GET /api/v1/schemas/:id —— 单个 schema 详情。
async fn get_schema(
    State(api): State<AppState>,
    Path(id_str): Path<String>,
) -> Result<impl IntoResponse, AppError> {
    let mgr = &api.mgr;
    let table_id = parse_table_id_str(&id_str)?;
    let info = mgr
        .get_table(table_id)
        .ok_or(SenseArcError::TableNotFound(table_id))?;
    Ok(Json(table_info_to_schema_definition(&info)))
}

/// POST /api/v1/schemas —— 创建 schema（映射为建表）。
async fn create_schema(
    State(api): State<AppState>,
    headers: HeaderMap,
    Json(req): Json<CreateSchemaRequest>,
) -> Result<impl IntoResponse, AppError> {
    let mgr = &api.mgr;
    let tenant_id = match (api.tenant_resolver)(&headers) {
        Ok(Some(t)) => t,
        Ok(None) => TenantId::DEFAULT,
        Err(e) => return Err(AppError(e)),
    };
    let columns: Vec<ColumnDef> = req
        .fields
        .iter()
        .enumerate()
        .map(|(i, f)| ColumnDef {
            column_id: i as u16,
            name: f.name.clone(),
            data_type: schema_str_to_data_type(&f.data_type),
            precision: 0.0,
            nullable: f.nullable,
            unit: f.unit.clone(),
            description: f.description.clone(),
            deadband: None,
            enum_values: None,
            deleted_at: None,
            range_low: 0.0,
            range_high: 0.0,
            instrument_range_low: None,
            instrument_range_high: None,
            display_digits: 2,
        })
        .collect();

    let info = TableInfo {
        table_id: 0,
        tenant_id,
        table_name: req.name,
        schema_id: 0,
        schema_version: 1,
        table_group: None,
        table_type: TableType::Raw,
        durability: Durability::Group,
        description: req.description,
        enabled: true,
        columns,
        deleted_at: None,
        created_at: now_nanos(),
        updated_at: now_nanos(),
    };

    let created = mgr.create_table(info).await?;
    Ok((
        StatusCode::CREATED,
        Json(table_info_to_schema_definition(&created)),
    ))
}

/// PUT /api/v1/schemas/:id —— 更新 schema（仅替换 fields，name/description 属 B 类）。
async fn update_schema(
    State(api): State<AppState>,
    Path(id_str): Path<String>,
    Json(req): Json<UpdateSchemaRequest>,
) -> Result<impl IntoResponse, AppError> {
    let mgr = &api.mgr;
    let table_id = parse_table_id_str(&id_str)?;

    // SchemaManager 无 rename/description 更新能力（B 类）；请求若携带则诚实告知忽略。
    if req.name.is_some() || req.description.is_some() {
        tracing::warn!(
            table_id,
            "schema name/description update not supported by SchemaManager; ignored"
        );
    }

    match req.fields {
        Some(fields) => {
            let columns: Vec<ColumnDef> = fields
                .iter()
                .enumerate()
                .map(|(i, f)| ColumnDef {
                    column_id: i as u16,
                    name: f.name.clone(),
                    data_type: schema_str_to_data_type(&f.data_type),
                    precision: 0.0,
                    nullable: f.nullable,
                    unit: f.unit.clone(),
                    description: f.description.clone(),
                    deadband: None,
                    enum_values: None,
                    deleted_at: None,
                    range_low: 0.0,
                    range_high: 0.0,
                    instrument_range_low: None,
                    instrument_range_high: None,
                    display_digits: 2,
                })
                .collect();
            let updated = mgr.alter_table(table_id, columns).await?;
            Ok(Json(table_info_to_schema_definition(&updated)))
        }
        None => {
            let info = mgr
                .get_table(table_id)
                .ok_or(SenseArcError::TableNotFound(table_id))?;
            Ok(Json(table_info_to_schema_definition(&info)))
        }
    }
}

/// DELETE /api/v1/schemas/:id —— 删除 schema。
async fn delete_schema(
    State(api): State<AppState>,
    Path(id_str): Path<String>,
) -> Result<impl IntoResponse, AppError> {
    let mgr = &api.mgr;
    let table_id = parse_table_id_str(&id_str)?;
    mgr.drop_table(table_id).await?;
    Ok(Json(serde_json::json!({ "deleted": true })))
}

// ─── Conversion helpers ───────────────────────────────────────────────────

fn column_request_to_def(column_id: u16, req: &ColumnDefRequest) -> ColumnDef {
    ColumnDef {
        column_id,
        name: req.name.clone(),
        data_type: parse_data_type(&req.data_type).unwrap_or(DataType::F64),
        precision: req.precision,
        nullable: req.nullable,
        unit: req.unit.clone(),
        description: req.description.clone(),
        deadband: req.deadband.as_ref().map(|d| DeadbandConfig {
            deadband_type: DeadbandType::from_str(&d.deadband_type).unwrap_or(DeadbandType::None),
            value: d.value,
        }),
        enum_values: req.enum_values.clone(),
        deleted_at: None,
        range_low: req.range_low,
        range_high: req.range_high,
        instrument_range_low: req.instrument_range_low,
        instrument_range_high: req.instrument_range_high,
        display_digits: req.display_digits,
    }
}

fn parse_data_type(s: &str) -> Result<DataType, SenseArcError> {
    match s.to_uppercase().as_str() {
        "F64" => Ok(DataType::F64),
        "F32" => Ok(DataType::F32),
        "I64" => Ok(DataType::I64),
        "U64" => Ok(DataType::U64),
        "I32" => Ok(DataType::I32),
        "BOOL" => Ok(DataType::Bool),
        "STRING" => Ok(DataType::String),
        "ENUM16" => Ok(DataType::Enum16),
        other => Err(SenseArcError::InvalidDataType(format!(
            "unknown data type: {}",
            other
        ))),
    }
}

fn data_type_to_str(dt: DataType) -> &'static str {
    match dt {
        DataType::F64 => "F64",
        DataType::F32 => "F32",
        DataType::I64 => "I64",
        DataType::U64 => "U64",
        DataType::I32 => "I32",
        DataType::Bool => "BOOL",
        DataType::String => "STRING",
        DataType::Enum16 => "ENUM16",
    }
}

fn table_info_to_response(info: &TableInfo) -> TableResponse {
    TableResponse {
        table_id: info.table_id,
        table_name: info.table_name.clone(),
        schema_id: info.schema_id,
        schema_version: info.schema_version,
        table_group: info.table_group.clone(),
        table_type: info.table_type.as_str().to_string(),
        durability: info.durability.as_str().to_string(),
        description: info.description.clone(),
        enabled: info.enabled,
        columns: info.columns.iter().map(column_to_response).collect(),
        deleted_at: info.deleted_at.map(|ts| ts.as_nanos()),
        created_at: info.created_at.as_nanos(),
        updated_at: info.updated_at.as_nanos(),
    }
}

fn column_to_response(col: &ColumnDef) -> ColumnResponse {
    ColumnResponse {
        column_id: col.column_id,
        name: col.name.clone(),
        data_type: data_type_to_str(col.data_type).to_string(),
        precision: col.precision,
        nullable: col.nullable,
        unit: col.unit.clone(),
        description: col.description.clone(),
        deadband: col.deadband.as_ref().map(|d| DeadbandResponse {
            deadband_type: d.deadband_type.as_str().to_string(),
            value: d.value,
        }),
        enum_values: col.enum_values.clone(),
        range_low: col.range_low,
        range_high: col.range_high,
        instrument_range_low: col.instrument_range_low,
        instrument_range_high: col.instrument_range_high,
        display_digits: col.display_digits,
    }
}

/// 后端 `DataType` → 前端 `SchemaDataType` 字符串（float/integer/boolean/string）。
///
/// 后端无 `timestamp` 物理类型，故永不产出 `timestamp`（诚实降级，不造假）。
fn data_type_to_schema_str(dt: DataType) -> &'static str {
    match dt {
        DataType::F64 | DataType::F32 => "float",
        DataType::I64 | DataType::U64 | DataType::I32 => "integer",
        DataType::Bool => "boolean",
        // Enum16 语义上代表枚举字符串，映射为 string 最贴近真实语义。
        DataType::String | DataType::Enum16 => "string",
    }
}

/// 前端 `SchemaDataType` 字符串 → 后端 `DataType`。
///
/// 前端 `timestamp` 后端无对应物理类型，按 epoch 整数（I64）落库（诚实降级，见交付说明）。
fn schema_str_to_data_type(s: &str) -> DataType {
    match s {
        "float" => DataType::F64,
        "integer" | "timestamp" => DataType::I64,
        "boolean" => DataType::Bool,
        _ => DataType::String,
    }
}

/// 后端 `ColumnDef` → 前端 `SchemaField`。
fn column_to_schema_field(col: &ColumnDef) -> FrontendSchemaField {
    FrontendSchemaField {
        name: col.name.clone(),
        data_type: data_type_to_schema_str(col.data_type).to_string(),
        unit: col.unit.clone(),
        description: col.description.clone(),
        nullable: col.nullable,
    }
}

/// 后端 `TableInfo` → 前端 `SchemaDefinition`。
///
/// `id` 取 `table_id` 的十进制字符串，list 与 detail 保持一致。
fn table_info_to_schema_definition(info: &TableInfo) -> FrontendSchemaDefinition {
    FrontendSchemaDefinition {
        id: info.table_id.to_string(),
        name: info.table_name.clone(),
        description: info.description.clone(),
        fields: info.columns.iter().map(column_to_schema_field).collect(),
        created_at: info.created_at.as_nanos() / 1_000_000,
        updated_at: info.updated_at.as_nanos() / 1_000_000,
    }
}

/// 将路径中的 `{id}` 解析为 `table_id`（列表与详情一致，均为十进制 table_id）。
fn parse_table_id_str(id_str: &str) -> Result<u32, SenseArcError> {
    id_str
        .parse::<u32>()
        .map_err(|_| SenseArcError::TableNotFound(0))
}

/// 获取当前时间作为 TimestampNanos。
fn now_nanos() -> TimestampNanos {
    use std::time::{SystemTime, UNIX_EPOCH};
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos() as i64;
    TimestampNanos(nanos)
}

// ─── AppError ─────────────────────────────────────────────────────────────

/// axum 可自动转换的错误类型。
struct AppError(SenseArcError);

impl From<SenseArcError> for AppError {
    fn from(e: SenseArcError) -> Self {
        AppError(e)
    }
}

impl IntoResponse for AppError {
    fn into_response(self) -> axum::response::Response {
        let status =
            StatusCode::from_u16(self.0.http_status()).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR);
        let body = Json(ErrorResponse {
            code: self.0.code(),
            message: self.0.to_string(),
        });
        (status, body).into_response()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A1：后端 DataType → 前端 SchemaDataType 字符串映射（诚实，无 timestamp 造假）。
    #[test]
    fn test_data_type_to_schema_str() {
        assert_eq!(data_type_to_schema_str(DataType::F64), "float");
        assert_eq!(data_type_to_schema_str(DataType::F32), "float");
        assert_eq!(data_type_to_schema_str(DataType::I64), "integer");
        assert_eq!(data_type_to_schema_str(DataType::U64), "integer");
        assert_eq!(data_type_to_schema_str(DataType::Bool), "boolean");
        assert_eq!(data_type_to_schema_str(DataType::String), "string");
        assert_eq!(data_type_to_schema_str(DataType::Enum16), "string");
    }

    /// A1：table → SchemaDefinition 映射（id 用 table_id 十进制，created/updated 转 epoch ms）。
    #[test]
    fn test_table_info_to_schema_definition() {
        let now = now_nanos();
        let info = TableInfo {
            table_id: 42,
            tenant_id: TenantId::DEFAULT,
            table_name: "sensors".into(),
            schema_id: 0,
            schema_version: 1,
            table_group: None,
            table_type: TableType::Raw,
            durability: Durability::Group,
            description: Some("desc".into()),
            enabled: true,
            columns: vec![ColumnDef {
                column_id: 0,
                name: "value".into(),
                data_type: DataType::F64,
                precision: 0.0,
                nullable: false,
                unit: Some("C".into()),
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
            created_at: now,
            updated_at: now,
        };
        let d = table_info_to_schema_definition(&info);
        assert_eq!(d.id, "42");
        assert_eq!(d.name, "sensors");
        assert_eq!(d.description.as_deref(), Some("desc"));
        assert_eq!(d.fields.len(), 1);
        assert_eq!(d.fields[0].data_type, "float");
        assert!(!d.fields[0].nullable);
    }
}
