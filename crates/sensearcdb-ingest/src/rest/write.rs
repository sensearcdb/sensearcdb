//! REST write handlers: POST /api/write/batch, /api/write/single.
//!
//! Accepts JSON payloads and converts them to PointBatch for engine ingestion.

use std::sync::Arc;

use axum::{
    extract::State,
    http::{HeaderMap, StatusCode},
    response::IntoResponse,
    Json,
};
use serde::{Deserialize, Serialize};
use tracing::debug;

use crate::batch::{BatchMeta, BatchSource, ColVec, PointBatch};
use crate::writer::EngineWriter;
use crate::column::resolve_column_id;
use crate::types::EngineWriteError;
use sensearcdb_common::{SenseArcError, TenantId};

/// 请求租户解析器（S5 Step 2）：由 bin 层注入，避免 ingest crate 反向依赖 bin。
///
/// 返回 `Ok(None)` = 平台主体/未注入，写入回退 `TenantId::DEFAULT`（N=1 兼容）；
/// `Err` = 非平台主体/API Key override（403）或未知 slug（404），显式回传不静默回退。
pub type TenantResolver =
    Arc<dyn Fn(&HeaderMap) -> Result<Option<TenantId>, SenseArcError> + Send + Sync>;

/// Resolves a table name to its `(table_id, is_writable)` pair.
///
/// Injected by the integration layer so that `sensearcdb-ingest` does not
/// depend on `sensearcdb-schema` at compile time. The concrete implementation
/// (`startup.rs`) wraps `SchemaMirror` and performs the
/// `table_name → table_id` + `is_writable` validation required by
/// `GAP2-ING-02`.
pub trait TableNameResolver: Send + Sync {
    /// Resolve a table name within a tenant.
    ///
    /// 跨租户允许同名表，故必须带 `tenant_id` 才能唯一定位。
    /// Returns `Some((table_id, is_writable))` when the table exists, or
    /// `None` when it does not.
    fn resolve(&self, tenant_id: TenantId, table_name: &str) -> Option<(u32, bool)>;
}

/// Shared state for write handlers.
#[derive(Clone)]
pub struct WriteState {
    pub writer: Arc<dyn EngineWriter>,
    /// Optional table-name resolver for `table_name → table_id` validation.
    pub resolver: Option<Arc<dyn TableNameResolver>>,
    /// 请求租户解析器（S5 Step 2）；`None` 时写入回退默认租户 0。
    pub tenant_resolver: Option<TenantResolver>,
}

/// Response wrapper for REST API.
#[derive(Serialize)]
struct ApiResponse<T: Serialize> {
    code: u16,
    message: String,
    data: Option<T>,
}

/// Batch write request payload.
#[derive(Debug, Deserialize)]
pub struct BatchWriteRequest {
    /// Target table name.
    pub table_name: String,
    /// Column data: column_name → [values].
    pub columns: std::collections::HashMap<String, serde_json::Value>,
    /// Row count.
    pub row_count: usize,
    /// Timestamps (epoch nanoseconds).
    #[serde(default)]
    pub timestamps: Vec<i64>,
    /// Quality codes (default: 0xC0 = Good).
    #[serde(default)]
    pub qualities: Vec<u8>,
    /// API key for authentication.
    #[serde(default)]
    pub api_key: String,
}

/// Batch write response.
#[derive(Debug, Serialize)]
pub struct BatchWriteResponse {
    pub points_written: u32,
    pub points_failed: u32,
    pub status: String,
}

/// Single write request payload.
#[derive(Debug, Deserialize)]
pub struct SingleWriteRequest {
    /// Target table name.
    pub table_name: String,
    /// Target column name.
    pub column_name: String,
    /// Timestamp (epoch nanoseconds).
    pub timestamp_ns: i64,
    /// Value (typed: number → I64/U64/F64, bool → Bool, string → String).
    ///
    /// The wide-table write path carries typed column values; the legacy
    /// hard-coded `f64` assumption is removed so that integer / boolean /
    /// string single-point writes are preserved end-to-end.
    pub value: serde_json::Value,
    /// Quality code (default: 0xC0 = Good).
    #[serde(default = "default_quality")]
    pub quality: u8,
    /// API key for authentication.
    #[serde(default)]
    pub api_key: String,
}

fn default_quality() -> u8 {
    0xC0
}

/// Resolve and validate a target table name against the optional resolver.
///
/// Returns `Ok(table_id)` when the table exists and is writable. When no
/// resolver is configured, `table_id` defaults to `0` (backward-compatible
/// direct write, logged for visibility).
fn resolve_target_table(
    state: &WriteState,
    tenant_id: TenantId,
    table_name: &str,
) -> Result<u32, (StatusCode, Json<ApiResponse<()>>)> {
    let Some(resolver) = &state.resolver else {
        tracing::warn!(
            table_name = table_name,
            "no table-name resolver configured; defaulting table_id to 0"
        );
        return Ok(0);
    };

    match resolver.resolve(tenant_id, table_name) {
        Some((table_id, true)) => Ok(table_id),
        Some((table_id, false)) => Err((
            StatusCode::FORBIDDEN,
            Json(ApiResponse {
                code: 403,
                message: format!("table '{}' (id={}) is not writable", table_name, table_id),
                data: None::<()>,
            }),
        )),
        None => Err((
            StatusCode::NOT_FOUND,
            Json(ApiResponse {
                code: 404,
                message: format!("table '{}' not found", table_name),
                data: None::<()>,
            }),
        )),
    }
}

/// Handle batch write: POST /api/write/batch
pub async fn handle_batch_write(
    State(state): State<WriteState>,
    headers: HeaderMap,
    Json(payload): Json<BatchWriteRequest>,
) -> impl IntoResponse {
    // S5 Step 2：数据归属主体 home 租户（服务端解析注入，None 回退默认租户）。
    let tenant_id = match state.tenant_resolver.as_ref().map(|r| (r)(&headers)) {
        Some(Ok(Some(t))) => t,
        Some(Ok(None)) | None => TenantId::DEFAULT,
        Some(Err(e)) => {
            return (
                StatusCode::from_u16(e.http_status()).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR),
                Json(ApiResponse {
                    code: e.code() as u16,
                    message: e.to_string(),
                    data: None::<()>,
                }),
            )
                .into_response();
        }
    };

    // Resolve table_name → table_id and validate writability（按请求 tenant 过滤，GAP2-ING-02）。
    let table_id = match resolve_target_table(&state, tenant_id, &payload.table_name) {
        Ok(id) => id,
        Err(resp) => return resp.into_response(),
    };

    // Resolve each column name → logical column id up front, then order columns
    // by id (ascending). This makes sparse column sets ({0,2}) explicit rather
    // than relying on HashMap iteration order, while plain column names keep a
    // deterministic dense layout (positional fallback).
    let mut entries: Vec<(&str, u16, &serde_json::Value)> =
        Vec::with_capacity(payload.columns.len());
    {
        let mut names: Vec<&String> = payload.columns.keys().collect();
        names.sort();
        for (pos, name) in names.into_iter().enumerate() {
            let column_id = resolve_column_id(name, pos as u16);
            entries.push((name.as_str(), column_id, &payload.columns[name]));
        }
    }
    entries.sort_by_key(|(_, column_id, _)| *column_id);

    // Convert JSON columns to ColVec
    let mut columns = Vec::with_capacity(entries.len());
    let mut column_names = Vec::with_capacity(entries.len());
    let mut column_ids = Vec::with_capacity(entries.len());

    for (col_name, column_id, col_value) in &entries {
        match convert_json_to_colvec(col_value, payload.row_count) {
            Ok(col) => {
                column_names.push(col_name.to_string());
                column_ids.push(*column_id);
                columns.push(col);
            }
            Err(e) => {
                return (
                    StatusCode::BAD_REQUEST,
                    Json(ApiResponse {
                        code: 400,
                        message: format!("column {} parse error: {}", col_name, e),
                        data: None::<()>,
                    }),
                )
                    .into_response();
            }
        }
    }

    let timestamps = if payload.timestamps.len() == payload.row_count {
        payload.timestamps
    } else {
        vec![chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0); payload.row_count]
    };

    let qualities = if payload.qualities.len() == payload.row_count {
        payload.qualities
    } else {
        vec![0xC0; payload.row_count]
    };

    let batch = PointBatch {
        table_id,
        tenant_id,
        timestamps,
        columns,
        column_names,
        column_ids,
        column_presence: Vec::new(),
        qualities,
        flags: vec![0; payload.row_count],
        metadata: BatchMeta {
            source: BatchSource::Rest {
                api_key_id: payload.api_key.clone(),
            },
            collected_at: chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0),
        },
    };

    let row_count = batch.row_count() as u32;

    match state.writer.write_batch(batch).await {
        Ok(receipt) => (
            StatusCode::OK,
            Json(ApiResponse {
                code: 200,
                message: "ok".into(),
                data: Some(BatchWriteResponse {
                    points_written: receipt.points_written,
                    points_failed: receipt.points_failed,
                    status: "accepted".into(),
                }),
            }),
        )
            .into_response(),
        Err(e) => (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(ApiResponse {
                code: 503,
                message: format!("write failed: {}", e),
                data: Some(BatchWriteResponse {
                    points_written: 0,
                    points_failed: row_count,
                    status: "rejected".into(),
                }),
            }),
        )
            .into_response(),
    }
}

/// Handle single write: POST /api/write/single
pub async fn handle_single_write(
    State(state): State<WriteState>,
    headers: HeaderMap,
    Json(payload): Json<SingleWriteRequest>,
) -> impl IntoResponse {
    // S5 Step 2：数据归属主体 home 租户（服务端解析注入，None 回退默认租户）。
    let tenant_id = match state.tenant_resolver.as_ref().map(|r| (r)(&headers)) {
        Some(Ok(Some(t))) => t,
        Some(Ok(None)) | None => TenantId::DEFAULT,
        Some(Err(e)) => {
            return (
                StatusCode::from_u16(e.http_status()).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR),
                Json(ApiResponse {
                    code: e.code() as u16,
                    message: e.to_string(),
                    data: None::<()>,
                }),
            )
                .into_response();
        }
    };

    // Resolve table_name → table_id and validate writability（按请求 tenant 过滤，GAP2-ING-02）。
    let table_id = match resolve_target_table(&state, tenant_id, &payload.table_name) {
        Ok(id) => id,
        Err(resp) => return resp.into_response(),
    };

    // Convert the typed JSON scalar to a physical ColVec (single element),
    // mirroring the typed gRPC single-write semantics — no `f64` hard-coding.
    let column = match convert_json_scalar_to_colvec(&payload.value) {
        Ok(col) => col,
        Err(e) => {
            return (
                StatusCode::BAD_REQUEST,
                Json(ApiResponse {
                    code: 400,
                    message: format!("value parse error: {}", e),
                    data: None::<()>,
                }),
            )
                .into_response();
        }
    };

    // Resolve the target column name → logical column id before writing. An
    // unqualified name (e.g. "value") degrades to column_id 0 — the single
    // column N=1 wide-table case — with a debug log; never a fabricated id.
    let column_id = resolve_column_id(&payload.column_name, 0);

    let mut batch = PointBatch::new(table_id);
    batch.tenant_id = tenant_id;
    batch.timestamps = vec![payload.timestamp_ns];
    batch.columns = vec![column];
    batch.column_names = vec![payload.column_name.clone()];
    batch.column_ids = vec![column_id];
    batch.qualities = vec![payload.quality];
    batch.flags = vec![0];
    batch.metadata = BatchMeta {
        source: BatchSource::Rest {
            api_key_id: payload.api_key.clone(),
        },
        collected_at: chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0),
    };

    match state.writer.write_batch(batch).await {
        Ok(receipt) => (
            StatusCode::OK,
            Json(ApiResponse {
                code: 200,
                message: "ok".into(),
                data: Some(BatchWriteResponse {
                    points_written: receipt.points_written,
                    points_failed: receipt.points_failed,
                    status: "accepted".into(),
                }),
            }),
        )
            .into_response(),
        Err(e) => (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(ApiResponse {
                code: 503,
                message: format!("write failed: {}", e),
                data: Some(BatchWriteResponse {
                    points_written: 0,
                    points_failed: 1,
                    status: "rejected".into(),
                }),
            }),
        )
            .into_response(),
    }
}

/// Convert a serde_json::Value to a ColVec.
fn convert_json_to_colvec(value: &serde_json::Value, row_count: usize) -> Result<ColVec, String> {
    match value {
        serde_json::Value::Array(arr) => {
            if arr.is_empty() {
                return Ok(ColVec::F64(vec![0.0; row_count]));
            }
            // Infer type from first element
            match &arr[0] {
                serde_json::Value::Number(_) => {
                    let vals: Vec<f64> = arr.iter().map(|v| v.as_f64().unwrap_or(0.0)).collect();
                    Ok(ColVec::F64(vals))
                }
                serde_json::Value::Bool(_) => {
                    let vals: Vec<u8> = arr
                        .iter()
                        .map(|v| {
                            if v.as_bool().unwrap_or(false) {
                                1u8
                            } else {
                                0u8
                            }
                        })
                        .collect();
                    Ok(ColVec::Bool(vals))
                }
                serde_json::Value::String(_) => {
                    let vals: Vec<String> = arr
                        .iter()
                        .map(|v| v.as_str().unwrap_or("").to_string())
                        .collect();
                    Ok(ColVec::String(vals))
                }
                _ => Err("unsupported JSON type".into()),
            }
        }
        _ => Err("expected JSON array for column values".into()),
    }
}

/// Convert a single JSON scalar to a typed single-element [`ColVec`].
///
/// JSON integers prefer `I64` (falling back to `U64` for values above
/// `i64::MAX`), non-integer numbers map to `F64`, booleans to `Bool`, and
/// strings to `String`. This aligns the REST single-write path with the typed
/// gRPC `single_write` semantics and removes the legacy `f64` assumption.
fn convert_json_scalar_to_colvec(value: &serde_json::Value) -> Result<ColVec, String> {
    match value {
        serde_json::Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                Ok(ColVec::I64(vec![i]))
            } else if let Some(u) = n.as_u64() {
                Ok(ColVec::U64(vec![u]))
            } else if let Some(f) = n.as_f64() {
                Ok(ColVec::F64(vec![f]))
            } else {
                Err("unsupported numeric value".into())
            }
        }
        serde_json::Value::Bool(b) => Ok(ColVec::Bool(vec![*b as u8])),
        serde_json::Value::String(s) => Ok(ColVec::String(vec![s.clone()])),
        _ => Err("expected scalar value (number, bool, or string)".into()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_convert_json_numbers() {
        let val = serde_json::json!([1.0, 2.0, 3.0]);
        let col = convert_json_to_colvec(&val, 3).unwrap();
        match col {
            ColVec::F64(v) => assert_eq!(v, vec![1.0, 2.0, 3.0]),
            _ => panic!("expected F64"),
        }
    }

    #[test]
    fn test_convert_json_bools() {
        let val = serde_json::json!([true, false, true]);
        let col = convert_json_to_colvec(&val, 3).unwrap();
        match col {
            ColVec::Bool(v) => assert_eq!(v, vec![1, 0, 1]),
            _ => panic!("expected Bool"),
        }
    }

    #[test]
    fn test_convert_json_strings() {
        let val = serde_json::json!(["a", "b", "c"]);
        let col = convert_json_to_colvec(&val, 3).unwrap();
        match col {
            ColVec::String(v) => {
                assert_eq!(v, vec!["a".to_string(), "b".to_string(), "c".to_string()])
            }
            _ => panic!("expected String"),
        }
    }

    #[test]
    fn test_convert_json_scalar_i64() {
        let val = serde_json::json!(42);
        match convert_json_scalar_to_colvec(&val).unwrap() {
            ColVec::I64(v) => assert_eq!(v, vec![42]),
            other => panic!("expected I64, got {:?}", other),
        }
    }

    #[test]
    fn test_convert_json_scalar_f64() {
        let val = serde_json::json!(3.5);
        match convert_json_scalar_to_colvec(&val).unwrap() {
            ColVec::F64(v) => assert_eq!(v, vec![3.5]),
            other => panic!("expected F64, got {:?}", other),
        }
    }

    #[test]
    fn test_convert_json_scalar_bool() {
        let val = serde_json::json!(true);
        match convert_json_scalar_to_colvec(&val).unwrap() {
            ColVec::Bool(v) => assert_eq!(v, vec![1]),
            other => panic!("expected Bool, got {:?}", other),
        }
    }

    #[test]
    fn test_convert_json_scalar_string() {
        let val = serde_json::json!("running");
        match convert_json_scalar_to_colvec(&val).unwrap() {
            ColVec::String(v) => assert_eq!(v, vec!["running".to_string()]),
            other => panic!("expected String, got {:?}", other),
        }
    }

    #[test]
    fn test_convert_json_scalar_rejects_non_scalar() {
        let val = serde_json::json!([1, 2]);
        assert!(convert_json_scalar_to_colvec(&val).is_err());
        let val = serde_json::json!(null);
        assert!(convert_json_scalar_to_colvec(&val).is_err());
    }
}
