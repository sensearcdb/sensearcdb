//! REST query handler: POST /api/query
//!
//! Accepts SQL queries and returns results in JSON or Arrow IPC format.

use std::sync::Arc;

use arrow::array::{
    Array, BooleanArray, Float32Array, Float64Array, Int32Array, Int64Array, StringArray,
    UInt16Array, UInt64Array,
};
use arrow::datatypes::DataType;
use arrow::record_batch::RecordBatch;
use axum::extract::State;
use axum::http::HeaderMap;
use axum::{http::StatusCode, response::IntoResponse, Json};
use serde::{Deserialize, Serialize};
use sensearcdb_common::{SenseArcError, TenantId};
use sensearcdb_engine::engine::Engine;
use tracing::debug;

use super::write::{self, TenantResolver};

/// Query request payload.
#[derive(Debug, Deserialize)]
pub struct QueryRequest {
    /// SQL query string.
    pub query: String,
    /// Named parameters (e.g., {"start": "2024-01-01T00:00:00Z"}).
    #[serde(default)]
    pub params: std::collections::HashMap<String, String>,
    /// Response format: "json" or "arrow".
    #[serde(default = "default_format")]
    pub format: String,
}

fn default_format() -> String {
    "json".into()
}

/// Query response wrapper.
#[derive(Debug, Serialize)]
pub struct QueryResponse {
    pub columns: Vec<String>,
    pub rows: Vec<Vec<serde_json::Value>>,
    pub row_count: usize,
    pub elapsed_ms: u64,
}

/// Shared state for the query handler.
#[derive(Clone)]
pub struct QueryState {
    /// 查询引擎；`None` 表示引擎未注入（应返回 503，绝不伪造空 rows）。
    pub engine: Option<Arc<Engine>>,
    /// 请求租户解析器（与 write.rs 同口径）；`None` 时回退默认租户 0。
    pub tenant_resolver: Option<TenantResolver>,
}

/// Handle query: POST /api/query
pub async fn handle_query(
    State(state): State<QueryState>,
    headers: HeaderMap,
    Json(payload): Json<QueryRequest>,
) -> impl IntoResponse {
    debug!(query = %payload.query, "REST query received");

    // 1. 解析租户（复用 write.rs 同口径：Ok(Some)→t；Ok(None)/None→DEFAULT；Err→按 http_status/code 回传）。
    let tenant_id = match state.tenant_resolver.as_ref().map(|r| (r)(&headers)) {
        Some(Ok(Some(t))) => t,
        Some(Ok(None)) | None => TenantId::DEFAULT,
        Some(Err(e)) => {
            return (
                StatusCode::from_u16(e.http_status()).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR),
                Json(serde_json::json!({
                    "code": e.code(),
                    "message": e.to_string(),
                    "data": null,
                })),
            )
                .into_response();
        }
    };

    // 2. format 只支持 json（Arrow IPC 走 gRPC QueryService）。
    if payload.format != "json" {
        return (
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({
                "code": 40004,
                "message": format!(
                    "format '{}' not supported on REST query; use gRPC QueryService or format=json",
                    payload.format
                ),
                "data": null,
            })),
        )
            .into_response();
    }

    // 3. REST 查询引擎不支持命名参数。
    if !payload.params.is_empty() {
        return (
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({
                "code": 40004,
                "message": "named parameters not supported by query engine",
                "data": null,
            })),
        )
            .into_response();
    }

    // 4. 引擎未注入：503 + code 50001，绝不返空 rows。
    let Some(engine) = &state.engine else {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(serde_json::json!({
                "code": 50001,
                "message": "query engine not available",
                "data": null,
            })),
        )
            .into_response();
    };

    // 5. 计时执行查询，手工遍历 RecordBatch 转 JSON（类型保真，I64/U64 不经 f64）。
    let start = std::time::Instant::now();
    match engine.query(tenant_id, &payload.query).await {
        Ok(qr) => {
            let elapsed_ms = start.elapsed().as_millis() as u64;
            let (columns, rows) = record_batches_to_json(&qr.batches);
            let row_count = rows.len();
            let response = QueryResponse {
                columns,
                rows,
                row_count,
                elapsed_ms,
            };
            (
                StatusCode::OK,
                Json(serde_json::json!({
                    "code": 0,
                    "message": "ok",
                    "data": response,
                })),
            )
                .into_response()
        }
        Err(e) => (
            StatusCode::from_u16(e.http_status()).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR),
            Json(serde_json::json!({
                "code": e.code(),
                "message": e.to_string(),
                "data": null,
            })),
        )
            .into_response(),
    }
}

/// 将多个 Arrow `RecordBatch` 手工拼接为 `(columns, rows)`。
///
/// - `columns` 取第一个 batch 的 schema 字段名；零 batch → `(vec![], vec![])`。
/// - 逐 batch 逐行：每行按列 `arrow_value_to_json` 转换。
fn record_batches_to_json(batches: &[RecordBatch]) -> (Vec<String>, Vec<Vec<serde_json::Value>>) {
    let Some(first) = batches.first() else {
        return (vec![], vec![]);
    };
    let columns: Vec<String> = first
        .schema()
        .fields()
        .iter()
        .map(|f| f.name().clone())
        .collect();

    let mut rows = Vec::new();
    for batch in batches {
        for row in 0..batch.num_rows() {
            let row_vals: Vec<serde_json::Value> = (0..batch.num_columns())
                .map(|c| arrow_value_to_json(batch.column(c).as_ref(), row))
                .collect();
            rows.push(row_vals);
        }
    }
    (columns, rows)
}

/// 单格 Arrow 值 → JSON。手工 downcast，`I64`/`U64` 走 `Number::from`，任何路径不经 f64。
fn arrow_value_to_json(arr: &dyn Array, row: usize) -> serde_json::Value {
    use serde_json::Value;

    if arr.is_null(row) {
        return Value::Null;
    }

    match arr.data_type() {
        DataType::Int64 => Value::from(arr.as_any().downcast_ref::<Int64Array>().unwrap().value(row)),
        DataType::UInt64 => {
            Value::from(arr.as_any().downcast_ref::<UInt64Array>().unwrap().value(row))
        }
        DataType::Int32 => Value::from(arr.as_any().downcast_ref::<Int32Array>().unwrap().value(row)),
        DataType::UInt16 => {
            Value::from(arr.as_any().downcast_ref::<UInt16Array>().unwrap().value(row))
        }
        DataType::Float32 => {
            // f32 → f64 无损。
            let f = arr.as_any().downcast_ref::<Float32Array>().unwrap().value(row) as f64;
            serde_json::Number::from_f64(f)
                .map(Value::Number)
                .unwrap_or(Value::Null)
        }
        DataType::Float64 => {
            let f = arr.as_any().downcast_ref::<Float64Array>().unwrap().value(row);
            serde_json::Number::from_f64(f)
                .map(Value::Number)
                .unwrap_or(Value::Null)
        }
        DataType::Boolean => {
            Value::from(arr.as_any().downcast_ref::<BooleanArray>().unwrap().value(row))
        }
        DataType::Utf8 => Value::from(
            arr.as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .value(row)
                .to_string(),
        ),
        other => Value::String(format!("unsupported arrow type: {other:?}")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::response::IntoResponse;
    use sensearcdb_common::{AppConfig, ConfigHandle, TypedValue};
    use sensearcdb_engine::engine::{DataPoint, PointBatch};

    // ─── helpers ───────────────────────────────────────────────────────────

    /// 用临时 `data_dir` 建一个真实引擎（`TempDir` 保证测试结束自动清理）。
    async fn test_engine() -> (Arc<Engine>, tempfile::TempDir) {
        let tmp = tempfile::TempDir::new().unwrap();
        let mut cfg = AppConfig::default();
        cfg.paths.data_dir = tmp.path().to_string_lossy().into_owned();
        let handle = ConfigHandle::new(cfg);
        let engine = Engine::new(&handle, None).await.expect("Engine::new");
        (Arc::new(engine), tmp)
    }

    /// 构造单点数据。
    fn point(table_id: u32, ts: i64, values: Vec<(u16, TypedValue)>) -> DataPoint {
        DataPoint {
            table_id,
            tenant_id: TenantId::DEFAULT,
            ts,
            server_ts: ts,
            values,
            qualities: vec![(0, 0xC0)],
            schema_version: 1,
            flags: 0,
        }
    }

    fn state_with(engine: Option<Arc<Engine>>) -> QueryState {
        QueryState {
            engine,
            tenant_resolver: None,
        }
    }

    async fn call(state: QueryState, req: QueryRequest) -> axum::response::Response {
        handle_query(State(state), HeaderMap::new(), Json(req))
            .await
            .into_response()
    }

    async fn body_json(resp: axum::response::Response) -> serde_json::Value {
        let body = axum::body::to_bytes(resp.into_body(), usize::MAX)
            .await
            .unwrap();
        serde_json::from_slice(&body).unwrap()
    }

    // ─── tests ─────────────────────────────────────────────────────────────

    /// 成功码全仓唯一 0（契约 O2/C5）。
    #[tokio::test]
    async fn test_query_success_code_zero() {
        let (engine, _tmp) = test_engine().await;
        engine
            .write_batch(PointBatch {
                points: vec![point(42, 1_000, vec![(0, TypedValue::F64(3.14))])],
            })
            .await
            .expect("write_batch");

        let req = QueryRequest {
            query: "SELECT * FROM table_42 WHERE ts BETWEEN 0 AND 1000000000".into(),
            params: std::collections::HashMap::new(),
            format: "json".into(),
        };
        let resp = call(state_with(Some(engine)), req).await;
        let v = body_json(resp).await;
        assert_eq!(v["code"].as_i64().unwrap(), 0);
        assert_eq!(v["message"].as_str().unwrap(), "ok");
        assert_eq!(v["data"]["row_count"].as_u64().unwrap(), 1);
    }

    /// json 返回真实 rows（非空 stub）。
    #[tokio::test]
    async fn test_query_returns_real_rows() {
        let (engine, _tmp) = test_engine().await;
        engine
            .write_batch(PointBatch {
                points: vec![
                    point(7, 100, vec![(0, TypedValue::F64(1.0))]),
                    point(7, 200, vec![(0, TypedValue::F64(2.0))]),
                ],
            })
            .await
            .expect("write_batch");

        let req = QueryRequest {
            query: "SELECT * FROM table_7 WHERE ts BETWEEN 0 AND 1000".into(),
            params: std::collections::HashMap::new(),
            format: "json".into(),
        };
        let resp = call(state_with(Some(engine)), req).await;
        let v = body_json(resp).await;
        assert_eq!(v["code"], 0);
        assert_eq!(v["data"]["row_count"], 2);
        // 列：ts + col_0。
        assert_eq!(v["data"]["columns"][0], "ts");
        assert_eq!(v["data"]["columns"][1], "col_0");
        let rows = v["data"]["rows"].as_array().unwrap();
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0][1].as_f64().unwrap(), 1.0);
        assert_eq!(rows[1][1].as_f64().unwrap(), 2.0);
    }

    /// I64 = 2^53+1 精确（若经 f64 会折损）。
    #[tokio::test]
    async fn test_query_i64_precision() {
        let (engine, _tmp) = test_engine().await;
        let big: i64 = (1i64 << 53) + 1;
        engine
            .write_batch(PointBatch {
                points: vec![point(8, 100, vec![(0, TypedValue::I64(big))])],
            })
            .await
            .expect("write_batch");

        let req = QueryRequest {
            query: "SELECT col_0 FROM table_8".into(),
            params: std::collections::HashMap::new(),
            format: "json".into(),
        };
        let resp = call(state_with(Some(engine)), req).await;
        let v = body_json(resp).await;
        assert_eq!(v["code"], 0);
        assert_eq!(v["data"]["rows"][0][0].as_i64().unwrap(), big);
    }

    /// U64 = u64::MAX 精确（不经 f64，也不 as i64 溢出）。
    #[tokio::test]
    async fn test_query_u64_precision() {
        let (engine, _tmp) = test_engine().await;
        engine
            .write_batch(PointBatch {
                points: vec![point(9, 100, vec![(0, TypedValue::U64(u64::MAX))])],
            })
            .await
            .expect("write_batch");

        let req = QueryRequest {
            query: "SELECT col_0 FROM table_9".into(),
            params: std::collections::HashMap::new(),
            format: "json".into(),
        };
        let resp = call(state_with(Some(engine)), req).await;
        let v = body_json(resp).await;
        assert_eq!(v["code"], 0);
        assert_eq!(v["data"]["rows"][0][0].as_u64().unwrap(), u64::MAX);
    }

    /// 空结果：code==0 且 rows 空（不是 stub，而是真查询无命中）。
    #[tokio::test]
    async fn test_query_empty_result_code_zero() {
        let (engine, _tmp) = test_engine().await;

        let req = QueryRequest {
            query: "SELECT * FROM table_999 WHERE ts BETWEEN 0 AND 1000".into(),
            params: std::collections::HashMap::new(),
            format: "json".into(),
        };
        let resp = call(state_with(Some(engine)), req).await;
        let v = body_json(resp).await;
        assert_eq!(v["code"], 0);
        assert_eq!(v["data"]["row_count"], 0);
        assert_eq!(v["data"]["rows"].as_array().unwrap().len(), 0);
    }

    /// SQL 语法错误（无 FROM）：code != 0（50001）。
    #[tokio::test]
    async fn test_query_sql_syntax_error() {
        let (engine, _tmp) = test_engine().await;

        let req = QueryRequest {
            query: "select 1".into(),
            params: std::collections::HashMap::new(),
            format: "json".into(),
        };
        let resp = call(state_with(Some(engine)), req).await;
        let v = body_json(resp).await;
        assert_ne!(v["code"].as_i64().unwrap(), 0);
    }

    /// 租户隔离：注入非默认租户解析器 → 读不到默认租户数据（rows 空）；默认租户 → 命中。
    #[tokio::test]
    async fn test_tenant_isolation() {
        let (engine, _tmp) = test_engine().await;
        engine
            .write_batch(PointBatch {
                points: vec![point(42, 1_000, vec![(0, TypedValue::F64(3.14))])],
            })
            .await
            .expect("write_batch");

        let other_resolver: TenantResolver = Arc::new(move |_: &HeaderMap| Ok(Some(TenantId(999))));
        let req = QueryRequest {
            query: "SELECT * FROM table_42 WHERE ts BETWEEN 0 AND 1000000000".into(),
            params: std::collections::HashMap::new(),
            format: "json".into(),
        };
        let resp = call(
            QueryState {
                engine: Some(engine.clone()),
                tenant_resolver: Some(other_resolver),
            },
            req,
        )
        .await;
        let v = body_json(resp).await;
        assert_eq!(v["code"], 0);
        assert_eq!(v["data"]["row_count"], 0, "跨租户读必须隔离");

        // 默认租户解析器 → 命中 1 行。
        let default_resolver: TenantResolver =
            Arc::new(move |_: &HeaderMap| Ok(Some(TenantId::DEFAULT)));
        let req = QueryRequest {
            query: "SELECT * FROM table_42 WHERE ts BETWEEN 0 AND 1000000000".into(),
            params: std::collections::HashMap::new(),
            format: "json".into(),
        };
        let resp = call(
            QueryState {
                engine: Some(engine.clone()),
                tenant_resolver: Some(default_resolver),
            },
            req,
        )
        .await;
        let v = body_json(resp).await;
        assert_eq!(v["code"], 0);
        assert_eq!(v["data"]["row_count"], 1);
    }

    /// format=arrow 拒绝（REST 只支持 json）。
    #[tokio::test]
    async fn test_format_arrow_rejected() {
        let req = QueryRequest {
            query: "SELECT * FROM table_1".into(),
            params: std::collections::HashMap::new(),
            format: "arrow".into(),
        };
        let resp = call(state_with(None), req).await;
        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
        let v = body_json(resp).await;
        assert_eq!(v["code"].as_i64().unwrap(), 40004);
    }

    /// params 非空拒绝。
    #[tokio::test]
    async fn test_params_rejected() {
        let mut params = std::collections::HashMap::new();
        params.insert("start".to_string(), "2024-01-01T00:00:00Z".to_string());
        let req = QueryRequest {
            query: "SELECT * FROM table_1".into(),
            params,
            format: "json".into(),
        };
        let resp = call(state_with(None), req).await;
        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
        let v = body_json(resp).await;
        assert_eq!(v["code"].as_i64().unwrap(), 40004);
    }

    /// engine=None → 503 + code 50001 + data null（绝不返空 rows）。
    #[tokio::test]
    async fn test_engine_none_returns_503() {
        let req = QueryRequest {
            query: "SELECT * FROM table_1 WHERE ts BETWEEN 0 AND 1000".into(),
            params: std::collections::HashMap::new(),
            format: "json".into(),
        };
        let resp = call(state_with(None), req).await;
        assert_eq!(resp.status(), StatusCode::SERVICE_UNAVAILABLE);
        let v = body_json(resp).await;
        assert_eq!(v["code"].as_i64().unwrap(), 50001);
        assert_eq!(v["message"].as_str().unwrap(), "query engine not available");
        assert_eq!(v["data"], serde_json::Value::Null);
    }

    /// 多 batch 拼接 + null 语义。
    #[test]
    fn test_record_batches_to_json_multi_batch_and_null() {
        use arrow::array::Int64Array;
        use arrow::datatypes::{Field, Schema};

        let schema = Arc::new(Schema::new(vec![
            Field::new("ts", DataType::Int64, false),
            Field::new("col_0", DataType::Int64, true),
        ]));

        let b1 = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![100i64, 200])),
                Arc::new(Int64Array::from(vec![Some(9_007_199_254_740_993i64), None])),
            ],
        )
        .unwrap();

        let b2 = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![300i64])),
                Arc::new(Int64Array::from(vec![Some(7i64)])),
            ],
        )
        .unwrap();

        let (columns, rows) = super::record_batches_to_json(&[b1, b2]);
        assert_eq!(columns, vec!["ts".to_string(), "col_0".to_string()]);
        assert_eq!(rows.len(), 3);
        assert_eq!(rows[0][1].as_i64().unwrap(), 9_007_199_254_740_993i64);
        assert_eq!(rows[1][1], serde_json::Value::Null);
        assert_eq!(rows[2][1].as_i64().unwrap(), 7);
    }
}
