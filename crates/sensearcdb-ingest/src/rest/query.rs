//! REST query handler: POST /api/query
//!
//! Accepts SQL queries and returns results in JSON or Arrow IPC format.

use axum::{http::StatusCode, response::IntoResponse, Json};
use serde::{Deserialize, Serialize};
use tracing::debug;

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

/// Handle query: POST /api/query
pub async fn handle_query(Json(payload): Json<QueryRequest>) -> impl IntoResponse {
    debug!(query = %payload.query, "REST query received");

    // In a full implementation, this would:
    // 1. Parse SQL
    // 2. Execute via engine QueryExecutor
    // 3. Return results

    // For now, return empty results
    let response = QueryResponse {
        columns: vec![],
        rows: vec![],
        row_count: 0,
        elapsed_ms: 0,
    };

    (
        StatusCode::OK,
        Json(serde_json::json!({
            "code": 0,
            "message": "ok",
            "data": response,
        })),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::response::IntoResponse;

    async fn body_code(resp: axum::response::Response) -> i64 {
        let body = axum::body::to_bytes(resp.into_body(), 1024).await.unwrap();
        let v: serde_json::Value = serde_json::from_slice(&body).unwrap();
        v["code"].as_i64().unwrap()
    }

    /// 成功码全仓唯一 0（契约 O2/C5）。
    #[tokio::test]
    async fn test_query_success_code_zero() {
        let req = QueryRequest {
            query: "select 1".into(),
            params: std::collections::HashMap::new(),
            format: "json".into(),
        };
        let resp = handle_query(Json(req)).await.into_response();
        assert_eq!(body_code(resp).await, 0);
    }
}
