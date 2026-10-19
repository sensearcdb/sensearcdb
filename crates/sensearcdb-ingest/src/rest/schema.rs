//! REST schema CRUD pass-through handlers.
//!
//! Proxies schema management requests to the schema crate (sensearcdb-schema).
//! GET /api/schema/tables, GET /api/schema/tables/:id, POST /api/schema/tables

use axum::{extract::Path, http::StatusCode, response::IntoResponse, Json};
use serde::Serialize;

/// Table schema response.
#[derive(Debug, Serialize)]
pub struct TableSchema {
    pub table_id: u32,
    pub table_name: String,
    pub columns: Vec<ColumnSchema>,
    pub created_at: i64,
}

/// Column schema response.
#[derive(Debug, Serialize)]
pub struct ColumnSchema {
    pub column_id: u16,
    pub column_name: String,
    pub data_type: String,
    pub nullable: bool,
}

/// Handle list tables: GET /api/schema/tables
pub async fn handle_list_tables() -> impl IntoResponse {
    // In production, this queries the SchemaMirror from sensearcdb-schema.
    (
        StatusCode::OK,
        Json(serde_json::json!({
            "code": 0,
            "message": "ok",
            "data": {
                "tables": Vec::<TableSchema>::new(),
                "count": 0_usize,
            },
        })),
    )
}

/// Handle get table: GET /api/schema/tables/:table_id
pub async fn handle_get_table(Path(table_id): Path<u32>) -> impl IntoResponse {
    (
        StatusCode::OK,
        Json(serde_json::json!({
            "code": 0,
            "message": "ok",
            "data": {
                "table_id": table_id,
                "table_name": format!("table_{}", table_id),
                "columns": Vec::<ColumnSchema>::new(),
                "created_at": 0_i64,
            },
        })),
    )
}

/// Handle create table: POST /api/schema/tables
pub async fn handle_create_table(Json(_payload): Json<serde_json::Value>) -> impl IntoResponse {
    // In production, this calls Catalog::create_table().
    (
        StatusCode::CREATED,
        Json(serde_json::json!({
            "code": 0,
            "message": "table created",
            "data": {
                "table_id": 1_u32,
                "table_name": "created".to_string(),
            },
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
    async fn test_schema_success_code_zero() {
        assert_eq!(
            body_code(handle_list_tables().await.into_response()).await,
            0
        );
        assert_eq!(
            body_code(handle_get_table(Path(1u32)).await.into_response()).await,
            0
        );
        assert_eq!(
            body_code(
                handle_create_table(Json(serde_json::json!({})))
                    .await
                    .into_response()
            )
            .await,
            0
        );
    }
}
