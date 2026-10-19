//! REST admin handlers: user/role/API key management.
//!
//! GET /api/admin/users, GET /api/admin/roles, POST /api/admin/apikeys

use axum::{http::StatusCode, response::IntoResponse, Json};
use serde::Serialize;
use tracing::info;

/// User record.
#[derive(Debug, Serialize)]
pub struct UserInfo {
    pub user_id: String,
    pub username: String,
    pub role: String,
    pub created_at: i64,
}

/// Role record.
#[derive(Debug, Serialize)]
pub struct RoleInfo {
    pub role_id: String,
    pub name: String,
    pub permissions: Vec<String>,
}

/// API key response.
#[derive(Debug, Serialize)]
pub struct ApiKeyResponse {
    pub key_id: String,
    pub api_key: String,
    pub name: String,
    pub created_at: i64,
}

/// Handle list users: GET /api/admin/users
pub async fn handle_list_users() -> impl IntoResponse {
    // In production, query from user database
    (
        StatusCode::OK,
        Json(serde_json::json!({
            "code": 0,
            "message": "ok",
            "data": {
                "users": [UserInfo {
                    user_id: "admin".into(),
                    username: "admin".into(),
                    role: "admin".into(),
                    created_at: 0,
                }],
                "count": 1_usize,
            },
        })),
    )
}

/// Handle list roles: GET /api/admin/roles
pub async fn handle_list_roles() -> impl IntoResponse {
    (
        StatusCode::OK,
        Json(serde_json::json!({
            "code": 0,
            "message": "ok",
            "data": {
                "roles": [
                    RoleInfo {
                        role_id: "admin".into(),
                        name: "Administrator".into(),
                        permissions: vec!["read".into(), "write".into(), "admin".into()],
                    },
                    RoleInfo {
                        role_id: "writer".into(),
                        name: "Data Writer".into(),
                        permissions: vec!["write".into()],
                    },
                    RoleInfo {
                        role_id: "reader".into(),
                        name: "Data Reader".into(),
                        permissions: vec!["read".into()],
                    },
                ],
                "count": 3_usize,
            },
        })),
    )
}

/// Handle create API key: POST /api/admin/apikeys
pub async fn handle_create_api_key(Json(payload): Json<serde_json::Value>) -> impl IntoResponse {
    let name = payload
        .get("name")
        .and_then(|v| v.as_str())
        .unwrap_or("default");

    let api_key = uuid::Uuid::new_v4().to_string();
    let key_id = format!("key-{}", &api_key[..8]);

    info!(name = name, key_id = %key_id, "API key created");

    (
        StatusCode::CREATED,
        Json(serde_json::json!({
            "code": 0,
            "message": "API key created",
            "data": ApiKeyResponse {
                key_id,
                api_key,
                name: name.to_string(),
                created_at: chrono::Utc::now().timestamp(),
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
    async fn test_admin_success_code_zero() {
        assert_eq!(
            body_code(handle_list_users().await.into_response()).await,
            0
        );
        assert_eq!(
            body_code(handle_list_roles().await.into_response()).await,
            0
        );
        assert_eq!(
            body_code(
                handle_create_api_key(Json(serde_json::json!({ "name": "test" })))
                    .await
                    .into_response()
            )
            .await,
            0
        );
    }
}
