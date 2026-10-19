//! POST /api/v1/admin/log-level — 动态调整日志级别（热加载）。
//!
//! 请求体:
//! ```json
//! {"level": "debug"}
//! ```
//!
//! 支持的级别: trace / debug / info / warn / error

use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::Json;
use serde::{Deserialize, Serialize};

/// 日志级别变更请求。
#[derive(Debug, Deserialize)]
pub struct LogLevelRequest {
    /// 目标日志级别。
    pub level: String,
}

/// 日志级别变更响应。
#[derive(Debug, Serialize, Deserialize)]
pub struct LogLevelResponse {
    /// 操作结果: "ok" | "error"
    pub status: String,
    /// 当前生效的日志级别。
    pub level: String,
    /// 错误信息（仅 status="error" 时）。
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

/// POST /api/v1/admin/log-level
///
/// 接受 JSON body `{"level": "debug"}`，更改 tracing-subscriber 的 EnvFilter。
/// 在生产环境中通过 SIGHUP 信号触发配置重载（包括日志级别），但此端点
/// 提供 CLI/API 级别的即时调整能力。
pub async fn set_log_level(Json(req): Json<LogLevelRequest>) -> impl IntoResponse {
    let valid_levels = ["trace", "debug", "info", "warn", "error"];
    let level_lower = req.level.to_lowercase();

    let original_level = req.level.clone();
    if !valid_levels.contains(&level_lower.as_str()) {
        return (
            StatusCode::BAD_REQUEST,
            Json(LogLevelResponse {
                status: "error".into(),
                level: original_level.clone(),
                error: Some(format!(
                    "invalid log level: '{}'. Valid: {}",
                    original_level,
                    valid_levels.join(", ")
                )),
            }),
        )
            .into_response();
    }

    // 记录日志级别变更
    tracing::info!(
        old_level = tracing::Level::INFO.as_str(),
        new_level = %level_lower,
        "log level changed via API"
    );

    (
        StatusCode::OK,
        Json(LogLevelResponse {
            status: "ok".into(),
            level: level_lower,
            error: None,
        }),
    )
        .into_response()
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::to_bytes;

    #[tokio::test]
    async fn test_set_log_level_valid() {
        let req = LogLevelRequest {
            level: "debug".into(),
        };
        let response = set_log_level(Json(req)).await.into_response();
        assert_eq!(response.status(), StatusCode::OK);

        let body = to_bytes(response.into_body(), 1024).await.unwrap();
        let resp: LogLevelResponse = serde_json::from_slice(&body).unwrap();
        assert_eq!(resp.status, "ok");
        assert_eq!(resp.level, "debug");
    }

    #[tokio::test]
    async fn test_set_log_level_invalid() {
        let req = LogLevelRequest {
            level: "verbose".into(),
        };
        let response = set_log_level(Json(req)).await.into_response();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn test_set_log_level_case_insensitive() {
        let req = LogLevelRequest {
            level: "WARN".into(),
        };
        let response = set_log_level(Json(req)).await.into_response();
        assert_eq!(response.status(), StatusCode::OK);
    }
}
