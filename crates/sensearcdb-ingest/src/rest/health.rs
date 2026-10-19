//! Health check endpoints.
//!
//! GET /health       — basic health check.
//! GET /health/ready — readiness probe: can the service accept requests?
//! GET /health/live  — liveness probe: is the service alive?

use axum::extract::State;
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::Json;
use serde::Serialize;
use std::sync::Arc;

use sensearcdb_common::SenseArcError;

/// Readiness 探测结果。
///
/// `ready=false` 时 `reason` 承载具体失败原因（如 `wal not writable` / `not leader`），
/// 替代 observe 旧实现的 `checks[]` 结构，供线上机器读 message 定位根因。
#[derive(Debug, Clone)]
pub struct ReadinessState {
    /// 是否就绪。
    pub ready: bool,
    /// 未就绪时的具体原因（就绪时为 `None`）。
    pub reason: Option<String>,
}

impl ReadinessState {
    /// 构造「就绪」状态。
    pub fn ready() -> Self {
        Self {
            ready: true,
            reason: None,
        }
    }

    /// 构造「未就绪」状态并附带失败原因。
    pub fn not_ready(reason: impl Into<String>) -> Self {
        Self {
            ready: false,
            reason: Some(reason.into()),
        }
    }
}

/// Readiness 探针——由二进制层（持有 `AppState`）构造闭包注入。
///
/// ingest 是库 crate，无法直接引用二进制层的 `AppState`；通过本函数对象把
/// 「WAL 可写 + MemTable 可用 + 集群 Leader」的聚合判定从二进制层注入进来。
pub type ReadinessProbe = Arc<dyn Fn() -> ReadinessState + Send + Sync>;

/// 库 crate 侧无 `AppState` 时的退化探针（恒就绪）。
///
/// 仅用于 `IngestLayer::create_rest_router` 等未接入二进制层状态的便捷入口；
/// 主进程 `src/routes.rs` 会注入真实三态探针。
pub fn always_ready_probe() -> ReadinessProbe {
    Arc::new(|| ReadinessState::ready())
}

/// Health 路由状态（readiness 探针）。
#[derive(Clone)]
pub struct HealthState {
    /// readiness 探针。
    pub readiness_probe: ReadinessProbe,
}

/// Health check response.
#[derive(Debug, Serialize)]
pub struct HealthResponse {
    pub status: String,
    pub version: String,
    pub uptime_secs: u64,
    pub collector_state: String,
}

/// Handle health: GET /health
pub async fn handle_health() -> impl IntoResponse {
    let response = HealthResponse {
        status: "healthy".into(),
        version: env!("CARGO_PKG_VERSION").to_string(),
        uptime_secs: 0, // In production, track actual uptime
        collector_state: "running".into(),
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

/// Handle readiness: GET /health/ready
///
/// 通过注入的 `ReadinessProbe` 执行真实探测：
/// - 就绪 → 200 / `{code:0, message:"ok", data:{ready:true}}`
/// - 未就绪 → 503 / `{code:50301, message:<原因>, data:{ready:false}}`
pub async fn handle_readiness(State(state): State<Arc<HealthState>>) -> impl IntoResponse {
    let probe = (state.readiness_probe)();
    let ready = probe.ready;
    let message = if ready {
        "ok".to_string()
    } else {
        probe.reason.unwrap_or_else(|| "not ready".to_string())
    };

    let status = if ready {
        StatusCode::OK
    } else {
        StatusCode::SERVICE_UNAVAILABLE
    };

    (
        status,
        Json(serde_json::json!({
            "code": if ready { 0 } else { SenseArcError::ServiceNotReady(message.clone()).code() },
            "message": message,
            "data": {
                "ready": ready,
            },
        })),
    )
}

/// Handle liveness: GET /health/live
pub async fn handle_liveness() -> impl IntoResponse {
    // Simple liveness check: if this handler executes, we're alive.
    (
        StatusCode::OK,
        Json(serde_json::json!({
            "code": 0,
            "message": "alive",
            "data": {
                "alive": true,
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

    fn ready_state(probe: ReadinessProbe) -> Arc<HealthState> {
        Arc::new(HealthState {
            readiness_probe: probe,
        })
    }

    /// 成功码全仓唯一 0（契约 O2/C5）。
    #[tokio::test]
    async fn test_health_success_code_zero() {
        assert_eq!(body_code(handle_health().await.into_response()).await, 0);
        assert_eq!(
            body_code(
                handle_readiness(State(ready_state(always_ready_probe())))
                    .await
                    .into_response(),
            )
            .await,
            0
        );
        assert_eq!(body_code(handle_liveness().await.into_response()).await, 0);
    }

    /// 未就绪时返回 503 + code 50301（非 Internal 50001）。
    #[tokio::test]
    async fn test_readiness_not_ready_returns_50301() {
        let probe: ReadinessProbe = Arc::new(|| ReadinessState::not_ready("wal not writable"));
        let resp = handle_readiness(State(ready_state(probe)))
            .await
            .into_response();
        assert_eq!(resp.status(), StatusCode::SERVICE_UNAVAILABLE);
        let body = axum::body::to_bytes(resp.into_body(), 1024).await.unwrap();
        let v: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(v["code"].as_i64().unwrap(), 50301);
        assert_eq!(v["message"].as_str().unwrap(), "wal not writable");
        assert_eq!(v["data"]["ready"].as_bool().unwrap(), false);
    }
}
