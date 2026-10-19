//! 回放 REST 端点。
//!
//! ## 端点
//!
//! - `POST /api/v1/replay/start` — 启动回放会话。
//! - `POST /api/v1/replay/pause` — 暂停回放。
//! - `POST /api/v1/replay/resume` — 恢复回放。
//! - `POST /api/v1/replay/stop` — 停止回放。
//! - `GET /api/v1/replay/sessions` — 列出所有会话。
//! - `GET /api/v1/replay/sessions/:id` — 获取会话详情。
//! - `GET /api/v1/replay/sessions/:id/events` — 获取会话事件流。

use axum::{
    extract::{Path, State},
    http::StatusCode,
    response::IntoResponse,
    Json,
};
use serde::{Deserialize, Serialize};
use std::sync::Arc;

use crate::replay::clock::ReplaySpeed;
use crate::replay::engine::ReplayEngine;
use crate::replay::session::{ReplayParams, ReplaySessionHandle};

/// 回放 API 共享状态。
#[derive(Clone)]
pub struct ReplayApiState {
    /// 回放引擎引用。
    pub engine: Arc<ReplayEngine>,
}

/// 启动回放请求体。
#[derive(Debug, Deserialize)]
pub struct StartReplayRequest {
    /// 要回放的 tag ID 列表。
    pub tag_ids: Vec<u32>,
    /// 起始时间戳 (epoch ns)。
    pub start_ts: i64,
    /// 结束时间戳 (epoch ns)。
    pub end_ts: i64,
    /// 回放速度（<=0=Max，>0 任意倍速），默认 1.0。
    #[serde(default = "default_speed")]
    pub speed: f64,
    /// 是否包含派生标签计算，默认 false。
    #[serde(default)]
    pub include_derived: bool,
    /// 是否包含告警评估，默认 false。
    #[serde(default)]
    pub include_alerts: bool,
}

fn default_speed() -> f64 {
    1.0
}

/// 启动回放响应体。
#[derive(Debug, Serialize)]
pub struct StartReplayResponse {
    pub session_id: String,
}

/// 会话控制请求体。
#[derive(Debug, Deserialize)]
pub struct SessionControlRequest {
    pub session_id: String,
}

/// 会话详情响应体。
#[derive(Debug, Serialize)]
pub struct SessionDetailResponse {
    pub session_id: String,
    pub state: String,
    pub tag_ids: Vec<u32>,
    pub speed: String,
    pub start_ts: i64,
    pub end_ts: i64,
    pub processed_count: u64,
    pub total_entries: u64,
    pub progress: f64,
    pub error_count: u64,
}

impl From<&ReplaySessionHandle> for SessionDetailResponse {
    fn from(s: &ReplaySessionHandle) -> Self {
        let stats = s.stats.read();
        Self {
            session_id: s.id.clone(),
            state: s.current_state().to_string(),
            tag_ids: s.params.tag_ids.clone(),
            speed: s.params.speed.to_string(),
            start_ts: s.params.start_ts,
            end_ts: s.params.end_ts,
            processed_count: stats.processed_count,
            total_entries: s.store.total_entries(),
            progress: s.progress(),
            error_count: stats.error_count,
        }
    }
}

/// 列出所有会话响应。
#[derive(Debug, Serialize)]
pub struct SessionListResponse {
    pub sessions: Vec<SessionDetailResponse>,
}

// ─── 路由 ───

/// 启动回放。
pub async fn start_replay(
    State(state): State<ReplayApiState>,
    Json(req): Json<StartReplayRequest>,
) -> impl IntoResponse {
    let params = ReplayParams {
        tag_ids: req.tag_ids,
        start_ts: req.start_ts,
        end_ts: req.end_ts,
        speed: ReplaySpeed::from_multiplier(req.speed),
        include_derived: req.include_derived,
        include_alerts: req.include_alerts,
    };

    match state.engine.start_session(params).await {
        Ok(session_id) => (
            StatusCode::CREATED,
            Json(serde_json::json!({
                "session_id": session_id,
                "status": "started"
            })),
        ),
        Err(e) => (
            StatusCode::CONFLICT,
            Json(serde_json::json!({
                "error": e.to_string()
            })),
        ),
    }
}

/// 暂停回放。
pub async fn pause_replay(
    State(state): State<ReplayApiState>,
    Json(req): Json<SessionControlRequest>,
) -> impl IntoResponse {
    match state.engine.pause_session(&req.session_id) {
        Ok(()) => (
            StatusCode::OK,
            Json(serde_json::json!({"status": "paused"})),
        ),
        Err(e) => (
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({"error": e.to_string()})),
        ),
    }
}

/// 恢复回放。
pub async fn resume_replay(
    State(state): State<ReplayApiState>,
    Json(req): Json<SessionControlRequest>,
) -> impl IntoResponse {
    match state.engine.resume_session(&req.session_id) {
        Ok(()) => (
            StatusCode::OK,
            Json(serde_json::json!({"status": "resumed"})),
        ),
        Err(e) => (
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({"error": e.to_string()})),
        ),
    }
}

/// 停止回放。
pub async fn stop_replay(
    State(state): State<ReplayApiState>,
    Json(req): Json<SessionControlRequest>,
) -> impl IntoResponse {
    match state.engine.stop_session(&req.session_id) {
        Ok(()) => (
            StatusCode::OK,
            Json(serde_json::json!({"status": "stopped"})),
        ),
        Err(e) => (
            StatusCode::NOT_FOUND,
            Json(serde_json::json!({"error": e.to_string()})),
        ),
    }
}

/// 列出所有会话。
pub async fn list_sessions(State(state): State<ReplayApiState>) -> impl IntoResponse {
    let ids = state.engine.list_sessions();
    let sessions: Vec<SessionDetailResponse> = ids
        .iter()
        .filter_map(|id| state.engine.get_session(id))
        .map(|s| SessionDetailResponse::from(s.as_ref()))
        .collect();

    (StatusCode::OK, Json(SessionListResponse { sessions }))
}

/// 获取单个会话详情。
pub async fn get_session(
    State(state): State<ReplayApiState>,
    Path(session_id): Path<String>,
) -> impl IntoResponse {
    match state.engine.get_session(&session_id) {
        Some(session) => {
            let detail = SessionDetailResponse::from(&*session);
            (
                StatusCode::OK,
                Json(serde_json::to_value(detail).unwrap_or_default()),
            )
                .into_response()
        }
        None => (
            StatusCode::NOT_FOUND,
            Json(serde_json::json!({"error": "session not found"})),
        )
            .into_response(),
    }
}

/// 获取会话事件流。
pub async fn get_session_events(
    State(state): State<ReplayApiState>,
    Path(session_id): Path<String>,
    axum::extract::Query(params): axum::extract::Query<std::collections::HashMap<String, String>>,
) -> impl IntoResponse {
    let limit: usize = params
        .get("limit")
        .and_then(|v| v.parse().ok())
        .unwrap_or(100);

    match state.engine.get_session(&session_id) {
        Some(session) => {
            let events = session.get_recent_events(limit);
            (
                StatusCode::OK,
                Json(serde_json::json!({
                    "session_id": session_id,
                    "events": events,
                })),
            )
        }
        None => (
            StatusCode::NOT_FOUND,
            Json(serde_json::json!({"error": "session not found"})),
        ),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::replay::clock::ReplaySpeed;

    #[test]
    fn test_start_request_defaults() {
        let json = r#"{"tag_ids":[1,2],"start_ts":0,"end_ts":1000}"#;
        let req: StartReplayRequest = serde_json::from_str(json).unwrap();
        assert_eq!(req.speed, 1.0);
        assert!(!req.include_derived);
    }

    #[test]
    fn test_session_detail_from_handle() {
        use crate::replay::session::ReplaySessionHandle;
        use crate::replay::store::ReplaySnapshotStore;

        let params = ReplayParams {
            tag_ids: vec![1],
            start_ts: 0,
            end_ts: 1000,
            speed: ReplaySpeed(2.0),
            include_derived: false,
            include_alerts: false,
        };
        let store = Arc::new(ReplaySnapshotStore::new("s1".into(), 4, 1000));
        let session = ReplaySessionHandle::new("s1".into(), params, store, 100);
        let detail = SessionDetailResponse::from(&session);
        assert_eq!(detail.session_id, "s1");
        assert_eq!(detail.state, "Idle");
        assert_eq!(detail.speed, "2x");
    }
}
