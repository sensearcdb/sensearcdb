//! REST API endpoints for partition management.
//!
//! # Endpoints
//!
//! | Method | Path | Description |
//! |--------|------|-------------|
//! | GET | `/api/v1/partitions` | List all partitions (optional `?table_name=`) |
//! | GET | `/api/v1/partitions/:table_id` | List partitions for a table |
//! | GET | `/api/v1/partitions/:table_id/:partition_ns` | Get a specific partition |
//! | POST | `/api/v1/partitions/:table_id/advance` | Advance to next partition |
//! | POST | `/api/v1/partitions/:table_id/:partition_ns/freeze` | Force-freeze a partition |

use crate::manager::PartitionManager;
use crate::state::PartitionState;
use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    response::IntoResponse,
    Json, Router,
};
use sensearcdb_common::route_registry::RouteRegistryExt;
use serde::{Deserialize, Serialize};
use std::sync::Arc;

/// Shared application state for REST handlers.
#[derive(Clone)]
pub struct AppState {
    pub manager: Arc<PartitionManager>,
}

/// Query parameters for listing partitions.
#[derive(Debug, Deserialize)]
pub struct ListQuery {
    /// Optional table name filter.
    pub table_name: Option<String>,
}

/// Response body for a single partition.
#[derive(Debug, Serialize)]
pub struct PartitionResponse {
    pub table_id: u32,
    pub partition_ns: i64,
    pub state: String,
    pub file_count: u32,
    pub total_bytes: u64,
    pub min_ts: Option<i64>,
    pub max_ts: Option<i64>,
    pub created_at: i64,
    pub frozen_at: Option<i64>,
    pub compacting_at: Option<i64>,
    pub archived_at: Option<i64>,
    pub freeze_count: u32,
}

impl From<&crate::state::PartitionInfo> for PartitionResponse {
    fn from(info: &crate::state::PartitionInfo) -> Self {
        Self {
            table_id: info.table_id,
            partition_ns: info.partition_ns,
            state: info.state.as_str().to_string(),
            file_count: info.file_count,
            total_bytes: info.total_bytes,
            min_ts: info.min_ts,
            max_ts: info.max_ts,
            created_at: info.created_at,
            frozen_at: info.frozen_at,
            compacting_at: info.compacting_at,
            archived_at: info.archived_at,
            freeze_count: info.freeze_count,
        }
    }
}

/// Response body for list endpoint.
#[derive(Debug, Serialize)]
pub struct ListPartitionsResponse {
    pub partitions: Vec<PartitionResponse>,
}

/// Response body for advance endpoint.
#[derive(Debug, Serialize)]
pub struct AdvancePartitionResponse {
    pub previous_partition_ns: i64,
    pub new_partition_ns: i64,
    pub previous_state: String,
    pub new_state: String,
}

/// Response body for freeze endpoint.
#[derive(Debug, Serialize)]
pub struct FreezePartitionResponse {
    pub success: bool,
    pub new_state: String,
    pub message: Option<String>,
}

/// Generic error response.
#[derive(Debug, Serialize)]
pub struct ErrorResponse {
    pub error: String,
}

/// Build the partition REST router.
///
/// Returns an `axum::Router` with all partition endpoints mounted.
/// The router should be nested under the main application router.
pub fn build_router(state: AppState) -> Router {
    Router::new()
        .register_route(
            "/api/v1/partitions",
            sensearcdb_common::route!(get(list_partitions)),
        )
        .register_route(
            "/api/v1/partitions/:table_id",
            sensearcdb_common::route!(get(list_table_partitions)),
        )
        .register_route(
            "/api/v1/partitions/:table_id/:partition_ns",
            sensearcdb_common::route!(get(get_partition)),
        )
        .register_route(
            "/api/v1/partitions/:table_id/advance",
            sensearcdb_common::route!(post(advance_partition)),
        )
        .register_route(
            "/api/v1/partitions/:table_id/:partition_ns/freeze",
            sensearcdb_common::route!(post(freeze_partition)),
        )
        .with_state(state)
}

/// GET /api/v1/partitions — List all partitions, optionally filtered by table_name.
async fn list_partitions(
    State(state): State<AppState>,
    Query(query): Query<ListQuery>,
) -> impl IntoResponse {
    let all = state.manager.list_all_partitions();

    let filtered: Vec<PartitionResponse> = if let Some(ref table_name) = query.table_name {
        // table_name filter: convert to table_id if numeric, else return empty
        if let Ok(table_id) = table_name.parse::<u32>() {
            all.iter()
                .filter(|info| info.table_id == table_id)
                .map(|info| PartitionResponse::from(info.as_ref()))
                .collect()
        } else {
            Vec::new()
        }
    } else {
        all.iter()
            .map(|info| PartitionResponse::from(info.as_ref()))
            .collect()
    };

    (
        StatusCode::OK,
        Json(ListPartitionsResponse {
            partitions: filtered,
        }),
    )
}

/// GET /api/v1/partitions/:table_id — List partitions for a specific table.
async fn list_table_partitions(
    State(state): State<AppState>,
    Path(table_id): Path<u32>,
) -> impl IntoResponse {
    let partitions = state.manager.list_partitions_by_table(table_id);
    let response: Vec<PartitionResponse> = partitions
        .iter()
        .map(|info| PartitionResponse::from(info.as_ref()))
        .collect();

    (
        StatusCode::OK,
        Json(ListPartitionsResponse {
            partitions: response,
        }),
    )
}

/// GET /api/v1/partitions/:table_id/:partition_ns — Get a specific partition.
async fn get_partition(
    State(state): State<AppState>,
    Path((table_id, partition_ns)): Path<(u32, i64)>,
) -> impl IntoResponse {
    match state.manager.get_partition(table_id, partition_ns) {
        Some(info) => {
            let resp = PartitionResponse::from(info.as_ref());
            (StatusCode::OK, Json(resp)).into_response()
        }
        None => (
            StatusCode::NOT_FOUND,
            Json(ErrorResponse {
                error: format!(
                    "partition not found: table_id={}, partition_ns={}",
                    table_id, partition_ns
                ),
            }),
        )
            .into_response(),
    }
}

/// POST /api/v1/partitions/:table_id/advance — Advance to the next partition.
async fn advance_partition(
    State(state): State<AppState>,
    Path(table_id): Path<u32>,
) -> impl IntoResponse {
    match state.manager.advance_partition(table_id).await {
        Ok(result) => {
            let resp = AdvancePartitionResponse {
                previous_partition_ns: result.previous_partition_ns,
                new_partition_ns: result.new_partition_ns,
                previous_state: result.previous_state.as_str().to_string(),
                new_state: result.new_state.as_str().to_string(),
            };
            (StatusCode::OK, Json(resp)).into_response()
        }
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ErrorResponse {
                error: format!("advance failed: {}", e),
            }),
        )
            .into_response(),
    }
}

/// POST /api/v1/partitions/:table_id/:partition_ns/freeze — Force-freeze a partition.
async fn freeze_partition(
    State(state): State<AppState>,
    Path((table_id, partition_ns)): Path<(u32, i64)>,
) -> impl IntoResponse {
    let info = match state.manager.get_partition(table_id, partition_ns) {
        Some(info) => info,
        None => {
            return (
                StatusCode::NOT_FOUND,
                Json(ErrorResponse {
                    error: format!(
                        "partition not found: table_id={}, partition_ns={}",
                        table_id, partition_ns
                    ),
                }),
            )
                .into_response();
        }
    };

    // Check if already frozen or beyond
    if info.state != PartitionState::Active {
        return (
            StatusCode::OK,
            Json(FreezePartitionResponse {
                success: true,
                new_state: info.state.as_str().to_string(),
                message: Some(format!(
                    "partition already in state: {}",
                    info.state.as_str()
                )),
            }),
        )
            .into_response();
    }

    // Advance the partition (which freezes the current one)
    match state.manager.advance_partition(table_id).await {
        Ok(result) => (
            StatusCode::OK,
            Json(FreezePartitionResponse {
                success: true,
                new_state: result.previous_state.as_str().to_string(),
                message: None,
            }),
        )
            .into_response(),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ErrorResponse {
                error: format!("freeze failed: {}", e),
            }),
        )
            .into_response(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::manager::PartitionManager;
    use axum::http::StatusCode;
    use sensearcdb_common::config::{AppConfig, ConfigHandle};
    use std::sync::Arc;
    use tempfile::tempdir;

    async fn make_test_state() -> (AppState, tempfile::TempDir) {
        let dir = tempdir().unwrap();
        let sac_dir = dir.path().join("sac");
        std::fs::create_dir_all(&sac_dir).unwrap();

        let mut cfg = AppConfig::default();
        cfg.paths.data_dir = dir.path().to_string_lossy().to_string();

        let handle = ConfigHandle::new(cfg);
        let engine = Arc::new(
            sensearcdb_engine::engine::Engine::new(&handle, None)
                .await
                .unwrap(),
        );
        let manager = Arc::new(PartitionManager::new(handle, engine).await.unwrap());
        (AppState { manager }, dir)
    }

    #[tokio::test]
    async fn test_list_partitions_empty() {
        let (state, _dir) = make_test_state().await;

        let response = list_partitions(State(state), Query(ListQuery { table_name: None })).await;
        assert_eq!(response.into_response().status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn test_list_table_partitions_empty() {
        let (state, _dir) = make_test_state().await;

        // Test directly via the handler function instead of routing
        let response = list_table_partitions(State(state), Path(999u32)).await;
        // The handler always returns 200 with an empty list for unknown tables
        assert_eq!(response.into_response().status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn test_get_partition_not_found() {
        let (state, _dir) = make_test_state().await;

        let response = get_partition(State(state), Path((999u32, 0i64))).await;
        assert_eq!(response.into_response().status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn test_advance_partition_fails_for_unknown_table() {
        let (state, _dir) = make_test_state().await;

        let response = advance_partition(State(state), Path(999u32)).await;
        // Should fail with 500 since there's no table 999 to advance
        assert_eq!(
            response.into_response().status(),
            StatusCode::INTERNAL_SERVER_ERROR
        );
    }
}
