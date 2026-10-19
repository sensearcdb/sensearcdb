//! gRPC service implementation for PartitionAdmin.
//!
//! Implements the `PartitionAdmin` service defined in `partition.proto`:
//! - `ListPartitions` — list all partitions for a table.
//! - `GetPartition` — get details for a specific partition.
//! - `FreezePartition` — force-freeze an active partition.
//! - `AdvancePartition` — advance to the next partition.

use crate::manager::PartitionManager;
use crate::proto::{
    partition_admin_server::PartitionAdmin, AdvancePartitionRequest, AdvancePartitionResponse,
    FreezePartitionRequest, FreezePartitionResponse, GetPartitionRequest, ListPartitionsRequest,
    ListPartitionsResponse, PartitionInfo as ProtoPartitionInfo,
};
use std::sync::Arc;
use tonic::{Request, Response, Status};

/// gRPC service implementing the `PartitionAdmin` trait.
pub struct PartitionAdminService {
    manager: Arc<PartitionManager>,
}

impl PartitionAdminService {
    /// Create a new PartitionAdmin gRPC service.
    pub fn new(manager: Arc<PartitionManager>) -> Self {
        Self { manager }
    }
}

/// Convert a domain `PartitionInfo` to a protobuf `PartitionInfo`.
fn to_proto_partition_info(
    info: &crate::state::PartitionInfo,
    duration_ms: u64,
) -> ProtoPartitionInfo {
    ProtoPartitionInfo {
        partition_id: (info.partition_ns as u64), // Use partition_ns as partition_id
        table_name: info.table_id.to_string(),
        state: info.state.as_str().to_string(),
        start_time_ns: info.partition_ns,
        end_time_ns: info.partition_ns + (duration_ms as i64) * 1_000_000,
        freeze_count: info.freeze_count as u64,
        row_count: 0, // Not tracked at partition level yet
        disk_bytes: info.total_bytes,
    }
}

#[tonic::async_trait]
impl PartitionAdmin for PartitionAdminService {
    /// List all partitions, optionally filtered by table name.
    async fn list_partitions(
        &self,
        request: Request<ListPartitionsRequest>,
    ) -> Result<Response<ListPartitionsResponse>, Status> {
        let req = request.into_inner();
        let config = self.manager.partition_config();

        let partitions: Vec<ProtoPartitionInfo> = if req.table_name.is_empty() {
            // Return all partitions across all tables
            self.manager
                .list_all_partitions()
                .iter()
                .map(|info| to_proto_partition_info(info.as_ref(), config.duration_ms))
                .collect()
        } else {
            // Parse table_name as u32 table_id
            let table_id: u32 = req
                .table_name
                .parse()
                .map_err(|_| Status::invalid_argument("table_name must be a valid table ID"))?;

            self.manager
                .list_partitions_by_table(table_id)
                .iter()
                .map(|info| to_proto_partition_info(info.as_ref(), config.duration_ms))
                .collect()
        };

        Ok(Response::new(ListPartitionsResponse { partitions }))
    }

    /// Get details for a specific partition.
    async fn get_partition(
        &self,
        request: Request<GetPartitionRequest>,
    ) -> Result<Response<ProtoPartitionInfo>, Status> {
        let req = request.into_inner();
        let config = self.manager.partition_config();

        // Parse table_name as u32
        let table_id: u32 = req
            .table_name
            .parse()
            .map_err(|_| Status::invalid_argument("table_name must be a valid table ID"))?;

        // partition_id in proto maps to partition_ns
        let partition_ns = req.partition_id as i64;

        match self.manager.get_partition(table_id, partition_ns) {
            Some(info) => Ok(Response::new(to_proto_partition_info(
                info.as_ref(),
                config.duration_ms,
            ))),
            None => Err(Status::not_found(format!(
                "partition not found: table_id={}, partition_ns={}",
                table_id, partition_ns
            ))),
        }
    }

    /// Force-freeze a partition.
    async fn freeze_partition(
        &self,
        request: Request<FreezePartitionRequest>,
    ) -> Result<Response<FreezePartitionResponse>, Status> {
        let req = request.into_inner();

        let table_id: u32 = req
            .table_name
            .parse()
            .map_err(|_| Status::invalid_argument("table_name must be a valid table ID"))?;

        let partition_ns = req.partition_id as i64;

        // Check that the partition exists and is Active
        let info = self
            .manager
            .get_partition(table_id, partition_ns)
            .ok_or_else(|| {
                Status::not_found(format!(
                    "partition not found: table_id={}, partition_ns={}",
                    table_id, partition_ns
                ))
            })?;

        // Advance the partition (this freezes the current one and creates a new Active)
        match self.manager.advance_partition(table_id).await {
            Ok(result) => Ok(Response::new(FreezePartitionResponse {
                success: true,
                new_state: result.previous_state.as_str().to_string(),
            })),
            Err(e) => {
                // If the partition was already Frozen (not Active), treat as success
                if !info.state.is_writable() {
                    Ok(Response::new(FreezePartitionResponse {
                        success: true,
                        new_state: info.state.as_str().to_string(),
                    }))
                } else {
                    Err(Status::internal(format!("freeze failed: {}", e)))
                }
            }
        }
    }

    /// Advance to the next partition.
    async fn advance_partition(
        &self,
        request: Request<AdvancePartitionRequest>,
    ) -> Result<Response<AdvancePartitionResponse>, Status> {
        let req = request.into_inner();

        let table_id: u32 = req
            .table_name
            .parse()
            .map_err(|_| Status::invalid_argument("table_name must be a valid table ID"))?;

        match self.manager.advance_partition(table_id).await {
            Ok(result) => Ok(Response::new(AdvancePartitionResponse {
                old_partition_id: result.previous_partition_ns as u64,
                new_partition_id: result.new_partition_ns as u64,
            })),
            Err(e) => Err(Status::internal(format!("advance failed: {}", e))),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use sensearcdb_common::config::{AppConfig, ConfigHandle};
    use std::sync::Arc;
    use tempfile::tempdir;

    async fn make_test_service() -> PartitionAdminService {
        let dir = tempdir().unwrap();
        let sac_dir = dir.path().join("sac");
        std::fs::create_dir_all(&sac_dir).unwrap();

        let mut cfg = AppConfig::default();
        cfg.paths.data_dir = dir.path().to_string_lossy().to_string();

        // Need to keep tempdir alive... but for tests this is fine
        let handle = ConfigHandle::new(cfg);
        let engine = Arc::new(
            sensearcdb_engine::engine::Engine::new(&handle, None)
                .await
                .unwrap(),
        );
        let manager = Arc::new(PartitionManager::new(handle, engine).await.unwrap());
        PartitionAdminService::new(manager)
    }

    #[tokio::test]
    async fn test_list_partitions_empty() {
        let svc = make_test_service().await;
        let req = Request::new(ListPartitionsRequest {
            table_name: String::new(),
        });
        let resp = svc.list_partitions(req).await.unwrap();
        let body = resp.into_inner();
        assert!(body.partitions.is_empty());
    }

    #[tokio::test]
    async fn test_get_partition_not_found() {
        let svc = make_test_service().await;
        let req = Request::new(GetPartitionRequest {
            table_name: "1".to_string(),
            partition_id: 0,
        });
        let result = svc.get_partition(req).await;
        assert!(result.is_err());
        assert_eq!(result.unwrap_err().code(), tonic::Code::NotFound);
    }

    #[tokio::test]
    async fn test_advance_partition_no_active() {
        let svc = make_test_service().await;
        let req = Request::new(AdvancePartitionRequest {
            table_name: "999".to_string(),
        });
        let result = svc.advance_partition(req).await;
        assert!(result.is_err());
        assert_eq!(result.unwrap_err().code(), tonic::Code::Internal);
    }

    #[tokio::test]
    async fn test_freeze_partition_not_found() {
        let svc = make_test_service().await;
        let req = Request::new(FreezePartitionRequest {
            table_name: "999".to_string(),
            partition_id: 0,
        });
        let result = svc.freeze_partition(req).await;
        assert!(result.is_err());
        assert_eq!(result.unwrap_err().code(), tonic::Code::NotFound);
    }

    #[tokio::test]
    async fn test_list_and_get_after_route() {
        let svc = make_test_service().await;

        // Route a write to auto-create a partition
        let ts = 1_703_980_800_000_000_000_i64;
        let info = svc.manager.route_to_partition(1, ts).unwrap();

        // List partitions for table 1
        let req_list = Request::new(ListPartitionsRequest {
            table_name: "1".to_string(),
        });
        let resp = svc.list_partitions(req_list).await.unwrap();
        let body = resp.into_inner();
        assert_eq!(body.partitions.len(), 1);

        // Get the partition
        let req_get = Request::new(GetPartitionRequest {
            table_name: "1".to_string(),
            partition_id: info.partition_ns as u64,
        });
        let resp_get = svc.get_partition(req_get).await.unwrap();
        let body_get = resp_get.into_inner();
        assert_eq!(body_get.state, "Active");
        assert_eq!(body_get.table_name, "1");
    }
}
