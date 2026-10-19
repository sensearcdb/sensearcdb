//! gRPC QueryService — SQL query execution with Arrow IPC streaming.
//!
//! Implements the `QueryService` proto service:
//! - `Query`: Execute SQL and stream results as Arrow IPC-format record batches.
//!
//! The service delegates to `Engine::query` (three-source merge: active
//! MemTable + frozen MemTable + SAC blocks, see `engine/src/query/executor.rs`),
//! then serializes each returned `RecordBatch` into a self-contained Arrow IPC
//! stream (schema + one batch) and streams it back to the client.

use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status};
use tracing::{debug, warn};

use sensearcdb_engine::engine::Engine;

use crate::grpc::auth::{resolve_tenant, GrpcTenantResolver};
use crate::proto::query_service_server::QueryService;
use crate::proto::{QueryRequest, QueryResponse};

/// gRPC QueryService implementation.
///
/// Holds a shared `Engine` handle and delegates SQL execution to the engine's
/// query executor. Results are streamed back as Arrow IPC record batches.
///
/// Tenant 从认证凭据派生（服务端信任边界）：优先读 interceptor 注入的
/// [`crate::grpc::auth::AuthContext`]，否则回退注入的 `GrpcTenantResolver`；
/// 两者都不可得 → 401（Q1 强制认证）。
pub struct GrpcQueryService {
    /// Shared storage engine (real three-source query executor).
    engine: Arc<Engine>,
    /// Per-query execution timeout in milliseconds.
    query_timeout_ms: u64,
    /// Request tenant resolver（无 interceptor 场景的兜底解析）。
    tenant_resolver: Option<GrpcTenantResolver>,
}

impl GrpcQueryService {
    /// Create a new QueryService backed by the given engine.
    pub fn new(engine: Arc<Engine>) -> Self {
        Self {
            engine,
            query_timeout_ms: 30_000,
            tenant_resolver: None,
        }
    }

    /// Set query timeout in milliseconds.
    pub fn with_timeout(mut self, timeout_ms: u64) -> Self {
        self.query_timeout_ms = timeout_ms;
        self
    }

    /// Attach a request tenant resolver.
    pub fn with_tenant_resolver(mut self, resolver: GrpcTenantResolver) -> Self {
        self.tenant_resolver = Some(resolver);
        self
    }
}

/// Arrow IPC record batch response stream.
pub type QueryResultStream =
    Pin<Box<dyn tokio_stream::Stream<Item = Result<QueryResponse, Status>> + Send>>;

#[tonic::async_trait]
impl QueryService for GrpcQueryService {
    type QueryStream = QueryResultStream;

    /// Execute a SQL query and stream results back.
    async fn query(
        &self,
        request: Request<QueryRequest>,
    ) -> Result<Response<Self::QueryStream>, Status> {
        // Q1 强制认证：tenant 必须能解析（interceptor 扩展 → resolver），否则 401。
        let tenant_id = resolve_tenant(&request, &self.tenant_resolver)?;
        let req = request.into_inner();
        debug!(query = %req.query, tenant_id = %tenant_id, "gRPC query received");

        let engine = self.engine.clone();
        let timeout_ms = self.query_timeout_ms;

        let (tx, rx) = mpsc::channel::<Result<QueryResponse, Status>>(16);

        // Execute the query off the request task and stream batches back as
        // they are produced. Empty results (no rows) legitimately produce an
        // empty stream; execution errors surface as an explicit `Status`.
        tokio::spawn(async move {
            let outcome = tokio::time::timeout(
                Duration::from_millis(timeout_ms),
                engine.query(tenant_id, &req.query),
            )
            .await;

            match outcome {
                Ok(Ok(query_result)) => {
                    if let Some(warning) = &query_result.warning {
                        warn!(warning = %warning, "query produced a warning");
                    }

                    for batch in query_result.batches {
                        let row_count = batch.num_rows() as u32;
                        match serialize_record_batch(&batch) {
                            Ok(arrow_batch) => {
                                if tx
                                    .send(Ok(QueryResponse {
                                        arrow_batch,
                                        row_count,
                                    }))
                                    .await
                                    .is_err()
                                {
                                    // Client disconnected; stop streaming.
                                    break;
                                }
                            }
                            Err(e) => {
                                warn!(error = %e, "failed to serialize Arrow RecordBatch");
                                let _ = tx
                                    .send(Err(Status::internal(format!(
                                        "arrow serialization failed: {}",
                                        e
                                    ))))
                                    .await;
                                break;
                            }
                        }
                    }
                }
                Ok(Err(e)) => {
                    warn!(error = %e, "query execution failed");
                    let _ = tx
                        .send(Err(Status::internal(format!("query failed: {}", e))))
                        .await;
                }
                Err(_elapsed) => {
                    warn!(timeout_ms, "query timed out");
                    let _ = tx
                        .send(Err(Status::deadline_exceeded("query timed out")))
                        .await;
                }
            }
        });

        Ok(Response::new(Box::pin(ReceiverStream::new(rx))))
    }
}

/// Serialize a single Arrow `RecordBatch` into a self-contained Arrow IPC
/// stream (schema header + one record batch), matching the proto contract
/// (`QueryResponse.arrow_batch` carries IPC-format bytes).
fn serialize_record_batch(
    batch: &arrow::record_batch::RecordBatch,
) -> Result<Vec<u8>, arrow::error::ArrowError> {
    use arrow::ipc::writer::StreamWriter;

    let mut buf = Vec::new();
    {
        let mut writer = StreamWriter::try_new(&mut buf, batch.schema().as_ref())?;
        writer.write(batch)?;
        writer.finish()?;
    }
    Ok(buf)
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Float64Array, Int64Array, UInt8Array};
    use arrow::datatypes::{DataType, Field, Schema};

    #[test]
    fn test_serialize_record_batch_to_ipc() {
        // 注意：本测试只验证 `serialize_record_batch` 的 Arrow IPC 序列化机制
        // （写出可被 StreamReader 消费的 IPC 流），NOT 查询响应的 schema 契约。
        // gRPC 查询 schema 是 typed 的（见 tests/qa_grpc_query_typed_e2e.rs），
        // 此处固定 3 列仅为最小化 IPC 往返验证，不表示「gRPC 恒返回 Float64 value」。
        let schema = Arc::new(Schema::new(vec![
            Field::new("ts", DataType::Int64, false),
            Field::new("value", DataType::Float64, false),
            Field::new("quality", DataType::UInt8, false),
        ]));
        let batch = arrow::record_batch::RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![100_i64, 200_i64])),
                Arc::new(Float64Array::from(vec![1.5_f64, 2.5_f64])),
                Arc::new(UInt8Array::from(vec![0xC0_u8, 0xC0_u8])),
            ],
        )
        .expect("record batch");

        let bytes = serialize_record_batch(&batch).expect("serialize");
        assert!(bytes.len() > 8);

        // The serialized stream begins with the 4-byte Arrow IPC continuation marker.
        assert_eq!(&bytes[..4], &[0xFF, 0xFF, 0xFF, 0xFF]);
    }
}
