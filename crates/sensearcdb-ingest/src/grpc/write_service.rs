//! gRPC WriteService — handles high-throughput streaming and single-point writes.
//!
//! Implements the `WriteService` proto service:
//! - `Ingest`: client-streaming batch ingest
//! - `SingleWrite`: single data point write

use std::sync::Arc;
use std::time::Instant;

use sensearcdb_common::write_attr;
use tonic::{Request, Response, Status, Streaming};
use tracing::warn;

use crate::batch::PointBatch;
use crate::writer::{EngineWriter, WriterType};
use crate::column::resolve_column_id;
use crate::grpc::auth::{resolve_tenant, GrpcTenantResolver};
use crate::proto::write_service_server::WriteService;
use crate::proto::{SingleWriteRequest, WriteReceipt as ProtoReceipt};
use crate::rest::write::TableNameResolver;
use crate::types::{EngineWriteError, WriteReceipt};
use sensearcdb_engine::engine::{DataPoint, PointBatch as EnginePointBatch};

/// 服务端攒批：单流多条 proto 消息按行数 + 空闲超时合并为一次引擎级跨表写入。
const BATCH_MAX_ROWS: usize = 1024;
const BATCH_TIMEOUT: std::time::Duration = std::time::Duration::from_millis(2);

/// gRPC WriteService implementation.
///
/// Delegates write operations to an `EngineWriter` (embedded or remote) and
/// resolves the wire-level `table_name` to a real `table_id` through an injected
/// `TableNameResolver` (see `GAP2-ING-02`).
///
/// Tenant 从认证凭据派生（服务端信任边界）：优先读 interceptor 注入的
/// [`crate::grpc::auth::AuthContext`]，否则回退注入的 `GrpcTenantResolver`；
/// 两者都不可得 → 401（Q1 强制认证，不静默回退默认租户）。
pub struct GrpcWriteService {
    writer: Arc<dyn EngineWriter>,
    resolver: Option<Arc<dyn TableNameResolver>>,
    tenant_resolver: Option<GrpcTenantResolver>,
}

impl GrpcWriteService {
    /// Create a new WriteService backed by the given writer.
    ///
    /// No resolver is attached: `table_name` defaults to `table_id = 0`
    /// (backward-compatible direct write, logged for visibility).
    pub fn new(writer: Arc<dyn EngineWriter>) -> Self {
        Self {
            writer,
            resolver: None,
            tenant_resolver: None,
        }
    }

    /// Attach a `table_name → table_id` resolver.
    pub fn with_resolver(mut self, resolver: Arc<dyn TableNameResolver>) -> Self {
        self.resolver = Some(resolver);
        self
    }

    /// Attach a request tenant resolver（无 interceptor 场景的兜底解析）。
    pub fn with_tenant_resolver(mut self, resolver: GrpcTenantResolver) -> Self {
        self.tenant_resolver = Some(resolver);
        self
    }

    /// Resolve a wire-level `table_name` to a `table_id`.
    ///
    /// Returns `None` when a resolver is attached but the table is unknown.
    /// When no resolver is attached, falls back to `Some(0)` for backward
    /// compatibility (direct embedded writes).
    fn resolve_table_id(
        &self,
        tenant_id: sensearcdb_common::TenantId,
        table_name: &str,
    ) -> Option<u32> {
        match &self.resolver {
            Some(resolver) => resolver
                .resolve(tenant_id, table_name)
                .map(|(id, _writable)| id),
            None => {
                tracing::warn!(
                    table_name = table_name,
                    "no table-name resolver configured; defaulting table_id to 0"
                );
                Some(0)
            }
        }
    }

    /// Process a single proto batch: resolve table → decode rows → write.
    ///
    /// Returns the per-batch written/failed counts. On engine backpressure it
    /// returns `Err(RESOURCE_EXHAUSTED)` so the caller stops consuming the
    /// stream — the batch was **not** written, so a client retry is safe.
    async fn ingest_one(
        &self,
        tenant_id: sensearcdb_common::TenantId,
        batch: crate::proto::MultiTableBatch,
    ) -> Result<IngestDelta, Status> {
        // 行式解码：逐 PointRow resolve table_name → table_id，直接产 engine DataPoint。
        // 未知表按 row 隔离计数 failed（不污染整批）。
        let server_ts = chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0);
        let conv_t0 = Instant::now();
        let (points, failed) = proto_rows_to_datapoints(
            batch,
            tenant_id,
            &|name| self.resolve_table_id(tenant_id, name),
            server_ts,
        );
        write_attr::add_convert_batch(conv_t0.elapsed().as_nanos() as u64);

        if failed > 0 {
            warn!(failed, "gRPC ingest: rows skipped (table not found)");
        }

        if points.is_empty() {
            return Ok(IngestDelta {
                rows_written: 0,
                rows_failed: failed,
            });
        }

        let n = points.len() as u64;
        match self
            .writer
            .write_engine_batch_retry(EnginePointBatch { points })
            .await
        {
            Ok(receipt) => Ok(IngestDelta {
                rows_written: receipt.points_written as u64,
                rows_failed: failed + receipt.points_failed as u64,
            }),
            Err(e) if e.is_backpressure() => {
                // 背压信号透出：引擎在等待排空后仍写不下（超时）时返回 RESOURCE_EXHAUSTED，
                // 让客户端重试同一批（该批未被写入，重试不会重复），
                // 而不是静默计数 total_failed 后继续收流丢点。
                warn!(error = %e, "gRPC ingest backpressure, returning RESOURCE_EXHAUSTED");
                Err(Status::resource_exhausted(format!(
                    "engine backpressure: {e}"
                )))
            }
            Err(e) => {
                warn!(error = %e, "gRPC ingest write failed");
                Ok(IngestDelta {
                    rows_written: 0,
                    rows_failed: failed + n,
                })
            }
        }
    }

    /// 攒批路径：单条 proto 消息 → resolve + 行式解码 → 追加到合并缓冲。
    /// 表不存在 / 非法行单条隔离计数失败，不污染整批。
    fn accumulate(
        &self,
        tenant_id: sensearcdb_common::TenantId,
        batch: crate::proto::MultiTableBatch,
        pending: &mut Vec<DataPoint>,
        total_failed: &mut u64,
    ) {
        if batch.points.is_empty() { return; } // 空消息：不写不计数
        let server_ts = chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0);
        let conv_t0 = Instant::now();
        let (points, failed) = proto_rows_to_datapoints(
            batch,
            tenant_id,
            &|name| self.resolve_table_id(tenant_id, name),
            server_ts,
        );
        write_attr::add_convert_batch(conv_t0.elapsed().as_nanos() as u64);
        if failed > 0 {
            warn!(failed, "gRPC ingest: rows skipped (table not found)");
        }
        *total_failed += failed;
        pending.extend(points);
    }

    /// flush 合并缓冲：一次 write_engine_batch_retry，背压转 RESOURCE_EXHAUSTED。
    async fn flush_pending(
        &self,
        pending: &mut Vec<DataPoint>,
        total_rows: &mut u64,
        total_failed: &mut u64,
    ) -> Result<(), Status> {
        if pending.is_empty() { return Ok(()); }
        let points = std::mem::take(pending);
        let n = points.len() as u64;
        match self.writer.write_engine_batch_retry(EnginePointBatch { points }).await {
            Ok(r) => { *total_rows += r.points_written as u64; *total_failed += r.points_failed as u64; }
            Err(e) if e.is_backpressure() => {
                warn!(error = %e, "gRPC ingest backpressure (merged), RESOURCE_EXHAUSTED");
                return Err(Status::resource_exhausted(format!("engine backpressure: {e}")));
            }
            Err(e) => { warn!(error = %e, "gRPC ingest merged write failed"); *total_failed += n; }
        }
        Ok(())
    }

    /// 攒批核心循环（Direct 路径）：把流中多条 proto 消息按「行数 + 空闲超时」合并为
    /// 1 次跨表引擎级写入。抽成泛型以用 `futures::stream::iter` 直接单测
    /// （`tonic::Streaming` 无法在进程内构造，仅在线路层由 `ingest` 持有）。
    async fn ingest_batched<S>(
        &self,
        tenant_id: sensearcdb_common::TenantId,
        mut stream: S,
        total_rows: &mut u64,
        total_failed: &mut u64,
    ) -> Result<(), Status>
    where
        S: futures::Stream<Item = Result<crate::proto::MultiTableBatch, Status>> + Unpin + Send,
    {
        use futures::StreamExt;
        let mut pending: Vec<DataPoint> = Vec::with_capacity(BATCH_MAX_ROWS);
        loop {
            let msg = if pending.is_empty() {
                stream.next().await
            } else {
                match tokio::time::timeout(BATCH_TIMEOUT, stream.next()).await {
                    Err(_elapsed) => {
                        self.flush_pending(&mut pending, total_rows, total_failed).await?;
                        continue;
                    }
                    Ok(m) => m,
                }
            };
            match msg {
                Some(Ok(batch)) => {
                    self.accumulate(tenant_id, batch, &mut pending, total_failed);
                    if pending.len() >= BATCH_MAX_ROWS {
                        self.flush_pending(&mut pending, total_rows, total_failed).await?;
                    }
                }
                Some(Err(e)) => return Err(e),
                None => {
                    self.flush_pending(&mut pending, total_rows, total_failed).await?;
                    break;
                }
            }
        }
        Ok(())
    }

    /// 逐条核心循环（Embedded/Remote 路径）：每条 proto 消息单独 `ingest_one`，
    /// 含 P2 归因埋点。抽成泛型以直接单测（同 `ingest_batched`）。
    async fn ingest_per_message<S>(
        &self,
        tenant_id: sensearcdb_common::TenantId,
        mut stream: S,
        total_rows: &mut u64,
        total_failed: &mut u64,
    ) -> Result<(), Status>
    where
        S: futures::Stream<Item = Result<crate::proto::MultiTableBatch, Status>> + Unpin + Send,
    {
        use futures::StreamExt;
        // P2 归因埋点：grpc 空闲 gap（消息返回时刻 − 上批处理结束时刻）。
        // 不跨 await 用 thread_local（await 点线程迁移），直接原子累加。
        let mut last_batch_end: Option<Instant> = None;

        loop {
            // P2 归因埋点：接收 + decode（prost decode 在内）。
            let recv_t0 = Instant::now();
            let msg = stream.next().await;
            write_attr::add_proto_receive(recv_t0.elapsed().as_nanos() as u64);
            let batch = match msg {
                Some(Ok(p)) => p,
                Some(Err(e)) => return Err(e),
                None => break,
            };
            let msg_recv = Instant::now();
            if let Some(prev) = last_batch_end {
                write_attr::add_grpc_stream_await(msg_recv.duration_since(prev).as_nanos() as u64);
            }
            let delta = self.ingest_one(tenant_id, batch).await?;
            *total_rows += delta.rows_written;
            *total_failed += delta.rows_failed;
            last_batch_end = Some(Instant::now());
            write_attr::add_grpc_batch_count();
        }
        Ok(())
    }
}

/// Per-batch write deltas accumulated by the `ingest` streaming loop.
#[derive(Debug)]
struct IngestDelta {
    rows_written: u64,
    rows_failed: u64,
}

#[tonic::async_trait]
impl WriteService for GrpcWriteService {
    /// Client-streaming ingest: receive a stream of MultiTableBatch and write them.
    async fn ingest(
        &self,
        request: Request<Streaming<crate::proto::MultiTableBatch>>,
    ) -> Result<Response<ProtoReceipt>, Status> {
        // Q1 强制认证：tenant 必须能解析（interceptor 扩展 → resolver），否则 401。
        let tenant_id = resolve_tenant(&request, &self.tenant_resolver)?;
        let stream = request.into_inner();
        let mut total_rows: u64 = 0;
        let mut total_failed: u64 = 0;

        if self.writer.writer_type() == WriterType::Direct {
            // 攒批路径：按行数 + 空闲超时把多条 proto 消息合并为 1 次跨表引擎级写入，
            // 摊销 per-call 固定开销（WAL 帧、snapshot fanout、memtable 分片分组、调度）。
            self.ingest_batched(tenant_id, stream, &mut total_rows, &mut total_failed)
                .await?;
        } else {
            // 原逐条路径（Embedded/Remote writer），含 P2 归因埋点。
            self.ingest_per_message(tenant_id, stream, &mut total_rows, &mut total_failed)
                .await?;
        }

        let status = if total_failed == 0 {
            "ok"
        } else if total_rows > 0 {
            "partial"
        } else {
            "rejected"
        };

        Ok(Response::new(ProtoReceipt {
            rows_written: total_rows,
            timestamp_ns: chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0) as u64,
            status: status.to_string(),
            error: if total_failed > 0 {
                format!("{} points failed", total_failed)
            } else {
                String::new()
            },
        }))
    }

    /// Single-point write for low-frequency use cases.
    async fn single_write(
        &self,
        request: Request<SingleWriteRequest>,
    ) -> Result<Response<ProtoReceipt>, Status> {
        // Q1 强制认证：tenant 必须能解析，否则 401。
        let tenant_id = resolve_tenant(&request, &self.tenant_resolver)?;
        let req = request.into_inner();

        // Resolve table_name → table_id on the server side（按请求 tenant 过滤，GAP2-ING-02）。
        let table_id = match self.resolve_table_id(tenant_id, &req.table_name) {
            Some(id) => id,
            None => {
                let now = chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0);
                return Ok(Response::new(ProtoReceipt {
                    rows_written: 0,
                    timestamp_ns: now as u64,
                    status: "rejected".into(),
                    error: format!("table '{}' not found", req.table_name),
                }));
            }
        };

        // Build a minimal PointBatch with one row
        let mut batch = PointBatch::new(table_id);
        batch.tenant_id = tenant_id;
        batch.timestamps = vec![req.timestamp_ns];

        // Extract value from oneof，按 proto Value 变体构造对应 ingest ColVec 变体，
        // 类型沿 ingest → engine → MemTable 链路保真（不再 `as f64` 折叠）。
        let column = if let Some(v) = req.value {
            match v {
                crate::proto::single_write_request::Value::F64Val(f) => {
                    crate::batch::ColVec::F64(vec![f])
                }
                crate::proto::single_write_request::Value::F32Val(f) => {
                    crate::batch::ColVec::F32(vec![f])
                }
                crate::proto::single_write_request::Value::I64Val(i) => {
                    crate::batch::ColVec::I64(vec![i])
                }
                crate::proto::single_write_request::Value::U64Val(u) => {
                    crate::batch::ColVec::U64(vec![u])
                }
                crate::proto::single_write_request::Value::I32Val(i) => {
                    crate::batch::ColVec::I32(vec![i])
                }
                crate::proto::single_write_request::Value::BoolVal(b) => {
                    crate::batch::ColVec::Bool(vec![b as u8])
                }
                crate::proto::single_write_request::Value::StringVal(s) => {
                    crate::batch::ColVec::String(vec![s])
                }
            }
        } else {
            crate::batch::ColVec::F64(vec![0.0])
        };

        batch.columns = vec![column];
        batch.column_names = vec![req.column_name.clone()];
        batch.column_ids = vec![resolve_column_id(&req.column_name, 0)];
        batch.qualities = vec![req.quality as u8];
        batch.flags = vec![0];

        let now = chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0);

        match self.writer.write_batch_retry(batch).await {
            Ok(receipt) => Ok(Response::new(ProtoReceipt {
                rows_written: receipt.points_written as u64,
                timestamp_ns: now as u64,
                status: "ok".into(),
                error: String::new(),
            })),
            Err(e) if e.is_backpressure() => {
                // 与 ingest 一致的背压语义：该点未被写入，等待排空超时后返回
                // RESOURCE_EXHAUSTED 让客户端重试，而非用 "rejected" receipt 造成静默丢点。
                warn!(error = %e, "gRPC single_write backpressure, returning RESOURCE_EXHAUSTED");
                Err(Status::resource_exhausted(format!(
                    "engine backpressure: {e}"
                )))
            }
            Err(e) => Ok(Response::new(ProtoReceipt {
                rows_written: 0,
                timestamp_ns: now as u64,
                status: "rejected".into(),
                error: format!("{}", e),
            })),
        }
    }
}

/// proto `ColumnValue` → `TypedValue`（oneof variant 即类型，8 变体全覆盖）。
///
/// 返回 `None` 当 `ColumnValue.value` 未命中任何 oneof（缺列 / 空值）。
fn proto_value_to_typed(v: &crate::proto::ColumnValue) -> Option<sensearcdb_engine::memtable::TypedValue> {
    use crate::proto::column_value::Value as Cv;
    use sensearcdb_engine::memtable::TypedValue;

    match &v.value {
        Some(Cv::F64Val(x)) => Some(TypedValue::F64(*x)),
        Some(Cv::F32Val(x)) => Some(TypedValue::F32(*x)),
        Some(Cv::I64Val(x)) => Some(TypedValue::I64(*x)),
        Some(Cv::U64Val(x)) => Some(TypedValue::U64(*x)),
        Some(Cv::I32Val(x)) => Some(TypedValue::I32(*x)),
        Some(Cv::BoolVal(x)) => Some(TypedValue::Bool(*x)),
        Some(Cv::StrVal(x)) => Some(TypedValue::String(x.clone())),
        Some(Cv::Enum16Val(x)) => Some(TypedValue::Enum16(*x as u16)),
        None => None,
    }
}

/// 单条 PointRow → engine DataPoint（跳过 ingest 列式中间态）。
///
/// values 由 row.values 直出（已含稀疏语义：缺列 = 不在 values）；
/// qualities 由单行 quality 展开到每列（缺省回落 0xC0）；
/// flags 由 row.flags 直出（缺省 0），补回现行列式 wire 丢失的 flags。
///
/// `server_ts` 是服务端自打的接收时间戳；ingest 路径下忽略 `row.server_ts_ns`
/// （不信任客户端时钟）。
fn proto_row_to_datapoint(
    row: &crate::proto::PointRow,
    table_id: u32,
    tenant_id: sensearcdb_common::TenantId,
    server_ts: i64,
) -> DataPoint {
    let mut values = Vec::with_capacity(row.values.len());
    let mut qualities = Vec::with_capacity(row.values.len());
    let quality = row.quality.unwrap_or(0xC0) as u8;

    for cv in &row.values {
        if let Some(tv) = proto_value_to_typed(cv) {
            values.push((cv.column_id as u16, tv));
            qualities.push((cv.column_id as u16, quality));
        }
    }

    DataPoint {
        table_id,
        tenant_id,
        ts: row.ts_ns,
        server_ts,
        values,
        qualities,
        schema_version: 0,
        flags: row.flags.unwrap_or(0) as u8,
    }
}

/// 单条 MultiTableBatch → `Vec<DataPoint>`。消息内做 name→id 去重缓存，
/// 宽表/同表多行场景同一 table_name 只 resolve 一次。
///
/// 返回 `(points, failed_rows)`：表不存在的 PointRow 计入 `failed`（隔离计数），
/// 成功的 row 照常产出 DataPoint（保序）。
fn proto_rows_to_datapoints(
    batch: crate::proto::MultiTableBatch,
    tenant_id: sensearcdb_common::TenantId,
    resolve: &dyn Fn(&str) -> Option<u32>,
    server_ts: i64,
) -> (Vec<DataPoint>, u64) {
    let mut points = Vec::with_capacity(batch.points.len());
    let mut failed: u64 = 0;
    let mut cache: std::collections::HashMap<String, u32> = std::collections::HashMap::new();

    for row in batch.points {
        let table_id = if let Some(&id) = cache.get(&row.table_name) {
            id
        } else {
            match resolve(&row.table_name) {
                Some(id) => {
                    cache.insert(row.table_name.clone(), id);
                    id
                }
                None => {
                    failed += 1;
                    continue;
                }
            }
        };
        points.push(proto_row_to_datapoint(&row, table_id, tenant_id, server_ts));
    }

    (points, failed)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::batch::ColVec;
    use crate::writer::point_batch_to_rows;

    /// 逐行 ts 经行式往返后必须逐行保留（PointRow.ts_ns 直出，不再折叠为全 min）。
    #[test]
    fn test_row_roundtrip_preserves_per_row_timestamps() {
        let mut batch = PointBatch::new(7);
        batch.timestamps = vec![100, 250, 500];
        batch.columns = vec![ColVec::F64(vec![1.0, 2.0, 3.0])];
        batch.column_names = vec!["val".into()];
        batch.qualities = vec![0xC0; 3];
        batch.flags = vec![0; 3];

        let rows = point_batch_to_rows(&batch, "sensors");
        assert_eq!(rows.len(), 3);
        assert_eq!(rows[0].ts_ns, 100);
        assert_eq!(rows[1].ts_ns, 250);
        assert_eq!(rows[2].ts_ns, 500);

        let mb = crate::proto::MultiTableBatch { points: rows };
        let (points, failed) = proto_rows_to_datapoints(
            mb,
            sensearcdb_common::TenantId(5),
            &|_| Some(7),
            999,
        );
        assert_eq!(failed, 0);
        assert_eq!(points.len(), 3);
        assert_eq!(points[0].ts, 100);
        assert_eq!(points[1].ts, 250);
        assert_eq!(points[2].ts, 500);
        assert_eq!(points[0].tenant_id, sensearcdb_common::TenantId(5));
    }

    use sensearcdb_common::TenantId;
    use std::sync::{Arc, Mutex};

    /// 记录写入 batch 的 `(table_id, tenant_id)`，用于断言服务端 tenant 注入。
    struct RecordingWriter {
        tenants: Mutex<Vec<TenantId>>,
        tables: Mutex<Vec<u32>>,
    }

    #[async_trait::async_trait]
    impl EngineWriter for RecordingWriter {
        async fn write_batch(&self, batch: PointBatch) -> Result<WriteReceipt, EngineWriteError> {
            self.tenants.lock().unwrap().push(batch.tenant_id);
            self.tables.lock().unwrap().push(batch.table_id);
            Ok(WriteReceipt::accepted(0, batch.row_count() as u32, 0))
        }

        fn is_connected(&self) -> bool {
            true
        }

        fn writer_type(&self) -> crate::writer::WriterType {
            crate::writer::WriterType::Direct
        }
    }

    fn single_write_req() -> SingleWriteRequest {
        SingleWriteRequest {
            table_name: "sensors".into(),
            column_name: "v".into(),
            timestamp_ns: 100,
            value: Some(crate::proto::single_write_request::Value::F64Val(1.0)),
            quality: 0xC0,
        }
    }

    #[tokio::test]
    async fn test_single_write_uses_resolved_tenant() {
        let writer = Arc::new(RecordingWriter {
            tenants: Mutex::new(vec![]),
            tables: Mutex::new(vec![]),
        });
        let service = GrpcWriteService::new(writer.clone())
            .with_tenant_resolver(Arc::new(|_m| Some(TenantId(42))));

        let resp = service
            .single_write(Request::new(single_write_req()))
            .await
            .unwrap();
        assert_eq!(resp.into_inner().status, "ok");
        assert_eq!(*writer.tenants.lock().unwrap(), vec![TenantId(42)]);
    }

    #[tokio::test]
    async fn test_single_write_no_credentials_401() {
        let writer = Arc::new(RecordingWriter {
            tenants: Mutex::new(vec![]),
            tables: Mutex::new(vec![]),
        });
        // 无 resolver、无 interceptor 扩展 → 401（Q1 强制认证）。
        let service = GrpcWriteService::new(writer.clone());

        let err = service
            .single_write(Request::new(single_write_req()))
            .await
            .unwrap_err();
        assert_eq!(err.code(), tonic::Code::Unauthenticated);
        assert!(writer.tenants.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn test_single_write_isolates_tenants_via_auth_context() {
        let writer = Arc::new(RecordingWriter {
            tenants: Mutex::new(vec![]),
            tables: Mutex::new(vec![]),
        });
        let service = GrpcWriteService::new(writer.clone()); // 无 resolver，走 interceptor 扩展

        for tenant in [TenantId(1), TenantId(2)] {
            let mut req = Request::new(single_write_req());
            req.extensions_mut().insert(crate::grpc::auth::AuthContext {
                client_id: format!("apikey:t{}", tenant.as_u32()),
                method: crate::grpc::auth::AuthMethod::ApiKey,
                permissions: vec![],
                tenant_id: Some(tenant),
                collector_id: Some("c1".into()),
            });
            service.single_write(req).await.unwrap();
        }

        assert_eq!(
            *writer.tenants.lock().unwrap(),
            vec![TenantId(1), TenantId(2)]
        );
    }

    /// 写 batch 时恒返回背压错误，用于断言 gRPC 层透出 `RESOURCE_EXHAUSTED`。
    struct BackpressureWriter;

    #[async_trait::async_trait]
    impl EngineWriter for BackpressureWriter {
        async fn write_batch(&self, _batch: PointBatch) -> Result<WriteReceipt, EngineWriteError> {
            Err(EngineWriteError::Backpressure)
        }

        async fn write_engine_batch_retry(
            &self,
            _batch: EnginePointBatch,
        ) -> Result<WriteReceipt, EngineWriteError> {
            Err(EngineWriteError::Backpressure)
        }

        fn is_connected(&self) -> bool {
            true
        }

        fn writer_type(&self) -> crate::writer::WriterType {
            crate::writer::WriterType::Direct
        }
    }

    /// 单点写遇背压：必须返回 `RESOURCE_EXHAUSTED`，不得用 "rejected" receipt 静默丢点。
    #[tokio::test]
    async fn test_single_write_backpressure_returns_resource_exhausted() {
        let writer = Arc::new(BackpressureWriter);
        let service = GrpcWriteService::new(writer.clone())
            .with_tenant_resolver(Arc::new(|_m| Some(TenantId(42))));

        let err = service
            .single_write(Request::new(single_write_req()))
            .await
            .unwrap_err();
        assert_eq!(err.code(), tonic::Code::ResourceExhausted);
    }

    /// 流式 ingest 单批遇背压：`ingest_one` 必须返回 `RESOURCE_EXHAUSTED`
    /// （该批未写入，可重试），由上层 `ingest` 据此中断收流。
    #[tokio::test]
    async fn test_ingest_backpressure_returns_resource_exhausted() {
        let writer = Arc::new(BackpressureWriter);
        let service = GrpcWriteService::new(writer.clone())
            .with_tenant_resolver(Arc::new(|_m| Some(TenantId(42))));

        let proto = crate::proto::MultiTableBatch {
            points: vec![crate::proto::PointRow {
                table_name: "sensors".to_string(),
                table_id: None,
                ts_ns: 100,
                server_ts_ns: None,
                values: vec![crate::proto::ColumnValue {
                    column_id: 0,
                    value: Some(crate::proto::column_value::Value::F64Val(1.0)),
                }],
                quality: Some(0xC0),
                flags: Some(0),
                write_seq: None,
            }],
        };

        let err = service.ingest_one(TenantId(42), proto).await.unwrap_err();
        assert_eq!(err.code(), tonic::Code::ResourceExhausted);
    }

    /// 表名解析器：恒返回 None（表不存在）。
    struct EmptyResolver;

    impl crate::rest::write::TableNameResolver for EmptyResolver {
        fn resolve(
            &self,
            _tenant_id: TenantId,
            _table_name: &str,
        ) -> Option<(u32, bool)> {
            None
        }
    }

    /// 非背压错误（如表不存在）仍按旧语义计数 failed，不中断收流。
    #[tokio::test]
    async fn test_ingest_one_non_backpressure_error_does_not_exhaust() {
        let writer = Arc::new(BackpressureWriter);
        // 挂一个会解析失败的 resolver：表不存在走 "rejected" 分支，不触发背压。
        let resolver: Arc<dyn crate::rest::write::TableNameResolver> = Arc::new(EmptyResolver);
        let service = GrpcWriteService::new(writer.clone())
            .with_tenant_resolver(Arc::new(|_m| Some(TenantId(42))))
            .with_resolver(resolver);

        let proto = crate::proto::MultiTableBatch {
            points: (0..5)
                .map(|i| crate::proto::PointRow {
                    table_name: "unknown_table".to_string(),
                    table_id: None,
                    ts_ns: 100 + i,
                    server_ts_ns: None,
                    values: vec![crate::proto::ColumnValue {
                        column_id: 0,
                        value: Some(crate::proto::column_value::Value::F64Val(i as f64)),
                    }],
                    quality: Some(0xC0),
                    flags: Some(0),
                    write_seq: None,
                })
                .collect(),
        };

        let delta = service.ingest_one(TenantId(42), proto).await.unwrap();
        assert_eq!(delta.rows_written, 0);
        assert_eq!(delta.rows_failed, 5);
    }

    // ───────────── 服务端攒批测试（Direct 路径合并写入）─────────────

    /// 构造一个单列 F64 的行式 MultiTableBatch（每个 ts 一行 PointRow）。
    fn proto_1col(
        table_name: &str,
        timestamps: Vec<i64>,
        values: Vec<f64>,
    ) -> crate::proto::MultiTableBatch {
        let points: Vec<crate::proto::PointRow> = timestamps
            .into_iter()
            .zip(values.into_iter())
            .map(|(ts, v)| crate::proto::PointRow {
                table_name: table_name.to_string(),
                table_id: None,
                ts_ns: ts,
                server_ts_ns: None,
                values: vec![crate::proto::ColumnValue {
                    column_id: 0,
                    value: Some(crate::proto::column_value::Value::F64Val(v)),
                }],
                quality: Some(0xC0),
                flags: Some(0),
                write_seq: None,
            })
            .collect();
        crate::proto::MultiTableBatch { points }
    }

    /// 把 Vec<proto::MultiTableBatch> 变成 ingest 核心循环所需的 `Stream`。
    fn proto_stream(
        batches: Vec<crate::proto::MultiTableBatch>,
    ) -> impl futures::Stream<Item = Result<crate::proto::MultiTableBatch, Status>> + Unpin + Send {
        let items: Vec<Result<crate::proto::MultiTableBatch, Status>> =
            batches.into_iter().map(Ok).collect();
        futures::stream::iter(items)
    }

    /// 固定 `table_name → table_id` 解析器，用于跨表合并测试。
    struct MapResolver {
        map: std::collections::HashMap<String, u32>,
    }

    impl crate::rest::write::TableNameResolver for MapResolver {
        fn resolve(&self, _tenant: TenantId, table_name: &str) -> Option<(u32, bool)> {
            self.map.get(table_name).map(|&id| (id, true))
        }
    }

    /// 记录合并引擎级批次的 Direct writer：`write_engine_batch_retry` 被调用时
    /// 记录整批 points 与调用次数（用于断言跨表合并正确性）。
    struct MergedRecorder {
        calls: std::sync::atomic::AtomicUsize,
        points: Mutex<Vec<DataPoint>>,
    }

    impl MergedRecorder {
        fn new() -> Self {
            Self {
                calls: std::sync::atomic::AtomicUsize::new(0),
                points: Mutex::new(Vec::new()),
            }
        }
    }

    #[async_trait::async_trait]
    impl EngineWriter for MergedRecorder {
        async fn write_batch(&self, batch: PointBatch) -> Result<WriteReceipt, EngineWriteError> {
            Ok(WriteReceipt::accepted(0, batch.row_count() as u32, 0))
        }

        async fn write_engine_batch_retry(
            &self,
            batch: EnginePointBatch,
        ) -> Result<WriteReceipt, EngineWriteError> {
            let n = batch.points.len();
            self.calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            self.points.lock().unwrap().extend(batch.points);
            Ok(WriteReceipt::accepted(0, n as u32, 0))
        }

        fn is_connected(&self) -> bool {
            true
        }

        fn writer_type(&self) -> WriterType {
            WriterType::Direct
        }
    }

    /// 非 Direct（Remote）writer：逐条路径仍走 `write_batch`（经 write_batch_retry 默认实现），
    /// 不应命中合并 `write_engine_batch_retry`。
    struct PerMessageWriter {
        calls: std::sync::atomic::AtomicUsize,
        merged_calls: std::sync::atomic::AtomicUsize,
    }

    #[async_trait::async_trait]
    impl EngineWriter for PerMessageWriter {
        async fn write_batch(&self, batch: PointBatch) -> Result<WriteReceipt, EngineWriteError> {
            self.calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            Ok(WriteReceipt::accepted(0, batch.row_count() as u32, 0))
        }

        async fn write_engine_batch_retry(
            &self,
            batch: EnginePointBatch,
        ) -> Result<WriteReceipt, EngineWriteError> {
            self.merged_calls
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            Ok(WriteReceipt::accepted(0, batch.points.len() as u32, 0))
        }

        fn is_connected(&self) -> bool {
            true
        }

        fn writer_type(&self) -> WriterType {
            WriterType::Remote
        }
    }

    /// 合并路径恒返回背压的 Direct writer，用于断言 flush_pending 转 RESOURCE_EXHAUSTED。
    struct MergedBackpressureWriter;

    #[async_trait::async_trait]
    impl EngineWriter for MergedBackpressureWriter {
        async fn write_batch(&self, _batch: PointBatch) -> Result<WriteReceipt, EngineWriteError> {
            Ok(WriteReceipt::accepted(0, 0, 0))
        }

        async fn write_engine_batch_retry(
            &self,
            _batch: EnginePointBatch,
        ) -> Result<WriteReceipt, EngineWriteError> {
            Err(EngineWriteError::Backpressure)
        }

        fn is_connected(&self) -> bool {
            true
        }

        fn writer_type(&self) -> WriterType {
            WriterType::Direct
        }
    }

    /// 跨表合并正确性：N 个不同 table_id 的 1 行 proto 消息 → 合并后
    /// `write_engine_batch_retry` 被调 1 次，merged points 顺序 = 到达顺序，
    /// 逐点 table_id/ts/value 正确。
    #[tokio::test]
    async fn test_ingest_batching_cross_table_merge() {
        use sensearcdb_engine::memtable::TypedValue;

        let recorder = Arc::new(MergedRecorder::new());
        let resolver: Arc<dyn crate::rest::write::TableNameResolver> = Arc::new(MapResolver {
            map: [("t1".to_string(), 1u32), ("t2".to_string(), 2), ("t3".to_string(), 3)]
                .into_iter()
                .collect(),
        });
        let service = GrpcWriteService::new(recorder.clone())
            .with_tenant_resolver(Arc::new(|_m| Some(TenantId::DEFAULT)))
            .with_resolver(resolver);

        let batches = vec![
            proto_1col("t1", vec![100], vec![1.0]),
            proto_1col("t2", vec![200], vec![2.0]),
            proto_1col("t3", vec![300], vec![3.0]),
        ];
        let mut rows = 0u64;
        let mut failed = 0u64;
        service
            .ingest_batched(TenantId::DEFAULT, proto_stream(batches), &mut rows, &mut failed)
            .await
            .unwrap();
        assert_eq!(rows, 3);
        assert_eq!(failed, 0);

        assert_eq!(recorder.calls.load(std::sync::atomic::Ordering::SeqCst), 1);
        let points = recorder.points.lock().unwrap();
        assert_eq!(points.len(), 3);
        assert_eq!(points[0].table_id, 1);
        assert_eq!(points[0].ts, 100);
        assert_eq!(points[0].values[0].1, TypedValue::F64(1.0));
        assert_eq!(points[1].table_id, 2);
        assert_eq!(points[1].ts, 200);
        assert_eq!(points[1].values[0].1, TypedValue::F64(2.0));
        assert_eq!(points[2].table_id, 3);
        assert_eq!(points[2].ts, 300);
        assert_eq!(points[2].values[0].1, TypedValue::F64(3.0));
    }

    /// 行数触发：3 条消息 × 400 行 = 1200 行 > BATCH_MAX_ROWS(1024)，
    /// 应触发 ≥1 次 flush，且合并写调用次数 < 消息数(3)。
    #[tokio::test]
    async fn test_ingest_batching_row_count_trigger() {
        let recorder = Arc::new(MergedRecorder::new());
        let service = GrpcWriteService::new(recorder.clone())
            .with_tenant_resolver(Arc::new(|_m| Some(TenantId::DEFAULT)));

        let batches = vec![
            proto_1col(
                "t",
                (0..400).map(|i| 100 + i).collect(),
                (0..400).map(|i| i as f64).collect(),
            ),
            proto_1col(
                "t",
                (400..800).map(|i| 100 + i).collect(),
                (400..800).map(|i| i as f64).collect(),
            ),
            proto_1col(
                "t",
                (800..1200).map(|i| 100 + i).collect(),
                (800..1200).map(|i| i as f64).collect(),
            ),
        ];
        let mut rows = 0u64;
        let mut failed = 0u64;
        service
            .ingest_batched(TenantId::DEFAULT, proto_stream(batches), &mut rows, &mut failed)
            .await
            .unwrap();
        assert_eq!(rows, 1200);
        assert_eq!(failed, 0);

        let calls = recorder.calls.load(std::sync::atomic::Ordering::SeqCst);
        assert!(calls >= 1, "应至少 flush 1 次");
        assert!(calls < 3, "合并写调用次数应小于消息数(3)，实际 {calls}");
        assert_eq!(recorder.points.lock().unwrap().len(), 1200);
    }

    /// 超时触发：发 1 行后不关闭流，空闲超时(~2ms)应在流结束前 flush 出该 1 点。
    #[tokio::test]
    async fn test_ingest_batching_timeout_flush() {
        use futures::StreamExt;

        let recorder = Arc::new(MergedRecorder::new());
        let service = GrpcWriteService::new(recorder.clone())
            .with_tenant_resolver(Arc::new(|_m| Some(TenantId::DEFAULT)));

        let (tx, rx) = tokio::sync::mpsc::channel::<crate::proto::MultiTableBatch>(8);
        let stream = tokio_stream::wrappers::ReceiverStream::new(rx).map(|m| Ok::<_, Status>(m));

        let handle = tokio::spawn(async move {
            let mut rows = 0u64;
            let mut failed = 0u64;
            let result = service
                .ingest_batched(TenantId::DEFAULT, stream, &mut rows, &mut failed)
                .await;
            (result, rows, failed)
        });

        tx.send(proto_1col("t", vec![100], vec![1.0])).await.unwrap();

        // 不关闭流，等空闲超时触发 flush。
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        assert_eq!(
            recorder.calls.load(std::sync::atomic::Ordering::SeqCst),
            1,
            "空闲超时应在流结束前 flush 出该 1 点"
        );

        drop(tx); // 结束流
        let (result, rows, failed) = handle.await.unwrap();
        result.unwrap();
        assert_eq!(rows, 1);
        assert_eq!(failed, 0);
    }

    /// 流结束 flush 残留：发 K(<1024) 行后 close 流 → rows_written == K。
    #[tokio::test]
    async fn test_ingest_batching_stream_end_flush_residual() {
        let recorder = Arc::new(MergedRecorder::new());
        let service = GrpcWriteService::new(recorder.clone())
            .with_tenant_resolver(Arc::new(|_m| Some(TenantId::DEFAULT)));

        let batches = vec![
            proto_1col("t", vec![100], vec![1.0]),
            proto_1col("t", vec![200], vec![2.0]),
            proto_1col("t", vec![300], vec![3.0]),
        ];
        let mut rows = 0u64;
        let mut failed = 0u64;
        service
            .ingest_batched(TenantId::DEFAULT, proto_stream(batches), &mut rows, &mut failed)
            .await
            .unwrap();
        assert_eq!(rows, 3);
        assert_eq!(failed, 0);
        assert_eq!(recorder.calls.load(std::sync::atomic::Ordering::SeqCst), 1);
        assert_eq!(recorder.points.lock().unwrap().len(), 3);
    }

    /// TableNotFound 单条跳过：1 条表不存在 + 2 条合法 → rows_written==2,
    /// rows_failed==1，流不中断（status=partial）。
    #[tokio::test]
    async fn test_ingest_batching_table_not_found_skips_single() {
        let recorder = Arc::new(MergedRecorder::new());
        let resolver: Arc<dyn crate::rest::write::TableNameResolver> = Arc::new(MapResolver {
            map: [("ok".to_string(), 7u32)].into_iter().collect(),
        });
        let service = GrpcWriteService::new(recorder.clone())
            .with_tenant_resolver(Arc::new(|_m| Some(TenantId::DEFAULT)))
            .with_resolver(resolver);

        let batches = vec![
            proto_1col("missing", vec![100], vec![1.0]), // 表不存在
            proto_1col("ok", vec![200], vec![2.0]),
            proto_1col("ok", vec![300], vec![3.0]),
        ];
        let mut rows = 0u64;
        let mut failed = 0u64;
        service
            .ingest_batched(TenantId::DEFAULT, proto_stream(batches), &mut rows, &mut failed)
            .await
            .unwrap();
        // 1 条表不存在被单条隔离计数失败，流不中断，2 条合法继续合并写入。
        assert_eq!(rows, 2);
        assert_eq!(failed, 1);
    }

    /// 合并路径背压：flush_pending 遇 Backpressure 必须转 RESOURCE_EXHAUSTED 中断流。
    #[tokio::test]
    async fn test_ingest_batching_merged_backpressure_returns_resource_exhausted() {
        let writer = Arc::new(MergedBackpressureWriter);
        let service = GrpcWriteService::new(writer.clone())
            .with_tenant_resolver(Arc::new(|_m| Some(TenantId::DEFAULT)));

        let batches = vec![proto_1col("t", vec![100], vec![1.0])];
        let mut rows = 0u64;
        let mut failed = 0u64;
        let err = service
            .ingest_batched(TenantId::DEFAULT, proto_stream(batches), &mut rows, &mut failed)
            .await
            .unwrap_err();
        assert_eq!(err.code(), tonic::Code::ResourceExhausted);
    }

    /// 非 Direct（Remote）writer 回归：服务端仍走逐条路径（每消息一次写入），
    /// 不启用攒批合并（3 条消息 → 3 次 write_engine_batch_retry，而非 1 次）。
    #[tokio::test]
    async fn test_ingest_non_direct_writer_uses_per_message_path() {
        let writer = Arc::new(PerMessageWriter {
            calls: std::sync::atomic::AtomicUsize::new(0),
            merged_calls: std::sync::atomic::AtomicUsize::new(0),
        });
        let service = GrpcWriteService::new(writer.clone())
            .with_tenant_resolver(Arc::new(|_m| Some(TenantId::DEFAULT)));

        let batches = vec![
            proto_1col("t", vec![100], vec![1.0]),
            proto_1col("t", vec![200], vec![2.0]),
            proto_1col("t", vec![300], vec![3.0]),
        ];
        let mut rows = 0u64;
        let mut failed = 0u64;
        service
            .ingest_per_message(TenantId::DEFAULT, proto_stream(batches), &mut rows, &mut failed)
            .await
            .unwrap();
        assert_eq!(rows, 3);
        assert_eq!(writer.calls.load(std::sync::atomic::Ordering::SeqCst), 0);
        assert_eq!(
            writer.merged_calls.load(std::sync::atomic::Ordering::SeqCst),
            3,
            "非 Direct 路径应按消息逐条 write_engine_batch_retry（3 次），不得合并为 1 次"
        );
    }

    // ───────────── QA 独立边界（fresh eyes，D 节，不采信工程师自测）─────────────

    /// D1 宽表 × 攒批：1 条含 2 列（F64 + I64）× 3 行的行式消息经攒批路径，
    /// `proto_rows_to_datapoints` 展开后每点必须含 2 列、值逐点正确（列不丢、值不错）。
    #[tokio::test]
    async fn qa_batching_wide_table_multi_column_multi_row() {
        use sensearcdb_engine::memtable::TypedValue;

        let recorder = Arc::new(MergedRecorder::new());
        let resolver: Arc<dyn crate::rest::write::TableNameResolver> = Arc::new(MapResolver {
            map: [("t".to_string(), 7u32)].into_iter().collect(),
        });
        let service = GrpcWriteService::new(recorder.clone())
            .with_tenant_resolver(Arc::new(|_m| Some(TenantId::DEFAULT)))
            .with_resolver(resolver);

        // 宽表：1 表 2 列（column_id 0=F64, 1=I64）× 3 行。
        let points: Vec<crate::proto::PointRow> = (0..3)
            .map(|i| crate::proto::PointRow {
                table_name: "t".to_string(),
                table_id: None,
                ts_ns: 100 + i as i64 * 100,
                server_ts_ns: None,
                values: vec![
                    crate::proto::ColumnValue {
                        column_id: 0,
                        value: Some(crate::proto::column_value::Value::F64Val(1.0 + i as f64)),
                    },
                    crate::proto::ColumnValue {
                        column_id: 1,
                        value: Some(crate::proto::column_value::Value::I64Val(10 + i as i64 * 10)),
                    },
                ],
                quality: Some(0xC0),
                flags: Some(0),
                write_seq: None,
            })
            .collect();
        let proto = crate::proto::MultiTableBatch { points };

        let mut rows = 0u64;
        let mut failed = 0u64;
        service
            .ingest_batched(
                TenantId::DEFAULT,
                proto_stream(vec![proto]),
                &mut rows,
                &mut failed,
            )
            .await
            .unwrap();
        assert_eq!(rows, 3);
        assert_eq!(failed, 0);
        assert_eq!(
            recorder.calls.load(std::sync::atomic::Ordering::SeqCst),
            1,
            "宽表 1 条消息应合并为 1 次引擎级写入"
        );

        let points = recorder.points.lock().unwrap();
        assert_eq!(points.len(), 3);
        for (i, p) in points.iter().enumerate() {
            assert_eq!(p.table_id, 7);
            assert_eq!(p.ts, 100 + i as i64 * 100);
            assert_eq!(p.values.len(), 2, "宽表列不得丢失，实际 {}", p.values.len());
            assert_eq!(p.values[0].0, 0);
            assert_eq!(p.values[0].1, TypedValue::F64(1.0 + i as f64));
            assert_eq!(p.values[1].0, 1);
            assert_eq!(p.values[1].1, TypedValue::I64(10 + i as i64 * 10));
        }
    }

    /// D2 同表同 ts LWW：同一窗口内两条同表同 ts 消息合并写入后，最终读回值 = 后写。
    /// 真引擎 memtable 单一 LWW（同 ts 后写覆盖旧映射），攒批必须保持到达顺序。
    #[tokio::test]
    async fn qa_batching_lww_same_table_same_ts() {
        use crate::writer::EngineDirectWriter;
        use sensearcdb_common::{AppConfig, ConfigHandle};
        use sensearcdb_engine::engine::Engine;
        use sensearcdb_engine::memtable::TypedValue;

        let tmp = tempfile::TempDir::new().unwrap();
        let mut cfg = AppConfig::default();
        cfg.paths.data_dir = tmp.path().to_string_lossy().into_owned();
        let handle = ConfigHandle::new(cfg);
        let engine = Arc::new(Engine::new(&handle, None).await.expect("engine"));

        let writer = Arc::new(EngineDirectWriter::new(engine.clone()));
        let resolver: Arc<dyn crate::rest::write::TableNameResolver> = Arc::new(MapResolver {
            map: [("t".to_string(), 7u32)].into_iter().collect(),
        });
        let service = GrpcWriteService::new(writer.clone())
            .with_tenant_resolver(Arc::new(|_m| Some(TenantId::DEFAULT)))
            .with_resolver(resolver);

        // 同表同 ts=100：先 1.0 后 2.0，合并到同一引擎批，LWW 后写覆盖。
        let batches = vec![
            proto_1col("t", vec![100], vec![1.0]),
            proto_1col("t", vec![100], vec![2.0]),
        ];
        let mut rows = 0u64;
        let mut failed = 0u64;
        service
            .ingest_batched(TenantId::DEFAULT, proto_stream(batches), &mut rows, &mut failed)
            .await
            .unwrap();
        assert_eq!(rows, 2);
        assert_eq!(failed, 0);

        let (cols, tss) = engine
            .query_table_columns(TenantId::DEFAULT, 7, &[0], 100, 100)
            .await
            .unwrap();
        assert_eq!(tss, vec![100], "同 ts 应 LWW 去重为 1 点，实际 {:?}", tss);
        assert_eq!(
            cols[0],
            vec![Some(TypedValue::F64(2.0))],
            "LWW：同表同 ts 后写必须覆盖先写"
        );
    }

    /// D3 攒批 + 背压排空守恒：真引擎 stall，窗口内多批合并为 1 个引擎批 →
    /// `write_engine_batch_retry` 遇 MemTableFull 排空后重写同一批，读回总点数精确。
    #[tokio::test]
    async fn qa_batching_backpressure_drain_conservation() {
        use crate::writer::EngineDirectWriter;
        use sensearcdb_common::{AppConfig, ConfigHandle};
        use sensearcdb_engine::engine::{DataPoint, Engine, PointBatch as EnginePointBatch};
        use sensearcdb_engine::memtable::TypedValue;

        let tmp = tempfile::TempDir::new().unwrap();
        let mut cfg = AppConfig::default();
        cfg.paths.data_dir = tmp.path().to_string_lossy().into_owned();
        let handle = ConfigHandle::new(cfg);
        let engine = Arc::new(Engine::new(&handle, None).await.expect("engine"));

        let engine_point = |table_id: u32, ts: i64, v: f64| DataPoint {
            table_id,
            tenant_id: TenantId::DEFAULT,
            ts,
            server_ts: ts + 1,
            values: vec![(0, TypedValue::F64(v))],
            qualities: vec![(0, 0xC0)],
            schema_version: 1,
            flags: 0,
        };
        // 制造 stall：table 7 写 65 点 + freeze（队列深度 65 > 64）。
        for i in 0..65 {
            engine
                .write_batch(EnginePointBatch {
                    points: vec![engine_point(7, i * 100, i as f64)],
                })
                .await
                .unwrap();
            engine.force_freeze();
        }
        assert!(engine.memtable.write_stalled(), "precondition: stalled");

        let writer = Arc::new(EngineDirectWriter::new(engine.clone()));
        let resolver: Arc<dyn crate::rest::write::TableNameResolver> = Arc::new(MapResolver {
            map: [("t".to_string(), 88u32)].into_iter().collect(),
        });
        let service = GrpcWriteService::new(writer.clone())
            .with_tenant_resolver(Arc::new(|_m| Some(TenantId::DEFAULT)))
            .with_resolver(resolver);

        // 3 条消息（table 88 各 1 点）合并为 1 个引擎批，流结束 flush 触发背压排空重试。
        let batches = vec![
            proto_1col("t", vec![1000], vec![1.0]),
            proto_1col("t", vec![2000], vec![2.0]),
            proto_1col("t", vec![3000], vec![3.0]),
        ];
        let mut rows = 0u64;
        let mut failed = 0u64;
        service
            .ingest_batched(TenantId::DEFAULT, proto_stream(batches), &mut rows, &mut failed)
            .await
            .unwrap();
        assert_eq!(rows, 3, "排空后应写入全部 3 点");
        assert_eq!(failed, 0);

        // 读回：table 88 在 [1000,3000] 应恰好 3 点（不丢不重）。
        let (cols, tss) = engine
            .query_table_columns(TenantId::DEFAULT, 88, &[0], 1000, 3000)
            .await
            .unwrap();
        assert_eq!(tss.len(), 3, "背压排空后不丢不重，应恰好 3 点，实际 {:?}", tss);
        assert_eq!(cols[0].len(), 3);
        assert_eq!(cols[0], vec![
            Some(TypedValue::F64(1.0)),
            Some(TypedValue::F64(2.0)),
            Some(TypedValue::F64(3.0)),
        ]);
    }

    /// D4 边界：pending 恰好等于 BATCH_MAX_ROWS 时触发 flush（1 条 1024 行消息 → 1 次写入）。
    #[tokio::test]
    async fn qa_batching_exact_batch_max_rows_flushes() {
        let recorder = Arc::new(MergedRecorder::new());
        let service = GrpcWriteService::new(recorder.clone())
            .with_tenant_resolver(Arc::new(|_m| Some(TenantId::DEFAULT)));

        let n = BATCH_MAX_ROWS; // 恰好 1024
        let batch = proto_1col(
            "t",
            (0..n).map(|i| 100 + i as i64).collect(),
            (0..n).map(|i| i as f64).collect(),
        );
        let mut rows = 0u64;
        let mut failed = 0u64;
        service
            .ingest_batched(
                TenantId::DEFAULT,
                proto_stream(vec![batch]),
                &mut rows,
                &mut failed,
            )
            .await
            .unwrap();
        assert_eq!(rows, BATCH_MAX_ROWS as u64);
        assert_eq!(failed, 0);
        assert_eq!(
            recorder.calls.load(std::sync::atomic::Ordering::SeqCst),
            1,
            "恰好 BATCH_MAX_ROWS 应触发一次 flush"
        );
        assert_eq!(
            recorder.points.lock().unwrap().len(),
            BATCH_MAX_ROWS,
            "不得丢点"
        );
    }

    // ───────────── 行式（MultiTableBatch）往返 / 红线专测 ─────────────

    /// 构造一个单值 PointRow（column_id + oneof 值）。
    fn row(
        table_name: &str,
        ts_ns: i64,
        column_id: u16,
        value: crate::proto::column_value::Value,
        quality: u32,
        flags: u32,
    ) -> crate::proto::PointRow {
        crate::proto::PointRow {
            table_name: table_name.to_string(),
            table_id: None,
            ts_ns,
            server_ts_ns: None,
            values: vec![crate::proto::ColumnValue {
                column_id: column_id as u32,
                value: Some(value),
            }],
            quality: Some(quality),
            flags: Some(flags),
            write_seq: None,
        }
    }

    /// 行式往返：多类型（含 Enum16）经 point_batch_to_rows → proto →
    /// proto_row_to_datapoint 逐点 variant 与值保真。
    #[test]
    fn test_row_roundtrip_multi_type_fidelity() {
        use sensearcdb_engine::memtable::TypedValue;

        // 7 种 ColVec 类型经 point_batch_to_rows 往返（ColVec 无 Enum16，Enum16 见下）。
        let mk = |col: ColVec| {
            let mut b = PointBatch::new(7);
            b.timestamps = vec![100];
            b.columns = vec![col];
            b.column_names = vec!["v".into()];
            b.column_ids = vec![0];
            b.qualities = vec![0xC0; 1];
            b.flags = vec![0; 1];
            b
        };
        let inputs: Vec<ColVec> = vec![
            ColVec::F64(vec![1.5]),
            ColVec::F32(vec![2.5_f32]),
            ColVec::I64(vec![-3]),
            ColVec::U64(vec![4]),
            ColVec::I32(vec![-5]),
            ColVec::Bool(vec![1]),
            ColVec::String(vec!["hello".to_string()]),
        ];
        let expected: Vec<TypedValue> = vec![
            TypedValue::F64(1.5),
            TypedValue::F32(2.5_f32),
            TypedValue::I64(-3),
            TypedValue::U64(4),
            TypedValue::I32(-5),
            TypedValue::Bool(true),
            TypedValue::String("hello".to_string()),
        ];
        for (i, col) in inputs.into_iter().enumerate() {
            let rows = point_batch_to_rows(&mk(col), "t7");
            let (points, failed) = proto_rows_to_datapoints(
                crate::proto::MultiTableBatch { points: rows },
                TenantId::DEFAULT,
                &|_| Some(7),
                0,
            );
            assert_eq!(failed, 0);
            assert_eq!(points.len(), 1);
            assert_eq!(points[0].values.len(), 1);
            assert_eq!(points[0].values[0].0, 0);
            assert_eq!(points[0].values[0].1, expected[i]);
        }

        // Enum16：ColVec 无该变体，直接构造 proto PointRow → proto_row_to_datapoint。
        let r = crate::proto::PointRow {
            table_name: "t7".to_string(),
            table_id: None,
            ts_ns: 100,
            server_ts_ns: None,
            values: vec![crate::proto::ColumnValue {
                column_id: 3,
                value: Some(crate::proto::column_value::Value::Enum16Val(65535)),
            }],
            quality: Some(0xC0),
            flags: Some(0),
            write_seq: None,
        };
        let dp = proto_row_to_datapoint(&r, 7, TenantId::DEFAULT, 0);
        assert_eq!(dp.values.len(), 1);
        assert_eq!(dp.values[0].0, 3);
        assert_eq!(dp.values[0].1, TypedValue::Enum16(65535));
    }

    /// 行式稀疏/presence：缺列 = 不在 values；oneof 空值跳过；空 values 行产出空 DataPoint。
    #[test]
    fn test_row_roundtrip_presence_sparse() {
        // (a) 一列有值 + 一列 oneof 为空 → 只产出有值那列。
        let r = crate::proto::PointRow {
            table_name: "t7".to_string(),
            table_id: None,
            ts_ns: 100,
            server_ts_ns: None,
            values: vec![
                crate::proto::ColumnValue {
                    column_id: 0,
                    value: Some(crate::proto::column_value::Value::F64Val(1.0)),
                },
                crate::proto::ColumnValue {
                    column_id: 1,
                    value: None,
                },
            ],
            quality: Some(0xC0),
            flags: Some(0),
            write_seq: None,
        };
        let dp = proto_row_to_datapoint(&r, 7, TenantId::DEFAULT, 0);
        assert_eq!(dp.values.len(), 1, "oneof 空值列必须被跳过");
        assert_eq!(dp.values[0].0, 0);
        assert_eq!(dp.qualities.len(), 1);

        // (b) 空 values 行 → 空 DataPoint（不误写 0）。
        let r2 = crate::proto::PointRow {
            table_name: "t7".to_string(),
            table_id: None,
            ts_ns: 200,
            server_ts_ns: None,
            values: vec![],
            quality: Some(0xC0),
            flags: Some(0),
            write_seq: None,
        };
        let dp2 = proto_row_to_datapoint(&r2, 7, TenantId::DEFAULT, 0);
        assert!(dp2.values.is_empty(), "空 values 行不得产出任何值");
        assert!(dp2.qualities.is_empty());
    }

    /// 行式 quality/flags 保真：单行 quality 展开到每列，缺省回落 0xC0；flags 直出。
    #[test]
    fn test_row_roundtrip_quality_flags_fidelity() {
        // quality=0x40（Uncertain）、flags=0x03（INTERPOLATED|BACKFILL）。
        let r = crate::proto::PointRow {
            table_name: "t7".to_string(),
            table_id: None,
            ts_ns: 100,
            server_ts_ns: None,
            values: vec![
                crate::proto::ColumnValue {
                    column_id: 0,
                    value: Some(crate::proto::column_value::Value::F64Val(1.0)),
                },
                crate::proto::ColumnValue {
                    column_id: 1,
                    value: Some(crate::proto::column_value::Value::I64Val(2)),
                },
            ],
            quality: Some(0x40),
            flags: Some(0x03),
            write_seq: None,
        };
        let dp = proto_row_to_datapoint(&r, 7, TenantId::DEFAULT, 0);
        assert_eq!(dp.flags, 0x03, "flags 必须保真（现行列式 wire 会丢）");
        assert_eq!(dp.qualities, vec![(0, 0x40), (1, 0x40)]);

        // quality/flags 缺省 → 回落 0xC0 / 0。
        let r2 = crate::proto::PointRow {
            table_name: "t7".to_string(),
            table_id: None,
            ts_ns: 100,
            server_ts_ns: None,
            values: vec![crate::proto::ColumnValue {
                column_id: 0,
                value: Some(crate::proto::column_value::Value::F64Val(1.0)),
            }],
            quality: None,
            flags: None,
            write_seq: None,
        };
        let dp2 = proto_row_to_datapoint(&r2, 7, TenantId::DEFAULT, 0);
        assert_eq!(dp2.flags, 0, "flags 缺省回落 0");
        assert_eq!(dp2.qualities, vec![(0, 0xC0)], "quality 缺省回落 0xC0");
    }

    /// write_seq 是 proto 层字段，prost 编码往返保真；ingest 解码路径不落 DataPoint
    /// （DataPoint 无 write_seq，LWW 由服务端 WAL 分配 seq，见方案 §1/§5.1）。
    #[test]
    fn test_row_write_seq_proto_roundtrip() {
        use prost::Message;
        let r = crate::proto::PointRow {
            table_name: "t7".to_string(),
            table_id: None,
            ts_ns: 100,
            server_ts_ns: None,
            values: vec![],
            quality: Some(0xC0),
            flags: Some(0),
            write_seq: Some(12345),
        };
        let mut buf = Vec::new();
        r.encode(&mut buf).unwrap();
        let decoded = crate::proto::PointRow::decode(&buf[..]).unwrap();
        assert_eq!(
            decoded.write_seq,
            Some(12345),
            "write_seq 字段必须 proto 往返保真"
        );

        // 采集器路径恒不填 write_seq。
        let mut b = PointBatch::new(7);
        b.timestamps = vec![100];
        b.columns = vec![ColVec::F64(vec![1.0])];
        b.column_names = vec!["v".into()];
        b.column_ids = vec![0];
        b.qualities = vec![0xC0; 1];
        b.flags = vec![0; 1];
        let rows = point_batch_to_rows(&b, "t7");
        assert_eq!(rows[0].write_seq, None, "采集器路径 write_seq 恒缺省");
    }

    /// 跨表不串表：N 个不同 table_name 的 PointRow 打包成一条 MultiTableBatch，
    /// proto_rows_to_datapoints 逐点 resolve 出正确 table_id，绝不张冠李戴。
    #[test]
    fn test_row_cross_table_no_mixup() {
        use sensearcdb_engine::memtable::TypedValue;

        let resolve = |name: &str| -> Option<u32> {
            match name {
                "t1" => Some(1),
                "t2" => Some(2),
                "t3" => Some(3),
                _ => None,
            }
        };
        // 顺序故意打乱（t1, t3, t2），验证逐点 resolve 不串表、保序。
        let batch = crate::proto::MultiTableBatch {
            points: vec![
                row("t1", 100, 0, crate::proto::column_value::Value::F64Val(1.0), 0xC0, 0),
                row("t3", 300, 0, crate::proto::column_value::Value::F64Val(3.0), 0xC0, 0),
                row("t2", 200, 0, crate::proto::column_value::Value::F64Val(2.0), 0xC0, 0),
            ],
        };
        let (points, failed) = proto_rows_to_datapoints(batch, TenantId::DEFAULT, &resolve, 0);
        assert_eq!(failed, 0);
        assert_eq!(points.len(), 3);
        assert_eq!(points[0].table_id, 1);
        assert_eq!(points[0].ts, 100);
        assert_eq!(points[0].values[0].1, TypedValue::F64(1.0));
        assert_eq!(points[1].table_id, 3);
        assert_eq!(points[1].ts, 300);
        assert_eq!(points[2].table_id, 2);
        assert_eq!(points[2].ts, 200);
    }

    /// 1表1tag 退化形态：1 表 1 列 1 行（column_id == 0）正确解码。
    #[test]
    fn test_row_single_table_single_tag_single_row() {
        use sensearcdb_engine::memtable::TypedValue;

        let batch = crate::proto::MultiTableBatch {
            points: vec![row(
                "t1",
                100,
                0,
                crate::proto::column_value::Value::F64Val(42.0),
                0xC0,
                0,
            )],
        };
        let (points, failed) =
            proto_rows_to_datapoints(batch, TenantId::DEFAULT, &|_| Some(1), 0);
        assert_eq!(failed, 0);
        assert_eq!(points.len(), 1);
        assert_eq!(points[0].table_id, 1);
        assert_eq!(points[0].ts, 100);
        assert_eq!(points[0].values, vec![(0, TypedValue::F64(42.0))]);
        assert_eq!(points[0].qualities, vec![(0, 0xC0)]);
    }

    // ───────────── QA 独立边界测试（D 节，fresh eyes，不采信工程师自测）─────────────

    /// QA 独立 D1：稀疏行 + 宽表混合。一条 MultiTableBatch 内同时含
    /// 1 列行、10 列宽表行、缺失部分列的稀疏行，完整往返（point_batch_to_rows
    /// 编码 → proto → proto_rows_to_datapoints 解码）后逐点逐列保真，
    /// 缺失列不被误写 0、column_id 不串位。
    #[test]
    fn qa_row_wire_mixed_sparse_wide_single_batch() {
        use sensearcdb_engine::memtable::TypedValue;

        // (1) 1 列行：1 表 1 列 1 行（1表1tag 退化形态）。
        let mut b1 = PointBatch::new(1);
        b1.timestamps = vec![100];
        b1.columns = vec![ColVec::F64(vec![42.0])];
        b1.column_names = vec!["v".into()];
        b1.column_ids = vec![0];
        b1.qualities = vec![0xC0; 1];
        b1.flags = vec![0; 1];

        // (2) 10 列宽表行：column_id 0..9，混 7 类型（含 U64::MAX / 负值 / String）。
        let mut b2 = PointBatch::new(2);
        b2.timestamps = vec![200];
        b2.columns = vec![
            ColVec::F64(vec![1.0]),
            ColVec::F32(vec![2.5]),
            ColVec::I64(vec![-3]),
            ColVec::U64(vec![u64::MAX]),
            ColVec::I32(vec![-5]),
            ColVec::Bool(vec![1]),
            ColVec::String(vec!["s".to_string()]),
            ColVec::F64(vec![7.5]),
            ColVec::I64(vec![9]),
            ColVec::Bool(vec![0]),
        ];
        b2.column_names = (0..10).map(|i| format!("c{i}")).collect();
        b2.column_ids = (0..10).collect();
        b2.qualities = vec![0xC0; 1];
        b2.flags = vec![0; 1];

        // (3) 稀疏行：3 列里 col_1 缺失（presence=false），只写 col_0/col_2。
        let mut b3 = PointBatch::new(3);
        b3.timestamps = vec![300];
        b3.columns = vec![
            ColVec::F64(vec![10.0]),
            ColVec::F64(vec![0.0]), // 占位值，presence=false 必须被跳过
            ColVec::I64(vec![30]),
        ];
        b3.column_names = vec!["c0".into(), "c1".into(), "c2".into()];
        b3.column_ids = vec![0, 1, 2];
        b3.column_presence = vec![vec![true], vec![false], vec![true]];
        b3.qualities = vec![0xC0; 1];
        b3.flags = vec![0; 1];

        // 编码：3 个列式批 → 行式 PointRow，合并进一条 MultiTableBatch。
        let mut rows = Vec::new();
        rows.extend(point_batch_to_rows(&b1, "t1"));
        rows.extend(point_batch_to_rows(&b2, "t2"));
        rows.extend(point_batch_to_rows(&b3, "t3"));
        assert_eq!(rows.len(), 3);

        // 解码：name→id 映射，逐点逐列断言。
        let resolve = |name: &str| -> Option<u32> {
            match name {
                "t1" => Some(1),
                "t2" => Some(2),
                "t3" => Some(3),
                _ => None,
            }
        };
        let (points, failed) = proto_rows_to_datapoints(
            crate::proto::MultiTableBatch { points: rows },
            TenantId::DEFAULT,
            &resolve,
            0,
        );
        assert_eq!(failed, 0);
        assert_eq!(points.len(), 3);

        // 行 0：1 列。
        assert_eq!(points[0].table_id, 1);
        assert_eq!(points[0].ts, 100);
        assert_eq!(points[0].values, vec![(0, TypedValue::F64(42.0))]);

        // 行 1：10 列，类型/值逐列保真（宽表列不得丢失）。
        assert_eq!(points[1].table_id, 2);
        assert_eq!(points[1].values.len(), 10, "宽表 10 列不得丢失");
        assert_eq!(
            points[1].values,
            vec![
                (0, TypedValue::F64(1.0)),
                (1, TypedValue::F32(2.5)),
                (2, TypedValue::I64(-3)),
                (3, TypedValue::U64(u64::MAX)),
                (4, TypedValue::I32(-5)),
                (5, TypedValue::Bool(true)),
                (6, TypedValue::String("s".to_string())),
                (7, TypedValue::F64(7.5)),
                (8, TypedValue::I64(9)),
                (9, TypedValue::Bool(false)),
            ]
        );

        // 行 2：稀疏，只 2 列，column_id 精确 0/2，col_1 不得误写 0、不得串位。
        assert_eq!(points[2].table_id, 3);
        assert_eq!(points[2].ts, 300);
        assert_eq!(
            points[2].values,
            vec![(0, TypedValue::F64(10.0)), (2, TypedValue::I64(30))],
            "稀疏行缺失列不得被误写，column_id 不得串位"
        );
    }

    /// QA 独立 D2：一条批内混 8 类型（F64/F32/I64/U64/I32/Bool/String/Enum16），
    /// 解码后 oneof 变体不变、值保真（含 Enum16 65535、U64::MAX、I64::MIN）。
    #[test]
    fn qa_row_wire_eight_types_one_batch() {
        use sensearcdb_engine::memtable::TypedValue;

        let mk = |id: u32, v: crate::proto::column_value::Value| crate::proto::ColumnValue {
            column_id: id,
            value: Some(v),
        };
        let row = crate::proto::PointRow {
            table_name: "t8".to_string(),
            table_id: None,
            ts_ns: 100,
            server_ts_ns: None,
            values: vec![
                mk(0, crate::proto::column_value::Value::F64Val(1.5)),
                mk(1, crate::proto::column_value::Value::F32Val(-2.5)),
                mk(2, crate::proto::column_value::Value::I64Val(i64::MIN)),
                mk(3, crate::proto::column_value::Value::U64Val(u64::MAX)),
                mk(4, crate::proto::column_value::Value::I32Val(-5)),
                mk(5, crate::proto::column_value::Value::BoolVal(true)),
                mk(6, crate::proto::column_value::Value::StrVal("héllo".to_string())),
                mk(7, crate::proto::column_value::Value::Enum16Val(65535)),
            ],
            quality: Some(0xC0),
            flags: Some(0),
            write_seq: None,
        };
        let (points, failed) = proto_rows_to_datapoints(
            crate::proto::MultiTableBatch { points: vec![row] },
            TenantId::DEFAULT,
            &|_| Some(8),
            0,
        );
        assert_eq!(failed, 0);
        assert_eq!(points.len(), 1);
        let dp = &points[0];
        assert_eq!(dp.table_id, 8);
        assert_eq!(dp.values.len(), 8, "8 类型不得丢失");
        assert_eq!(dp.values[0], (0, TypedValue::F64(1.5)));
        assert_eq!(dp.values[1], (1, TypedValue::F32(-2.5)));
        assert_eq!(dp.values[2], (2, TypedValue::I64(i64::MIN)));
        assert_eq!(dp.values[3], (3, TypedValue::U64(u64::MAX)));
        assert_eq!(dp.values[4], (4, TypedValue::I32(-5)));
        assert_eq!(dp.values[5], (5, TypedValue::Bool(true)));
        assert_eq!(dp.values[6], (6, TypedValue::String("héllo".to_string())));
        assert_eq!(dp.values[7], (7, TypedValue::Enum16(65535)), "Enum16 不丢");
    }

    /// QA 独立 D3：flags 不同值（0..=7 + 255）经 point_batch_to_rows →
    /// proto → proto_rows_to_datapoints 往返保真（现行列式 wire 会丢 flags）。
    #[test]
    fn qa_row_wire_flags_roundtrip() {
        use sensearcdb_engine::memtable::TypedValue;

        let flags: Vec<u8> = vec![0, 1, 2, 3, 4, 5, 6, 7, 255];
        let mut b = PointBatch::new(9);
        b.timestamps = flags.iter().map(|&i| 100 + i as i64).collect();
        b.columns = vec![ColVec::F64(flags.iter().map(|&i| i as f64).collect())];
        b.column_names = vec!["v".into()];
        b.column_ids = vec![0];
        b.qualities = vec![0xC0; flags.len()];
        b.flags = flags.clone();

        let rows = point_batch_to_rows(&b, "t9");
        assert_eq!(rows.len(), flags.len());
        let (points, failed) = proto_rows_to_datapoints(
            crate::proto::MultiTableBatch { points: rows },
            TenantId::DEFAULT,
            &|_| Some(9),
            0,
        );
        assert_eq!(failed, 0);
        for (i, f) in flags.iter().enumerate() {
            assert_eq!(points[i].flags, *f, "flags {f} 必须往返保真（行式补回）");
            assert_eq!(points[i].values[0], (0, TypedValue::F64(*f as f64)));
        }
    }

    /// QA 独立 D4：write_seq optional 的 None vs Some 区分——采集器路径恒 None，
    /// prost 编码往返 Some 保真（含 0 与 u64::MAX 边界），不伪造 Some。
    #[test]
    fn qa_row_wire_write_seq_none_vs_some() {
        use prost::Message;

        // 采集器编码路径：write_seq 恒 None。
        let mut b = PointBatch::new(7);
        b.timestamps = vec![100];
        b.columns = vec![ColVec::F64(vec![1.0])];
        b.column_names = vec!["v".into()];
        b.column_ids = vec![0];
        b.qualities = vec![0xC0; 1];
        b.flags = vec![0; 1];
        let rows = point_batch_to_rows(&b, "t7");
        assert_eq!(rows[0].write_seq, None, "采集器路径不得伪造 Some(write_seq)");

        // Some 值（含 0 / 1 / u64::MAX）经 prost 往返保真；None 保持 None。
        for seq in [Some(0u64), Some(1), Some(u64::MAX), None] {
            let r = crate::proto::PointRow {
                table_name: "t".to_string(),
                table_id: None,
                ts_ns: 0,
                server_ts_ns: None,
                values: vec![],
                quality: None,
                flags: None,
                write_seq: seq,
            };
            let mut buf = Vec::new();
            r.encode(&mut buf).unwrap();
            let decoded = crate::proto::PointRow::decode(&buf[..]).unwrap();
            assert_eq!(decoded.write_seq, seq, "write_seq {seq:?} 往返必须区分 None/Some");
        }
    }

    /// C1 独立边界（fresh eyes）：行式 `MultiTableBatch`（多表 + 稀疏 + 多类型）→
    /// 服务端 decode → engine `DataPoint` → snapshot `update_batch` → 读回逐点保真，
    /// 并覆盖同 ts 高 write_seq 的 LWW 决胜与不存在表的 failed 隔离计数。
    #[test]
    fn qa_regress_e2e_rowwire_multitable_sparse_multitype_to_snapshot() {
        use crate::proto::column_value::Value as Cv;
        use crate::proto::{ColumnValue, MultiTableBatch, PointRow};
        use sensearcdb_common::{AppConfig, ConfigHandle, DataType, TypedValue};
        use sensearcdb_snapshot::columnar::PendingPoint;
        use sensearcdb_snapshot::tag_metadata::SchemaMirrorRef;
        use sensearcdb_snapshot::{
            SnapshotMetrics, SnapshotStore, SnapshotSubscriptionManager, TagMetadataStore,
        };
        use std::sync::Arc;

        struct RegressMirror;
        impl SchemaMirrorRef for RegressMirror {
            fn get_table_name(&self, _: u32) -> Option<String> {
                Some("t".into())
            }
            fn get_table_description(&self, _: u32) -> Option<String> {
                None
            }
            fn get_range_low(&self, _: u32, _: u16) -> f64 {
                0.0
            }
            fn get_range_high(&self, _: u32, _: u16) -> f64 {
                100.0
            }
            fn get_instrument_range_low(&self, _: u32, _: u16) -> Option<f64> {
                None
            }
            fn get_instrument_range_high(&self, _: u32, _: u16) -> Option<f64> {
                None
            }
            fn get_unit(&self, _: u32, _: u16) -> Option<String> {
                None
            }
            fn get_display_digits(&self, _: u32, _: u16) -> u8 {
                2
            }
            fn get_data_type(&self, _: u32, _: u16) -> DataType {
                DataType::F64
            }
            fn is_enabled(&self, _: u32) -> bool {
                true
            }
        }

        // 多表：t_a(101) 3 列 {F64,I64,String}、t_b(102) 2 列 {Bool,F32}、
        // t_c(103) 3 列 {U64,I32,Enum16}；t_d 不存在（验证 failed 隔离）。
        let batch = MultiTableBatch {
            points: vec![
                // t_a 全列（ts=1000，模拟 WAL 逐点 seq=1）。
                PointRow {
                    table_name: "t_a".into(),
                    table_id: None,
                    ts_ns: 1000,
                    server_ts_ns: None,
                    values: vec![
                        ColumnValue {
                            column_id: 0,
                            value: Some(Cv::F64Val(3.14)),
                        },
                        ColumnValue {
                            column_id: 1,
                            value: Some(Cv::I64Val(-7)),
                        },
                        ColumnValue {
                            column_id: 2,
                            value: Some(Cv::StrVal("s1".into())),
                        },
                    ],
                    quality: Some(0xC0),
                    flags: Some(0),
                    write_seq: None,
                },
                // t_a 稀疏：只写 col0（同 ts=1000，seq=2 → LWW 覆盖 col0）。
                PointRow {
                    table_name: "t_a".into(),
                    table_id: None,
                    ts_ns: 1000,
                    server_ts_ns: None,
                    values: vec![ColumnValue {
                        column_id: 0,
                        value: Some(Cv::F64Val(9.99)),
                    }],
                    quality: Some(0x00),
                    flags: Some(0),
                    write_seq: None,
                },
                PointRow {
                    table_name: "t_b".into(),
                    table_id: None,
                    ts_ns: 2000,
                    server_ts_ns: None,
                    values: vec![
                        ColumnValue {
                            column_id: 0,
                            value: Some(Cv::BoolVal(true)),
                        },
                        ColumnValue {
                            column_id: 1,
                            value: Some(Cv::F32Val(1.5f32)),
                        },
                    ],
                    quality: Some(0xC0),
                    flags: Some(0),
                    write_seq: None,
                },
                PointRow {
                    table_name: "t_c".into(),
                    table_id: None,
                    ts_ns: 3000,
                    server_ts_ns: None,
                    values: vec![
                        ColumnValue {
                            column_id: 0,
                            value: Some(Cv::U64Val(u64::MAX)),
                        },
                        ColumnValue {
                            column_id: 1,
                            value: Some(Cv::I32Val(i32::MIN)),
                        },
                        ColumnValue {
                            column_id: 2,
                            value: Some(Cv::Enum16Val(7u32)),
                        },
                    ],
                    quality: Some(0xC0),
                    flags: Some(0),
                    write_seq: None,
                },
                // t_d 不存在 → failed 隔离。
                PointRow {
                    table_name: "t_d".into(),
                    table_id: None,
                    ts_ns: 4000,
                    server_ts_ns: None,
                    values: vec![ColumnValue {
                        column_id: 0,
                        value: Some(Cv::F64Val(1.0)),
                    }],
                    quality: Some(0xC0),
                    flags: Some(0),
                    write_seq: None,
                },
            ],
        };

        let resolve = |name: &str| match name {
            "t_a" => Some(101u32),
            "t_b" => Some(102u32),
            "t_c" => Some(103u32),
            _ => None,
        };

        let (points, failed) =
            proto_rows_to_datapoints(batch, sensearcdb_common::TenantId::DEFAULT, &resolve, 999_999);
        assert_eq!(failed, 1, "t_d 不存在必须计入 failed 隔离计数");
        assert_eq!(points.len(), 4, "4 条成功 rows（t_a×2 + t_b + t_c）");

        // 服务端 decode → engine DataPoint 级保真（多表 / 稀疏 / 多类型）。
        assert_eq!(points[0].table_id, 101);
        assert_eq!(points[0].values.len(), 3, "t_a 首行全列 3 列");
        assert_eq!(points[1].table_id, 101);
        assert_eq!(points[1].values.len(), 1, "t_a 稀疏行只 1 列（缺列不产假 null）");
        assert_eq!(points[2].table_id, 102);
        assert_eq!(points[2].values[1], (1u16, TypedValue::F32(1.5f32)));
        assert_eq!(points[3].table_id, 103);
        assert_eq!(points[3].values[0], (0u16, TypedValue::U64(u64::MAX)));
        assert_eq!(points[3].values[1], (1u16, TypedValue::I32(i32::MIN)));

        // engine DataPoint → snapshot update_batch（模拟 WAL 逐点 write_seq）。
        let cfg = {
            let mut c = AppConfig::default();
            c.snapshot.shard_multiplier = 1;
            ConfigHandle::new(c)
        };
        let sub_mgr = Arc::new(SnapshotSubscriptionManager::new());
        let metadata = Arc::new(TagMetadataStore::new(Arc::new(RegressMirror)));
        let store = SnapshotStore::new(&cfg, sub_mgr, metadata, SnapshotMetrics::noop());

        let seqs: Vec<Option<u64>> = vec![Some(1), Some(2), Some(1), Some(1)];
        let pending: Vec<PendingPoint<'_>> = points
            .iter()
            .zip(seqs.iter())
            .map(|(p, seq)| PendingPoint {
                tenant_id: p.tenant_id,
                table_id: p.table_id,
                values: p.values.as_slice(),
                qualities: p.qualities.as_slice(),
                ts: p.ts,
                server_ts: p.server_ts,
                write_seq: *seq,
                is_derived: false,
                writer_session_id: None,
            })
            .collect();
        store.update_batch(&pending);

        // 读回逐点保真。
        let e_a = store.get(101).expect("t_a 应有最新值");
        assert_eq!(
            e_a.values,
            vec![
                (0u16, TypedValue::F64(9.99)),
                (1u16, TypedValue::I64(-7)),
                (2u16, TypedValue::String("s1".into())),
            ],
            "t_a：同 ts 高 seq 覆盖 col0，稀疏 col1/col2 原样保留"
        );
        assert_eq!(
            e_a.write_seqs,
            vec![(0u16, Some(2)), (1u16, Some(1)), (2u16, Some(1))],
            "LWW 后 write_seqs 按列保真"
        );
        assert_eq!(e_a.qualities, vec![(0u16, 0x00), (1u16, 0xC0), (2u16, 0xC0)]);

        let e_b = store.get(102).expect("t_b 应有最新值");
        assert_eq!(
            e_b.values,
            vec![(0u16, TypedValue::Bool(true)), (1u16, TypedValue::F32(1.5f32))]
        );

        let e_c = store.get(103).expect("t_c 应有最新值");
        assert_eq!(
            e_c.values,
            vec![
                (0u16, TypedValue::U64(u64::MAX)),
                (1u16, TypedValue::I32(i32::MIN)),
                (2u16, TypedValue::Enum16(7u16)),
            ]
        );
        assert!(store.get(999).is_none(), "不存在的表不应有值");
    }

}
