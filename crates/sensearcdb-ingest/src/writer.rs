//! EngineWriter trait and implementations: Embedded (local mpsc), Direct (engine), and Grpc (remote tonic).
//!
//! The EngineWriter abstraction decouples the collector from the actual
//! write destination, enabling both embedded (same-process) and remote
//! (gRPC) deployment modes.
//!
//! # Writers
//!
//! - `EmbeddedWriter` — tokio mpsc channel to a consumer task
//! - `EngineDirectWriter` — directly calls `Engine::write_batch()`, converts
//!   ingest `PointBatch` to engine `PointBatch` inline
//! - `GrpcWriter` — tonic gRPC client streaming to a remote engine

use async_trait::async_trait;
use std::sync::Arc;

use sensearcdb_common::write_attr;
use sensearcdb_engine::memtable::TypedValue;

use crate::batch::{ColVec, PointBatch};
use crate::proto::write_service_client::WriteServiceClient;
use crate::types::{EngineWriteError, WriteReceipt, WriteStatus};
use tonic::transport::Channel;

/// The EngineWriter trait abstracts where PointBatch data is sent.
///
/// Implementations may write to a local engine channel (EmbeddedWriter),
/// directly to a local engine (EngineDirectWriter), or to a remote
/// gRPC service (GrpcWriter).
#[async_trait]
pub trait EngineWriter: Send + Sync + 'static {
    /// Write a batch of data points.
    ///
    /// Returns a receipt on success or an error describing the failure.
    async fn write_batch(&self, batch: PointBatch) -> Result<WriteReceipt, EngineWriteError>;

    /// 背压等待 + 幂等重试（方案 C）。
    ///
    /// 默认实现 = 直接 `write_batch`（Embedded/Grpc 无本机引擎可等待）。
    /// `EngineDirectWriter` 覆写为：背压时 `engine.wait_writable` 排空后重写同一批，
    /// 仅排空超时才把背压错误透出（供上层转 RESOURCE_EXHAUSTED）。
    async fn write_batch_retry(&self, batch: PointBatch) -> Result<WriteReceipt, EngineWriteError> {
        self.write_batch(batch).await
    }

    /// 写一个已合并的引擎级批次（跨表 Vec<DataPoint>）。
    /// 仅 EngineDirectWriter 覆写；服务端攒批只在 writer_type()==Direct 时启用，
    /// 其余 writer 命中此默认实现即接线错误，直接拒绝而非静默走错路径。
    async fn write_engine_batch_retry(
        &self,
        _batch: sensearcdb_engine::engine::PointBatch,
    ) -> Result<WriteReceipt, EngineWriteError> {
        Err(EngineWriteError::Rejected(
            "merged engine batch write unsupported by this writer".to_string(),
        ))
    }

    /// Write multiple batches, returning a single aggregated receipt.
    ///
    /// 默认实现：逐个 `write_batch`，累加 `points_written` / `points_failed` /
    /// `latency_ms`；遇到第一个 `Err` 立即透出（fail-fast，不继续写后续批次）。
    /// `GrpcWriter` 覆写为「单 stream 多消息」的 `send_batches`，大幅减少 RPC 往返。
    async fn write_batches(&self, batches: Vec<PointBatch>) -> Result<WriteReceipt, EngineWriteError> {
        let mut total_written = 0u32;
        let mut total_failed = 0u32;
        let mut total_latency = 0u64;
        for batch in batches {
            match self.write_batch(batch).await {
                Ok(receipt) => {
                    total_written = total_written.saturating_add(receipt.points_written);
                    total_failed = total_failed.saturating_add(receipt.points_failed);
                    total_latency = total_latency.saturating_add(receipt.latency_ms);
                }
                Err(e) => return Err(e),
            }
        }
        Ok(WriteReceipt {
            points_written: total_written,
            points_failed: total_failed,
            latency_ms: total_latency,
            write_status: if total_failed == 0 {
                WriteStatus::Accepted { wal_seq: 0 }
            } else {
                WriteStatus::Rejected {
                    reason: format!("{} points failed", total_failed),
                }
            },
        })
    }

    /// Check if the writer is currently connected to the engine.
    fn is_connected(&self) -> bool;

    /// Return the type of this writer.
    fn writer_type(&self) -> WriterType;
}

/// Identifies the writer deployment mode.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WriterType {
    /// Same-process embedded engine writer (mpsc channel).
    Embedded,
    /// Same-process direct engine writer (no channel, inline call).
    Direct,
    /// Remote gRPC engine writer.
    Remote,
}

// ─── EmbeddedWriter ─────────────────────────────────────────────────────────

/// EmbeddedWriter sends PointBatch via a tokio mpsc channel to the local engine.
///
/// Channel capacity is sized according to: `C = N × F × T_buf / avg_batch × K`.
pub struct EmbeddedWriter {
    /// Channel sender to the engine's write path.
    tx: tokio::sync::mpsc::Sender<PointBatch>,
    /// Channel capacity.
    capacity: usize,
}

impl EmbeddedWriter {
    /// Create a new EmbeddedWriter.
    ///
    /// # Arguments
    /// * `tx` — mpsc sender connected to the engine's write path.
    /// * `capacity` — channel buffer capacity.
    pub fn new(tx: tokio::sync::mpsc::Sender<PointBatch>, capacity: usize) -> Self {
        Self { tx, capacity }
    }

    /// Returns the channel capacity.
    pub fn capacity(&self) -> usize {
        self.capacity
    }
}

#[async_trait]
impl EngineWriter for EmbeddedWriter {
    async fn write_batch(&self, batch: PointBatch) -> Result<WriteReceipt, EngineWriteError> {
        let row_count = batch.row_count() as u32;
        let start = std::time::Instant::now();

        match self.tx.try_send(batch) {
            Ok(()) => {
                let latency_ms = start.elapsed().as_millis() as u64;
                Ok(WriteReceipt::accepted(0, row_count, latency_ms))
            }
            Err(tokio::sync::mpsc::error::TrySendError::Full(_)) => {
                Err(EngineWriteError::ChannelFull)
            }
            Err(tokio::sync::mpsc::error::TrySendError::Closed(_)) => {
                Err(EngineWriteError::NotReachable)
            }
        }
    }

    fn is_connected(&self) -> bool {
        !self.tx.is_closed()
    }

    fn writer_type(&self) -> WriterType {
        WriterType::Embedded
    }
}

// ─── EngineDirectWriter ─────────────────────────────────────────────────────

/// EngineDirectWriter calls `Engine::write_batch()` directly — no channel indirection.
///
/// Converts the ingest `PointBatch` (columnar) into the engine's `PointBatch`
/// (row-oriented `Vec<DataPoint>`) inline before each call.
///
/// This is the primary embedded writer used in production. It skips the
/// mpsc channel entirely, providing the lowest latency by calling into
/// the engine's Group Commit write path directly.
pub struct EngineDirectWriter {
    /// Reference to the storage engine.
    engine: Arc<sensearcdb_engine::engine::Engine>,
}

impl EngineDirectWriter {
    /// Create a new EngineDirectWriter connected to the given engine.
    pub fn new(engine: Arc<sensearcdb_engine::engine::Engine>) -> Self {
        Self { engine }
    }

    /// Return a clone of the internal engine reference.
    pub fn engine(&self) -> Arc<sensearcdb_engine::engine::Engine> {
        Arc::clone(&self.engine)
    }
}

#[async_trait]
impl EngineWriter for EngineDirectWriter {
    async fn write_batch(&self, batch: PointBatch) -> Result<WriteReceipt, EngineWriteError> {
        let row_count = batch.row_count() as u32;
        let start = std::time::Instant::now();

        // Convert ingest (columnar) PointBatch → engine (row-oriented) PointBatch
        let engine_batch = convert_to_engine_batch(&batch);

        match self.engine.write_batch(engine_batch).await {
            Ok(result) => {
                let latency_ms = start.elapsed().as_millis() as u64;
                Ok(WriteReceipt::accepted(
                    result.wal_seq_start,
                    row_count,
                    latency_ms,
                ))
            }
            Err(e) => Err(map_engine_error(e)),
        }
    }

    /// 背压等待 + 幂等重试：引擎 memtable 停滞时先 `wait_writable` 排空（force_freeze
    /// + request_flush），超时内恢复可写则重写**同一批**，否则把背压错误透出。
    ///
    /// 幂等安全依据：`Engine::write_batch` 在 `write_stalled()` 时于落 WAL **之前**
    /// 返回 `MemTableFull`，因此被拒批次从未写入，重试不会重复。
    async fn write_batch_retry(&self, batch: PointBatch) -> Result<WriteReceipt, EngineWriteError> {
        // 排空等待上限：超过则放弃等待，转回 RESOURCE_EXHAUSTED（拒绝式背压兜底）。
        const RETRY_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(5);

        let row_count = batch.row_count() as u32;
        // 转换一次持有；重试时再从原始 ingest batch 重新转换（happy path 零额外 clone）。
        let mut engine_batch = convert_to_engine_batch(&batch);

        loop {
            let start = std::time::Instant::now();
            match self.engine.write_batch(engine_batch).await {
                Ok(result) => {
                    let latency_ms = start.elapsed().as_millis() as u64;
                    return Ok(WriteReceipt::accepted(
                        result.wal_seq_start,
                        row_count,
                        latency_ms,
                    ));
                }
                Err(sensearcdb_common::SenseArcError::MemTableFull) => {
                    // 背压：等待排空后重试同一批；超时则透出 Backpressure。
                    if self.engine.wait_writable(RETRY_TIMEOUT).await {
                        engine_batch = convert_to_engine_batch(&batch);
                        continue;
                    }
                    return Err(EngineWriteError::Backpressure);
                }
                Err(e) => return Err(map_engine_error(e)),
            }
        }
    }

    /// 写一个已合并的引擎级批次（跨表 Vec<DataPoint>），与 `write_batch_retry`
    /// 同幂等语义，但跳过 convert、直接对已合并 `Vec<DataPoint>` 重试。
    ///
    /// 幂等依据：`engine.write_batch` 的 `write_stalled` 检查在落 WAL 之前返回
    /// `MemTableFull`，被拒批次从未写入，重试不重复。
    async fn write_engine_batch_retry(
        &self,
        batch: sensearcdb_engine::engine::PointBatch,
    ) -> Result<WriteReceipt, EngineWriteError> {
        const RETRY_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(5);
        let row_count = batch.points.len() as u32;
        let points = batch.points;
        loop {
            let start = std::time::Instant::now();
            let attempt = sensearcdb_engine::engine::PointBatch { points: points.clone() };
            match self.engine.write_batch(attempt).await {
                Ok(result) => return Ok(WriteReceipt::accepted(
                    result.wal_seq_start, row_count, start.elapsed().as_millis() as u64,
                )),
                Err(sensearcdb_common::SenseArcError::MemTableFull) => {
                    if self.engine.wait_writable(RETRY_TIMEOUT).await {
                        continue;
                    }
                    return Err(EngineWriteError::Backpressure);
                }
                Err(e) => return Err(map_engine_error(e)),
            }
        }
    }

    fn is_connected(&self) -> bool {
        true // Direct connection — always connected
    }

    fn writer_type(&self) -> WriterType {
        WriterType::Direct
    }
}

/// Map an engine-level error into the ingest-layer error type, preserving the
/// backpressure signal (`SenseArcError::MemTableFull`) so the gRPC layer can
/// surface it as `RESOURCE_EXHAUSTED` instead of silently dropping the batch.
fn map_engine_error(e: sensearcdb_common::SenseArcError) -> EngineWriteError {
    match e {
        sensearcdb_common::SenseArcError::MemTableFull => EngineWriteError::Backpressure,
        other => EngineWriteError::Rejected(format!("engine error: {}", other)),
    }
}

/// Convert an ingest `PointBatch` (columnar SoA) into the engine's
/// `PointBatch` (row-oriented `Vec<DataPoint>`).
///
/// This is an inline conversion — no heap copies beyond the necessary
/// per-row allocations. The engine's `PointBatch` format uses `Vec<(u16, TypedValue)>`
/// for values and `Vec<(u16, u8)>` for qualities, keyed by column_id.
///
/// Public so the integration layer (cluster-mode writer in `src/startup.rs`)
/// can reuse the exact same conversion before serializing for Raft replication.
pub fn convert_to_engine_batch(batch: &PointBatch) -> sensearcdb_engine::engine::PointBatch {
    // P2 归因埋点：ingest PointBatch → engine DataPoint（行式逐行分配）。
    let conv_t0 = std::time::Instant::now();
    let row_count = batch.timestamps.len();
    let mut points: Vec<sensearcdb_engine::engine::DataPoint> = Vec::with_capacity(row_count);
    let server_ts = chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0);

    // Explicit column ids are required on the write path so a sparse column set
    // ({0, 2}) maps to its physical slots instead of being squeezed by array
    // position. Legacy producers that never populate `column_ids` fall back to
    // the positional (dense) id, preserving the pre-fix behavior.
    if batch.column_ids.is_empty() {
        tracing::debug!(
            table_id = batch.table_id,
            "PointBatch has no explicit column_ids; falling back to positional ids"
        );
    }

    for row_idx in 0..row_count {
        let mut values: Vec<(u16, TypedValue)> = Vec::with_capacity(batch.columns.len());
        let mut qualities: Vec<(u16, u8)> = Vec::with_capacity(batch.columns.len());

        for (col_idx, col) in batch.columns.iter().enumerate() {
            // 显式 column_id 贯穿：用 batch.column_ids 映射，而非 `col_idx as u16`。
            let column_id = batch.column_id_at(col_idx);

            // Per-row null：presence[col_idx][row_idx] == false 表示该行此列为 null，
            // 跳过不 emit → memtable 落 presence=false → 读取返回 None（不误写成 0）。
            if let Some(presence) = batch.column_presence.get(col_idx) {
                if !presence.get(row_idx).copied().unwrap_or(true) {
                    continue;
                }
            }

            // 多类型直通：按 ingest ColVec 变体构造对应 TypedValue，不再 `as f64` 折叠。
            let value = match col {
                ColVec::F64(v) => TypedValue::F64(v.get(row_idx).copied().unwrap_or(0.0)),
                ColVec::F32(v) => TypedValue::F32(v.get(row_idx).copied().unwrap_or(0.0)),
                ColVec::I64(v) => TypedValue::I64(v.get(row_idx).copied().unwrap_or(0)),
                ColVec::U64(v) => TypedValue::U64(v.get(row_idx).copied().unwrap_or(0)),
                ColVec::I32(v) => TypedValue::I32(v.get(row_idx).copied().unwrap_or(0)),
                ColVec::Bool(v) => TypedValue::Bool(v.get(row_idx).copied().unwrap_or(0) != 0),
                ColVec::String(v) => {
                    TypedValue::String(v.get(row_idx).cloned().unwrap_or_default())
                }
            };
            values.push((column_id, value));

            let quality = batch.qualities.get(row_idx).copied().unwrap_or(0xC0);
            qualities.push((column_id, quality));
        }

        let flags = batch.flags.get(row_idx).copied().unwrap_or(0);

        points.push(sensearcdb_engine::engine::DataPoint {
            table_id: batch.table_id,
            tenant_id: batch.tenant_id,
            ts: batch.timestamps[row_idx],
            server_ts,
            values,
            qualities,
            schema_version: 0,
            flags,
        });
    }

    write_attr::add_convert_engine(conv_t0.elapsed().as_nanos() as u64);
    sensearcdb_engine::engine::PointBatch { points }
}

// ─── GrpcWriter ─────────────────────────────────────────────────────────────

/// GrpcWriter sends PointBatch via tonic gRPC client streaming to a remote engine.
///
/// Holds a `WriteServiceClient<Channel>` for the 5201 `WriteService` and pushes
/// each batch through the client-streaming `ingest` RPC. The wire format carries
/// a `table_name` (not `table_id`), so the writer resolves the name through an
/// injected `table_id → table_name` closure — the ingest crate itself never
/// depends on the schema crate.
const GRPC_LIST_MAX_BYTES: usize = 2 * 1024 * 1024;

pub struct GrpcWriter {
    /// The engine service endpoint URL.
    endpoint: String,
    /// Lazily-connected tonic `WriteServiceClient`.
    client: tokio::sync::RwLock<Option<WriteServiceClient<Channel>>>,
    /// Resolves `table_id` to the wire-level `table_name`.
    table_name_resolver: Arc<dyn Fn(u32) -> Option<String> + Send + Sync>,
    /// Channel reconnect backoff (ms).
    reconnect_backoff_ms: u64,
    /// 可选 API Key：随每次 ingest 注入 `x-api-key` metadata，供服务端推导 tenant。
    api_key: Option<String>,
    /// 单条 MultiTableBatch 消息的字节预算：把 N 个行式 PointRow 按此上限打包。
    list_max_bytes: usize,
}

impl GrpcWriter {
    /// Create a new GrpcWriter pointing at a remote engine endpoint.
    pub fn new(endpoint: String) -> Self {
        Self {
            endpoint,
            client: tokio::sync::RwLock::new(None),
            table_name_resolver: Arc::new(|_| None),
            reconnect_backoff_ms: 1_000,
            api_key: None,
            list_max_bytes: GRPC_LIST_MAX_BYTES,
        }
    }

    /// Set the byte budget for a single `MultiTableBatch` message.
    pub fn with_list_max_bytes(mut self, bytes: usize) -> Self {
        self.list_max_bytes = bytes;
        self
    }

    /// Set the reconnect backoff duration in milliseconds.
    pub fn with_reconnect_backoff(mut self, ms: u64) -> Self {
        self.reconnect_backoff_ms = ms;
        self
    }

    /// Inject a `table_id → table_name` resolver for wire serialization.
    pub fn with_table_name_resolver(
        mut self,
        resolver: Arc<dyn Fn(u32) -> Option<String> + Send + Sync>,
    ) -> Self {
        self.table_name_resolver = resolver;
        self
    }

    /// Inject an API key，随每次 ingest 注入 `x-api-key` metadata（服务端据此推导 tenant）。
    pub fn with_api_key(mut self, api_key: Option<String>) -> Self {
        self.api_key = api_key;
        self
    }

    /// Attempt to connect to the remote engine.
    ///
    /// Builds a tonic `Channel` via `Endpoint::connect`. The channel connects
    /// lazily on the first RPC; this only fails when the endpoint URI is invalid.
    pub async fn connect(&self) -> Result<(), String> {
        let endpoint = tonic::transport::Endpoint::from_shared(self.endpoint.clone())
            .map_err(|e| format!("invalid endpoint '{}': {}", self.endpoint, e))?;
        let channel = endpoint
            .connect()
            .await
            .map_err(|e| format!("failed to connect to '{}': {}", self.endpoint, e))?;
        *self.client.write().await = Some(WriteServiceClient::new(channel));
        tracing::info!(endpoint = %self.endpoint, "grpc writer connected");
        Ok(())
    }

    /// Disconnect from the remote engine.
    pub async fn disconnect(&self) {
        *self.client.write().await = None;
        tracing::info!(endpoint = %self.endpoint, "grpc writer disconnected");
    }

    /// Send a single PointBatch to the remote engine via client-streaming `ingest`.
    pub async fn send_batch(&self, batch: PointBatch) -> Result<WriteReceipt, EngineWriteError> {
        self.send_batches(vec![batch]).await
    }

    /// Send multiple PointBatches in a single client-streaming `ingest` RPC.
    ///
    /// 每个 batch 分别解析 `table_name`（任一解析为 `None` → `TableNotFound`），
    /// 各自转 proto 后作为**同一条 stream 的多条消息**一次性发出，服务端在一个
    /// 流循环内逐条写入并聚合返回单个 receipt —— 把 N 次 RPC 降为 1 次。
    pub async fn send_batches(
        &self,
        batches: Vec<PointBatch>,
    ) -> Result<WriteReceipt, EngineWriteError> {
        let total_row_count: u32 = batches.iter().map(|b| b.row_count() as u32).sum();
        let start = std::time::Instant::now();

        let mut client = self
            .client
            .read()
            .await
            .clone()
            .ok_or(EngineWriteError::NotReachable)?;

        // Resolve the wire-level table name for every batch before sending anything.
        // 任一解析失败 → TableNotFound（发流前全量 resolve，杜绝前缀已写歧义）。
        let mut rows = Vec::with_capacity(total_row_count as usize);
        for batch in &batches {
            let table_name = (self.table_name_resolver)(batch.table_id)
                .ok_or(EngineWriteError::TableNotFound(batch.table_id))?;
            rows.extend(point_batch_to_rows(batch, &table_name));
        }

        let stream = futures::stream::iter(pack_rows_into_batches(rows, self.list_max_bytes));

        // 携带 x-api-key，使服务端能从认证凭据推导 tenant（远程采集器与 embedded 同源）。
        let mut request = tonic::Request::new(stream);
        if let Some(key) = self.api_key.as_ref() {
            let value = tonic::metadata::MetadataValue::try_from(key.as_str()).map_err(|e| {
                EngineWriteError::Rejected(format!("invalid api key metadata: {e}"))
            })?;
            request.metadata_mut().insert("x-api-key", value);
        }

        let response = client.ingest(request).await.map_err(|e| {
            if e.code() == tonic::Code::ResourceExhausted {
                // Server signaled backpressure: the stream was aborted before all
                // batches were written, so the caller must buffer the whole chunk
                // to store-and-forward (zero-loss priority).
                EngineWriteError::Backpressure
            } else {
                EngineWriteError::Rejected(format!("grpc ingest failed: {}", e))
            }
        })?;

        let receipt = response.into_inner();
        let latency_ms = start.elapsed().as_millis() as u64;

        match receipt.status.as_str() {
            "ok" => Ok(WriteReceipt::accepted(
                0,
                receipt.rows_written as u32,
                latency_ms,
            )),
            "partial" => Ok(WriteReceipt {
                points_written: receipt.rows_written as u32,
                points_failed: total_row_count.saturating_sub(receipt.rows_written as u32),
                latency_ms,
                write_status: WriteStatus::Rejected {
                    reason: if receipt.error.is_empty() {
                        "partial write".to_string()
                    } else {
                        receipt.error
                    },
                },
            }),
            _ => Err(EngineWriteError::Rejected(if receipt.error.is_empty() {
                format!("grpc ingest rejected (status={})", receipt.status)
            } else {
                receipt.error
            })),
        }
    }
}

#[async_trait]
impl EngineWriter for GrpcWriter {
    async fn write_batch(&self, batch: PointBatch) -> Result<WriteReceipt, EngineWriteError> {
        self.send_batch(batch).await
    }

    async fn write_batches(&self, batches: Vec<PointBatch>) -> Result<WriteReceipt, EngineWriteError> {
        self.send_batches(batches).await
    }

    fn is_connected(&self) -> bool {
        self.client
            .try_read()
            .map(|guard| guard.is_some())
            .unwrap_or(false)
    }

    fn writer_type(&self) -> WriterType {
        WriterType::Remote
    }
}

/// 列式 ingest `PointBatch` → 行式 `Vec<PointRow>`（转置，含 presence 跳过）。
///
/// 这是 `convert_to_engine_batch`（writer.rs:310）的 wire 镜像：
/// 逐行逐列迭代，presence==false 跳过（缺列 = 不在 values），
/// ColVec 变体 → proto `ColumnValue` oneof。
pub fn point_batch_to_rows(
    batch: &PointBatch,
    table_name: &str,
) -> Vec<crate::proto::PointRow> {
    use crate::proto::column_value::Value as Cv;

    let row_count = batch.timestamps.len();
    let mut rows = Vec::with_capacity(row_count);

    for row_idx in 0..row_count {
        let mut values = Vec::with_capacity(batch.columns.len());
        for (col_idx, col) in batch.columns.iter().enumerate() {
            let column_id = batch.column_id_at(col_idx);

            // Per-row null：presence[col_idx][row_idx] == false 表示该行此列为 null，
            // 跳过不 emit → 行式天然稀疏（缺列 = 不在 values）。
            if let Some(presence) = batch.column_presence.get(col_idx) {
                if !presence.get(row_idx).copied().unwrap_or(true) {
                    continue;
                }
            }

            let value = match col {
                ColVec::F64(v) => Cv::F64Val(v.get(row_idx).copied().unwrap_or(0.0)),
                ColVec::F32(v) => Cv::F32Val(v.get(row_idx).copied().unwrap_or(0.0)),
                ColVec::I64(v) => Cv::I64Val(v.get(row_idx).copied().unwrap_or(0)),
                ColVec::U64(v) => Cv::U64Val(v.get(row_idx).copied().unwrap_or(0)),
                ColVec::I32(v) => Cv::I32Val(v.get(row_idx).copied().unwrap_or(0)),
                ColVec::Bool(v) => Cv::BoolVal(v.get(row_idx).copied().unwrap_or(0) != 0),
                ColVec::String(v) => Cv::StrVal(v.get(row_idx).cloned().unwrap_or_default()),
            };
            values.push(crate::proto::ColumnValue {
                column_id: column_id as u32,
                value: Some(value),
            });
        }

        rows.push(crate::proto::PointRow {
            table_name: table_name.to_string(),
            table_id: None,
            ts_ns: batch.timestamps[row_idx],
            server_ts_ns: None,
            values,
            quality: Some(batch.qualities.get(row_idx).copied().unwrap_or(0xC0) as u32),
            flags: Some(batch.flags.get(row_idx).copied().unwrap_or(0) as u32),
            write_seq: None,
        });
    }

    rows
}

/// 把 `Vec<PointRow>` 按字节预算打包成尽量少的 `MultiTableBatch`（不丢不重排）。
///
/// 复用原 `pack_batches_into_lists` 的字节预算逻辑，但单位从 `PointBatch` 换成
/// `PointRow`。每条消息 payload 保持在 `max_bytes` 内（`prost::Message::encoded_len`）。
fn pack_rows_into_batches(
    rows: Vec<crate::proto::PointRow>,
    max_bytes: usize,
) -> Vec<crate::proto::MultiTableBatch> {
    use prost::Message;
    let mut batches = Vec::new();
    let mut cur: Vec<crate::proto::PointRow> = Vec::new();
    let mut cur_bytes = 0usize;
    for r in rows {
        let n = r.encoded_len();
        if !cur.is_empty() && cur_bytes + n > max_bytes {
            batches.push(crate::proto::MultiTableBatch {
                points: std::mem::take(&mut cur),
            });
            cur_bytes = 0;
        }
        cur_bytes += n;
        cur.push(r);
    }
    if !cur.is_empty() {
        batches.push(crate::proto::MultiTableBatch { points: cur });
    }
    batches
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::batch::BatchMeta;

    #[tokio::test]
    async fn test_embedded_writer_send() {
        let (tx, mut rx) = tokio::sync::mpsc::channel::<PointBatch>(16);
        let writer = EmbeddedWriter::new(tx, 16);
        assert!(writer.is_connected());
        assert_eq!(writer.writer_type(), WriterType::Embedded);

        let mut batch = PointBatch::new(1);
        batch.timestamps = vec![100, 200];
        batch.columns = vec![crate::batch::ColVec::F64(vec![1.0, 2.0])];
        batch.column_names = vec!["val".into()];
        batch.qualities = vec![0xC0; 2];
        batch.flags = vec![0; 2];

        let receipt = writer.write_batch(batch).await.unwrap();
        assert_eq!(receipt.points_written, 2);
        assert!(receipt.is_fully_accepted());

        // Verify the batch arrived on the channel
        let received = rx.recv().await;
        assert!(received.is_some());
    }

    #[tokio::test]
    async fn test_embedded_writer_channel_full() {
        let (tx, _rx) = tokio::sync::mpsc::channel::<PointBatch>(1);
        let writer = EmbeddedWriter::new(tx, 1);

        // Fill the channel
        let batch1 = PointBatch::new(1);
        let _ = writer.write_batch(batch1).await;

        // Second send should fail with ChannelFull
        let batch2 = PointBatch::new(2);
        let result = writer.write_batch(batch2).await;
        assert!(matches!(result, Err(EngineWriteError::ChannelFull)));
    }

    #[tokio::test]
    async fn test_engine_direct_writer_type() {
        // Cannot easily test EngineDirectWriter without a real Engine,
        // but we can verify that the type is correct through construction.
        // The writer type is Direct (not Embedded).
        // (Full integration test requires a real Engine instance.)
    }

    /// 方案 C 集成测试：引擎 memtable 停滞时，`write_batch_retry` 应等待排空后
    /// 重写同一批成功（幂等），而不是立即返回 `Backpressure`。
    #[tokio::test]
    async fn test_engine_direct_writer_write_batch_retry_drains_backpressure() {
        use sensearcdb_common::{AppConfig, ConfigHandle, TenantId};
        use sensearcdb_engine::engine::{DataPoint, Engine, PointBatch as EnginePointBatch};

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

        // 制造 stall：队列深度 65 > 硬阈值 64（写 1 点 + freeze × 65）。
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

        let writer = EngineDirectWriter::new(engine.clone());

        // 构造一个 ingest 单行 batch。
        let mut batch = PointBatch::new(7);
        batch.timestamps = vec![9999];
        batch.columns = vec![ColVec::F64(vec![1.0])];
        batch.column_names = vec!["v".into()];
        batch.column_ids = vec![0];
        batch.qualities = vec![0xC0];
        batch.flags = vec![0];

        // 背压等待 + 重试：排空后应成功写入同一批。
        let receipt = writer
            .write_batch_retry(batch)
            .await
            .expect("write_batch_retry 应等待排空后成功");
        assert_eq!(receipt.points_written, 1);
        assert!(receipt.is_fully_accepted());
    }

    // ───────────── QA 独立边界（fresh eyes，不依赖工程师自测复述）─────────────

    /// QA 独立：write_batch_retry 端到端不丢点——背压排空后该批被写入且可查询读回。
    /// 用独立 table 77 避开制造 stall 的 table 7，断言恰好 1 点、值保真。
    #[tokio::test]
    async fn qa_write_batch_retry_no_loss_queryable() {
        use sensearcdb_common::{AppConfig, ConfigHandle, TenantId};
        use sensearcdb_engine::engine::{DataPoint, Engine, PointBatch as EnginePointBatch};

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

        let writer = EngineDirectWriter::new(engine.clone());

        // 独立 table 77，1 点 ts=9999 value=42.0。
        let mut batch = PointBatch::new(77);
        batch.timestamps = vec![9999];
        batch.columns = vec![ColVec::F64(vec![42.0])];
        batch.column_names = vec!["v".into()];
        batch.column_ids = vec![0];
        batch.qualities = vec![0xC0];
        batch.flags = vec![0];

        let receipt = writer
            .write_batch_retry(batch)
            .await
            .expect("背压排空后应成功写入");
        assert_eq!(receipt.points_written, 1);

        // 读回：table 77 在 [9999,9999] 应恰好 1 点、值 42.0（不丢不重）。
        let (cols, tss) = engine
            .query_table_columns(TenantId::DEFAULT, 77, &[0], 9999, 9999)
            .await
            .unwrap();
        assert_eq!(tss, vec![9999]);
        assert_eq!(cols[0], vec![Some(TypedValue::F64(42.0))]);
    }

    /// QA 独立：非背压错误（配额拒绝）不经过 wait_writable、不被误吞为 Backpressure，
    /// 立即按原路径返回 Rejected（不做 5s 等待重试）。
    #[tokio::test]
    async fn qa_write_batch_retry_quota_rejection_not_retried() {
        use sensearcdb_common::{AppConfig, ConfigHandle, TenantId};
        use sensearcdb_engine::engine::Engine;
        use sensearcdb_engine::quota::{QuotaSnapshot, QuotaSource, TenantQuotaGate};

        struct RejectAll;
        impl QuotaSource for RejectAll {
            fn snapshot(&self, _tenant: TenantId) -> Option<QuotaSnapshot> {
                Some(QuotaSnapshot {
                    points_limit: Some(0), // 0 点上限 → 任意批被拒
                    ..Default::default()
                })
            }
        }

        let tmp = tempfile::TempDir::new().unwrap();
        let mut cfg = AppConfig::default();
        cfg.paths.data_dir = tmp.path().to_string_lossy().into_owned();
        let handle = ConfigHandle::new(cfg);
        let engine = Arc::new(Engine::new(&handle, None).await.expect("engine"));
        engine.set_quota_gate(Arc::new(TenantQuotaGate::new(Arc::new(RejectAll))));

        let writer = EngineDirectWriter::new(engine.clone());

        let mut batch = PointBatch::new(7);
        batch.timestamps = vec![100];
        batch.columns = vec![ColVec::F64(vec![1.0])];
        batch.column_names = vec!["v".into()];
        batch.column_ids = vec![0];
        batch.qualities = vec![0xC0];
        batch.flags = vec![0];

        let err = writer.write_batch_retry(batch).await.unwrap_err();
        assert!(
            matches!(err, EngineWriteError::Rejected(_)),
            "配额拒绝应走 Rejected 原路径，不得被当作 Backpressure 重试/等待：{err:?}"
        );
    }

    /// QA 独立：多流并发背压（16 个 writer）→ 无死锁/livelock，全部在超时内排空后成功。
    #[tokio::test]
    async fn qa_write_batch_retry_concurrent_backpressure_no_deadlock() {
        use sensearcdb_common::{AppConfig, ConfigHandle, TenantId};
        use sensearcdb_engine::engine::{DataPoint, Engine, PointBatch as EnginePointBatch};

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

        let mut handles = Vec::new();
        for t in 0..16u32 {
            let w = Arc::clone(&writer);
            handles.push(tokio::spawn(async move {
                let mut batch = PointBatch::new(100 + t);
                batch.timestamps = vec![t as i64];
                batch.columns = vec![ColVec::F64(vec![t as f64])];
                batch.column_names = vec!["v".into()];
                batch.column_ids = vec![0];
                batch.qualities = vec![0xC0];
                batch.flags = vec![0];
                w.write_batch_retry(batch).await
            }));
        }

        for h in handles {
            let receipt = h
                .await
                .expect("join")
                .expect("并发背压下每个写都应在排空后成功");
            assert_eq!(receipt.points_written, 1);
        }
    }

    #[tokio::test]
    async fn test_grpc_writer_not_connected() {
        let writer = GrpcWriter::new("http://127.0.0.1:5201".into());
        assert!(!writer.is_connected());
        assert_eq!(writer.writer_type(), WriterType::Remote);

        let batch = PointBatch::new(1);
        let result = writer.write_batch(batch).await;
        assert!(matches!(result, Err(EngineWriteError::NotReachable)));
    }

    #[test]
    fn test_point_batch_to_rows_roundtrip() {
        let mut batch = PointBatch::new(5);
        batch.timestamps = vec![100, 200, 300];
        batch.columns = vec![
            ColVec::F64(vec![1.0, 2.0, 3.0]),
            ColVec::I64(vec![10, 20, 30]),
        ];
        batch.column_names = vec!["a".into(), "b".into()];
        batch.qualities = vec![0xC0; 3];
        batch.flags = vec![0; 3];

        let rows = point_batch_to_rows(&batch, "sensors");
        assert_eq!(rows.len(), 3);
        // 行 0：2 列，column_id 位置回退 0/1，值多类型保真。
        assert_eq!(rows[0].table_name, "sensors");
        assert_eq!(rows[0].ts_ns, 100);
        assert_eq!(rows[0].quality, Some(0xC0));
        assert_eq!(rows[0].flags, Some(0));
        assert_eq!(rows[0].values.len(), 2);
        assert_eq!(rows[0].values[0].column_id, 0);
        assert_eq!(
            rows[0].values[0].value,
            Some(crate::proto::column_value::Value::F64Val(1.0))
        );
        assert_eq!(rows[0].values[1].column_id, 1);
        assert_eq!(
            rows[0].values[1].value,
            Some(crate::proto::column_value::Value::I64Val(10))
        );
        // 行 2：ts 与值保真。
        assert_eq!(rows[2].ts_ns, 300);
        assert_eq!(
            rows[2].values[1].value,
            Some(crate::proto::column_value::Value::I64Val(30))
        );
    }

    /// presence=false 的单元格在 point_batch_to_rows 必须被跳过（缺列 = 不在 values），
    /// 与 convert_to_engine_batch 的稀疏语义一致。
    #[test]
    fn test_point_batch_to_rows_skips_null_cells() {
        let mut batch = PointBatch::new(5);
        batch.timestamps = vec![100, 200];
        batch.columns = vec![ColVec::F64(vec![1.0, 0.0])];
        batch.column_names = vec!["col_0".into()];
        batch.column_ids = vec![0];
        batch.column_presence = vec![vec![true, false]];
        batch.qualities = vec![0xC0; 2];
        batch.flags = vec![0; 2];

        let rows = point_batch_to_rows(&batch, "t5");
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].values.len(), 1);
        assert_eq!(
            rows[0].values[0].value,
            Some(crate::proto::column_value::Value::F64Val(1.0))
        );
        assert!(
            rows[1].values.is_empty(),
            "null cell 必须被跳过，不得误写 0: {:?}",
            rows[1].values
        );
    }

    /// 打包辅助：3 个 PointRow 在字节预算内 → 1 条 MultiTableBatch 内 3 个 point。
    #[test]
    fn test_pack_rows_into_batches_single_batch() {
        let mk = |id: u32| {
            let mut b = PointBatch::new(id);
            b.timestamps = vec![100];
            b.columns = vec![ColVec::F64(vec![id as f64])];
            b.column_names = vec!["val".into()];
            b.qualities = vec![0xC0; 1];
            b.flags = vec![0; 1];
            point_batch_to_rows(&b, &format!("table_{id}"))
        };
        let mut rows = Vec::new();
        for id in [7u32, 8, 9] {
            rows.extend(mk(id));
        }
        let batches = pack_rows_into_batches(rows, GRPC_LIST_MAX_BYTES);
        assert_eq!(batches.len(), 1, "3 个小 row 应打包进 1 条 batch");
        assert_eq!(batches[0].points.len(), 3);
        assert_eq!(batches[0].points[0].table_name, "table_7");
        assert_eq!(batches[0].points[2].table_name, "table_9");
    }

    /// T5 字节预算切分：单 row 均 < 预算、总和 > 预算 → 多条 batch，每条
    /// `encoded_len() ≤ 预算`，row 不丢不重排。
    #[test]
    fn test_pack_rows_into_batches_byte_budget_split() {
        use prost::Message;

        // 每个 row 用一个 ~500KB 的 String 值撑大，5 个 → 总 ~2.5MB > 2MiB。
        let big = "x".repeat(500_000);
        let mk = |id: u32| crate::proto::PointRow {
            table_name: format!("t{id}"),
            table_id: None,
            ts_ns: 0,
            server_ts_ns: None,
            values: vec![crate::proto::ColumnValue {
                column_id: 0,
                value: Some(crate::proto::column_value::Value::StrVal(big.clone())),
            }],
            quality: Some(0xC0),
            flags: Some(0),
            write_seq: None,
        };
        let rows: Vec<_> = (0..5).map(mk).collect();
        for r in &rows {
            assert!(r.encoded_len() < GRPC_LIST_MAX_BYTES, "单 row 应小于预算");
        }

        let batches = pack_rows_into_batches(rows, GRPC_LIST_MAX_BYTES);
        assert!(
            batches.len() > 1,
            "总和超预算应切成多条 batch，实际 {}",
            batches.len()
        );
        for b in &batches {
            assert!(
                b.encoded_len() <= GRPC_LIST_MAX_BYTES,
                "batch 超预算: {} > {}",
                b.encoded_len(),
                GRPC_LIST_MAX_BYTES
            );
        }

        // 不丢不重排：按序收集回填 table_name == t0..t4。
        let names: Vec<String> = batches
            .iter()
            .flat_map(|b| b.points.iter().map(|p| p.table_name.clone()))
            .collect();
        assert_eq!(names, vec!["t0", "t1", "t2", "t3", "t4"]);
    }

    /// QA 独立（D 节）：单个 row 的 `encoded_len` 已超 `max_bytes` 时，
    /// `pack_rows_into_batches` 仍把它单独放进一条 batch（不 panic、不丢、不重排）。
    #[test]
    fn qa_pack_single_oversized_row_own_batch() {
        use prost::Message;

        let big = "x".repeat(1000);
        let r = crate::proto::PointRow {
            table_name: "big".to_string(),
            table_id: None,
            ts_ns: 0,
            server_ts_ns: None,
            values: vec![crate::proto::ColumnValue {
                column_id: 0,
                value: Some(crate::proto::column_value::Value::StrVal(big)),
            }],
            quality: Some(0xC0),
            flags: Some(0),
            write_seq: None,
        };
        assert!(r.encoded_len() > 10, "precondition：单 row 必须超 max_bytes");

        let batches = pack_rows_into_batches(vec![r], 10);
        assert_eq!(
            batches.len(),
            1,
            "单个超限 row 必须单独成 1 条 batch，不丢不 panic"
        );
        assert_eq!(batches[0].points.len(), 1);
        assert_eq!(batches[0].points[0].table_name, "big");
    }

    /// 治碎片化正确性：presence=false 的单元格在 convert_to_engine_batch 必须被
    /// 跳过（不 emit 到 engine DataPoint），引擎落 presence=false，读回为 None，
    /// 绝不把缺失列误写成 0。
    #[test]
    fn test_convert_to_engine_batch_skips_null_cells() {
        let mut batch = PointBatch::new(5);
        batch.timestamps = vec![100, 200];
        // col_0 值 [1.0, 0.0]，但第 2 行 presence=false（缺失），值占位 0.0 不得写入。
        batch.columns = vec![ColVec::F64(vec![1.0, 0.0])];
        batch.column_names = vec!["col_0".into()];
        batch.column_ids = vec![0];
        batch.column_presence = vec![vec![true, false]];
        batch.qualities = vec![0xC0; 2];
        batch.flags = vec![0; 2];

        let engine_batch = convert_to_engine_batch(&batch);

        // 行 0：col_0 有值（1.0）。
        assert_eq!(engine_batch.points[0].values.len(), 1);
        assert_eq!(engine_batch.points[0].values[0].0, 0);
        assert_eq!(
            engine_batch.points[0].values[0].1,
            sensearcdb_engine::memtable::TypedValue::F64(1.0)
        );

        // 行 1：col_0 缺失（presence=false），必须被跳过，而非写成 0.0。
        assert!(
            engine_batch.points[1].values.is_empty(),
            "null cell must be skipped, got {:?}",
            engine_batch.points[1].values
        );
    }

    /// presence 位图为空（密集列 / 旧客户端）时，值必须全量 emit，不丢行。
    #[test]
    fn test_convert_to_engine_batch_dense_no_presence() {
        let mut batch = PointBatch::new(5);
        batch.timestamps = vec![100, 200];
        batch.columns = vec![ColVec::F64(vec![1.0, 2.0])];
        batch.column_names = vec!["col_0".into()];
        batch.column_ids = vec![0];
        batch.column_presence = Vec::new(); // 无 presence → 全密集
        batch.qualities = vec![0xC0; 2];
        batch.flags = vec![0; 2];

        let engine_batch = convert_to_engine_batch(&batch);
        assert_eq!(engine_batch.points.len(), 2);
        assert_eq!(engine_batch.points[0].values.len(), 1);
        assert_eq!(engine_batch.points[1].values.len(), 1);
        assert_eq!(
            engine_batch.points[1].values[0].1,
            sensearcdb_engine::memtable::TypedValue::F64(2.0)
        );
    }

    /// A minimal in-process writer that records the batches pushed by the
    /// gRPC `WriteService`, used to prove `GrpcWriter` really reaches the wire.
    struct CountingWriter {
        received: std::sync::atomic::AtomicUsize,
        rows: std::sync::atomic::AtomicUsize,
        /// `table_id`s of every received batch, in arrival order.
        table_ids: std::sync::Mutex<Vec<u32>>,
        /// `tenant_id`s of every received batch, in arrival order.
        tenant_ids: std::sync::Mutex<Vec<sensearcdb_common::TenantId>>,
    }

    impl CountingWriter {
        fn new() -> Self {
            Self {
                received: std::sync::atomic::AtomicUsize::new(0),
                rows: std::sync::atomic::AtomicUsize::new(0),
                table_ids: std::sync::Mutex::new(Vec::new()),
                tenant_ids: std::sync::Mutex::new(Vec::new()),
            }
        }
    }

    #[async_trait]
    impl EngineWriter for CountingWriter {
        async fn write_batch(&self, batch: PointBatch) -> Result<WriteReceipt, EngineWriteError> {
            self.received
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            self.rows
                .fetch_add(batch.row_count(), std::sync::atomic::Ordering::SeqCst);
            self.table_ids.lock().unwrap().push(batch.table_id);
            self.tenant_ids.lock().unwrap().push(batch.tenant_id);
            Ok(WriteReceipt::accepted(0, batch.row_count() as u32, 0))
        }

        /// 行式重构后服务端 per-message 路径统一走 `write_engine_batch_retry`
        /// （`write_service.rs::ingest_one`），不再经 `write_batch`。这里记录：
        /// `received` = 收到的消息条数（1 条 MultiTableBatch = 1 次调用）、
        /// `rows` = 总点数（PointRow 数）、逐点 `table_id` / `tenant_id` 保真。
        async fn write_engine_batch_retry(
            &self,
            batch: sensearcdb_engine::engine::PointBatch,
        ) -> Result<WriteReceipt, EngineWriteError> {
            let n = batch.points.len();
            self.received
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            self.rows.fetch_add(n, std::sync::atomic::Ordering::SeqCst);
            {
                let mut ids = self.table_ids.lock().unwrap();
                let mut tenants = self.tenant_ids.lock().unwrap();
                for p in &batch.points {
                    ids.push(p.table_id);
                    tenants.push(p.tenant_id);
                }
            }
            Ok(WriteReceipt::accepted(0, n as u32, 0))
        }

        fn is_connected(&self) -> bool {
            true
        }

        /// 非 Direct（Remote）：这些 gRPC 往返测试专门验证「服务端非 Direct 时
        /// 仍走原逐条路径」的回归，Direct 攒批路径由 write_service 的专项测试覆盖。
        fn writer_type(&self) -> WriterType {
            WriterType::Remote
        }
    }

    /// A `table_name → table_id` resolver with a fixed, in-memory mapping,
    /// used to exercise the real server-side resolution path.
    struct StaticTableNameResolver {
        map: std::collections::HashMap<String, u32>,
    }

    impl crate::rest::write::TableNameResolver for StaticTableNameResolver {
        fn resolve(
            &self,
            _tenant_id: sensearcdb_common::TenantId,
            table_name: &str,
        ) -> Option<(u32, bool)> {
            self.map.get(table_name).map(|&id| (id, true))
        }
    }

    /// Start the real 5201 `WriteService` server on an ephemeral port and
    /// return `(addr, counting_writer, server_handle)`.
    async fn start_test_grpc_server() -> (
        std::net::SocketAddr,
        Arc<CountingWriter>,
        tokio::task::JoinHandle<()>,
    ) {
        start_test_grpc_server_with_resolver(None).await
    }

    /// Start the real 5201 `WriteService` server with an optional
    /// `table_name → table_id` resolver attached to the service.
    ///
    /// 默认注入固定 default-tenant resolver，保证「无认证的写往返」旧测试可继续
    /// 通过（服务端现在要求 tenant 可解析）。
    async fn start_test_grpc_server_with_resolver(
        resolver: Option<Arc<dyn crate::rest::write::TableNameResolver>>,
    ) -> (
        std::net::SocketAddr,
        Arc<CountingWriter>,
        tokio::task::JoinHandle<()>,
    ) {
        let tenant_resolver: crate::grpc::auth::GrpcTenantResolver =
            Arc::new(|_m| Some(sensearcdb_common::TenantId::DEFAULT));
        start_test_grpc_server_full(resolver, tenant_resolver).await
    }

    /// Start the real 5201 `WriteService` server with a fixed tenant resolver.
    async fn start_test_grpc_server_with_tenant_resolver(
        tenant_resolver: crate::grpc::auth::GrpcTenantResolver,
    ) -> (
        std::net::SocketAddr,
        Arc<CountingWriter>,
        tokio::task::JoinHandle<()>,
    ) {
        start_test_grpc_server_full(None, tenant_resolver).await
    }

    /// Shared server bootstrap for the writer tests.
    async fn start_test_grpc_server_full(
        resolver: Option<Arc<dyn crate::rest::write::TableNameResolver>>,
        tenant_resolver: crate::grpc::auth::GrpcTenantResolver,
    ) -> (
        std::net::SocketAddr,
        Arc<CountingWriter>,
        tokio::task::JoinHandle<()>,
    ) {
        use tonic::transport::Server;

        let counting = Arc::new(CountingWriter::new());
        let mut service = crate::grpc::write_service::GrpcWriteService::new(counting.clone())
            .with_tenant_resolver(tenant_resolver);
        if let Some(resolver) = resolver {
            service = service.with_resolver(resolver);
        }
        let svc = crate::proto::write_service_server::WriteServiceServer::new(service);

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        // Build an accept stream by hand: avoids the `net` feature gate on
        // `tokio_stream::wrappers::TcpListenerStream`.
        let incoming = futures::stream::unfold(listener, |listener| async move {
            match listener.accept().await {
                Ok((stream, _peer)) => Some((Ok::<_, std::io::Error>(stream), listener)),
                Err(e) => Some((Err(e), listener)),
            }
        });

        let handle = tokio::spawn(async move {
            Server::builder()
                .add_service(svc)
                .serve_with_incoming(incoming)
                .await
                .unwrap();
        });

        (addr, counting, handle)
    }

    #[tokio::test]
    async fn test_grpc_writer_real_push() {
        let (addr, counting, server_handle) = start_test_grpc_server().await;

        let endpoint = format!("http://{}", addr);
        let resolver: Arc<dyn Fn(u32) -> Option<String> + Send + Sync> =
            Arc::new(|id| Some(format!("table_{}", id)));
        let writer = GrpcWriter::new(endpoint).with_table_name_resolver(resolver);
        writer.connect().await.unwrap();
        assert!(writer.is_connected());

        let mut batch = PointBatch::new(7);
        batch.timestamps = vec![100, 200];
        batch.columns = vec![ColVec::F64(vec![1.0, 2.0])];
        batch.column_names = vec!["val".into()];
        batch.qualities = vec![0xC0; 2];
        batch.flags = vec![0; 2];

        let receipt = writer.write_batch(batch).await.unwrap();
        assert!(receipt.is_fully_accepted());
        assert_eq!(receipt.points_written, 2);

        // Give the server a moment to drain the stream, then assert it arrived.
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        assert_eq!(
            counting.received.load(std::sync::atomic::Ordering::SeqCst),
            1
        );
        assert_eq!(counting.rows.load(std::sync::atomic::Ordering::SeqCst), 2);

        server_handle.abort();
    }

    #[tokio::test]
    async fn test_grpc_writer_real_push_resolves_table_id() {
        use crate::rest::write::TableNameResolver;

        // Server resolves the wire `table_name` ("sensors") to a real id (42),
        // proving the `table_name → table_id` path is exercised end-to-end.
        let resolver: Arc<dyn TableNameResolver> = Arc::new(StaticTableNameResolver {
            map: [("sensors".to_string(), 42u32)].into_iter().collect(),
        });
        let (addr, counting, server_handle) =
            start_test_grpc_server_with_resolver(Some(resolver)).await;

        let endpoint = format!("http://{}", addr);
        // Client-side resolver: internal table_id 7 → wire table_name "sensors".
        let name_resolver: Arc<dyn Fn(u32) -> Option<String> + Send + Sync> =
            Arc::new(|_| Some("sensors".to_string()));
        let writer = GrpcWriter::new(endpoint).with_table_name_resolver(name_resolver);
        writer.connect().await.unwrap();
        assert!(writer.is_connected());

        let mut batch = PointBatch::new(7);
        batch.timestamps = vec![100];
        batch.columns = vec![ColVec::F64(vec![1.0])];
        batch.column_names = vec!["val".into()];
        batch.qualities = vec![0xC0; 1];
        batch.flags = vec![0; 1];

        let receipt = writer.write_batch(batch).await.unwrap();
        assert!(receipt.is_fully_accepted());

        // Give the server a moment to drain the stream, then assert the batch
        // arrived with the *resolved* table_id (42), not the fallback 0 or the
        // client's internal id 7.
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        assert_eq!(
            counting.received.load(std::sync::atomic::Ordering::SeqCst),
            1
        );
        let ids = counting.table_ids.lock().unwrap();
        assert_eq!(ids.as_slice(), &[42]);

        server_handle.abort();
    }

    #[tokio::test]
    async fn test_grpc_write_injects_tenant_from_resolver() {
        use sensearcdb_common::TenantId;

        // 服务端注入固定 tenant resolver → 写入的 batch 必须携带该 tenant。
        let tenant_resolver: crate::grpc::auth::GrpcTenantResolver =
            Arc::new(|_m| Some(TenantId(5)));
        let (addr, counting, server_handle) =
            start_test_grpc_server_with_tenant_resolver(tenant_resolver).await;

        let endpoint = format!("http://{}", addr);
        let name_resolver: Arc<dyn Fn(u32) -> Option<String> + Send + Sync> =
            Arc::new(|id| Some(format!("table_{}", id)));
        let writer = GrpcWriter::new(endpoint).with_table_name_resolver(name_resolver);
        writer.connect().await.unwrap();

        let mut batch = PointBatch::new(7);
        batch.timestamps = vec![100];
        batch.columns = vec![ColVec::F64(vec![1.0])];
        batch.column_names = vec!["val".into()];
        batch.qualities = vec![0xC0; 1];
        batch.flags = vec![0; 1];

        let receipt = writer.write_batch(batch).await.unwrap();
        assert!(receipt.is_fully_accepted());

        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        let tenants = counting.tenant_ids.lock().unwrap();
        assert_eq!(tenants.as_slice(), &[TenantId(5)]);

        server_handle.abort();
    }

    #[tokio::test]
    async fn test_grpc_writer_table_not_found() {
        let (addr, _counting, server_handle) = start_test_grpc_server().await;

        let endpoint = format!("http://{}", addr);
        let resolver: Arc<dyn Fn(u32) -> Option<String> + Send + Sync> = Arc::new(|_| None);
        let writer = GrpcWriter::new(endpoint).with_table_name_resolver(resolver);
        writer.connect().await.unwrap();
        assert!(writer.is_connected());

        let batch = PointBatch::new(42);
        let result = writer.write_batch(batch).await;
        assert!(matches!(result, Err(EngineWriteError::TableNotFound(42))));

        server_handle.abort();
    }

    /// 一个返回可配置 receipt 的 writer，用于验证 `write_batches` 默认实现聚合。
    struct AggregationWriter {
        /// 每个 batch 依次返回的 (points_written, points_failed, latency_ms)。
        receipts: std::sync::Mutex<std::collections::VecDeque<(u32, u32, u64)>>,
        calls: std::sync::atomic::AtomicUsize,
    }

    #[async_trait]
    impl EngineWriter for AggregationWriter {
        async fn write_batch(&self, _batch: PointBatch) -> Result<WriteReceipt, EngineWriteError> {
            self.calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            let (w, f, l) = self
                .receipts
                .lock()
                .unwrap()
                .pop_front()
                .unwrap_or((0, 0, 0));
            Ok(WriteReceipt {
                points_written: w,
                points_failed: f,
                latency_ms: l,
                write_status: if f == 0 {
                    WriteStatus::Accepted { wal_seq: 0 }
                } else {
                    WriteStatus::Rejected {
                        reason: "test failure".to_string(),
                    }
                },
            })
        }

        fn is_connected(&self) -> bool {
            true
        }

        fn writer_type(&self) -> WriterType {
            WriterType::Direct
        }
    }

    /// `write_batches` 默认实现：points_written / points_failed / latency_ms 求和，
    /// 且调用次数等于 batch 数。
    #[tokio::test]
    async fn test_write_batches_default_aggregation() {
        use std::collections::VecDeque;

        let writer = AggregationWriter {
            receipts: std::sync::Mutex::new(VecDeque::from([
                (2u32, 0u32, 5u64),
                (1, 3, 7),
                (4, 0, 2),
            ])),
            calls: std::sync::atomic::AtomicUsize::new(0),
        };

        let receipt = writer
            .write_batches(vec![
                PointBatch::new(1),
                PointBatch::new(2),
                PointBatch::new(3),
            ])
            .await
            .unwrap();

        assert_eq!(receipt.points_written, 7);
        assert_eq!(receipt.points_failed, 3);
        assert_eq!(receipt.latency_ms, 14);
        assert!(!receipt.is_fully_accepted());
        assert_eq!(
            writer.calls.load(std::sync::atomic::Ordering::SeqCst),
            3
        );
    }

    /// `send_batches`：N 个 PointBatch 的 rows 转置后打包进 1 条 `MultiTableBatch`
    /// （1 条消息内 N 个 PointRow），服务端逐行 decode 后收到 1 条消息、rows 等于
    /// 总和、逐点 table_id 保真（3 表 6 行，顺序 = 到达顺序）。
    #[tokio::test]
    async fn test_grpc_writer_send_batches_multi_message() {
        use crate::rest::write::TableNameResolver;

        // 服务端挂 table_name → table_id 解析器，让跨表行数可逐点断言（不串表）。
        let server_resolver: Arc<dyn TableNameResolver> = Arc::new(StaticTableNameResolver {
            map: [
                ("table_7".to_string(), 7u32),
                ("table_8".to_string(), 8),
                ("table_9".to_string(), 9),
            ]
            .into_iter()
            .collect(),
        });
        let (addr, counting, server_handle) =
            start_test_grpc_server_with_resolver(Some(server_resolver)).await;

        let endpoint = format!("http://{}", addr);
        let resolver: Arc<dyn Fn(u32) -> Option<String> + Send + Sync> =
            Arc::new(|id| Some(format!("table_{}", id)));
        let writer = GrpcWriter::new(endpoint).with_table_name_resolver(resolver);
        writer.connect().await.unwrap();
        assert!(writer.is_connected());

        let mk = |table_id: u32, n: usize| {
            let mut b = PointBatch::new(table_id);
            b.timestamps = (0..n).map(|i| 100 + i as i64).collect();
            b.columns = vec![ColVec::F64((0..n).map(|i| i as f64).collect())];
            b.column_names = vec!["val".into()];
            b.qualities = vec![0xC0; n];
            b.flags = vec![0; n];
            b
        };
        let batches = vec![mk(7, 1), mk(8, 2), mk(9, 3)];

        let receipt = writer.send_batches(batches).await.unwrap();
        assert!(receipt.is_fully_accepted());
        assert_eq!(receipt.points_written, 6);

        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        // 行式跨表：3 个 batch 的 6 行打包进 1 条 MultiTableBatch → 服务端收到 1 条消息。
        assert_eq!(
            counting.received.load(std::sync::atomic::Ordering::SeqCst),
            1
        );
        assert_eq!(counting.rows.load(std::sync::atomic::Ordering::SeqCst), 6);
        // 逐点 table_id 保真：3 表（7/8/9），顺序 = 到达顺序，不串表。
        let ids = counting.table_ids.lock().unwrap();
        assert_eq!(ids.as_slice(), &[7, 8, 8, 9, 9, 9]);

        server_handle.abort();
    }

    /// `send_batches`：任一 batch 表名解析失败 → `TableNotFound`（发流前即失败）。
    #[tokio::test]
    async fn test_grpc_writer_send_batches_table_not_found() {
        let (addr, _counting, server_handle) = start_test_grpc_server().await;

        let endpoint = format!("http://{}", addr);
        let resolver: Arc<dyn Fn(u32) -> Option<String> + Send + Sync> = Arc::new(|id| {
            if id == 99 {
                None
            } else {
                Some(format!("table_{}", id))
            }
        });
        let writer = GrpcWriter::new(endpoint).with_table_name_resolver(resolver);
        writer.connect().await.unwrap();

        let result = writer
            .send_batches(vec![PointBatch::new(7), PointBatch::new(99)])
            .await;
        assert!(matches!(result, Err(EngineWriteError::TableNotFound(99))));

        server_handle.abort();
    }

    // ───────────── QA 独立边界（fresh eyes，不采信工程师自测）─────────────

    /// QA 独立：`send_batches` 多批中某一批 table_name 解析失败时，必须在**发流前**
    /// 返回 `TableNotFound`，服务端收到 0 批（不能发一半流再报错，杜绝前缀已写歧义）。
    #[tokio::test]
    async fn qa_send_batches_partial_table_not_found_sends_zero_batches() {
        let (addr, counting, server_handle) = start_test_grpc_server().await;

        let endpoint = format!("http://{}", addr);
        // 只有 table_id 99 解析失败，其余成功。
        let resolver: Arc<dyn Fn(u32) -> Option<String> + Send + Sync> = Arc::new(|id| {
            if id == 99 {
                None
            } else {
                Some(format!("table_{}", id))
            }
        });
        let writer = GrpcWriter::new(endpoint).with_table_name_resolver(resolver);
        writer.connect().await.unwrap();

        // 前两批可解析，第三批 table_id=99 解析失败。
        let result = writer
            .send_batches(vec![
                PointBatch::new(7),
                PointBatch::new(8),
                PointBatch::new(99),
            ])
            .await;
        assert!(matches!(result, Err(EngineWriteError::TableNotFound(99))));

        // 关键：解析失败发生在发流之前，服务端必须收到 0 批。
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
        assert_eq!(
            counting.received.load(std::sync::atomic::Ordering::SeqCst),
            0,
            "解析失败必须在发流前返回，服务端不得收到任何批次"
        );

        server_handle.abort();
    }

    /// QA 独立：`write_batches` 默认实现空 vec → Ok(points_written=0)，零调用、零失败。
    #[tokio::test]
    async fn qa_write_batches_empty_vec_ok_zero() {
        let writer = AggregationWriter {
            receipts: std::sync::Mutex::new(std::collections::VecDeque::new()),
            calls: std::sync::atomic::AtomicUsize::new(0),
        };

        let receipt = writer.write_batches(vec![]).await.unwrap();
        assert_eq!(receipt.points_written, 0);
        assert_eq!(receipt.points_failed, 0);
        assert_eq!(receipt.latency_ms, 0);
        assert!(receipt.is_fully_accepted());
        assert_eq!(
            writer.calls.load(std::sync::atomic::Ordering::SeqCst),
            0,
            "空 vec 不得调用任何 write_batch"
        );
    }

    /// QA 独立：`write_batches` 默认实现 fail-fast——首个 Err 立即返回，后续批不被写。
    struct FailFastWriter {
        calls: std::sync::atomic::AtomicUsize,
    }

    #[async_trait]
    impl EngineWriter for FailFastWriter {
        async fn write_batch(&self, _batch: PointBatch) -> Result<WriteReceipt, EngineWriteError> {
            self.calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            Err(EngineWriteError::NotReachable)
        }

        fn is_connected(&self) -> bool {
            true
        }

        fn writer_type(&self) -> WriterType {
            WriterType::Direct
        }
    }

    #[tokio::test]
    async fn qa_write_batches_fail_fast_stops_after_first_err() {
        let writer = FailFastWriter {
            calls: std::sync::atomic::AtomicUsize::new(0),
        };

        let result = writer
            .write_batches(vec![
                PointBatch::new(1),
                PointBatch::new(2),
                PointBatch::new(3),
            ])
            .await;
        assert!(matches!(result, Err(EngineWriteError::NotReachable)));
        assert_eq!(
            writer.calls.load(std::sync::atomic::Ordering::SeqCst),
            1,
            "fail-fast：首个 Err 后必须停止，后续两批不得被写"
        );
    }

    /// QA 独立：`write_batches` 默认实现用真实 `CountingWriter` 聚合 3 批（row 1/2/3），
    /// 求和 points_written=6，且逐批调用（calls=3）。
    #[tokio::test]
    async fn qa_write_batches_default_aggregates_counts_via_counting_writer() {
        let writer = CountingWriter::new();
        let mk = |table_id: u32, n: usize| {
            let mut b = PointBatch::new(table_id);
            b.timestamps = vec![100i64; n];
            b.columns = vec![ColVec::F64(vec![1.0; n])];
            b.column_names = vec!["val".into()];
            b.qualities = vec![0xC0; n];
            b.flags = vec![0; n];
            b
        };

        let receipt = writer
            .write_batches(vec![mk(1, 1), mk(2, 2), mk(3, 3)])
            .await
            .unwrap();
        assert_eq!(receipt.points_written, 6);
        assert_eq!(receipt.points_failed, 0);
        assert!(receipt.is_fully_accepted());
        assert_eq!(
            writer.received.load(std::sync::atomic::Ordering::SeqCst),
            3,
            "默认实现应逐批调用 write_batch 共 3 次"
        );
    }

    /// QA 独立：merged 路径（`write_engine_batch_retry`）端到端不丢点——背压排空后
    /// 该批被写入且可查询读回。用独立 table 78 避开制造 stall 的 table 7。
    #[tokio::test]
    async fn qa_write_engine_batch_retry_no_loss_queryable() {
        use sensearcdb_common::{AppConfig, ConfigHandle, TenantId};
        use sensearcdb_engine::engine::{DataPoint, Engine, PointBatch as EnginePointBatch};

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

        let writer = EngineDirectWriter::new(engine.clone());

        // merged 路径：直接给已合并的 engine batch（table 78，1 点 ts=9999 value=42.0）。
        let merged = EnginePointBatch {
            points: vec![engine_point(78, 9999, 42.0)],
        };
        let receipt = writer
            .write_engine_batch_retry(merged)
            .await
            .expect("merged 背压排空后应成功写入");
        assert_eq!(receipt.points_written, 1);

        // 读回：table 78 在 [9999,9999] 应恰好 1 点、值 42.0（不丢不重）。
        let (cols, tss) = engine
            .query_table_columns(TenantId::DEFAULT, 78, &[0], 9999, 9999)
            .await
            .unwrap();
        assert_eq!(tss, vec![9999]);
        assert_eq!(cols[0], vec![Some(TypedValue::F64(42.0))]);
    }

    /// Direct 服务端替身：覆写 `write_engine_batch_retry` 记录合并 points，
    /// 用于验证「真实 gRPC 线 + Direct 攒批」生产路径（偏差 3 闭环，fresh eyes）。
    struct DirectMergedWriter {
        merged_calls: std::sync::atomic::AtomicUsize,
        points: std::sync::Mutex<Vec<sensearcdb_engine::engine::DataPoint>>,
    }

    impl DirectMergedWriter {
        fn new() -> Self {
            Self {
                merged_calls: std::sync::atomic::AtomicUsize::new(0),
                points: std::sync::Mutex::new(Vec::new()),
            }
        }
    }

    #[async_trait]
    impl EngineWriter for DirectMergedWriter {
        async fn write_batch(&self, batch: PointBatch) -> Result<WriteReceipt, EngineWriteError> {
            Ok(WriteReceipt::accepted(0, batch.row_count() as u32, 0))
        }

        async fn write_engine_batch_retry(
            &self,
            batch: sensearcdb_engine::engine::PointBatch,
        ) -> Result<WriteReceipt, EngineWriteError> {
            let n = batch.points.len();
            self.merged_calls
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
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

    /// 启动真实 5201 WriteService，服务端 writer 为 `DirectMergedWriter`（Direct 攒批路径）。
    async fn start_test_grpc_server_direct(
        resolver: Option<Arc<dyn crate::rest::write::TableNameResolver>>,
    ) -> (
        std::net::SocketAddr,
        Arc<DirectMergedWriter>,
        tokio::task::JoinHandle<()>,
    ) {
        use tonic::transport::Server;

        let writer = Arc::new(DirectMergedWriter::new());
        let tenant_resolver: crate::grpc::auth::GrpcTenantResolver =
            Arc::new(|_m| Some(sensearcdb_common::TenantId::DEFAULT));
        let mut service = crate::grpc::write_service::GrpcWriteService::new(writer.clone())
            .with_tenant_resolver(tenant_resolver);
        if let Some(r) = resolver {
            service = service.with_resolver(r);
        }
        let svc = crate::proto::write_service_server::WriteServiceServer::new(service);

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let incoming = futures::stream::unfold(listener, |listener| async move {
            match listener.accept().await {
                Ok((stream, _peer)) => Some((Ok::<_, std::io::Error>(stream), listener)),
                Err(e) => Some((Err(e), listener)),
            }
        });
        let handle = tokio::spawn(async move {
            Server::builder()
                .add_service(svc)
                .serve_with_incoming(incoming)
                .await
                .unwrap();
        });

        (addr, writer, handle)
    }

    /// QA 独立：偏差 3 闭环——Direct 服务端 + 真实 gRPC 往返 + 攒批。
    /// GrpcWriter 客户端 1 次 `send_batches`(3 批) = 1 条 stream 3 条消息，
    /// 服务端 Direct 攒批合并为 1 次 `write_engine_batch_retry`，逐点 table_id 正确。
    #[tokio::test]
    async fn qa_grpc_direct_server_batches_merge() {
        let resolver: Arc<dyn crate::rest::write::TableNameResolver> =
            Arc::new(StaticTableNameResolver {
                map: [
                    ("t1".to_string(), 1u32),
                    ("t2".to_string(), 2),
                    ("t3".to_string(), 3),
                ]
                .into_iter()
                .collect(),
            });
        let (addr, merged, server_handle) = start_test_grpc_server_direct(Some(resolver)).await;

        let endpoint = format!("http://{}", addr);
        let name_resolver: Arc<dyn Fn(u32) -> Option<String> + Send + Sync> =
            Arc::new(|id| Some(format!("t{}", id)));
        let writer = GrpcWriter::new(endpoint).with_table_name_resolver(name_resolver);
        writer.connect().await.unwrap();
        assert!(writer.is_connected());

        let mut batches = Vec::new();
        for (id, ts, v) in [(1u32, 100i64, 1.0f64), (2, 200, 2.0), (3, 300, 3.0)] {
            let mut b = PointBatch::new(id);
            b.timestamps = vec![ts];
            b.columns = vec![ColVec::F64(vec![v])];
            b.column_names = vec!["v".into()];
            b.qualities = vec![0xC0; 1];
            b.flags = vec![0; 1];
            batches.push(b);
        }
        let receipt = writer.send_batches(batches).await.unwrap();
        assert!(receipt.is_fully_accepted());
        assert_eq!(receipt.points_written, 3);

        // 服务端 Direct 攒批：3 条消息合并为 1 次 write_engine_batch_retry。
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        assert_eq!(
            merged.merged_calls.load(std::sync::atomic::Ordering::SeqCst),
            1,
            "Direct 服务端应把 3 条消息攒批合并为 1 次引擎级写入"
        );
        let points = merged.points.lock().unwrap();
        assert_eq!(points.len(), 3);
        assert_eq!(points[0].table_id, 1);
        assert_eq!(points[0].ts, 100);
        assert_eq!(points[0].values[0].1, TypedValue::F64(1.0));
        assert_eq!(points[1].table_id, 2);
        assert_eq!(points[1].ts, 200);
        assert_eq!(points[2].table_id, 3);
        assert_eq!(points[2].ts, 300);

        server_handle.abort();
    }
}
