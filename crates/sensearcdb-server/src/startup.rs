//! SenseArcDB 单机版启动序列（Open Core 装配层）。
//!
//! 管理开源子系统的初始化顺序：
//! Schema → Engine → Snapshot → Replay → Partition → CQ → Stream → Derived → Observe。
//!
//! 与完整版差异：
//! - 无 TDE / Lifecycle / Security / 集群 / 分片 / 边缘 / 事件 / 告警 / AI 五引擎 /
//!   通知 / WS 网关 / 采集器调度。
//! - Engine 以 `encryptor = None` 构造（TDE 关闭）。
//! - 配额闸口不接线（单机无限写）。

use std::sync::Arc;

use anyhow::Context;
use arc_swap::ArcSwap;
use prometheus::Registry;

use async_trait::async_trait;

use sensearcdb_common::{
    agg_result_type, AggFunction, ColumnRef, ConfigHandle, DataType, Durability, TenantId,
    TimestampNanos, TypedValue,
};
use sensearcdb_engine::encryption::SharedEncryptor;
use sensearcdb_engine::engine::{DurabilityResolver, Engine};
use sensearcdb_engine::wal::{WalReaper, DEFAULT_WAL_RECYCLE_KEEP_SEGMENTS};
use sensearcdb_partition::PartitionManager;
use sensearcdb_schema::{
    Catalog, ColumnDef, SchemaEvent, SchemaEventBus, SchemaManager, SchemaMirror, TableInfo,
    TableType,
};
use sensearcdb_snapshot::{
    read_snapshot_watermark, recover, tag_metadata::SchemaMirrorRef, OwnedPendingPoint,
    RecoveryConfig, RecoveryError, ReplayDerived, ReplayDerivedOutput, ReplayDerivedPoint,
    ReplayEngine, SacDataRow, SacReaderError, SacRecoveryReader, SacRecoveryReaderRef, SacScanRow,
    SnapshotCheckpointer, SnapshotFlusher, SnapshotMetrics, SnapshotPendingBuffer, SnapshotStore,
    SnapshotSubscriptionManager, TagMetadataStore, TypedSnapshotValue, WalReplayReader,
    WalReplayReaderRef, WalReplayRecord, WalWatermarkProvider,
};

// ─── Wave 3 ───
use sensearcdb_cq::definition::CqGranularity;
use sensearcdb_cq::engine::{CqQueryExecutor, CqWriteTarget, WriteEvent as CqWriteEvent};
use sensearcdb_cq::layout::{aux_column_id, aux_column_type, CqAuxColumn};
use sensearcdb_cq::CqDefinitionStore;
use sensearcdb_cq::CqEngine;
use sensearcdb_derived::{
    evaluate_series, DerivedFlusher, DerivedTagEngine, DerivedTagNode, DerivedTagStore, SchemaAccess,
    SnapshotRead, WriteSink,
};
use sensearcdb_stream::{
    ProjectionColumn, StreamCatalog, StreamComputeEngine, StreamTask, StreamTaskStore, TaskId,
};

// ─── Wave 6 ───
use sensearcdb_deploy::bootstrap::BootstrapArgs;
use sensearcdb_deploy::{Bootstrap, ProcessLock};
use sensearcdb_observe::ObserveEngine;

/// 应用全局状态——启动后各开源子系统句柄均通过此结构访问。
///
/// 部分字段（config/replay_engine/snapshot_registry/engine_metrics/tenant_manager/
/// quota_store/observe_engine/process_lock）在单机版路由面暂未消费，但按 Open Core
/// 装配约定保留句柄（配额/租户生命周期等后续步骤接线），故整结构标记 `allow(dead_code)`。
#[derive(Clone)]
#[allow(dead_code)]
pub struct AppState {
    /// 全局配置句柄（支持热加载）。
    pub config: ConfigHandle,
    /// Schema 管理器。
    pub schema_mgr: Arc<SchemaManager>,
    /// Schema 内存镜像（供接入层 table_name→table_id 解析）。
    pub schema_mirror: Arc<SchemaMirror>,
    /// 存储引擎。
    pub engine: Arc<Engine>,
    /// 实时快照存储（ArcSwap 单一真相）。
    pub snapshot_store: Arc<ArcSwap<SnapshotStore>>,
    /// 后台快照检查点（定期把 SnapshotStore 持久化为 snapshot.bin）。
    pub snapshot_checkpointer: Arc<SnapshotCheckpointer>,
    /// 历史回放引擎。
    pub replay_engine: Arc<ReplayEngine>,
    /// Snapshot Prometheus 指标注册表。
    pub snapshot_registry: Registry,
    /// 引擎 Prometheus 指标。
    pub engine_metrics: sensearcdb_engine::metrics::EngineMetrics,
    /// 分区管理器。
    pub partition_manager: Arc<PartitionManager>,
    /// CQ 引擎（定时聚合加速）。
    pub cq_engine: Option<Arc<tokio::sync::RwLock<CqEngine>>>,
    /// 流计算引擎（数据驱动窗口聚合）。
    pub stream_engine: Arc<tokio::sync::RwLock<StreamComputeEngine>>,
    /// 派生标签引擎共享句柄（构造期一次性取出的内层 `Arc<DerivedTagEngine>`）。
    pub derived_engine_arc: Option<Arc<DerivedTagEngine>>,
    /// 租户管理器。
    pub tenant_manager: Arc<sensearcdb_schema::TenantManager>,
    /// 数据面配额存储（单机版不接线闸口，仅保留句柄）。
    pub quota_store: Arc<sensearcdb_schema::TenantQuotaStore>,
    /// 可观测性引擎（5101 metrics / health / slow_query）。
    pub observe_engine: Option<Arc<ObserveEngine>>,
    /// 进程锁（启动时获取，随进程生命周期持有）。
    pub process_lock: Arc<ProcessLock>,
    /// 快照订阅管理器（5206 SubscribeSnapshots 用）。
    pub snapshot_subscriptions: Arc<SnapshotSubscriptionManager>,
    /// 分区调度器优雅关闭 sender（持有它使 oneshot 通道不关闭，scheduler 保持运行；
    /// 用 `Arc` 包装以兼容 `AppState` 的 `Clone` derive）。
    pub partition_scheduler_shutdown: Option<Arc<tokio::sync::oneshot::Sender<()>>>,
}

// ─── SchemaMirrorDurabilityResolver ────────────────────────────────────────

/// 将 `SchemaMirror` 桥接为 engine 的 `DurabilityResolver`（表级持久化强度解析）。
struct SchemaMirrorDurabilityResolver {
    mirror: Arc<SchemaMirror>,
}

impl DurabilityResolver for SchemaMirrorDurabilityResolver {
    fn resolve(&self, table_id: u32) -> Durability {
        self.mirror
            .get_table(table_id)
            .map(|t| t.durability)
            .unwrap_or(Durability::Group)
    }
}

// ─── SchemaMirrorAdapter ────────────────────────────────────────────────────

/// 将 `SchemaMirror` 桥接为 `SchemaMirrorRef` trait 的实现。
struct SchemaMirrorAdapter {
    mirror: Arc<SchemaMirror>,
}

impl SchemaMirrorRef for SchemaMirrorAdapter {
    fn get_table_name(&self, table_id: u32) -> Option<String> {
        self.mirror
            .get_table(table_id)
            .map(|t| t.table_name.clone())
    }

    fn get_table_description(&self, table_id: u32) -> Option<String> {
        self.mirror
            .get_table(table_id)
            .and_then(|t| t.description.clone())
    }

    fn get_range_low(&self, table_id: u32, column_id: u16) -> f64 {
        self.mirror
            .get_table(table_id)
            .and_then(|t| {
                t.columns
                    .iter()
                    .find(|c| c.column_id == column_id)
                    .map(|c| c.range_low)
            })
            .unwrap_or(0.0)
    }

    fn get_range_high(&self, table_id: u32, column_id: u16) -> f64 {
        self.mirror
            .get_table(table_id)
            .and_then(|t| {
                t.columns
                    .iter()
                    .find(|c| c.column_id == column_id)
                    .map(|c| c.range_high)
            })
            .unwrap_or(100.0)
    }

    fn get_instrument_range_low(&self, table_id: u32, column_id: u16) -> Option<f64> {
        self.mirror.get_table(table_id).and_then(|t| {
            t.columns
                .iter()
                .find(|c| c.column_id == column_id)
                .and_then(|c| c.instrument_range_low)
        })
    }

    fn get_instrument_range_high(&self, table_id: u32, column_id: u16) -> Option<f64> {
        self.mirror.get_table(table_id).and_then(|t| {
            t.columns
                .iter()
                .find(|c| c.column_id == column_id)
                .and_then(|c| c.instrument_range_high)
        })
    }

    fn get_unit(&self, table_id: u32, column_id: u16) -> Option<String> {
        self.mirror.get_table(table_id).and_then(|t| {
            t.columns
                .iter()
                .find(|c| c.column_id == column_id)
                .and_then(|c| c.unit.clone())
        })
    }

    fn get_display_digits(&self, table_id: u32, column_id: u16) -> u8 {
        self.mirror
            .get_table(table_id)
            .and_then(|t| {
                t.columns
                    .iter()
                    .find(|c| c.column_id == column_id)
                    .map(|c| c.display_digits as u8)
            })
            .unwrap_or(2)
    }

    fn get_data_type(&self, table_id: u32, column_id: u16) -> DataType {
        self.mirror
            .get_table(table_id)
            .and_then(|t| {
                t.columns
                    .iter()
                    .find(|c| c.column_id == column_id)
                    .map(|c| c.data_type)
            })
            .unwrap_or(DataType::F64)
    }

    fn is_enabled(&self, table_id: u32) -> bool {
        self.mirror
            .get_table(table_id)
            .map(|t| t.enabled)
            .unwrap_or(false)
    }
}

// ─── SacReaderAdapter ────────────────────────────────────────────────────────

/// 将 engine crate 的 `SacReader` 适配为 snapshot crate 的 `SacRangeReader` trait。
struct SacReaderAdapter {
    sac_dir: String,
    encryptor: SharedEncryptor,
}

impl sensearcdb_snapshot::SacRangeReader for SacReaderAdapter {
    fn read_range(
        &self,
        tenant_id: sensearcdb_common::TenantId,
        table_id: u32,
        start_ts: i64,
        end_ts: i64,
    ) -> Result<Vec<SacDataRow>, SacReaderError> {
        use sensearcdb_engine::sac::SacReader;
        use std::path::Path;

        // SAC v9 扁平布局：`{sac_dir}/{tenant_id}/{partition_ns}_{level}_{seq}.sac`。
        let tenant_dir = Path::new(&self.sac_dir).join(tenant_id.as_u32().to_string());
        if !tenant_dir.exists() {
            return Ok(Vec::new());
        }

        let mut all_rows: Vec<SacDataRow> = Vec::new();

        let files = std::fs::read_dir(&tenant_dir).map_err(|e| {
            SacReaderError::ReadError(format!(
                "failed to read tenant dir '{}': {}",
                tenant_dir.display(),
                e
            ))
        })?;

        for file_entry in files {
            let file_entry = file_entry
                .map_err(|e| SacReaderError::ReadError(format!("file entry error: {}", e)))?;
            let file_path = file_entry.path();
            if file_path.extension().and_then(|ext| ext.to_str()) != Some("sac") {
                continue;
            }

            match SacReader::read_range(
                &file_path,
                tenant_id,
                table_id,
                start_ts,
                end_ts,
                &[],
                self.encryptor.clone(),
            ) {
                Ok(rows) => {
                    for row in rows {
                        let value = row.values.first().map(|(_, v)| v.as_f64()).unwrap_or(0.0);
                        let quality = row.qualities.first().map(|(_, q)| *q).unwrap_or(0);
                        all_rows.push(SacDataRow {
                            tenant_id,
                            ts: row.ts,
                            value,
                            quality,
                            server_ts: row.ts,
                            is_derived: false,
                            values: row.values,
                            qualities: row.qualities,
                        });
                    }
                }
                Err(e) => {
                    tracing::warn!(
                        file = %file_path.display(),
                        error = %e,
                        "failed to read SAC file, skipping"
                    );
                }
            }
        }

        all_rows.sort_by_key(|r| r.ts);
        Ok(all_rows)
    }
}

// ─── 崩溃恢复适配器 ─────────────────────────────────────────────────────────

/// 将 engine 的 SAC 文件适配为 snapshot 恢复用的 `SacRecoveryReader`。
struct EngineSacRecoveryReader {
    sac_dir: String,
    encryptor: SharedEncryptor,
}

impl SacRecoveryReader for EngineSacRecoveryReader {
    fn scan_all(&self) -> Result<Vec<SacScanRow>, RecoveryError> {
        use sensearcdb_engine::sac::reader::SacRowIter;
        let mut rows: Vec<SacScanRow> = Vec::new();

        let root = std::path::Path::new(&self.sac_dir);
        if !root.exists() {
            return Ok(rows);
        }

        for tenant_entry in std::fs::read_dir(root)? {
            let tenant_dir = tenant_entry?.path();
            if !tenant_dir.is_dir() {
                continue;
            }
            let tenant_id: u32 = match tenant_dir
                .file_name()
                .and_then(|s| s.to_str())
                .and_then(|s| s.parse().ok())
            {
                Some(id) => id,
                None => continue,
            };

            for file_entry in std::fs::read_dir(&tenant_dir)? {
                let file_path = file_entry?.path();
                if file_path.extension().and_then(|e| e.to_str()) != Some("sac") {
                    continue;
                }
                match SacRowIter::new_all(
                    &file_path,
                    sensearcdb_common::TenantId(tenant_id),
                    i64::MIN,
                    i64::MAX,
                    &[],
                    self.encryptor.clone(),
                ) {
                    Ok(iter) => {
                        for r in iter {
                            if let Ok(r) = r {
                                rows.push(SacScanRow {
                                    tenant_id: sensearcdb_common::TenantId(tenant_id),
                                    table_id: r.table_id,
                                    ts: r.ts,
                                    server_ts: r.ts,
                                    values: r.values,
                                    qualities: r.qualities,
                                });
                            }
                        }
                    }
                    Err(e) => {
                        tracing::warn!(
                            file = %file_path.display(),
                            error = %e,
                            "failed to open SAC file for recovery scan, skipping"
                        );
                    }
                }
            }
        }

        Ok(rows)
    }
}

/// 将 engine 的 WAL 适配为 snapshot 恢复用的 `WalReplayReader`。
struct EngineWalReplayReader {
    wal_dir: String,
    encryptor: SharedEncryptor,
}

impl WalReplayReader for EngineWalReplayReader {
    fn read_after(&self, after_seq: u64) -> Result<Vec<WalReplayRecord>, RecoveryError> {
        use sensearcdb_engine::wal::WalReader;
        let reader = WalReader::new_with_encryptor(&self.wal_dir, self.encryptor.clone());
        let records = reader
            .replay_after(after_seq, u32::MAX)
            .map_err(|e| RecoveryError::Other(format!("wal replay: {}", e)))?;

        let mut out: Vec<WalReplayRecord> = Vec::new();
        for rec in records {
            for point in &rec.points {
                out.push(WalReplayRecord {
                    tenant_id: point.tenant_id,
                    table_id: point.table_id,
                    ts: point.ts,
                    server_ts: point.server_ts,
                    seq: rec.raft_index,
                    values: point.values.clone(),
                    qualities: point.qualities.clone(),
                    schema_version: point.schema_version,
                });
            }
        }
        out.sort_by_key(|r| r.seq);
        Ok(out)
    }
}

/// 将 engine 的 WAL 水位适配为 snapshot crate 的 `WalWatermarkProvider` trait。
struct EngineWalWatermarkProvider {
    engine: Arc<Engine>,
}

impl WalWatermarkProvider for EngineWalWatermarkProvider {
    fn current_watermark(&self) -> u64 {
        self.engine.wal.current_watermark()
    }
}

// ─── Wave 2 Adapters ────────────────────────────────────────────────────────

/// 将 `SchemaMirror` 桥接为 ingest 的 `TableNameResolver`（GAP2-ING-02）。
pub struct SchemaTableResolver {
    mirror: Arc<SchemaMirror>,
}

impl SchemaTableResolver {
    pub fn new(mirror: Arc<SchemaMirror>) -> Self {
        Self { mirror }
    }
}

impl sensearcdb_ingest::rest::write::TableNameResolver for SchemaTableResolver {
    fn resolve(
        &self,
        tenant_id: sensearcdb_common::TenantId,
        table_name: &str,
    ) -> Option<(u32, bool)> {
        let table = self.mirror.get_table_by_name(tenant_id, table_name)?;
        let writable = table.enabled && table.deleted_at.is_none();
        Some((table.table_id, writable))
    }
}

/// 写入路径分区路由 writer（GAP2-PAR-01）。
pub struct PartitionRoutingWriter {
    inner: sensearcdb_ingest::EngineDirectWriter,
    partitions: Arc<PartitionManager>,
}

impl PartitionRoutingWriter {
    pub fn new(engine: Arc<Engine>, partitions: Arc<PartitionManager>) -> Self {
        Self {
            inner: sensearcdb_ingest::EngineDirectWriter::new(engine),
            partitions,
        }
    }
}

#[async_trait]
impl sensearcdb_ingest::EngineWriter for PartitionRoutingWriter {
    async fn write_batch(
        &self,
        batch: sensearcdb_ingest::PointBatch,
    ) -> Result<sensearcdb_ingest::WriteReceipt, sensearcdb_ingest::EngineWriteError> {
        for ts in &batch.timestamps {
            if let Err(e) = self.partitions.route_to_partition(batch.table_id, *ts) {
                tracing::warn!(
                    table_id = batch.table_id,
                    ts = *ts,
                    error = %e,
                    "write rejected by partition routing"
                );
                return Err(sensearcdb_ingest::EngineWriteError::Rejected(format!(
                    "partition route: {}",
                    e
                )));
            }
        }
        self.inner.write_batch(batch).await
    }

    fn is_connected(&self) -> bool {
        true
    }

    fn writer_type(&self) -> sensearcdb_ingest::WriterType {
        sensearcdb_ingest::WriterType::Direct
    }
}

// ─── Wave 3 Adapters ────────────────────────────────────────────────────────

/// StreamSink adapter: writes stream window results via Engine::write_batch().
struct EngineStreamSink {
    engine: Arc<Engine>,
    mirror: Arc<SchemaMirror>,
}

impl sensearcdb_stream::StreamSink for EngineStreamSink {
    fn write_batch(
        &self,
        target_table_id: u32,
        rows: &[(i64, Vec<TypedValue>)],
    ) -> Result<(), String> {
        use sensearcdb_engine::engine::{DataPoint, PointBatch};
        let now_ns = chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0);
        let tenant_id = self
            .mirror
            .get_table(target_table_id)
            .map(|t| t.tenant_id)
            .unwrap_or_else(|| {
                tracing::warn!(
                    table_id = target_table_id,
                    "stream sink mirror miss; fallback DEFAULT tenant"
                );
                TenantId::DEFAULT
            });
        let points: Vec<DataPoint> = rows
            .iter()
            .map(|(ts_ms, values)| {
                let ts_ns = ts_ms * 1_000_000;
                DataPoint {
                    table_id: target_table_id,
                    tenant_id,
                    ts: ts_ns,
                    server_ts: now_ns,
                    values: values
                        .iter()
                        .enumerate()
                        .map(|(i, tv)| (i as u16, tv.clone()))
                        .collect(),
                    qualities: values
                        .iter()
                        .enumerate()
                        .map(|(i, _)| (i as u16, 192u8))
                        .collect(),
                    schema_version: 1,
                    flags: 0,
                }
            })
            .collect();

        let batch = PointBatch { points };
        let engine = self.engine.clone();
        tokio::task::block_in_place(move || {
            let rt = tokio::runtime::Handle::current();
            rt.block_on(async move {
                engine
                    .write_batch(batch)
                    .await
                    .map_err(|e| format!("{}", e))?;
                Ok(())
            })
        })
    }
}

/// CQ 查询执行器 adapter：真调 `Engine::query_table_columns` 三源合并。
struct CqQueryExecutorAdapter {
    engine: Arc<Engine>,
    mirror: Arc<SchemaMirror>,
}

impl CqQueryExecutor for CqQueryExecutorAdapter {
    fn query_raw_with_ts(
        &self,
        table_id: u32,
        column_ids: &[u16],
        time_start: i64,
        time_end: i64,
    ) -> Result<(Vec<Vec<Option<TypedValue>>>, Vec<i64>), sensearcdb_common::SenseArcError> {
        let tenant_id = self
            .mirror
            .get_table(table_id)
            .map(|t| t.tenant_id)
            .unwrap_or_else(|| {
                tracing::warn!(
                    table_id,
                    "cq query adapter mirror miss; fallback DEFAULT tenant"
                );
                TenantId::DEFAULT
            });
        let engine = self.engine.clone();
        let cids = column_ids.to_vec();
        let result = tokio::task::block_in_place(move || {
            let rt = tokio::runtime::Handle::current();
            rt.block_on(async move {
                engine
                    .query_table_columns(tenant_id, table_id, &cids, time_start, time_end)
                    .await
            })
        })?;

        Ok(result)
    }
}

/// CQ 写入目标 adapter：将聚合结果经 `Engine::write_batch` 写入 CQ 目标表。
struct CqWriteTargetAdapter {
    engine: Arc<Engine>,
    mirror: Arc<SchemaMirror>,
}

impl CqWriteTarget for CqWriteTargetAdapter {
    fn write_batch(
        &self,
        table_id: u32,
        timestamp: i64,
        column_values: &[(u16, TypedValue)],
    ) -> Result<(), sensearcdb_common::SenseArcError> {
        use sensearcdb_engine::engine::{DataPoint, PointBatch};
        let now_ns = chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0);
        let tenant_id = self
            .mirror
            .get_table(table_id)
            .map(|t| t.tenant_id)
            .unwrap_or_else(|| {
                tracing::warn!(
                    table_id,
                    "cq write adapter mirror miss; fallback DEFAULT tenant"
                );
                TenantId::DEFAULT
            });
        let point = DataPoint {
            table_id,
            tenant_id,
            ts: timestamp,
            server_ts: now_ns,
            values: column_values.iter().map(|(c, v)| (*c, v.clone())).collect(),
            qualities: column_values.iter().map(|(c, _)| (*c, 0xC0u8)).collect(),
            schema_version: 1,
            flags: 0,
        };
        let batch = PointBatch {
            points: vec![point],
        };
        let engine = self.engine.clone();
        tokio::task::block_in_place(move || {
            let rt = tokio::runtime::Handle::current();
            rt.block_on(async move { engine.write_batch(batch).await.map(|_| ()) })
        })
    }
}

// ─── CqSchemaSynchronizer ─────────────────────────────────────────────────

/// 从列定义中提取数值列（CQ 可聚合列），返回 `(column_id, data_type)`。
fn numeric_column_ids(columns: &[ColumnDef]) -> Vec<(u16, DataType)> {
    columns
        .iter()
        .filter(|c| c.deleted_at.is_none())
        .filter(|c| {
            matches!(
                c.data_type,
                DataType::F64 | DataType::F32 | DataType::I64 | DataType::U64 | DataType::I32
            )
        })
        .map(|c| (c.column_id, c.data_type))
        .collect()
}

/// CQ 目标表命名约定：`cq_{gran}_{source_id}`。
fn cq_table_name(source_table_id: u32, gran: CqGranularity) -> String {
    format!("cq_{}_{}", gran.short_name(), source_table_id)
}

/// 构造 CQ 目标表的正式列定义（固定布局 `[value, _sum, _count, _min, _max, _first_ts, _last_ts]`）。
fn build_cq_columns(columns: &[(u16, DataType)], aggregations: &[AggFunction]) -> Vec<ColumnDef> {
    let ids: Vec<u16> = columns.iter().map(|(id, _)| *id).collect();
    let mut cols = Vec::with_capacity(columns.len() * 7);
    for (i, &(src_id, src_type)) in columns.iter().enumerate() {
        let func = aggregations.get(i).copied().unwrap_or(AggFunction::Avg);
        cols.push(cq_column_def(
            src_id,
            format!("value_{src_id}"),
            agg_result_type(func, src_type),
        ));
        for kind in CqAuxColumn::ALL {
            let id = aux_column_id(&ids, i, kind);
            let dt = aux_column_type(kind, src_type);
            cols.push(cq_column_def(
                id,
                format!("value_{src_id}{}", kind.name()),
                dt,
            ));
        }
    }
    cols
}

/// 构造单个 CQ 列定义（默认元数据）。
fn cq_column_def(column_id: u16, name: String, data_type: DataType) -> ColumnDef {
    ColumnDef {
        column_id,
        name,
        data_type,
        precision: 0.0,
        nullable: true,
        unit: None,
        description: None,
        deadband: None,
        enum_values: None,
        deleted_at: None,
        range_low: 0.0,
        range_high: 0.0,
        instrument_range_low: None,
        instrument_range_high: None,
        display_digits: 2,
    }
}

/// 构造 CQ 目标表 `TableInfo`（`TableType::Cq`）。
fn cq_table_info(name: &str, columns: Vec<ColumnDef>, tenant_id: TenantId) -> TableInfo {
    let now = TimestampNanos(chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0));
    TableInfo {
        table_id: 0,
        tenant_id,
        table_name: name.to_string(),
        schema_id: 0,
        schema_version: 1,
        table_group: None,
        table_type: TableType::Cq,
        durability: Durability::Group,
        description: Some("CQ pre-aggregation table".into()),
        enabled: true,
        columns,
        deleted_at: None,
        created_at: now,
        updated_at: now,
    }
}

/// CQ Schema 同步器：订阅 `SchemaEventBus`，将 DDL 变更同步到 CQ 引擎。
struct CqSchemaSynchronizer {
    schema_mgr: Arc<SchemaManager>,
    mirror: Arc<SchemaMirror>,
    cq_engine: Arc<tokio::sync::RwLock<CqEngine>>,
    config: ConfigHandle,
    known_columns: parking_lot::Mutex<std::collections::HashMap<u32, Vec<(u16, DataType)>>>,
    last_reconcile: parking_lot::Mutex<Option<std::time::Instant>>,
}

impl CqSchemaSynchronizer {
    fn new(
        schema_mgr: Arc<SchemaManager>,
        mirror: Arc<SchemaMirror>,
        cq_engine: Arc<tokio::sync::RwLock<CqEngine>>,
        config: ConfigHandle,
    ) -> Self {
        Self {
            schema_mgr,
            mirror,
            cq_engine,
            config,
            known_columns: parking_lot::Mutex::new(std::collections::HashMap::new()),
            last_reconcile: parking_lot::Mutex::new(None),
        }
    }

    const MIN_RECONCILE_INTERVAL: std::time::Duration = std::time::Duration::from_secs(5);

    fn seed_known_columns(&self) {
        let mut known = self.known_columns.lock();
        for entry in self.mirror.tables.iter() {
            let info = entry.value();
            if info.table_type == TableType::Raw && info.deleted_at.is_none() {
                known.insert(info.table_id, numeric_column_ids(&info.columns));
            }
        }
    }

    async fn reconcile_cq_tables(&self) {
        if !self.config.get().cq.enabled {
            return;
        }

        let raw_tables: Vec<(u32, TenantId, Vec<(u16, DataType)>)> = {
            let mut v = Vec::new();
            for entry in self.mirror.tables.iter() {
                let info = entry.value();
                if info.table_type == TableType::Raw && info.deleted_at.is_none() {
                    v.push((
                        info.table_id,
                        info.tenant_id,
                        numeric_column_ids(&info.columns),
                    ));
                }
            }
            v
        };
        let raw_ids: std::collections::HashSet<u32> =
            raw_tables.iter().map(|(id, _, _)| *id).collect();

        let db_defined: std::collections::HashSet<u32> = {
            let cq = self.cq_engine.read().await;
            match cq.persisted_source_tables() {
                Ok(ids) => ids,
                Err(e) => {
                    tracing::error!(
                        "CQ reconcile: failed to read persisted source tables: {e}; skipping"
                    );
                    return;
                }
            }
        };

        let missing: Vec<_> = raw_tables
            .into_iter()
            .filter(|(id, _, _)| !db_defined.contains(id))
            .collect();
        let total_missing = missing.len();
        for (table_id, tenant_id, columns) in missing {
            let aggregations = vec![AggFunction::Avg; columns.len()];
            let target_ids = self
                .ensure_cq_tables(table_id, tenant_id, &columns, &aggregations)
                .await;
            {
                let mut cq = self.cq_engine.write().await;
                cq.on_table_created(table_id, &target_ids, &columns);
            }
            if let Err(e) = self
                .cq_engine
                .read()
                .await
                .persist_definitions_sync(table_id)
            {
                tracing::error!(
                    "CQ reconcile: persist definitions for table {table_id} failed: {e}"
                );
            }
        }

        let orphaned: Vec<u32> = db_defined
            .iter()
            .filter(|id| !raw_ids.contains(id))
            .copied()
            .collect();
        for source_id in &orphaned {
            self.prune_cq_tables(*source_id).await;
        }

        if total_missing > 0 || !orphaned.is_empty() {
            tracing::info!(
                "CQ schema reconciled: {total_missing} missing, {} orphaned",
                orphaned.len()
            );
        }
    }

    async fn run(self: Arc<Self>) {
        let mut rx = self.schema_mgr.event_bus.subscribe();
        loop {
            match rx.recv().await {
                Ok(event) => self.handle_event(event).await,
                Err(tokio::sync::broadcast::error::RecvError::Lagged(n)) => {
                    tracing::warn!("CQ schema sync lagged by {n} events; reseeding + reconciling");
                    self.seed_known_columns();

                    let now = std::time::Instant::now();
                    let should_reconcile = {
                        let mut last = self.last_reconcile.lock();
                        match *last {
                            Some(t) if now.duration_since(t) < Self::MIN_RECONCILE_INTERVAL => {
                                false
                            }
                            _ => {
                                *last = Some(now);
                                true
                            }
                        }
                    };
                    if should_reconcile {
                        self.reconcile_cq_tables().await;
                    }
                }
                Err(tokio::sync::broadcast::error::RecvError::Closed) => {
                    tracing::info!("CQ schema sync channel closed");
                    break;
                }
            }
        }
    }

    async fn handle_event(&self, event: SchemaEvent) {
        if !self.config.get().cq.enabled {
            return;
        }
        match event {
            SchemaEvent::TableCreated {
                table_id,
                table_info,
            } => {
                if table_info.table_type != TableType::Raw {
                    return;
                }
                let columns = numeric_column_ids(&table_info.columns);
                self.known_columns.lock().insert(table_id, columns.clone());

                let aggregations = vec![AggFunction::Avg; columns.len()];
                let tenant_id = table_info.tenant_id;
                let target_ids = self
                    .ensure_cq_tables(table_id, tenant_id, &columns, &aggregations)
                    .await;
                let mut cq = self.cq_engine.write().await;
                cq.on_table_created(table_id, &target_ids, &columns);
            }
            SchemaEvent::TableDeleted { table_id } => {
                self.known_columns.lock().remove(&table_id);
                self.prune_cq_tables(table_id).await;
            }
            SchemaEvent::TableAltered { table_id, .. } => {
                let Some(info) = self.mirror.get_table(table_id) else {
                    return;
                };
                if info.table_type != TableType::Raw {
                    return;
                }
                let new_cols = numeric_column_ids(&info.columns);
                let old_cols = self
                    .known_columns
                    .lock()
                    .get(&table_id)
                    .cloned()
                    .unwrap_or_default();

                let old_ids: Vec<u16> = old_cols.iter().map(|(id, _)| *id).collect();
                let new_ids: Vec<u16> = new_cols.iter().map(|(id, _)| *id).collect();
                let added: Vec<(u16, DataType)> = new_cols
                    .iter()
                    .copied()
                    .filter(|(id, _)| !old_ids.contains(id))
                    .collect();
                let dropped: Vec<u16> = old_ids
                    .iter()
                    .copied()
                    .filter(|id| !new_ids.contains(id))
                    .collect();
                self.known_columns.lock().insert(table_id, new_cols.clone());

                {
                    let mut cq = self.cq_engine.write().await;
                    cq.on_table_altered(table_id, &added, &dropped);
                }
                let aggregations = vec![AggFunction::Avg; new_cols.len()];
                let tenant_id = info.tenant_id;
                self.rebuild_cq_tables(table_id, tenant_id, &new_cols, &aggregations)
                    .await;
            }
        }
    }

    async fn ensure_cq_tables(
        &self,
        source_table_id: u32,
        tenant_id: TenantId,
        columns: &[(u16, DataType)],
        aggregations: &[AggFunction],
    ) -> [u32; 4] {
        let mut target_ids = [0u32; 4];
        for (i, gran) in CqGranularity::all().iter().enumerate() {
            let name = cq_table_name(source_table_id, *gran);
            let column_defs = build_cq_columns(columns, aggregations);

            if let Some(existing) = self.schema_mgr.get_table_by_name(tenant_id, &name) {
                target_ids[i] = existing.table_id;
                continue;
            }

            match self
                .schema_mgr
                .create_table_silent(cq_table_info(&name, column_defs, tenant_id))
                .await
            {
                Ok(created) => target_ids[i] = created.table_id,
                Err(e) => {
                    tracing::error!("failed to create CQ table {name}: {e}");
                    if let Some(existing) = self.schema_mgr.get_table_by_name(tenant_id, &name) {
                        target_ids[i] = existing.table_id;
                    }
                }
            }
        }
        target_ids
    }

    async fn rebuild_cq_tables(
        &self,
        source_table_id: u32,
        tenant_id: TenantId,
        columns: &[(u16, DataType)],
        aggregations: &[AggFunction],
    ) {
        for gran in CqGranularity::all() {
            let name = cq_table_name(source_table_id, gran);
            let Some(existing) = self.schema_mgr.get_table_by_name(tenant_id, &name) else {
                continue;
            };
            let column_defs = build_cq_columns(columns, aggregations);
            if let Err(e) = self
                .schema_mgr
                .alter_table(existing.table_id, column_defs)
                .await
            {
                tracing::error!("failed to alter CQ table {name}: {e}");
            }
        }
    }

    async fn prune_cq_tables(&self, source_table_id: u32) {
        let target_ids: Vec<u32> = {
            let cq = self.cq_engine.read().await;
            cq.get_definitions_for_table(source_table_id)
                .iter()
                .map(|d| d.target_table_id)
                .collect()
        };
        {
            let mut cq = self.cq_engine.write().await;
            cq.on_table_deleted(source_table_id);
        }
        for tid in target_ids {
            let _ = self.schema_mgr.drop_table(tid).await;
        }
    }
}

// ─── StreamSchemaSynchronizer ──────────────────────────────────────────────

/// 收集 Stream 任务引用的全部源列 ID（聚合列 + 透传列 + 分区键）。
fn stream_referenced_columns(task: &StreamTask) -> Vec<u16> {
    let mut cols: Vec<u16> = task
        .projection
        .columns
        .iter()
        .map(|c| match c {
            ProjectionColumn::Aggregate { source_column, .. } => *source_column,
            ProjectionColumn::Passthrough { source_column, .. } => *source_column,
        })
        .collect();
    if let Some(partition) = &task.partition_by {
        cols.push(partition.source_column);
    }
    cols
}

struct StreamSchemaSynchronizer {
    schema_mgr: Arc<SchemaManager>,
    mirror: Arc<SchemaMirror>,
    stream_engine: Arc<tokio::sync::RwLock<StreamComputeEngine>>,
}

impl StreamSchemaSynchronizer {
    fn new(
        schema_mgr: Arc<SchemaManager>,
        mirror: Arc<SchemaMirror>,
        stream_engine: Arc<tokio::sync::RwLock<StreamComputeEngine>>,
    ) -> Self {
        Self {
            schema_mgr,
            mirror,
            stream_engine,
        }
    }

    async fn run(self: Arc<Self>) {
        let mut rx = self.schema_mgr.event_bus.subscribe();
        loop {
            match rx.recv().await {
                Ok(event) => self.handle_event(event).await,
                Err(tokio::sync::broadcast::error::RecvError::Lagged(n)) => {
                    tracing::warn!(
                        "Stream schema sync lagged by {n} events; orphan cleanup may be incomplete"
                    );
                }
                Err(tokio::sync::broadcast::error::RecvError::Closed) => {
                    tracing::info!("Stream schema sync channel closed");
                    break;
                }
            }
        }
    }

    async fn handle_event(&self, event: SchemaEvent) {
        match event {
            SchemaEvent::TableCreated { .. } => {}
            SchemaEvent::TableDeleted { table_id } => {
                let task_ids: Vec<TaskId> = {
                    let engine = self.stream_engine.read().await;
                    engine
                        .get_tasks()
                        .into_iter()
                        .filter(|t| t.source_table_id == table_id || t.target_table_id == table_id)
                        .map(|t| t.id)
                        .collect()
                };

                for task_id in task_ids {
                    let mut engine = self.stream_engine.write().await;
                    if let Err(e) = engine.drop_task(task_id).await {
                        tracing::warn!(
                            task_id = %task_id,
                            error = %e,
                            "failed to drop orphaned stream task"
                        );
                    }
                }
            }
            SchemaEvent::TableAltered { table_id, .. } => {
                let Some(info) = self.mirror.get_table(table_id) else {
                    return;
                };
                let existing: std::collections::HashSet<u16> = info
                    .columns
                    .iter()
                    .filter(|c| c.deleted_at.is_none())
                    .map(|c| c.column_id)
                    .collect();

                let tasks: Vec<StreamTask> = {
                    let engine = self.stream_engine.read().await;
                    engine
                        .get_tasks()
                        .into_iter()
                        .filter(|t| t.source_table_id == table_id)
                        .collect()
                };

                for task in tasks {
                    let referenced = stream_referenced_columns(&task);
                    if let Some(missing) =
                        referenced.iter().copied().find(|c| !existing.contains(c))
                    {
                        let reason = format!("source column {missing} dropped by ALTER TABLE");
                        let mut engine = self.stream_engine.write().await;
                        if let Err(e) = engine.mark_error(task.id, reason).await {
                            tracing::warn!(
                                task_id = %task.id,
                                error = %e,
                                "failed to mark stream task error after column drop"
                            );
                        }
                    }
                }
            }
        }
    }
}

/// Derived SnapshotRead adapter：从 SnapshotStore 读取源 tag 当前值（列级）。
struct DerivedSnapshotReadAdapter {
    snapshot: Arc<ArcSwap<SnapshotStore>>,
}

impl SnapshotRead for DerivedSnapshotReadAdapter {
    fn batch_get_columns(
        &self,
        columns: &[ColumnRef],
    ) -> std::collections::HashMap<ColumnRef, (f64, u8, i64)> {
        let mut table_ids: Vec<u32> = columns.iter().map(|c| c.table_id).collect();
        table_ids.sort_unstable();
        table_ids.dedup();

        let table_map: std::collections::HashMap<u32, TypedSnapshotValue> = self
            .snapshot
            .load()
            .batch_get_typed(&table_ids)
            .into_iter()
            .collect();

        columns
            .iter()
            .filter_map(|col| {
                let entry = table_map.get(&col.table_id)?;
                let value = entry
                    .values
                    .iter()
                    .find(|(cid, _)| *cid == col.column_id)
                    .map(|(_, v)| v.as_f64())
                    .unwrap_or(f64::NAN);
                let quality = entry
                    .qualities
                    .iter()
                    .find(|(cid, _)| *cid == col.column_id)
                    .map(|(_, q)| *q)
                    .unwrap_or(0);
                Some((*col, (value, quality, entry.ts)))
            })
            .collect()
    }
}

/// Derived WriteSink adapter：将派生结果经 `Engine::write_batch` 写入标准路径。
struct DerivedWriteSinkAdapter {
    engine: Arc<Engine>,
    mirror: Arc<SchemaMirror>,
}

impl WriteSink for DerivedWriteSinkAdapter {
    fn write_batch(&self, entries: &[(u32, String, f64, u8, i64)]) {
        if entries.is_empty() {
            return;
        }
        use sensearcdb_engine::engine::{DataPoint, PointBatch};
        use sensearcdb_engine::memtable::TypedValue;
        let now_ns = chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0);
        let points: Vec<DataPoint> = entries
            .iter()
            .map(|(table_id, _tag, value, quality, ts)| DataPoint {
                table_id: *table_id,
                tenant_id: self
                    .mirror
                    .get_table(*table_id)
                    .map(|t| t.tenant_id)
                    .unwrap_or_else(|| {
                        tracing::warn!(
                            table_id = *table_id,
                            "derived sink mirror miss; fallback DEFAULT tenant"
                        );
                        TenantId::DEFAULT
                    }),
                ts: *ts,
                server_ts: now_ns,
                values: vec![(0u16, TypedValue::F64(*value))],
                qualities: vec![(0u16, *quality)],
                schema_version: 1,
                flags: 0,
            })
            .collect();
        let batch = PointBatch { points };
        let engine = self.engine.clone();
        tokio::task::block_in_place(move || {
            let rt = tokio::runtime::Handle::current();
            rt.block_on(async move {
                if let Err(e) = engine.write_batch(batch).await {
                    tracing::warn!(error = %e, "derived write sink failed");
                }
            })
        });
    }
}

/// Derived SchemaAccess adapter：从 SchemaMirror 解析表名/列名（列级）。
struct DerivedSchemaAccessAdapter {
    mirror: Arc<SchemaMirror>,
}

impl SchemaAccess for DerivedSchemaAccessAdapter {
    fn table_tenant(&self, table_id: u32) -> Option<TenantId> {
        self.mirror.get_table(table_id).map(|t| t.tenant_id)
    }

    fn resolve_column_ref(
        &self,
        tenant_id: TenantId,
        table_name: &str,
        column_name: Option<&str>,
    ) -> Option<ColumnRef> {
        match column_name {
            None => self
                .mirror
                .get_table_by_name(tenant_id, table_name)
                .map(|t| ColumnRef::single(t.table_id)),
            Some(col) => self.mirror.resolve_column(tenant_id, table_name, col),
        }
    }
}

/// Stream Catalog adapter：用 SchemaMirror/SchemaManager 实现 `StreamCatalog`（P1-12）。
pub struct StreamCatalogAdapter {
    mirror: Arc<SchemaMirror>,
    schema_mgr: Arc<SchemaManager>,
}

impl StreamCatalogAdapter {
    pub fn new(mirror: Arc<SchemaMirror>, schema_mgr: Arc<SchemaManager>) -> Self {
        Self { mirror, schema_mgr }
    }
}

impl StreamCatalog for StreamCatalogAdapter {
    fn resolve_table(&self, tenant_id: TenantId, table_name: &str) -> Option<u32> {
        self.mirror
            .get_table_by_name(tenant_id, table_name)
            .map(|t| t.table_id)
    }

    fn resolve_column(&self, table_id: u32, column_name: &str) -> Option<u16> {
        self.mirror
            .get_table(table_id)?
            .columns
            .iter()
            .find(|c| c.name == column_name)
            .map(|c| c.column_id)
    }

    fn resolve_column_type(&self, table_id: u32, column_name: &str) -> Option<DataType> {
        self.mirror
            .get_table(table_id)?
            .columns
            .iter()
            .find(|c| c.name == column_name)
            .map(|c| c.data_type)
    }

    fn auto_create_target_table(
        &self,
        tenant_id: TenantId,
        table_name: &str,
        columns: &[(String, DataType)],
    ) -> Result<u32, String> {
        let now = TimestampNanos(chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0));
        let column_defs: Vec<ColumnDef> = columns
            .iter()
            .enumerate()
            .map(|(i, (name, data_type))| ColumnDef {
                column_id: i as u16,
                name: name.clone(),
                data_type: *data_type,
                precision: 0.0,
                nullable: true,
                unit: None,
                description: None,
                deadband: None,
                enum_values: None,
                deleted_at: None,
                range_low: 0.0,
                range_high: 0.0,
                instrument_range_low: None,
                instrument_range_high: None,
                display_digits: 2,
            })
            .collect();

        let info = TableInfo {
            table_id: 0,
            tenant_id,
            table_name: table_name.to_string(),
            schema_id: 0,
            schema_version: 1,
            table_group: None,
            table_type: TableType::Stream,
            durability: Durability::Group,
            description: Some("Stream compute target table".into()),
            enabled: true,
            columns: column_defs,
            deleted_at: None,
            created_at: now,
            updated_at: now,
        };

        let mgr = self.schema_mgr.clone();
        let created = tokio::task::block_in_place(move || {
            let rt = tokio::runtime::Handle::current();
            rt.block_on(async move { mgr.create_table(info).await })
        })
        .map_err(|e| format!("failed to create stream target table: {e}"))?;

        Ok(created.table_id)
    }

    fn table_exists(&self, table_id: u32) -> bool {
        self.mirror.get_table(table_id).is_some()
    }

    fn table_name(&self, table_id: u32) -> Option<String> {
        self.mirror
            .get_table(table_id)
            .map(|t| t.table_name.clone())
    }
}

// ─── 回放派生适配器（依赖倒置，注入 ReplayEngine）────────────────────────

/// 派生定义来源的迟绑定槽：`ReplayEngine` 在 step 5 构造（早于 `DerivedTagEngine`），
/// 故先持有空槽，Derived 初始化后回填。
type DerivedEngineSlot =
    parking_lot::RwLock<Option<Arc<tokio::sync::RwLock<Arc<DerivedTagEngine>>>>>;

/// 回放派生重算适配器。
struct ReplayDerivedAdapter {
    derived_engine: Arc<DerivedEngineSlot>,
}

#[async_trait]
impl ReplayDerived for ReplayDerivedAdapter {
    async fn recompute(
        &self,
        sac_reader: &sensearcdb_snapshot::SacReaderRef,
        start_ts: i64,
        end_ts: i64,
    ) -> ReplayDerivedOutput {
        let engine = match self.derived_engine.read().clone() {
            Some(e) => e,
            None => {
                tracing::warn!("replay derived recompute: derived engine not wired yet");
                return ReplayDerivedOutput {
                    definition_count: 0,
                    points: Vec::new(),
                    derived_streams: std::collections::HashMap::new(),
                };
            }
        };

        let (nodes, topo) = {
            let guard = engine.read().await;
            let graph = guard.dep_graph().read();
            (
                graph.export_nodes(),
                graph.topological_sort().unwrap_or_default(),
            )
        };

        let definition_count = nodes.len();
        if definition_count == 0 {
            return ReplayDerivedOutput {
                definition_count: 0,
                points: Vec::new(),
                derived_streams: std::collections::HashMap::new(),
            };
        }

        let node_map: std::collections::HashMap<String, DerivedTagNode> =
            nodes.into_iter().map(|n| (n.name.clone(), n)).collect();

        let mut derived_streams: std::collections::HashMap<String, Vec<(i64, f64, u8)>> =
            std::collections::HashMap::new();
        let mut points: Vec<ReplayDerivedPoint> = Vec::new();

        for name in &topo {
            let Some(node) = node_map.get(name) else {
                continue;
            };

            let mut sources: std::collections::HashMap<String, Vec<(i64, f64, u8)>> =
                std::collections::HashMap::new();
            for (source_name, col_ref) in &node.source_table_ids {
                if let Some(series) = derived_streams.get(source_name) {
                    sources.insert(source_name.clone(), series.clone());
                } else if node_map.contains_key(source_name) {
                    sources.insert(source_name.clone(), Vec::new());
                } else {
                    match sac_reader.read_range(
                        sensearcdb_common::TenantId::DEFAULT,
                        col_ref.table_id,
                        start_ts,
                        end_ts,
                    ) {
                        Ok(rows) => {
                            let series: Vec<(i64, f64, u8)> = rows
                                .into_iter()
                                .map(|r| {
                                    let value = r
                                        .values
                                        .iter()
                                        .find(|(cid, _)| *cid == col_ref.column_id)
                                        .map(|(_, v)| v.as_f64())
                                        .unwrap_or(f64::NAN);
                                    let quality = r
                                        .qualities
                                        .iter()
                                        .find(|(cid, _)| *cid == col_ref.column_id)
                                        .map(|(_, q)| *q)
                                        .unwrap_or(0);
                                    (r.ts, value, quality)
                                })
                                .collect();
                            sources.insert(source_name.clone(), series);
                        }
                        Err(e) => {
                            tracing::warn!(
                                source = %source_name,
                                table_id = col_ref.table_id,
                                column_id = col_ref.column_id,
                                error = %e,
                                "replay derived: SAC read failed, treating source as empty"
                            );
                            sources.insert(source_name.clone(), Vec::new());
                        }
                    }
                }
            }

            let out = evaluate_series(node, &sources);
            for &(ts, value, quality) in &out {
                points.push(ReplayDerivedPoint {
                    table_id: node.table_id,
                    value,
                    quality,
                    event_ts: ts,
                });
            }
            derived_streams.insert(name.clone(), out);
        }

        ReplayDerivedOutput {
            definition_count,
            points,
            derived_streams,
        }
    }
}

// ─── EngineFanoutSink ─────────────────────────────────────────────────────

/// 统一批级 fanout：把一次写入扇出到 snapshot / derived / stream / cq 四路。
struct EngineFanoutSink {
    #[allow(dead_code)]
    snapshot: Arc<ArcSwap<SnapshotStore>>,
    pending: Arc<SnapshotPendingBuffer>,
    schema_mirror: Arc<SchemaMirror>,
    stream: parking_lot::RwLock<Option<Arc<tokio::sync::RwLock<StreamComputeEngine>>>>,
    derived: parking_lot::RwLock<Option<Arc<tokio::sync::RwLock<Arc<DerivedTagEngine>>>>>,
    cq: parking_lot::RwLock<Option<Arc<tokio::sync::RwLock<CqEngine>>>>,
    batch_seq: std::sync::atomic::AtomicU64,
}

impl EngineFanoutSink {
    fn set_stream(&self, engine: Arc<tokio::sync::RwLock<StreamComputeEngine>>) {
        *self.stream.write() = Some(engine);
    }

    fn set_derived(&self, engine: Arc<tokio::sync::RwLock<Arc<DerivedTagEngine>>>) {
        *self.derived.write() = Some(engine);
    }

    fn set_cq(&self, engine: Arc<tokio::sync::RwLock<CqEngine>>) {
        *self.cq.write() = Some(engine);
    }

    fn fanout_batch(
        &self,
        points: &[sensearcdb_engine::engine::DataPoint],
        seqs: &[Option<u64>],
    ) {
        let derived = self.derived.read().clone();
        let stream = self.stream.read().clone();
        let cq = self.cq.read().clone();

        // 1. snapshot 一路：整批转 owned、一次锁 append。
        let pending_batch: Vec<OwnedPendingPoint> = points
            .iter()
            .zip(seqs.iter())
            .map(|(point, seq)| OwnedPendingPoint {
                tenant_id: point.tenant_id,
                table_id: point.table_id,
                values: point.values.clone(),
                qualities: point.qualities.clone(),
                ts: point.ts,
                server_ts: point.server_ts,
                write_seq: *seq,
                is_derived: false,
                writer_session_id: None,
            })
            .collect();
        self.pending.append_batch(pending_batch);

        // 三路各拿一次 try_read，快路径判空后共享一个 batch_id。
        let derived_guard = derived.as_ref().and_then(|d| d.try_read().ok());
        let stream_guard = stream.as_ref().and_then(|s| s.try_read().ok());
        let cq_guard = cq.as_ref().and_then(|c| c.try_read().ok());

        let has_derived = derived_guard.as_ref().map_or(false, |g| g.has_derived_tags());
        let has_stream = stream_guard.as_ref().map_or(false, |g| g.total_listeners() > 0);
        let has_cq = cq_guard.as_ref().map_or(false, |g| g.has_cq_definitions());

        if !(has_derived || has_stream || has_cq) {
            return;
        }

        let batch_id = self
            .batch_seq
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);

        // 2. derived：循环外收集去重 source_keys → 一次 mark_dirty。
        if has_derived {
            if let Some(guard) = derived_guard.as_ref() {
                let mut source_keys: Vec<(TenantId, u32, String)> = Vec::new();
                for point in points.iter() {
                    if let Some(name) = self
                        .schema_mirror
                        .get_table(point.table_id)
                        .map(|t| t.table_name.clone())
                    {
                        source_keys.push((point.tenant_id, point.table_id, name));
                    }
                }
                source_keys.sort_unstable_by_key(|(_, tid, _)| *tid);
                source_keys.dedup_by_key(|(_, tid, _)| *tid);
                let max_ts = points.iter().map(|p| p.ts).max().unwrap_or(0);
                guard.mark_dirty(&source_keys, batch_id, max_ts);
            }
        }

        // 3. stream：一次 on_write_batch。
        if has_stream {
            if let Some(guard) = stream_guard.as_ref() {
                let entries: Vec<(u32, i64, &[(u16, TypedValue)])> = points
                    .iter()
                    .map(|p| (p.table_id, p.ts / 1_000_000, p.values.as_slice()))
                    .collect();
                guard.on_write_batch(&entries, batch_id);
            }
        }

        // 4. cq：按 table_id 分组 timestamps → 每表一次 on_data_written。
        if has_cq {
            if let Some(guard) = cq_guard.as_ref() {
                let mut grouped: std::collections::HashMap<u32, Vec<i64>> =
                    std::collections::HashMap::new();
                for point in points.iter() {
                    grouped.entry(point.table_id).or_default().push(point.ts);
                }
                for (table_id, timestamps) in grouped {
                    guard.on_data_written(CqWriteEvent {
                        table_id,
                        timestamps,
                        batch_id,
                    });
                }
            }
        }
    }
}

impl sensearcdb_engine::engine::SnapshotUpdateSink for EngineFanoutSink {
    fn on_write(
        &self,
        tenant_id: sensearcdb_common::TenantId,
        table_id: u32,
        values: &[(u16, TypedValue)],
        qualities: &[(u16, u8)],
        ts: i64,
        server_ts: i64,
        write_seq: Option<u64>,
    ) {
        let point = sensearcdb_engine::engine::DataPoint {
            table_id,
            tenant_id,
            ts,
            server_ts,
            values: values.to_vec(),
            qualities: qualities.to_vec(),
            schema_version: 0,
            flags: 0,
        };
        let seqs = [write_seq];
        self.fanout_batch(&[point], &seqs);
    }

    fn on_write_batch(&self, points: &[sensearcdb_engine::engine::DataPoint], seqs: &[u64]) {
        let seqs_opt: Vec<Option<u64>> = seqs.iter().map(|&s| Some(s)).collect();
        self.fanout_batch(points, &seqs_opt);
    }
}

// ─── 启动参数适配 ───────────────────────────────────────────────────────────

/// 将 `bootstrap(config_path)` 的字符串路径适配为 deploy 的 `BootstrapArgs`。
struct StartupBootstrapArgs {
    config_path: Option<String>,
}

impl BootstrapArgs for StartupBootstrapArgs {
    fn config_path(&self) -> Option<&str> {
        self.config_path.as_deref()
    }
}

// ─── systemConfig 持久化覆盖加载 ─────────────────────────────────────────

/// 启动时从 config.db `system_config` 表加载白名单字段覆盖值并热 swap。
fn apply_persisted_system_config(config_handle: &ConfigHandle) {
    use sensearcdb_common::{apply_whitelist_value, SystemConfigStore};

    let db_path = config_handle.get().paths.config_db_path();
    let store = match SystemConfigStore::open(&db_path) {
        Ok(store) => store,
        Err(e) => {
            tracing::warn!(error = %e, "failed to open system_config store; using defaults");
            return;
        }
    };

    let overrides = match store.load_all() {
        Ok(overrides) => overrides,
        Err(e) => {
            tracing::warn!(error = %e, "failed to load system_config overrides");
            return;
        }
    };

    if overrides.is_empty() {
        return;
    }

    let mut new_cfg = (*config_handle.get()).as_ref().clone();
    let mut applied = 0usize;
    for (key, value) in &overrides {
        let Ok(num) = value.parse::<i64>() else {
            tracing::warn!(key = %key, value = %value, "skip non-integer system_config override");
            continue;
        };
        match apply_whitelist_value(&mut new_cfg, key, num) {
            Ok(()) => applied += 1,
            Err(e) => tracing::warn!(key = %key, error = %e, "skip invalid system_config override"),
        }
    }
    if applied > 0 {
        config_handle.swap(new_cfg);
        tracing::info!(count = applied, "applied persisted system_config overrides");
    }
}

// ─── 启动序列 ───────────────────────────────────────────────────────────────

/// 执行完整的单机启动序列，返回所有开源子系统的句柄。
pub async fn bootstrap(config_path: &str) -> anyhow::Result<AppState> {
    // ═══ 0. 部署安全检查链 ═══
    let bootstrap_args = StartupBootstrapArgs {
        config_path: Some(config_path.to_string()),
    };
    let (pre_config, config_source_path) = Bootstrap::load_config(&bootstrap_args)
        .map_err(|e| anyhow::anyhow!("config load failed: {}", e))?;
    // 日志初始化：重复初始化不阻断启动。
    let _ = sensearcdb_observe::init_tracing(&pre_config.observe);

    let (process_lock, deploy_bootstrap) = Bootstrap::run_with_config(pre_config, config_source_path)
        .await
        .map_err(|e| anyhow::anyhow!("deploy bootstrap failed: {}", e))?;
    let app_config = deploy_bootstrap.config;
    let config_handle = ConfigHandle::new(app_config);
    apply_persisted_system_config(&config_handle);
    let cfg = config_handle.get();

    tracing::info!(
        config_path = %config_path,
        data_dir = %cfg.paths.data_dir,
        "configuration loaded (6-level)"
    );

    let process_lock = Arc::new(process_lock);
    tracing::info!(pid = process_lock.pid(), "process lock acquired");

    // ═══ 1. Schema 子系统 ═══
    tracing::info!("initializing Schema subsystem...");
    let catalog = Arc::new(Catalog::open(&cfg.paths.config_db_path()).with_context(|| {
        format!(
            "failed to open catalog at '{}'",
            cfg.paths.config_db_path().display()
        )
    })?);
    sensearcdb_schema::migration::run_migrations(catalog.as_ref())
        .context("failed to run schema migrations")?;
    let tenant_manager = Arc::new(
        sensearcdb_schema::TenantManager::new(catalog.clone())
            .with_cooling_period_days(cfg.tenant.deletion_cooling_period_days),
    );
    tenant_manager
        .ensure_default_tenant()
        .context("failed to ensure default tenant")?;
    let schema_mirror = Arc::new(SchemaMirror::new());
    let event_bus = Arc::new(SchemaEventBus::new(16_384));
    let schema_mgr = Arc::new(SchemaManager::new(
        catalog.clone(),
        schema_mirror.clone(),
        event_bus,
    ));
    schema_mgr
        .load_all()
        .context("failed to load schema from catalog")?;

    tracing::info!(
        table_count = schema_mgr.list_tables().len(),
        "Schema subsystem initialized"
    );

    // ═══ 2. 存储引擎（TDE 关闭：encryptor = None）═══
    tracing::info!("initializing Engine subsystem...");
    let engine = Arc::new(
        Engine::new(&config_handle, None)
            .await
            .context("failed to initialize storage engine")?,
    );
    engine.set_durability_resolver(Arc::new(SchemaMirrorDurabilityResolver {
        mirror: schema_mirror.clone(),
    }));
    let engine_metrics = engine.metrics.clone();
    tracing::info!("Engine subsystem initialized");

    // 单机版跳过配额闸口接线（engine 不设 gate 即无限写）。
    let quota_store = Arc::new(sensearcdb_schema::TenantQuotaStore::new(catalog.clone()));

    // ═══ 3. 实时快照 ═══
    tracing::info!("initializing Snapshot subsystem...");
    let sub_mgr = Arc::new(SnapshotSubscriptionManager::new());

    let schema_mirror_ref: Arc<dyn SchemaMirrorRef> = Arc::new(SchemaMirrorAdapter {
        mirror: schema_mirror.clone(),
    });
    let metadata = Arc::new(TagMetadataStore::new(schema_mirror_ref.clone()));

    let snapshot_registry = Registry::new();
    let snapshot_metrics = SnapshotMetrics::new(&snapshot_registry)
        .context("failed to register snapshot prometheus metrics")?;

    // 三层崩溃恢复：snapshot.bin → SAC 全量扫描 → WAL 增量。
    let shard_count = std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(1)
        * cfg.snapshot.shard_multiplier;
    let recovery_config = RecoveryConfig {
        snapshot_path: cfg.paths.snapshot_file(),
        shard_count,
        sac_watermark: engine.sac_watermark().current(),
    };
    let sac_recovery: SacRecoveryReaderRef = Arc::new(EngineSacRecoveryReader {
        sac_dir: cfg.paths.sac_dir().to_string_lossy().into_owned(),
        encryptor: None,
    });
    let wal_recovery: WalReplayReaderRef = Arc::new(EngineWalReplayReader {
        wal_dir: cfg.paths.wal_dir().to_string_lossy().into_owned(),
        encryptor: None,
    });
    let recovery_result = recover(
        &recovery_config,
        sub_mgr.clone(),
        metadata.clone(),
        snapshot_metrics.clone(),
        Some(sac_recovery),
        Some(wal_recovery),
        None, // snapshot_encryptor（TDE 关闭）
    )
    .map_err(|e| anyhow::anyhow!("failed to recover snapshot: {}", e))?;
    tracing::info!(
        source = ?recovery_result.source,
        entry_count = recovery_result.entry_count,
        duration_secs = recovery_result.duration_secs,
        "snapshot recovered"
    );
    let snapshot_store: Arc<ArcSwap<SnapshotStore>> =
        Arc::new(ArcSwap::new(Arc::new(recovery_result.store)));

    // 方案 A：快照三层恢复完成后，回放 WAL tail 重建 MemTable（历史查询缓冲）。
    // 必须早于下方 set_snapshot_sink（回放期间 sink 为 None，双重保证不 fanout）。
    match engine.recover_memtable_from_wal() {
        Ok((frames, points)) => {
            tracing::info!(frames, points, "MemTable rebuilt from WAL replay (startup)");
        }
        Err(e) => {
            tracing::warn!(error = %e, "MemTable WAL replay failed (best-effort, history tail may be missing)");
        }
    }

    // 后台快照检查点。
    let snapshot_checkpointer = Arc::new(SnapshotCheckpointer::new(
        snapshot_store.clone(),
        recovery_config.snapshot_path.clone(),
        config_handle.clone(),
        snapshot_metrics.clone(),
        None, // snapshot_encryptor（TDE 关闭）
        Some(Arc::new(EngineWalWatermarkProvider {
            engine: engine.clone(),
        })),
    ));
    {
        let checkpointer = snapshot_checkpointer.clone();
        tokio::spawn(async move {
            checkpointer.run().await;
        });
    }
    tracing::info!("SnapshotCheckpointer started (interval-based persistence)");

    // snapshot pending 缓冲 + 独立 flusher。
    let pending_cfg = config_handle.get();
    let snapshot_pending = Arc::new(SnapshotPendingBuffer::new(
        pending_cfg.snapshot.flush_batch_size,
        pending_cfg.snapshot.flush_bytes,
    ));
    let snapshot_flusher = Arc::new(SnapshotFlusher::new(
        snapshot_pending.clone(),
        snapshot_store.clone(),
        config_handle.clone(),
        snapshot_metrics.clone(),
    ));
    {
        let flusher = snapshot_flusher.clone();
        tokio::spawn(async move { flusher.run().await; });
    }
    tracing::info!("SnapshotFlusher started (interval/threshold-based batch update_batch)");

    // C3：WAL 旧段回收后台任务。
    {
        let reaper = WalReaper::new(cfg.paths.wal_dir(), DEFAULT_WAL_RECYCLE_KEEP_SEGMENTS);
        let snapshot_path = recovery_config.snapshot_path.clone();
        let engine_for_reaper = engine.clone();
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(std::time::Duration::from_secs(60));
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            loop {
                interval.tick().await;
                let wal_watermark = read_snapshot_watermark(&snapshot_path).max(0) as u64;
                let s = engine_for_reaper.sac_watermark().current();
                let r = wal_watermark.min(s);
                let active_segment = engine_for_reaper.wal.active_segment_seq();
                let wal_dir = engine_for_reaper.wal.wal_dir();
                let reaper_for_run = reaper.clone();

                let result =
                    tokio::task::spawn_blocking(move || reaper_for_run.reap(r, active_segment))
                        .await;

                match result {
                    Ok(Ok(stats)) => {
                        if stats.deleted > 0 {
                            tracing::info!(
                                r = r,
                                wal_dir = %wal_dir.display(),
                                candidates = stats.candidate_count,
                                deleted = stats.deleted,
                                remaining = stats.remaining,
                                "WAL reaper cycle deleted old segments"
                            );
                        }
                    }
                    Ok(Err(e)) => tracing::warn!(error = %e, "WAL reaper cycle failed"),
                    Err(e) => tracing::warn!(error = %e, "WAL reaper spawn_blocking failed"),
                }
            }
        });
    }
    tracing::info!("WAL reaper started (60s interval)");

    // 接入写入回调：WAL → MemTable → SnapshotStore + Stream/Derived/CQ。
    let fanout = Arc::new(EngineFanoutSink {
        snapshot: snapshot_store.clone(),
        pending: snapshot_pending.clone(),
        schema_mirror: schema_mirror.clone(),
        stream: parking_lot::RwLock::new(None),
        derived: parking_lot::RwLock::new(None),
        cq: parking_lot::RwLock::new(None),
        batch_seq: std::sync::atomic::AtomicU64::new(0),
    });
    let fanout_sink: Arc<dyn sensearcdb_engine::engine::SnapshotUpdateSink> = fanout.clone();
    engine.set_snapshot_sink(fanout_sink);

    tracing::info!(
        shard_count = snapshot_store.load().bucket_count(),
        "Snapshot subsystem initialized"
    );

    // ═══ 4. 历史回放 ═══
    tracing::info!("initializing Replay subsystem...");
    let sac_reader: sensearcdb_snapshot::SacReaderRef = Arc::new(SacReaderAdapter {
        sac_dir: cfg.paths.sac_dir().to_string_lossy().into_owned(),
        encryptor: None,
    });
    let replay_metrics = SnapshotMetrics::new(&Registry::new())
        .context("failed to register replay prometheus metrics")?;

    // 派生重算迟绑定槽（告警评估单机版不接线，传 None）。
    let derived_slot: Arc<DerivedEngineSlot> = Arc::new(parking_lot::RwLock::new(None));

    let replay_derived: Option<Arc<dyn ReplayDerived>> = if cfg.derived.enabled {
        Some(Arc::new(ReplayDerivedAdapter {
            derived_engine: derived_slot.clone(),
        }))
    } else {
        None
    };

    let replay_engine = Arc::new(ReplayEngine::new(
        &config_handle,
        sac_reader,
        replay_metrics,
        snapshot_store.load().bucket_count(),
        replay_derived,
        None, // alerts（单机版不接线告警回放）
    ));

    tracing::info!("Replay subsystem initialized (SacReaderAdapter)");

    // ═══ 5. 分区管理 ═══
    tracing::info!("initializing Partition subsystem...");
    let partition_manager = Arc::new(
        PartitionManager::new(config_handle.clone(), Arc::clone(&engine))
            .await
            .map_err(|e| anyhow::anyhow!("failed to initialize partition manager: {}", e))?,
    );

    let (scheduler_tx, scheduler_rx) = tokio::sync::oneshot::channel::<()>();
    let _scheduler_handle = partition_manager.start_scheduler(scheduler_rx);
    tracing::info!("PartitionScheduler started");

    tracing::info!(
        table_count = partition_manager.list_all_partitions().len(),
        "Partition subsystem initialized"
    );

    // ═══ 6. CQ 引擎 ═══
    tracing::info!("initializing CQ Engine...");
    let cq_query_executor: Arc<dyn CqQueryExecutor> = Arc::new(CqQueryExecutorAdapter {
        engine: Arc::clone(&engine),
        mirror: schema_mirror.clone(),
    });
    let cq_write_target: Arc<dyn CqWriteTarget> = Arc::new(CqWriteTargetAdapter {
        engine: Arc::clone(&engine),
        mirror: schema_mirror.clone(),
    });

    let cq_definition_store = match CqDefinitionStore::from_shared_conn(catalog.shared_conn()) {
        Ok(store) => {
            tracing::info!("CQ definition store opened on shared catalog connection");
            Some(Arc::new(store))
        }
        Err(e) => {
            tracing::warn!(error = %e, "failed to open CQ definition store; CQ definitions will be memory-only");
            None
        }
    };

    let cq_engine = Arc::new(tokio::sync::RwLock::new(CqEngine::new(
        cq_query_executor,
        cq_write_target,
        config_handle.clone(),
        cq_definition_store,
        catalog.clone(),
    )));
    {
        let mut cq = cq_engine.write().await;
        if let Err(e) = cq.restore_definitions() {
            tracing::warn!(error = %e, "failed to restore CQ definitions from config.db");
        }
        if let Err(e) = cq.start().await {
            tracing::warn!(error = %e, "CQ engine failed to start");
        }
    }
    fanout.set_cq(cq_engine.clone());

    let cq_schema_sync = Arc::new(CqSchemaSynchronizer::new(
        schema_mgr.clone(),
        schema_mirror.clone(),
        cq_engine.clone(),
        config_handle.clone(),
    ));
    cq_schema_sync.seed_known_columns();
    cq_schema_sync.reconcile_cq_tables().await;
    {
        let sync = cq_schema_sync.clone();
        tokio::spawn(async move { sync.run().await });
    }
    tracing::info!("CQ Engine initialized");

    // ═══ 7. Stream 引擎 ═══
    tracing::info!("initializing Stream Compute Engine...");
    let stream_sink: Arc<dyn sensearcdb_stream::StreamSink> = Arc::new(EngineStreamSink {
        engine: Arc::clone(&engine),
        mirror: schema_mirror.clone(),
    });
    let stream_task_store = Arc::new(
        StreamTaskStore::open(&cfg.paths.config_db_path())
            .context("failed to open Stream task store")?,
    );
    let stream_engine = Arc::new(tokio::sync::RwLock::new(StreamComputeEngine::new(
        config_handle.clone(),
        stream_sink,
        stream_task_store,
    )));
    {
        let mut se = stream_engine.write().await;
        if let Err(e) = se.start().await {
            tracing::warn!(error = %e, "Stream Compute Engine failed to start");
        }
    }
    fanout.set_stream(stream_engine.clone());

    let stream_schema_sync = Arc::new(StreamSchemaSynchronizer::new(
        schema_mgr.clone(),
        schema_mirror.clone(),
        stream_engine.clone(),
    ));
    {
        let sync = stream_schema_sync.clone();
        tokio::spawn(async move { sync.run().await });
    }
    tracing::info!("Stream Compute Engine initialized");

    // ═══ 8. Derived 引擎 ═══
    tracing::info!("initializing Derived Tag Engine...");
    let derived_snapshot: Arc<dyn SnapshotRead> = Arc::new(DerivedSnapshotReadAdapter {
        snapshot: snapshot_store.clone(),
    });
    let derived_writer: Arc<dyn WriteSink> = Arc::new(DerivedWriteSinkAdapter {
        engine: Arc::clone(&engine),
        mirror: schema_mirror.clone(),
    });
    let derived_schema: Arc<dyn SchemaAccess> = Arc::new(DerivedSchemaAccessAdapter {
        mirror: schema_mirror.clone(),
    });
    let derived_store = Arc::new(
        DerivedTagStore::open(&cfg.paths.config_db_path())
            .context("failed to open derived tag store")?,
    );
    let derived_engine_arc = Arc::new(DerivedTagEngine::new(
        derived_snapshot,
        derived_writer,
        derived_schema,
        config_handle.clone(),
        Some(derived_store),
    ));
    let derived_engine = Arc::new(tokio::sync::RwLock::new(derived_engine_arc.clone()));
    {
        let de = derived_engine.read().await;
        if let Err(e) = de.restore_definitions() {
            tracing::warn!(error = %e, "failed to restore derived tag definitions from config.db");
        }
    }
    fanout.set_derived(derived_engine.clone());
    *derived_slot.write() = Some(derived_engine.clone());
    tracing::info!("Derived Tag Engine initialized");

    // derived 去抖 flusher。
    {
        let pre_flush: Arc<dyn Fn() + Send + Sync> = {
            let sf = snapshot_flusher.clone();
            Arc::new(move || sf.flush_now())
        };
        let derived_flusher = Arc::new(DerivedFlusher::new(
            derived_engine_arc.clone(),
            config_handle.clone(),
            Some(pre_flush),
        ));
        tokio::spawn(async move { derived_flusher.run().await; });
    }
    tracing::info!("DerivedFlusher started (debounce interval-based, pre-flush snapshot)");

    // ═══ 9. 可观测性（5101 metrics）═══
    tracing::info!("initializing Observe subsystem...");
    let observe_engine = Arc::new(ObserveEngine::new(cfg.observe.clone()));
    match sensearcdb_observe::serve::serve_metrics(observe_engine.clone()).await {
        Ok(_handle) => {
            tracing::info!("Observe metrics server (5101) started");
        }
        Err(e) => {
            tracing::warn!(error = %e, "failed to start observe metrics server (5101), continuing");
        }
    }
    tracing::info!("Observe subsystem initialized");

    // 6. 汇总
    tracing::info!("All subsystems initialized successfully");
    drop(cfg); // release config guard

    Ok(AppState {
        config: config_handle,
        schema_mgr,
        schema_mirror,
        engine,
        snapshot_store,
        snapshot_checkpointer,
        replay_engine,
        snapshot_registry,
        engine_metrics,
        partition_manager,
        cq_engine: Some(cq_engine),
        stream_engine,
        derived_engine_arc: Some(derived_engine_arc),
        tenant_manager,
        quota_store,
        observe_engine: Some(observe_engine),
        process_lock,
        snapshot_subscriptions: sub_mgr,
        partition_scheduler_shutdown: Some(Arc::new(scheduler_tx)),
    })
}
