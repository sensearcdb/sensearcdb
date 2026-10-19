//! 崩溃恢复路径 — 三层恢复策略。
//!
//! ## 三层恢复优先级
//!
//! 1. **snapshot.bin 优先**：如果存在并校验通过，直接反序列化，最快恢复路径。
//! 2. **SAC 全量扫描兜底**：snapshot.bin 不存在或损坏时，扫描 SAC 文件重建快照。
//! 3. **WAL 增量重放**：从 snapshot.bin 记录的版本号之后的 WAL 帧重放。
//!
//! ## 恢复流程
//!
//! ```text
//! 启动
//!   │
//!   ├─ snapshot.bin 存在且有效？
//!   │   ├─ Yes → 反序列化 → 记录版本号 seq_v
//!   │   └─ No  → SAC 全量扫描 → 构建 SnapshotStore → seq_v = SAC 最新 seq
//!   │
//!   └─ WAL 增量重放
//!       └─ 从 seq > seq_v 的帧回放至最新
//!          (schema_version 不匹配 → skip + log，绝不断路)
//! ```
//!
//! ## WAL 重放策略
//!
//! 对齐 §7.8.3: WAL 重放中 `schema_version` 不匹配的帧
//! **只 skip + log，绝不 return Err**。
//!
//! ## Trait 抽象
//!
//! 为避免与 `sensearcdb-engine` 的交叉依赖，恢复路径通过
//! [`SacRecoveryReader`] 和 [`WalReplayReader`] trait 注入外部读取器。
//! T05 集成时由上层 `main.rs` 注入 engine crate 的真实实现。

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Instant;
use tracing;

use crate::columnar::{ColumnarShard, PendingPoint};
use crate::encryption::{SharedSnapshotEncryptor, SnapshotEncryptor};
use crate::entry::SnapshotEntry;
use crate::metrics::SnapshotMetrics;
use crate::serialize;
use crate::store::SnapshotStore;
use crate::subscription::SnapshotSubscriptionManager;
use crate::tag_metadata::TagMetadataStore;
use sensearcdb_common::{SenseArcError, TenantId, TypedValue};

// ─── Recovery Traits (avoid circular dep on sensearcdb-engine) ───

/// SAC 全量扫描结果中的一行数据。
///
/// 每个 `SacScanRow` 对应 SAC 文件中一个 table 在某一时刻的数据点。
/// 恢复时按 `table_id` 分组并取最新 `ts` 作为该 tag 的当前值。
#[derive(Debug, Clone)]
pub struct SacScanRow {
    /// 租户 ID（从 SAC 路径 tenant 目录段解析）。
    pub tenant_id: TenantId,
    /// 目标表 ID (tag id)。
    pub table_id: u32,
    /// 源时间戳（epoch 纳秒）。
    pub ts: i64,
    /// 服务器接收时间戳（epoch 纳秒）。
    pub server_ts: i64,
    /// 多列值（column_id 升序）。
    pub values: Vec<(u16, TypedValue)>,
    /// 多列质量码（与 values 对齐）。
    pub qualities: Vec<(u16, u8)>,
}

/// SAC 全量扫描读取器 trait。
///
/// 崩溃恢复通过此 trait 扫描全部 SAC 文件以重建快照，
/// 避免对 `sensearcdb-engine` crate 的编译期交叉依赖。
///
/// ## T05 集成
///
/// 上层 `main.rs` 注入 Engine 的 `SacReader` 实现：
/// 遍历所有 partition 的 SAC 文件，解码每个 Block，
/// 返回所有 `(table_id, ts, value, quality)` 行。
pub trait SacRecoveryReader: Send + Sync {
    /// 扫描全部 SAC 文件，返回所有数据行。
    ///
    /// 调用方负责按 `table_id` 分组并取最新 `ts` 作为当前快照值。
    fn scan_all(&self) -> Result<Vec<SacScanRow>, RecoveryError>;
}

/// WAL 增量重放中的单条记录。
///
/// 从 WAL 帧解码后的数据点，用于逐条回放到 SnapshotStore。
#[derive(Debug, Clone)]
pub struct WalReplayRecord {
    /// 租户 ID（WAL payload 已含 tenant，S1 落盘）。
    pub tenant_id: TenantId,
    /// 目标表 ID (tag id)。
    pub table_id: u32,
    /// 源时间戳（epoch 纳秒）。
    pub ts: i64,
    /// 服务器接收时间戳（epoch 纳秒）。
    pub server_ts: i64,
    /// WAL2 帧的单机自增 seq（`raft_index`，`raft_term=0`）。
    ///
    /// 3.3 档 A：恢复边界从「时间水位」切「精确 seq」，相同 server_ts 的多个 batch
    /// 也能按 seq 严格区分（`seq > snapshot_seq` 才回放，不丢不重）。
    pub seq: u64,
    /// 多列值（column_id 升序）。
    pub values: Vec<(u16, TypedValue)>,
    /// 多列质量码（与 values 对齐）。
    pub qualities: Vec<(u16, u8)>,
    /// Schema 版本号（用于 schema_version 不匹配检测）。
    pub schema_version: u32,
}

/// WAL 增量重放读取器 trait。
///
/// 崩溃恢复通过此 trait 读取指定 seq 之后的 WAL 记录，
/// 避免对 `sensearcdb-engine` crate 的编译期交叉依赖。
///
/// ## 3.3 档 A 语义
///
/// `after_seq` 为 snapshot.bin 里记录的 WAL2 `raft_index`（单机自增 seq）。
/// 读取器只返回 `raft_index > after_seq` 的记录，实现精确边界对齐
/// （不再使用 server_ts 时间水位 + 安全余量）。
///
/// ## T05 集成
///
/// 上层 `main.rs` 注入 Engine 的 `WalReader` 实现：
/// 按段序号遍历 WAL 段文件，解码每个帧为 `WalReplayRecord` 列表。
pub trait WalReplayReader: Send + Sync {
    /// 读取 seq 严格大于 `after_seq` 的所有 WAL 记录。
    ///
    /// 返回按 seq 升序排列的记录列表。
    fn read_after(&self, after_seq: u64) -> Result<Vec<WalReplayRecord>, RecoveryError>;
}

/// No-op SAC 恢复读取器。
///
/// 所有扫描返回空结果。用于：
/// - 单元测试（不需要真实 SAC 文件）
/// - 未集成 engine crate 时的默认行为
#[derive(Debug, Clone)]
pub struct NoopSacRecoveryReader;

impl SacRecoveryReader for NoopSacRecoveryReader {
    fn scan_all(&self) -> Result<Vec<SacScanRow>, RecoveryError> {
        tracing::debug!("NoopSacRecoveryReader: returning empty scan");
        Ok(Vec::new())
    }
}

/// No-op WAL 重放读取器。
///
/// 所有读取返回空结果。用于：
/// - 单元测试（不需要真实 WAL 文件）
/// - 未集成 engine crate 时的默认行为
#[derive(Debug, Clone)]
pub struct NoopWalReplayReader;

impl WalReplayReader for NoopWalReplayReader {
    fn read_after(&self, _after_seq: u64) -> Result<Vec<WalReplayRecord>, RecoveryError> {
        tracing::debug!("NoopWalReplayReader: returning empty records");
        Ok(Vec::new())
    }
}

/// SAC 读取器包装（Arc trait object）。
pub type SacRecoveryReaderRef = Arc<dyn SacRecoveryReader>;

/// WAL 读取器包装（Arc trait object）。
pub type WalReplayReaderRef = Arc<dyn WalReplayReader>;

// ─── Recovery Result Types ───

/// 恢复结果。
#[derive(Debug)]
pub struct RecoveryResult {
    /// 恢复的 SnapshotStore。
    pub store: SnapshotStore,
    /// 恢复来源。
    pub source: RecoverySource,
    /// 恢复耗时（秒）。
    pub duration_secs: f64,
    /// 恢复的条目数。
    pub entry_count: usize,
}

/// 恢复来源。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RecoverySource {
    /// 从 snapshot.bin 恢复。
    SnapshotBin,
    /// 从 SAC 全量扫描恢复。
    SacScan,
    /// 从 WAL 恢复（无 snapshot.bin 且无 SAC）。
    WalOnly,
}

/// 恢复配置。
#[derive(Debug, Clone)]
pub struct RecoveryConfig {
    /// snapshot.bin 文件路径。
    pub snapshot_path: PathBuf,
    /// 分片数量（从 SnapshotConfig 读取）。
    pub shard_count: usize,
    /// SAC 落盘水位 `S`（数据 seq）。
    ///
    /// 恢复层 2（SAC 全量扫描兜底）从 `read_after(S)` 开始回放 WAL 增量，
    /// 而非硬编码 `read_after(0)` 全量回放（C4）。`0` = 无水位（兼容旧数据 /
    /// 首次启动），等价全量回放。
    pub sac_watermark: u64,
}

// ─── Public API ───

/// 执行三层恢复。
///
/// # Arguments
///
/// - `config`: 恢复配置。
/// - `subscription_manager`: 订阅管理器。
/// - `metadata`: 标签元数据存储。
/// - `metrics`: Prometheus 指标。
/// - `sac_reader`: SAC 全量扫描读取器（可选，None 时 SAC 路径返回空）。
/// - `wal_reader`: WAL 增量重放读取器（可选，None 时 WAL 路径返回空）。
/// - `snapshot_encryptor`: snapshot.bin 解密器（可选，None = TDE 关闭；密文快照无密钥硬报错）。
///
/// # Returns
///
/// `RecoveryResult` 或 `RecoveryError`。
pub fn recover(
    config: &RecoveryConfig,
    subscription_manager: Arc<SnapshotSubscriptionManager>,
    metadata: Arc<TagMetadataStore>,
    metrics: SnapshotMetrics,
    sac_reader: Option<SacRecoveryReaderRef>,
    wal_reader: Option<WalReplayReaderRef>,
    snapshot_encryptor: SharedSnapshotEncryptor,
) -> Result<RecoveryResult, RecoveryError> {
    let start = Instant::now();
    let snapshot_path = &config.snapshot_path;

    let (store, source) = recover_inner(
        snapshot_path,
        config,
        subscription_manager,
        metadata,
        metrics.clone(),
        &sac_reader,
        &wal_reader,
        &snapshot_encryptor,
    )?;

    let elapsed = start.elapsed().as_secs_f64();
    metrics
        .recovery_duration
        .with_label_values(&[])
        .observe(elapsed);

    let entry_count = store.total_entries();
    tracing::info!(
        source = ?source,
        entry_count = entry_count,
        duration_secs = elapsed,
        "snapshot recovery complete"
    );

    Ok(RecoveryResult {
        store,
        source,
        duration_secs: elapsed,
        entry_count,
    })
}

// ─── Internal Recovery Logic ───

/// 内部恢复逻辑。
fn recover_inner(
    snapshot_path: &Path,
    config: &RecoveryConfig,
    subscription_manager: Arc<SnapshotSubscriptionManager>,
    metadata: Arc<TagMetadataStore>,
    metrics: SnapshotMetrics,
    sac_reader: &Option<SacRecoveryReaderRef>,
    wal_reader: &Option<WalReplayReaderRef>,
    snapshot_encryptor: &SharedSnapshotEncryptor,
) -> Result<(SnapshotStore, RecoverySource), RecoveryError> {
    // ─── 第一层: snapshot.bin 优先 ───
    if snapshot_path.exists() {
        match try_recover_from_snapshot(
            config,
            subscription_manager.clone(),
            metadata.clone(),
            metrics.clone(),
            snapshot_encryptor,
        ) {
            Ok((store, _version, wal_watermark)) => {
                // 快照 + WAL 增量合并恢复（GAP-SNP-07），按时间水位对齐回放边界。
                apply_wal_increments(&store, wal_reader, wal_watermark)?;
                tracing::info!("recovered from snapshot.bin (+ WAL increments)");
                return Ok((store, RecoverySource::SnapshotBin));
            }
            Err(e) => {
                // TDE：密文快照无密钥必须硬报错，禁止回退到 SAC（绝不把密文当明文解析）。
                if e.is_no_encryptor() {
                    tracing::error!(
                        error = %e,
                        path = %snapshot_path.display(),
                        "encrypted snapshot.bin but no TDE key; refusing to fall back"
                    );
                    return Err(e);
                }
                tracing::warn!(
                    path = %snapshot_path.display(),
                    reason = "corrupt_or_unreadable",
                    error = %e,
                    "snapshot.bin recovery failed, falling back to SAC full rescan (slow recovery path)"
                );
            }
        }
    } else {
        tracing::warn!(
            path = %snapshot_path.display(),
            reason = "missing",
            "snapshot.bin not found, falling back to SAC full rescan (slow recovery path)"
        );
    }

    // ─── 第二层: SAC 全量扫描兜底 ───
    match try_recover_from_sac(
        config,
        subscription_manager.clone(),
        metadata.clone(),
        metrics.clone(),
        sac_reader,
    ) {
        Ok(store) => {
            // C4（I3）：SAC 落盘水位 S 已覆盖「已 flush 进 SAC」的数据，从 read_after(S)
            // 回放 WAL 增量（而非硬编码 0 全量回放）；S=0 等价全量回放（兼容旧数据）。
            apply_wal_increments(&store, wal_reader, config.sac_watermark as i64)?;
            tracing::info!("recovered from SAC scan (+ WAL increments)");
            return Ok((store, RecoverySource::SacScan));
        }
        Err(e) => {
            tracing::warn!(
                error = %e,
                "SAC scan recovery failed, falling back to WAL-only"
            );
        }
    }

    // ─── 第三层: WAL 增量重放 ───
    // WAL-only 路径：无 snapshot 也无 SAC，从 epoch 0 开始读取全部 WAL
    let store = try_recover_wal_only(
        config,
        subscription_manager,
        metadata,
        metrics,
        wal_reader,
        0,
    )?;
    tracing::info!("recovered from WAL-only");
    Ok((store, RecoverySource::WalOnly))
}

// ─── Layer 1: snapshot.bin ───

/// 尝试从 snapshot.bin 恢复。
///
/// 返回 `(store, global_version, wal_watermark)`。
fn try_recover_from_snapshot(
    config: &RecoveryConfig,
    subscription_manager: Arc<SnapshotSubscriptionManager>,
    metadata: Arc<TagMetadataStore>,
    metrics: SnapshotMetrics,
    snapshot_encryptor: &SharedSnapshotEncryptor,
) -> Result<(SnapshotStore, u64, i64), RecoveryError> {
    let encryptor: Option<&dyn SnapshotEncryptor> = snapshot_encryptor.as_deref();
    let (shards, global_version, wal_watermark) = serialize::deserialize_from_file(
        &config.snapshot_path,
        config.shard_count,
        &metrics,
        encryptor,
    )?;

    let store = SnapshotStore::from_buckets(
        shards,
        global_version,
        subscription_manager,
        metadata,
        metrics,
    );

    Ok((store, global_version, wal_watermark))
}

// ─── Layer 2: SAC Full Scan ───

/// 尝试从 SAC 全量扫描恢复。
///
/// 通过 [`SacRecoveryReader`] trait 扫描全部 SAC 文件，
/// 对每个 tag 取最新时间戳的值作为当前快照状态。
///
/// 当 `sac_reader` 为 `None` 时，使用内部 `NoopSacRecoveryReader` 返回空结果，
/// 使调用方可以回退到下一层恢复路径。
fn try_recover_from_sac(
    config: &RecoveryConfig,
    subscription_manager: Arc<SnapshotSubscriptionManager>,
    metadata: Arc<TagMetadataStore>,
    metrics: SnapshotMetrics,
    sac_reader: &Option<SacRecoveryReaderRef>,
) -> Result<SnapshotStore, RecoveryError> {
    // 解析读取器：None 时返回 Err，让调用方回退到下一层（WAL）
    let reader = match sac_reader {
        Some(reader) => reader,
        None => {
            tracing::info!("no SAC reader configured, falling back to WAL");
            return Err(RecoveryError::Other("no SAC reader configured".into()));
        }
    };

    let rows: Vec<SacScanRow> = reader.scan_all()?;

    // 按 (tenant_id, table_id, column_id) 列级归并（S2：不跨租户合并）：
    // 每列独立取最新 ts 的四元组，避免稀疏宽表重放时整行覆盖丢列（G2）。
    // 使用严格 `>` 保证「相同 ts 时先到者胜」（与旧整行去重语义一致）。
    #[derive(Default)]
    struct ColumnAcc {
        values: HashMap<u16, TypedValue>,
        qualities: HashMap<u16, u8>,
        ts: HashMap<u16, i64>,
        server_ts: HashMap<u16, i64>,
    }

    let mut accs: HashMap<(TenantId, u32), ColumnAcc> =
        HashMap::with_capacity(rows.len().min(1024));

    for row in &rows {
        let key = (row.tenant_id, row.table_id);
        let acc = accs.entry(key).or_default();
        for (cid, value) in row.values.iter() {
            let quality = row
                .qualities
                .iter()
                .find(|(qc, _)| qc == cid)
                .map(|(_, q)| *q)
                .unwrap_or(0xC0);
            // 列级最新 ts 胜出（严格 >，先到者胜于相同 ts）。
            let overwrite = match acc.ts.get(cid) {
                Some(existing_ts) => row.ts > *existing_ts,
                None => true,
            };
            if overwrite {
                acc.values.insert(*cid, value.clone());
                acc.qualities.insert(*cid, quality);
                acc.ts.insert(*cid, row.ts);
                acc.server_ts.insert(*cid, row.server_ts);
            }
        }
    }

    let dedup_count = accs.len();
    tracing::info!(
        sac_rows = rows.len(),
        dedup_tags = dedup_count,
        "SAC scan complete (column-level merge), building SnapshotStore"
    );

    // 构建分片
    let mut shards: Vec<ColumnarShard> = (0..config.shard_count)
        .map(|_| ColumnarShard::new())
        .collect();

    let mut version_counter: u64 = 0;
    for ((tenant_id, table_id), acc) in accs {
        version_counter += 1;
        let shard_idx = fxhash::hash(&table_id) as usize % config.shard_count;

        let mut values: Vec<(u16, TypedValue)> = acc.values.into_iter().collect();
        values.sort_by_key(|(c, _)| *c);
        let mut qualities: Vec<(u16, u8)> = acc.qualities.into_iter().collect();
        qualities.sort_by_key(|(c, _)| *c);
        let mut ts_values: Vec<(u16, i64)> = acc.ts.into_iter().collect();
        ts_values.sort_by_key(|(c, _)| *c);
        let mut server_ts_values: Vec<(u16, i64)> = acc.server_ts.into_iter().collect();
        server_ts_values.sort_by_key(|(c, _)| *c);

        let ts = ts_values.iter().map(|(_, t)| *t).max().unwrap_or(0);
        let server_ts = server_ts_values.iter().map(|(_, t)| *t).max().unwrap_or(0);

        let entry = SnapshotEntry {
            tenant_id,
            values,
            qualities,
            ts,
            server_ts,
            version: version_counter,
            prev_value: None,
            prev_ts: None,
            is_derived: false,
            writer_session_id: None,
            ttl_expired: false,
            ts_values,
            server_ts_values,
            // SAC 扫描行无逐点 seq → None（空 Vec = 全列 None）。
            write_seqs: Vec::new(),
        };
        shards[shard_idx].upsert_from_entry(table_id, entry);
    }

    let store = SnapshotStore::from_buckets(
        shards,
        version_counter,
        subscription_manager,
        metadata,
        metrics,
    );

    tracing::info!(
        entry_count = store.total_entries(),
        version = version_counter,
        "SAC recovery SnapshotStore built"
    );

    Ok(store)
}

// ─── Layer 3: WAL Incremental Replay ───

/// 尝试从 WAL 增量重放恢复。
///
/// 通过 [`WalReplayReader`] trait 读取 `after_seq`（精确 WAL2 seq）之后的所有 WAL 记录，
/// 逐条回放到 SnapshotStore。
///
/// ## schema_version 不匹配处理
///
/// 对齐 §7.8.3：schema_version 不匹配的帧 **只 skip + log，绝不 return Err**。
/// 恢复是尽力路径，不能因为单个帧的 schema 问题中断整个流程。
///
/// 当 `wal_reader` 为 `None` 时，使用内部 `NoopWalReplayReader` 返回空结果。
fn try_recover_wal_only(
    config: &RecoveryConfig,
    subscription_manager: Arc<SnapshotSubscriptionManager>,
    metadata: Arc<TagMetadataStore>,
    metrics: SnapshotMetrics,
    wal_reader: &Option<WalReplayReaderRef>,
    after_seq: u64,
) -> Result<SnapshotStore, RecoveryError> {
    // 解析读取器：None 时使用 Noop（返回空）
    let records: Vec<WalReplayRecord> = match wal_reader {
        Some(reader) => reader.read_after(after_seq)?,
        None => {
            tracing::info!("no WAL reader configured, WAL replay returns empty");
            Vec::new()
        }
    };

    // ─── schema_version 校验：只 skip + log，绝不 return Err ───
    // WAL 重放是尽力恢复路径。即使 schema_version 完全无法识别，
    // 也只记录日志并跳过该帧，继续处理后续帧。
    for record in &records {
        if record.schema_version == 0 {
            // schema_version = 0 表示未设置或损坏，仍然尽力回放
            tracing::debug!(
                table_id = record.table_id,
                ts = record.ts,
                "WAL record with schema_version=0, replaying anyway"
            );
        }
    }

    // S4：构建空 store，一次性批量回放（不再逐点 merge_columns_typed + upsert_from_entry）。
    // version 由 update_batch 内 global_version.next() 分配，语义从「逐 record 递增」
    // 变为「变化条目数」，可接受（version 仅用于脏标记 + notify）。
    let store = SnapshotStore::from_buckets(
        (0..config.shard_count).map(|_| ColumnarShard::new()).collect(),
        0,
        subscription_manager,
        metadata,
        metrics,
    );
    let points = wal_records_to_pending(&records);
    store.update_batch(&points);

    tracing::info!(
        wal_records = records.len(),
        entry_count = store.total_entries(),
        after_seq = after_seq,
        "WAL replay SnapshotStore built"
    );

    Ok(store)
}

// ─── WAL 增量回放（合并到已有 SnapshotStore） ───

/// 把 WAL 重放记录批量转为 `PendingPoint`（写路径批内元素）。
///
/// 每列 `write_seq = Some(record.seq)`、`is_derived = false`、`writer_session_id = None`，
/// 直接借用 `record.values` / `record.qualities`。
fn wal_records_to_pending(records: &[WalReplayRecord]) -> Vec<PendingPoint<'_>> {
    records
        .iter()
        .map(|r| PendingPoint {
            tenant_id: r.tenant_id,
            table_id: r.table_id,
            values: &r.values,
            qualities: &r.qualities,
            ts: r.ts,
            server_ts: r.server_ts,
            write_seq: Some(r.seq),
            is_derived: false,
            writer_session_id: None,
        })
        .collect()
}

/// 在已恢复的 SnapshotStore 上叠加 WAL 增量。
///
/// 通过 [`WalReplayReader`] 读取 `wal_watermark`（精确 seq，严格 `> seq`）之后的 WAL
/// 记录，批量 `store.update_batch()` 回放。由于 SnapshotStore 语义为「每 tag 最新值」，
/// 列级 ts max 保证最终状态正确。
///
/// ## 3.3 档 A 语义
///
/// `wal_watermark` 存的是 WAL2 `raft_index`（单机自增 seq）。切 seq 后**不再需要
/// 时间安全余量**——seq 精确单调，`seq > watermark` 即回放、`seq <= watermark` 即跳过，
/// 相同 server_ts 的多 batch 也能按 seq 严格区分，杜绝「丢/重」。
///
/// `wal_watermark = 0` 表示无水位（SAC / WAL-only 路径或旧文件），等价于全量回放。
fn apply_wal_increments(
    store: &SnapshotStore,
    wal_reader: &Option<WalReplayReaderRef>,
    wal_watermark: i64,
) -> Result<(), RecoveryError> {
    let Some(reader) = wal_reader else {
        tracing::info!("no WAL reader configured, skipping WAL increment replay");
        return Ok(());
    };

    // 精确 seq 边界：snapshot.bin 的 wal_watermark 即为已包含的最大 WAL2 seq。
    let after_seq = wal_watermark.max(0) as u64;
    let records = reader.read_after(after_seq)?;
    if records.is_empty() {
        return Ok(());
    }

    // S4：批量回放（不再逐点 update_typed）。
    let points = wal_records_to_pending(&records);
    store.update_batch(&points);

    tracing::info!(
        wal_records = records.len(),
        wal_watermark = wal_watermark,
        replay_after_seq = after_seq,
        "applied WAL increments onto recovered snapshot"
    );

    Ok(())
}

// ─── 恢复错误 ───

/// 恢复过程中的错误。
#[derive(Debug, thiserror::Error)]
pub enum RecoveryError {
    #[error("snapshot file not found: {0}")]
    SnapshotNotFound(String),
    #[error("serialization error: {0}")]
    Serialize(#[from] serialize::SerializeError),
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
    #[error("recovery failed: {0}")]
    Other(String),
}

impl RecoveryError {
    /// 是否为「密文快照但无密钥」错误（用于恢复路径硬报错、禁止回退到 SAC）。
    pub fn is_no_encryptor(&self) -> bool {
        match self {
            RecoveryError::Serialize(e) => e.is_no_encryptor(),
            _ => false,
        }
    }
}

/// 恢复错误 → 统一错误收敛（内部基础设施错误）。
impl From<RecoveryError> for SenseArcError {
    fn from(e: RecoveryError) -> Self {
        SenseArcError::Internal(e.to_string())
    }
}

// ─── Tests ───

#[cfg(test)]
mod tests {
    use super::*;
    use crate::subscription::SnapshotSubscriptionManager;
    use crate::tag_metadata::{SchemaMirrorRef, TagMetadataStore};
    use sensearcdb_common::{DataType, TypedValue};
    use tempfile::tempdir;

    /// 便捷构造：单列 f64 值列表（column_id = 0）。
    fn values_of(value: f64) -> Vec<(u16, TypedValue)> {
        vec![(0, TypedValue::F64(value))]
    }

    /// 便捷构造：单列质量码列表（column_id = 0）。
    fn qualities_of(quality: u8) -> Vec<(u16, u8)> {
        vec![(0, quality)]
    }

    struct TestSchemaMirror;
    impl SchemaMirrorRef for TestSchemaMirror {
        fn get_table_name(&self, _: u32) -> Option<String> {
            Some("test".into())
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

    /// 辅助函数：构建默认的 SnapshotStore
    fn make_sub_mgr() -> Arc<SnapshotSubscriptionManager> {
        Arc::new(SnapshotSubscriptionManager::new())
    }

    fn make_metadata() -> Arc<TagMetadataStore> {
        Arc::new(TagMetadataStore::new(Arc::new(TestSchemaMirror)))
    }

    fn make_metrics() -> SnapshotMetrics {
        SnapshotMetrics::noop()
    }

    // ─── No-reader tests (backward compatible: readers = None) ───

    #[test]
    fn test_recover_no_snapshot() {
        let dir = tempdir().unwrap();
        let config = RecoveryConfig {
            snapshot_path: dir.path().join("nonexistent.bin"),
            shard_count: 4,
            sac_watermark: 0,
        };

        let result = recover(
            &config,
            make_sub_mgr(),
            make_metadata(),
            make_metrics(),
            None,
            None,
            None,
        );
        // Should fall through SAC (noop → empty) → WAL (noop → empty)
        assert!(result.is_ok());
        let rr = result.unwrap();
        assert_eq!(rr.entry_count, 0);
    }

    #[test]
    fn test_recover_from_snapshot_bin() {
        use crate::serialize;
        use crate::store::SnapshotStore;
        use sensearcdb_common::ConfigHandle;

        let dir = tempdir().unwrap();
        let snapshot_path = dir.path().join("recovery_test.bin");
        let shard_count = 4;

        // Build a store, populate it, then serialize to snapshot.bin
        let cfg = ConfigHandle::new(sensearcdb_common::AppConfig::default());
        let sub_mgr = Arc::new(SnapshotSubscriptionManager::new());
        let metadata = Arc::new(TagMetadataStore::new(Arc::new(TestSchemaMirror)));
        let metrics = SnapshotMetrics::noop();

        // Create a store and populate it with 100 entries
        {
            let store =
                SnapshotStore::new(&cfg, sub_mgr.clone(), metadata.clone(), metrics.clone());
            for i in 1u32..=100u32 {
                store.update_typed(
                    sensearcdb_common::TenantId::DEFAULT,
                    i,
                    &[(0, TypedValue::F64(i as f64))],
                    &[(0, 0xC0)],
                    1000 + i as i64,
                    2000 + i as i64,
                    None,
                    false,
                    None,
                );
            }
            // Snapshot to file
            let (guards, version) = store.snapshot_all_buckets();
            serialize::serialize_to_file(&snapshot_path, &guards, version, &metrics, None, 0)
                .expect("should serialize successfully");
        }

        // Now recover from the snapshot.bin
        let config = RecoveryConfig {
            snapshot_path: snapshot_path.clone(),
            shard_count,
            sac_watermark: 0,
        };

        let result = recover(
            &config,
            Arc::new(SnapshotSubscriptionManager::new()),
            Arc::new(TagMetadataStore::new(Arc::new(TestSchemaMirror))),
            SnapshotMetrics::noop(),
            None,
            None,
            None,
        );

        assert!(result.is_ok());
        let rr = result.unwrap();
        assert_eq!(rr.source, RecoverySource::SnapshotBin);
        assert_eq!(rr.entry_count, 100);
        assert!(rr.duration_secs >= 0.0);

        // Verify recovered data
        let entry = rr.store.get(1).unwrap();
        assert!((entry.value() - 1.0).abs() < 1e-12);
        assert_eq!(entry.quality(), 0xC0);
        assert_eq!(entry.ts, 1001);

        // Verify entry 100
        let entry100 = rr.store.get(100).unwrap();
        assert!((entry100.value() - 100.0).abs() < 1e-12);
    }

    #[test]
    fn test_recover_from_corrupt_snapshot_falls_back() {
        let dir = tempdir().unwrap();
        let snapshot_path = dir.path().join("corrupt.bin");

        // Write garbage data as "snapshot.bin"
        std::fs::write(&snapshot_path, b"not a valid snapshot file").unwrap();

        let config = RecoveryConfig {
            snapshot_path,
            shard_count: 4,
            sac_watermark: 0,
        };

        let result = recover(
            &config,
            make_sub_mgr(),
            make_metadata(),
            make_metrics(),
            None,
            None,
            None,
        );

        // Should fall through to SAC/WAL (noop → empty) — not crash
        assert!(result.is_ok());
        let rr = result.unwrap();
        // With no readers, SAC noop returns empty → SacScan source
        assert_ne!(rr.source, RecoverySource::SnapshotBin);
    }

    #[test]
    fn test_recovery_result_fields() {
        let rr = RecoveryResult {
            store: SnapshotStore::new(
                &sensearcdb_common::ConfigHandle::new(sensearcdb_common::AppConfig::default()),
                Arc::new(SnapshotSubscriptionManager::new()),
                Arc::new(TagMetadataStore::new(Arc::new(TestSchemaMirror))),
                SnapshotMetrics::noop(),
            ),
            source: RecoverySource::SnapshotBin,
            duration_secs: 0.123,
            entry_count: 42,
        };

        assert_eq!(rr.source, RecoverySource::SnapshotBin);
        assert!((rr.duration_secs - 0.123).abs() < 1e-9);
        assert_eq!(rr.entry_count, 42);
    }

    // ─── SAC recovery with mock reader ───

    /// 模拟 SAC 读取器：返回预定义的数据行，用于测试恢复逻辑。
    struct MockSacReader {
        rows: Vec<SacScanRow>,
    }

    impl SacRecoveryReader for MockSacReader {
        fn scan_all(&self) -> Result<Vec<SacScanRow>, RecoveryError> {
            Ok(self.rows.clone())
        }
    }

    #[test]
    fn test_recover_from_sac_with_reader() {
        let dir = tempdir().unwrap();
        let config = RecoveryConfig {
            snapshot_path: dir.path().join("nonexistent.bin"),
            shard_count: 4,
            sac_watermark: 0,
        };

        // Build mock SAC data: 50 tags, some with multiple timestamps (latest wins)
        let mut rows = Vec::new();
        for tag_id in 1u32..=50u32 {
            rows.push(SacScanRow {
                tenant_id: sensearcdb_common::TenantId::DEFAULT,
                table_id: tag_id,
                ts: 1000 + tag_id as i64,
                server_ts: 2000 + tag_id as i64,
                values: values_of(tag_id as f64 * 10.0),
                qualities: qualities_of(0xC0),
            });
            // Older duplicate for tag 1 — should be overwritten by newer ts
            if tag_id == 1 {
                rows.push(SacScanRow {
                    tenant_id: sensearcdb_common::TenantId::DEFAULT,
                    table_id: 1,
                    ts: 500,
                    server_ts: 1500,
                    values: values_of(999.0),
                    qualities: qualities_of(0x00),
                });
            }
        }

        let sac_reader: SacRecoveryReaderRef = Arc::new(MockSacReader { rows });

        let result = recover(
            &config,
            make_sub_mgr(),
            make_metadata(),
            make_metrics(),
            Some(sac_reader),
            None,
            None,
        );

        assert!(result.is_ok());
        let rr = result.unwrap();
        assert_eq!(rr.source, RecoverySource::SacScan);
        // 50 unique tags (dedup applied)
        assert_eq!(rr.entry_count, 50);

        // Tag 1 should have the latest value (ts=1001), not the old one (ts=500)
        let entry = rr.store.get(1).unwrap();
        assert!(
            (entry.value() - 10.0).abs() < 1e-12,
            "expected 10.0, got {}",
            entry.value()
        );
        assert_eq!(entry.quality(), 0xC0);
        assert_eq!(entry.ts, 1001);
    }

    /// S2 Step 5：recover() 重建 SnapshotEntry 时回填 tenant_id（不混租户）。
    #[test]
    fn test_recover_from_sac_preserves_tenant_id() {
        let dir = tempdir().unwrap();
        let config = RecoveryConfig {
            snapshot_path: dir.path().join("nonexistent.bin"),
            shard_count: 4,
            sac_watermark: 0,
        };

        // tenant=0 表 1、tenant=1 表 2（table_id 全局唯一，跨租户不重叠）。
        let rows = vec![
            SacScanRow {
                tenant_id: TenantId::DEFAULT,
                table_id: 1,
                ts: 1000,
                server_ts: 2000,
                values: values_of(1.0),
                qualities: qualities_of(0xC0),
            },
            SacScanRow {
                tenant_id: TenantId(1),
                table_id: 2,
                ts: 3000,
                server_ts: 4000,
                values: values_of(2.0),
                qualities: qualities_of(0xC0),
            },
        ];

        let sac_reader: SacRecoveryReaderRef = Arc::new(MockSacReader { rows });

        let result = recover(
            &config,
            make_sub_mgr(),
            make_metadata(),
            make_metrics(),
            Some(sac_reader),
            None,
            None,
        );

        assert!(result.is_ok());
        let rr = result.unwrap();
        assert_eq!(rr.source, RecoverySource::SacScan);
        assert_eq!(rr.entry_count, 2);

        let e0 = rr.store.get(1).expect("tenant 0 table 1");
        assert_eq!(e0.tenant_id, TenantId::DEFAULT);

        let e1 = rr.store.get(2).expect("tenant 1 table 2");
        assert_eq!(e1.tenant_id, TenantId(1));
    }

    /// G2 恢复列级重建：SAC 稀疏行重放后 entry 含所有列（无丢列）。
    #[test]
    fn test_recover_from_sac_column_level_merge() {
        let dir = tempdir().unwrap();
        let config = RecoveryConfig {
            snapshot_path: dir.path().join("nonexistent.bin"),
            shard_count: 4,
            sac_watermark: 0,
        };

        // 同一 tag 的两条 SAC 行：先写 col0 @ ts=100，再写 col1 @ ts=200。
        // 旧整行替换会丢掉 col0；列级 merge 后应保留两列。
        let rows = vec![
            SacScanRow {
                tenant_id: TenantId::DEFAULT,
                table_id: 7,
                ts: 100,
                server_ts: 1000,
                values: vec![(0, TypedValue::F64(1.0))],
                qualities: vec![(0, 0xC0)],
            },
            SacScanRow {
                tenant_id: TenantId::DEFAULT,
                table_id: 7,
                ts: 200,
                server_ts: 2000,
                values: vec![(1, TypedValue::I64(42))],
                qualities: vec![(1, 0x00)],
            },
        ];

        let sac_reader: SacRecoveryReaderRef = Arc::new(MockSacReader { rows });

        let result = recover(
            &config,
            make_sub_mgr(),
            make_metadata(),
            make_metrics(),
            Some(sac_reader),
            None,
            None,
        );

        assert!(result.is_ok());
        let rr = result.unwrap();
        assert_eq!(rr.source, RecoverySource::SacScan);
        assert_eq!(rr.entry_count, 1);

        let entry = rr.store.get(7).unwrap();
        assert_eq!(entry.values.len(), 2, "稀疏 SAC 重建不得丢列");
        assert_eq!(entry.values[0], (0, TypedValue::F64(1.0)));
        assert_eq!(entry.values[1], (1, TypedValue::I64(42)));
        assert_eq!(entry.qualities, vec![(0, 0xC0), (1, 0x00)]);
        assert_eq!(entry.ts_values, vec![(0, 100), (1, 200)]);
        assert_eq!(entry.ts, 200);
    }

    #[test]
    fn test_sac_dedup_same_ts_first_wins() {
        // When two rows have the same ts, the first one encountered wins
        // because dedup uses `>` (strict greater), not `>=`
        let rows = vec![
            SacScanRow {
                tenant_id: sensearcdb_common::TenantId::DEFAULT,
                table_id: 42,
                ts: 100,
                server_ts: 200,
                values: values_of(1.0),
                qualities: qualities_of(0xC0),
            },
            SacScanRow {
                tenant_id: sensearcdb_common::TenantId::DEFAULT,
                table_id: 42,
                ts: 100, // same ts as first
                server_ts: 300,
                values: values_of(2.0),
                qualities: qualities_of(0x00),
            },
        ];

        let mut latest: HashMap<u32, (Vec<(u16, TypedValue)>, Vec<(u16, u8)>, i64, i64)> =
            HashMap::new();
        for row in &rows {
            let entry = latest.entry(row.table_id).or_insert_with(|| {
                (
                    row.values.clone(),
                    row.qualities.clone(),
                    row.ts,
                    row.server_ts,
                )
            });
            // `>` means equal ts does NOT overwrite
            if row.ts > entry.2 {
                *entry = (
                    row.values.clone(),
                    row.qualities.clone(),
                    row.ts,
                    row.server_ts,
                );
            }
        }

        let (values, qualities, ts, _server_ts) = latest[&42].clone();
        assert!(
            (values[0].1.as_f64() - 1.0).abs() < 1e-12,
            "same ts: first value should win, got {:?}",
            values
        );
        assert_eq!(
            qualities,
            qualities_of(0xC0),
            "same ts: first quality should win"
        );
        assert_eq!(ts, 100);
    }

    #[test]
    fn test_sac_dedup_latest_wins() {
        // Direct test of the dedup logic: multiple rows for same tag, latest ts wins
        let rows = vec![
            SacScanRow {
                tenant_id: sensearcdb_common::TenantId::DEFAULT,
                table_id: 42,
                ts: 100,
                server_ts: 200,
                values: values_of(1.0),
                qualities: qualities_of(0xC0),
            },
            SacScanRow {
                tenant_id: sensearcdb_common::TenantId::DEFAULT,
                table_id: 42,
                ts: 300,
                server_ts: 400,
                values: values_of(3.0),
                qualities: qualities_of(0x00),
            },
            SacScanRow {
                tenant_id: sensearcdb_common::TenantId::DEFAULT,
                table_id: 42,
                ts: 200,
                server_ts: 300,
                values: values_of(2.0),
                qualities: qualities_of(0x80),
            },
        ];

        let mut latest: HashMap<u32, (Vec<(u16, TypedValue)>, Vec<(u16, u8)>, i64, i64)> =
            HashMap::new();
        for row in &rows {
            let entry = latest.entry(row.table_id).or_insert_with(|| {
                (
                    row.values.clone(),
                    row.qualities.clone(),
                    row.ts,
                    row.server_ts,
                )
            });
            if row.ts > entry.2 {
                *entry = (
                    row.values.clone(),
                    row.qualities.clone(),
                    row.ts,
                    row.server_ts,
                );
            }
        }

        let (values, qualities, ts, _server_ts) = latest[&42].clone();
        assert!(
            (values[0].1.as_f64() - 3.0).abs() < 1e-12,
            "latest value should be 3.0"
        );
        assert_eq!(qualities, qualities_of(0x00));
        assert_eq!(ts, 300);
    }

    // ─── WAL recovery with mock reader ───

    /// 模拟 WAL 读取器：返回预定义的记录，用于测试恢复逻辑。
    struct MockWalReader {
        records: Vec<WalReplayRecord>,
    }

    impl WalReplayReader for MockWalReader {
        fn read_after(&self, after_seq: u64) -> Result<Vec<WalReplayRecord>, RecoveryError> {
            // Filter by seq to simulate real WAL reader behavior（3.3 档 A 精确边界）。
            let filtered: Vec<WalReplayRecord> = self
                .records
                .iter()
                .filter(|r| r.seq > after_seq)
                .cloned()
                .collect();
            Ok(filtered)
        }
    }

    #[test]
    fn test_recover_wal_only_with_reader() {
        let dir = tempdir().unwrap();
        let config = RecoveryConfig {
            snapshot_path: dir.path().join("nonexistent.bin"),
            shard_count: 4,
            sac_watermark: 0,
        };

        // Build mock WAL data: 4 records, including some duplicates
        let records = vec![
            WalReplayRecord {
                tenant_id: sensearcdb_common::TenantId::DEFAULT,
                table_id: 1,
                ts: 100,
                server_ts: 200,
                seq: 1,
                values: values_of(10.0),
                qualities: qualities_of(0xC0),
                schema_version: 1,
            },
            WalReplayRecord {
                tenant_id: sensearcdb_common::TenantId::DEFAULT,
                table_id: 2,
                ts: 200,
                server_ts: 300,
                seq: 2,
                values: values_of(20.0),
                qualities: qualities_of(0xC0),
                schema_version: 1,
            },
            WalReplayRecord {
                tenant_id: sensearcdb_common::TenantId::DEFAULT,
                table_id: 1,
                ts: 300,
                server_ts: 400,
                seq: 3,
                values: values_of(15.0),
                qualities: qualities_of(0x00),
                schema_version: 1,
            },
            WalReplayRecord {
                tenant_id: sensearcdb_common::TenantId::DEFAULT,
                table_id: 3,
                ts: 400,
                server_ts: 500,
                seq: 4,
                values: values_of(30.0),
                qualities: qualities_of(0xC0),
                schema_version: 1,
            },
        ];

        let wal_reader: WalReplayReaderRef = Arc::new(MockWalReader { records });

        let result = recover(
            &config,
            make_sub_mgr(),
            make_metadata(),
            make_metrics(),
            None,
            Some(wal_reader),
            None,
        );

        assert!(result.is_ok());
        let rr = result.unwrap();
        assert_eq!(rr.source, RecoverySource::WalOnly);
        // 3 unique tags (tag 1 appears twice, last write wins)
        assert_eq!(rr.entry_count, 3);

        // Tag 1: last write wins → value=15.0, quality=0x00
        let entry1 = rr.store.get(1).unwrap();
        assert!((entry1.value() - 15.0).abs() < 1e-12);
        assert_eq!(entry1.quality(), 0x00);
        assert_eq!(entry1.ts, 300);

        // Tag 2
        let entry2 = rr.store.get(2).unwrap();
        assert!((entry2.value() - 20.0).abs() < 1e-12);

        // Tag 3
        let entry3 = rr.store.get(3).unwrap();
        assert!((entry3.value() - 30.0).abs() < 1e-12);
    }

    /// ⑦ 三路径一致性：同一批乱序数据，在线 update_typed、SAC 重建、WAL-only 重建
    /// 三者得到完全相同的列级 ts max 终态。
    #[test]
    fn test_three_path_consistency_out_of_order() {
        use sensearcdb_common::ConfigHandle;

        let dir = tempdir().unwrap();
        let snapshot_path = dir.path().join("nonexistent.bin");

        // 规范数据（table 7）：
        //   A: col0 @ ts=200 = 2.0（后写 col0 @ ts=100 = 1.0 应被拒）
        //   B: col1 @ ts=300 = 3.0（后写 col1 @ ts=150 = 1.5 应被拒）
        // ts max 终态：col0=2.0@200、col1=3.0@300。
        let expected_values = vec![(0u16, TypedValue::F64(2.0)), (1u16, TypedValue::F64(3.0))];
        let expected_qualities = vec![(0u16, 0xC0u8), (1u16, 0x00u8)];
        let expected_ts = vec![(0u16, 200i64), (1u16, 300i64)];
        let expected_sts = vec![(0u16, 2000i64), (1u16, 3000i64)];

        // ── 路径 1：在线 update_typed ──
        let cfg = ConfigHandle::new(sensearcdb_common::AppConfig::default());
        let online = SnapshotStore::new(&cfg, make_sub_mgr(), make_metadata(), make_metrics());
        online.update_typed(
            TenantId::DEFAULT,
            7,
            &[(0, TypedValue::F64(2.0))],
            &[(0, 0xC0)],
            200,
            2000,
            None,
            false,
            None,
        );
        online.update_typed(
            TenantId::DEFAULT,
            7,
            &[(1, TypedValue::F64(3.0))],
            &[(1, 0x00)],
            300,
            3000,
            None,
            false,
            None,
        );
        // 乱序：col0 回退 → 拒；col1 回退 → 拒
        online.update_typed(
            TenantId::DEFAULT,
            7,
            &[(0, TypedValue::F64(1.0))],
            &[(0, 0x00)],
            100,
            1000,
            None,
            false,
            None,
        );
        online.update_typed(
            TenantId::DEFAULT,
            7,
            &[(1, TypedValue::F64(1.5))],
            &[(1, 0xC0)],
            150,
            1500,
            None,
            false,
            None,
        );
        let online_entry = online.get(7).unwrap();

        // ── 路径 2：WAL-only 重建 ──
        let wal_records = vec![
            WalReplayRecord {
                tenant_id: TenantId::DEFAULT,
                table_id: 7,
                ts: 200,
                server_ts: 2000,
                seq: 1,
                values: vec![(0, TypedValue::F64(2.0))],
                qualities: vec![(0, 0xC0)],
                schema_version: 1,
            },
            WalReplayRecord {
                tenant_id: TenantId::DEFAULT,
                table_id: 7,
                ts: 300,
                server_ts: 3000,
                seq: 2,
                values: vec![(1, TypedValue::F64(3.0))],
                qualities: vec![(1, 0x00)],
                schema_version: 1,
            },
            WalReplayRecord {
                tenant_id: TenantId::DEFAULT,
                table_id: 7,
                ts: 100,
                server_ts: 1000,
                seq: 3,
                values: vec![(0, TypedValue::F64(1.0))],
                qualities: vec![(0, 0x00)],
                schema_version: 1,
            },
            WalReplayRecord {
                tenant_id: TenantId::DEFAULT,
                table_id: 7,
                ts: 150,
                server_ts: 1500,
                seq: 4,
                values: vec![(1, TypedValue::F64(1.5))],
                qualities: vec![(1, 0xC0)],
                schema_version: 1,
            },
        ];
        let wal_reader: WalReplayReaderRef = Arc::new(MockWalReader {
            records: wal_records,
        });
        let wal_config = RecoveryConfig {
            snapshot_path: snapshot_path.clone(),
            shard_count: 4,
            sac_watermark: 0,
        };
        let wal_rr = recover(
            &wal_config,
            make_sub_mgr(),
            make_metadata(),
            make_metrics(),
            None,
            Some(wal_reader),
            None,
        )
        .unwrap();
        let wal_entry = wal_rr.store.get(7).unwrap();

        // ── 路径 3：SAC 重建 ──
        let sac_rows = vec![
            SacScanRow {
                tenant_id: TenantId::DEFAULT,
                table_id: 7,
                ts: 200,
                server_ts: 2000,
                values: vec![(0, TypedValue::F64(2.0))],
                qualities: vec![(0, 0xC0)],
            },
            SacScanRow {
                tenant_id: TenantId::DEFAULT,
                table_id: 7,
                ts: 300,
                server_ts: 3000,
                values: vec![(1, TypedValue::F64(3.0))],
                qualities: vec![(1, 0x00)],
            },
            SacScanRow {
                tenant_id: TenantId::DEFAULT,
                table_id: 7,
                ts: 100,
                server_ts: 1000,
                values: vec![(0, TypedValue::F64(1.0))],
                qualities: vec![(0, 0x00)],
            },
            SacScanRow {
                tenant_id: TenantId::DEFAULT,
                table_id: 7,
                ts: 150,
                server_ts: 1500,
                values: vec![(1, TypedValue::F64(1.5))],
                qualities: vec![(1, 0xC0)],
            },
        ];
        let sac_reader: SacRecoveryReaderRef = Arc::new(MockSacReader { rows: sac_rows });
        let sac_config = RecoveryConfig {
            snapshot_path: snapshot_path.clone(),
            shard_count: 4,
            sac_watermark: 0,
        };
        let sac_rr = recover(
            &sac_config,
            make_sub_mgr(),
            make_metadata(),
            make_metrics(),
            Some(sac_reader),
            None,
            None,
        )
        .unwrap();
        let sac_entry = sac_rr.store.get(7).unwrap();

        // ── 三路径终态一致 ──
        assert_eq!(online_entry.values, expected_values);
        assert_eq!(online_entry.values, sac_entry.values);
        assert_eq!(online_entry.values, wal_entry.values);
        assert_eq!(online_entry.qualities, expected_qualities);
        assert_eq!(online_entry.qualities, sac_entry.qualities);
        assert_eq!(online_entry.qualities, wal_entry.qualities);
        assert_eq!(online_entry.ts_values, expected_ts);
        assert_eq!(online_entry.ts_values, sac_entry.ts_values);
        assert_eq!(online_entry.ts_values, wal_entry.ts_values);
        assert_eq!(online_entry.server_ts_values, expected_sts);
        assert_eq!(online_entry.server_ts_values, sac_entry.server_ts_values);
        assert_eq!(online_entry.server_ts_values, wal_entry.server_ts_values);
        assert_eq!(online_entry.ts, 300);
        assert_eq!(online_entry.ts, sac_entry.ts);
        assert_eq!(online_entry.ts, wal_entry.ts);
        assert_eq!(online_entry.server_ts, 3000);
        assert_eq!(online_entry.server_ts, sac_entry.server_ts);
        assert_eq!(online_entry.server_ts, wal_entry.server_ts);
    }

    /// WAL-only 重建对「非 column_id 升序」的 record.values/qualities 保序后再归并。
    #[test]
    fn test_recover_wal_only_unsorted_record_values() {
        let dir = tempdir().unwrap();
        let config = RecoveryConfig {
            snapshot_path: dir.path().join("nonexistent.bin"),
            shard_count: 4,
            sac_watermark: 0,
        };

        // 单条 WAL 记录，values/qualities 未按 column_id 升序（模拟写入到达顺序）。
        let records = vec![WalReplayRecord {
            tenant_id: TenantId::DEFAULT,
            table_id: 5,
            ts: 500,
            server_ts: 5000,
            seq: 1,
            values: vec![
                (2, TypedValue::F64(3.0)),
                (0, TypedValue::F64(1.0)),
                (1, TypedValue::I64(2)),
            ],
            qualities: vec![(2, 0xC0), (0, 0x00), (1, 0xC0)],
            schema_version: 1,
        }];
        let wal_reader: WalReplayReaderRef = Arc::new(MockWalReader { records });
        let rr = recover(
            &config,
            make_sub_mgr(),
            make_metadata(),
            make_metrics(),
            None,
            Some(wal_reader),
            None,
        )
        .unwrap();

        let entry = rr.store.get(5).unwrap();
        // 归并前已按 column_id 升序，ts/server_ts 与列对齐。
        assert_eq!(
            entry.values,
            vec![
                (0, TypedValue::F64(1.0)),
                (1, TypedValue::I64(2)),
                (2, TypedValue::F64(3.0)),
            ]
        );
        assert_eq!(entry.qualities, vec![(0, 0x00), (1, 0xC0), (2, 0xC0)]);
        assert_eq!(entry.ts_values, vec![(0, 500), (1, 500), (2, 500)]);
        assert_eq!(
            entry.server_ts_values,
            vec![(0, 5000), (1, 5000), (2, 5000)]
        );
    }

    #[test]
    fn test_wal_reader_respects_after_seq() {
        // Direct test: WalReplayReader::read_after filters correctly by seq
        let records = vec![
            WalReplayRecord {
                tenant_id: sensearcdb_common::TenantId::DEFAULT,
                table_id: 1,
                ts: 100,
                server_ts: 200,
                seq: 100,
                values: values_of(1.0),
                qualities: qualities_of(0xC0),
                schema_version: 1,
            },
            WalReplayRecord {
                tenant_id: sensearcdb_common::TenantId::DEFAULT,
                table_id: 2,
                ts: 500,
                server_ts: 600,
                seq: 500,
                values: values_of(2.0),
                qualities: qualities_of(0xC0),
                schema_version: 1,
            },
            WalReplayRecord {
                tenant_id: sensearcdb_common::TenantId::DEFAULT,
                table_id: 3,
                ts: 1000,
                server_ts: 1100,
                seq: 1000,
                values: values_of(3.0),
                qualities: qualities_of(0xC0),
                schema_version: 1,
            },
        ];

        let reader = MockWalReader { records };

        // after_seq = 300 → only records with seq > 300
        let filtered = reader.read_after(300).unwrap();
        assert_eq!(filtered.len(), 2);
        assert_eq!(filtered[0].table_id, 2);
        assert_eq!(filtered[1].table_id, 3);

        // after_seq = 0 → all records
        let all = reader.read_after(0).unwrap();
        assert_eq!(all.len(), 3);

        // after_seq = 2000 → none
        let none = reader.read_after(2000).unwrap();
        assert!(none.is_empty());
    }

    #[test]
    fn test_wal_after_seq_boundary_excluded() {
        // Records with seq == after_seq should be excluded (strict >)
        let records = vec![
            WalReplayRecord {
                tenant_id: sensearcdb_common::TenantId::DEFAULT,
                table_id: 1,
                ts: 500,
                server_ts: 600,
                seq: 500, // exactly at boundary
                values: values_of(10.0),
                qualities: qualities_of(0xC0),
                schema_version: 1,
            },
            WalReplayRecord {
                tenant_id: sensearcdb_common::TenantId::DEFAULT,
                table_id: 2,
                ts: 501,
                server_ts: 601,
                seq: 501,
                values: values_of(20.0),
                qualities: qualities_of(0xC0),
                schema_version: 1,
            },
        ];

        let reader = MockWalReader { records };

        // after_seq = 500 → only records with seq > 500
        let filtered = reader.read_after(500).unwrap();
        assert_eq!(filtered.len(), 1, "seq == after_seq should be excluded");
        assert_eq!(filtered[0].table_id, 2);
        assert!((filtered[0].values[0].1.as_f64() - 20.0).abs() < 1e-12);
    }

    #[test]
    fn test_wal_schema_version_zero_still_replays() {
        // schema_version=0 should not cause skip; WAL replay is best-effort
        let dir = tempdir().unwrap();
        let config = RecoveryConfig {
            snapshot_path: dir.path().join("nonexistent.bin"),
            shard_count: 4,
            sac_watermark: 0,
        };

        let records = vec![WalReplayRecord {
            tenant_id: sensearcdb_common::TenantId::DEFAULT,
            table_id: 1,
            ts: 100,
            server_ts: 200,
            seq: 1,
            values: values_of(42.0),
            qualities: qualities_of(0xC0),
            schema_version: 0, // unset / corrupted
        }];

        let wal_reader: WalReplayReaderRef = Arc::new(MockWalReader { records });

        let result = recover(
            &config,
            make_sub_mgr(),
            make_metadata(),
            make_metrics(),
            None,
            Some(wal_reader),
            None,
        );

        assert!(result.is_ok());
        let rr = result.unwrap();
        assert_eq!(rr.entry_count, 1);
        let entry = rr.store.get(1).unwrap();
        assert!((entry.value() - 42.0).abs() < 1e-12);
    }

    #[test]
    fn test_recover_sac_fallback_to_wal() {
        // SAC reader returns error → falls through to WAL
        struct FailingSacReader;
        impl SacRecoveryReader for FailingSacReader {
            fn scan_all(&self) -> Result<Vec<SacScanRow>, RecoveryError> {
                Err(RecoveryError::Other("SAC scan failed".into()))
            }
        }

        let dir = tempdir().unwrap();
        let config = RecoveryConfig {
            snapshot_path: dir.path().join("nonexistent.bin"),
            shard_count: 4,
            sac_watermark: 0,
        };

        let sac_reader: SacRecoveryReaderRef = Arc::new(FailingSacReader);
        let wal_records = vec![WalReplayRecord {
            tenant_id: sensearcdb_common::TenantId::DEFAULT,
            table_id: 99,
            ts: 500,
            server_ts: 600,
            seq: 1,
            values: values_of(99.0),
            qualities: qualities_of(0xC0),
            schema_version: 1,
        }];
        let wal_reader: WalReplayReaderRef = Arc::new(MockWalReader {
            records: wal_records,
        });

        let result = recover(
            &config,
            make_sub_mgr(),
            make_metadata(),
            make_metrics(),
            Some(sac_reader),
            Some(wal_reader),
            None,
        );

        assert!(result.is_ok());
        let rr = result.unwrap();
        // SAC failed, fell through to WAL
        assert_eq!(rr.source, RecoverySource::WalOnly);
        assert_eq!(rr.entry_count, 1);
        let entry = rr.store.get(99).unwrap();
        assert!((entry.value() - 99.0).abs() < 1e-12);
    }

    #[test]
    fn test_recover_snapshot_bin_supersedes_sac() {
        // When snapshot.bin exists and is valid, it should be used even if SAC reader is available
        use crate::serialize;
        use crate::store::SnapshotStore;
        use sensearcdb_common::ConfigHandle;

        let dir = tempdir().unwrap();
        let snapshot_path = dir.path().join("valid_snapshot.bin");
        let shard_count = 4;

        // Create valid snapshot.bin with 5 entries
        let cfg = ConfigHandle::new(sensearcdb_common::AppConfig::default());
        let sub_mgr = Arc::new(SnapshotSubscriptionManager::new());
        let metadata = Arc::new(TagMetadataStore::new(Arc::new(TestSchemaMirror)));
        let metrics = SnapshotMetrics::noop();

        {
            let store =
                SnapshotStore::new(&cfg, sub_mgr.clone(), metadata.clone(), metrics.clone());
            for i in 1u32..=5u32 {
                store.update_typed(
                    sensearcdb_common::TenantId::DEFAULT,
                    i,
                    &[(0, TypedValue::F64(i as f64 * 10.0))],
                    &[(0, 0xC0)],
                    1000,
                    2000,
                    None,
                    false,
                    None,
                );
            }
            let (guards, version) = store.snapshot_all_buckets();
            serialize::serialize_to_file(&snapshot_path, &guards, version, &metrics, None, 0)
                .unwrap();
        }

        let config = RecoveryConfig {
            snapshot_path,
            shard_count,
            sac_watermark: 0,
        };

        // SAC reader has different data — should NOT be used
        let sac_rows = vec![SacScanRow {
            tenant_id: sensearcdb_common::TenantId::DEFAULT,
            table_id: 999,
            ts: 5000,
            server_ts: 6000,
            values: values_of(999.0),
            qualities: qualities_of(0xC0),
        }];
        let sac_reader: SacRecoveryReaderRef = Arc::new(MockSacReader { rows: sac_rows });

        let result = recover(
            &config,
            Arc::new(SnapshotSubscriptionManager::new()),
            Arc::new(TagMetadataStore::new(Arc::new(TestSchemaMirror))),
            SnapshotMetrics::noop(),
            Some(sac_reader),
            None,
            None,
        );

        assert!(result.is_ok());
        let rr = result.unwrap();
        // snapshot.bin wins — SAC not consulted
        assert_eq!(rr.source, RecoverySource::SnapshotBin);
        assert_eq!(rr.entry_count, 5);
        // Tag 999 from SAC should NOT be present
        assert!(rr.store.get(999).is_none());
        // Tag 1 from snapshot.bin should be present
        assert!(rr.store.get(1).is_some());
    }

    // ─── Trait object safety tests ───

    #[test]
    fn test_trait_objects_are_send_sync() {
        // Compile-time assertion: trait objects must be Send + Sync
        fn _assert_send_sync<T: Send + Sync>(_t: T) {}

        let sac: SacRecoveryReaderRef = Arc::new(NoopSacRecoveryReader);
        _assert_send_sync(sac);

        let wal: WalReplayReaderRef = Arc::new(NoopWalReplayReader);
        _assert_send_sync(wal);
    }

    // ─── TDE（O-3）恢复路径 ───

    /// XOR 型加密桩（与 serialize.rs 内的桩一致，供恢复路径测试）。
    struct SnapshotTestEncryptor;

    impl crate::encryption::SnapshotEncryptor for SnapshotTestEncryptor {
        fn encrypt(
            &self,
            plaintext: &[u8],
        ) -> Result<Vec<u8>, crate::encryption::SnapshotEncryptError> {
            let mut out = Vec::with_capacity(28 + plaintext.len());
            out.extend_from_slice(&[0xA1u8; 12]);
            out.extend_from_slice(&[0xB2u8; 16]);
            for b in plaintext {
                out.push(b ^ 0x5A);
            }
            Ok(out)
        }

        fn decrypt(
            &self,
            blob: &[u8],
            _dek_version: u32,
        ) -> Result<Vec<u8>, crate::encryption::SnapshotEncryptError> {
            Ok(blob[28..].iter().map(|b| b ^ 0x5A).collect())
        }

        fn active_dek_version(&self) -> u32 {
            1
        }

        fn is_enabled(&self) -> bool {
            true
        }
    }

    /// 写加密快照 → `recover()` 带密钥解密恢复（source = SnapshotBin）。
    #[test]
    fn test_recover_encrypted_snapshot_bin_roundtrip() {
        use crate::serialize;
        use crate::store::SnapshotStore;
        use sensearcdb_common::ConfigHandle;

        let dir = tempdir().unwrap();
        let snapshot_path = dir.path().join("encrypted_snapshot.bin");
        let shard_count = 2;

        let cfg = ConfigHandle::new(sensearcdb_common::AppConfig::default());
        let sub_mgr = Arc::new(SnapshotSubscriptionManager::new());
        let metadata = Arc::new(TagMetadataStore::new(Arc::new(TestSchemaMirror)));
        let metrics = SnapshotMetrics::noop();

        // 写加密快照。
        {
            let store =
                SnapshotStore::new(&cfg, sub_mgr.clone(), metadata.clone(), metrics.clone());
            for i in 1u32..=10u32 {
                store.update_typed(
                    sensearcdb_common::TenantId::DEFAULT,
                    i,
                    &[(0, TypedValue::F64(i as f64))],
                    &[(0, 0xC0)],
                    1000 + i as i64,
                    2000 + i as i64,
                    None,
                    false,
                    None,
                );
            }
            let (guards, version) = store.snapshot_all_buckets();
            let enc = SnapshotTestEncryptor;
            serialize::serialize_to_file(&snapshot_path, &guards, version, &metrics, Some(&enc), 0)
                .expect("serialize encrypted snapshot");
        }

        let config = RecoveryConfig {
            snapshot_path: snapshot_path.clone(),
            shard_count,
            sac_watermark: 0,
        };

        let enc: SharedSnapshotEncryptor = Some(Arc::new(SnapshotTestEncryptor));
        let result = recover(
            &config,
            Arc::new(SnapshotSubscriptionManager::new()),
            Arc::new(TagMetadataStore::new(Arc::new(TestSchemaMirror))),
            SnapshotMetrics::noop(),
            None,
            None,
            enc,
        );

        assert!(result.is_ok());
        let rr = result.unwrap();
        assert_eq!(rr.source, RecoverySource::SnapshotBin);
        assert_eq!(rr.entry_count, 10);
        assert!((rr.store.get(1).unwrap().value() - 1.0).abs() < 1e-12);
    }

    /// 写加密快照 → `recover()` 无密钥必须硬报错（禁止回退到 SAC）。
    #[test]
    fn test_recover_encrypted_snapshot_no_key_hard_errors() {
        use crate::serialize;
        use crate::store::SnapshotStore;
        use sensearcdb_common::ConfigHandle;

        let dir = tempdir().unwrap();
        let snapshot_path = dir.path().join("encrypted_snapshot.bin");
        let shard_count = 2;

        let cfg = ConfigHandle::new(sensearcdb_common::AppConfig::default());
        let sub_mgr = Arc::new(SnapshotSubscriptionManager::new());
        let metadata = Arc::new(TagMetadataStore::new(Arc::new(TestSchemaMirror)));
        let metrics = SnapshotMetrics::noop();

        {
            let store =
                SnapshotStore::new(&cfg, sub_mgr.clone(), metadata.clone(), metrics.clone());
            store.update_typed(
                sensearcdb_common::TenantId::DEFAULT,
                1,
                &[(0, TypedValue::F64(1.0))],
                &[(0, 0xC0)],
                1000,
                1001,
                None,
                false,
                None,
            );
            let (guards, version) = store.snapshot_all_buckets();
            let enc = SnapshotTestEncryptor;
            serialize::serialize_to_file(&snapshot_path, &guards, version, &metrics, Some(&enc), 0)
                .expect("serialize encrypted snapshot");
        }

        let config = RecoveryConfig {
            snapshot_path,
            shard_count,
            sac_watermark: 0,
        };

        // 即使有 SAC reader，密文快照无密钥也必须硬报错，不回退。
        let sac_reader: SacRecoveryReaderRef = Arc::new(NoopSacRecoveryReader);
        let result = recover(
            &config,
            Arc::new(SnapshotSubscriptionManager::new()),
            Arc::new(TagMetadataStore::new(Arc::new(TestSchemaMirror))),
            SnapshotMetrics::noop(),
            Some(sac_reader),
            None,
            None, // 无密钥
        );

        assert!(
            result.is_err(),
            "encrypted snapshot without key must hard-error"
        );
        assert!(result.err().unwrap().is_no_encryptor());
    }

    // ─── 恢复边界对齐（3.3 档 A：精确 seq）───

    /// 记录 `read_after` 收到的 after_seq 的 WAL 读取器，用于断言恢复边界。
    struct CapturingWalReader {
        calls: std::sync::Mutex<Vec<u64>>,
    }

    impl WalReplayReader for CapturingWalReader {
        fn read_after(&self, after_seq: u64) -> Result<Vec<WalReplayRecord>, RecoveryError> {
            self.calls.lock().unwrap().push(after_seq);
            Ok(Vec::new())
        }
    }

    /// snapshot.bin 带 seq 水位恢复时，WAL 回放必须用 `read_after(seq)`（精确边界，
    /// 不再减时间安全余量），而非硬编码 `read_after(0)`。
    #[test]
    fn test_snapshot_recovery_uses_wal_watermark_boundary() {
        use crate::serialize;
        use crate::store::SnapshotStore;
        use sensearcdb_common::ConfigHandle;

        let dir = tempdir().unwrap();
        let snapshot_path = dir.path().join("snapshot.bin");
        let shard_count = 4;
        let watermark: i64 = 42;

        // 写带 seq 水位的 snapshot.bin。
        let cfg = ConfigHandle::new(sensearcdb_common::AppConfig::default());
        let sub_mgr = Arc::new(SnapshotSubscriptionManager::new());
        let metadata = Arc::new(TagMetadataStore::new(Arc::new(TestSchemaMirror)));
        let metrics = SnapshotMetrics::noop();
        {
            let store =
                SnapshotStore::new(&cfg, sub_mgr.clone(), metadata.clone(), metrics.clone());
            store.update_typed(
                sensearcdb_common::TenantId::DEFAULT,
                1,
                &[(0, TypedValue::F64(42.0))],
                &[(0, 0xC0)],
                1000,
                2000,
                None,
                false,
                None,
            );
            let (guards, version) = store.snapshot_all_buckets();
            serialize::serialize_to_file(
                &snapshot_path,
                &guards,
                version,
                &metrics,
                None,
                watermark,
            )
            .unwrap();
        }

        let capturing = Arc::new(CapturingWalReader {
            calls: std::sync::Mutex::new(Vec::new()),
        });
        let wal_reader: WalReplayReaderRef = capturing.clone();

        let config = RecoveryConfig {
            snapshot_path,
            shard_count,
            sac_watermark: 0,
        };
        let result = recover(
            &config,
            Arc::new(SnapshotSubscriptionManager::new()),
            Arc::new(TagMetadataStore::new(Arc::new(TestSchemaMirror))),
            SnapshotMetrics::noop(),
            None,
            Some(wal_reader),
            None,
        );

        assert!(result.is_ok());
        let calls = capturing.calls.lock().unwrap();
        assert_eq!(calls.len(), 1, "snapshot 恢复应只调用一次 read_after");
        assert_eq!(
            calls[0], watermark as u64,
            "应使用精确 seq 水位（不减时间安全余量）而非 0"
        );
        assert_ne!(calls[0], 0);
    }

    /// C4（I3）：SAC 兜底恢复必须用 `read_after(S)` 而非硬编码 `read_after(0)`。
    #[test]
    fn test_sac_recovery_uses_sac_watermark_boundary() {
        let dir = tempdir().unwrap();
        // 无 snapshot.bin → 走 SAC 扫描 → WAL 增量回放。
        let config = RecoveryConfig {
            snapshot_path: dir.path().join("nonexistent.bin"),
            shard_count: 4,
            sac_watermark: 42,
        };

        // SAC 返回空（无数据），但仍是成功路径。
        let sac_reader: SacRecoveryReaderRef = Arc::new(MockSacReader { rows: Vec::new() });

        let capturing = Arc::new(CapturingWalReader {
            calls: std::sync::Mutex::new(Vec::new()),
        });
        let wal_reader: WalReplayReaderRef = capturing.clone();

        let result = recover(
            &config,
            make_sub_mgr(),
            make_metadata(),
            make_metrics(),
            Some(sac_reader),
            Some(wal_reader),
            None,
        );

        assert!(result.is_ok());
        let calls = capturing.calls.lock().unwrap();
        assert_eq!(calls.len(), 1, "SAC 恢复应只调用一次 read_after");
        assert_eq!(calls[0], 42, "SAC 兜底恢复应使用落盘水位 S（而非硬编码 0）");
    }

    /// D4 安全护栏：崩溃恢复回归 — 相同时间戳多 batch 的 seq 边界不丢不重。
    ///
    /// 场景（时间水位方案会出错的关键场景）：快照水位 seq=10，WAL 里有 seq 9/10/11/12
    /// 四帧，且四帧 `server_ts` **完全相同**。时间水位会把 seq 11/12 一起过滤掉 → 丢数据；
    /// seq 精确边界则 `seq > 10` 严格回放 seq 11/12 → 最终值取 seq 12（source ts 递增，
    /// 列级 ts max 收敛到最新一帧）。
    #[test]
    fn test_seq_boundary_no_loss_no_dup_same_ts_multi_batch() {
        use crate::serialize;
        use crate::store::SnapshotStore;
        use sensearcdb_common::ConfigHandle;

        let dir = tempdir().unwrap();
        let snapshot_path = dir.path().join("snapshot.bin");
        let shard_count = 4;
        // 快照水位 = seq 10：snapshot.bin 内已包含 seq<=10 的所有写入。
        let watermark: i64 = 10;

        let cfg = ConfigHandle::new(sensearcdb_common::AppConfig::default());
        let sub_mgr = Arc::new(SnapshotSubscriptionManager::new());
        let metadata = Arc::new(TagMetadataStore::new(Arc::new(TestSchemaMirror)));
        let metrics = SnapshotMetrics::noop();
        {
            // 快照内 tag=1 的当前值 = seq 10 写入的值（2.0）。
            let store =
                SnapshotStore::new(&cfg, sub_mgr.clone(), metadata.clone(), metrics.clone());
            store.update_typed(
                sensearcdb_common::TenantId::DEFAULT,
                1,
                &[(0, TypedValue::F64(2.0))],
                &[(0, 0xC0)],
                1000,
                1000,
                None,
                false,
                None,
            );
            let (guards, version) = store.snapshot_all_buckets();
            serialize::serialize_to_file(
                &snapshot_path,
                &guards,
                version,
                &metrics,
                None,
                watermark,
            )
            .unwrap();
        }

        // WAL 四帧：server_ts 全部 = 1000（相同时间戳多 batch），仅 seq 不同。
        let records = vec![
            WalReplayRecord {
                tenant_id: sensearcdb_common::TenantId::DEFAULT,
                table_id: 1,
                ts: 1000,
                server_ts: 1000,
                seq: 9,
                values: values_of(0.9),
                qualities: qualities_of(0xC0),
                schema_version: 1,
            },
            WalReplayRecord {
                tenant_id: sensearcdb_common::TenantId::DEFAULT,
                table_id: 1,
                ts: 1000,
                server_ts: 1000,
                seq: 10,
                values: values_of(2.0),
                qualities: qualities_of(0xC0),
                schema_version: 1,
            },
            WalReplayRecord {
                tenant_id: sensearcdb_common::TenantId::DEFAULT,
                table_id: 1,
                ts: 1001,
                server_ts: 1000,
                seq: 11,
                values: values_of(3.0),
                qualities: qualities_of(0xC0),
                schema_version: 1,
            },
            WalReplayRecord {
                tenant_id: sensearcdb_common::TenantId::DEFAULT,
                table_id: 1,
                ts: 1002,
                server_ts: 1000,
                seq: 12,
                values: values_of(4.0),
                qualities: qualities_of(0xC0),
                schema_version: 1,
            },
        ];
        let wal_reader: WalReplayReaderRef = Arc::new(MockWalReader { records });

        let config = RecoveryConfig {
            snapshot_path,
            shard_count,
            sac_watermark: 0,
        };
        let result = recover(
            &config,
            Arc::new(SnapshotSubscriptionManager::new()),
            Arc::new(TagMetadataStore::new(Arc::new(TestSchemaMirror))),
            SnapshotMetrics::noop(),
            None,
            Some(wal_reader),
            None,
        );

        assert!(result.is_ok());
        let rr = result.unwrap();
        assert_eq!(rr.source, RecoverySource::SnapshotBin);

        // 最终值必须为 seq=12 的 4.0：seq 9/10 不重放（不重），seq 11/12 都回放（不丢），
        // 列级 ts max（source ts 递增）收敛到 seq 12。
        let entry = rr.store.get(1).unwrap();
        assert!(
            (entry.value() - 4.0).abs() < 1e-12,
            "seq 边界回放后最终值应为 4.0（seq 12），实际 {}",
            entry.value()
        );
    }

    /// FAIL-2 回归：相同 source ts + 不同 seq 的 WAL 增量回放必须取更高 seq（不丢不重）。
    ///
    /// 快照水位 seq=1（内含 ts=1000 的值 1.0），WAL 里有 seq=2（同样 ts=1000 的值 2.0）。
    /// 旧「源 ts 严格 >」会拒绝 seq=2 → 值停在 1.0；重放语义应让 seq=2 覆盖 → 2.0。
    #[test]
    fn test_replay_same_ts_higher_seq_wins() {
        use crate::serialize;
        use crate::store::SnapshotStore;
        use sensearcdb_common::ConfigHandle;

        let dir = tempdir().unwrap();
        let snapshot_path = dir.path().join("snapshot.bin");
        let watermark: i64 = 1;

        let cfg = ConfigHandle::new(sensearcdb_common::AppConfig::default());
        let sub_mgr = Arc::new(SnapshotSubscriptionManager::new());
        let metadata = Arc::new(TagMetadataStore::new(Arc::new(TestSchemaMirror)));
        let metrics = SnapshotMetrics::noop();
        {
            // 快照内 tag=1 的当前值 = seq 1 写入的值（1.0），ts=1000。
            let store =
                SnapshotStore::new(&cfg, sub_mgr.clone(), metadata.clone(), metrics.clone());
            store.update_typed(
                sensearcdb_common::TenantId::DEFAULT,
                1,
                &[(0, TypedValue::F64(1.0))],
                &[(0, 0xC0)],
                1000,
                1000,
                None,
                false,
                None,
            );
            let (guards, version) = store.snapshot_all_buckets();
            serialize::serialize_to_file(
                &snapshot_path,
                &guards,
                version,
                &metrics,
                None,
                watermark,
            )
            .unwrap();
        }

        // WAL 只有 seq=2：ts 与快照完全相同，仅 seq 更大。
        let records = vec![WalReplayRecord {
            tenant_id: sensearcdb_common::TenantId::DEFAULT,
            table_id: 1,
            ts: 1000,
            server_ts: 1000,
            seq: 2,
            values: values_of(2.0),
            qualities: qualities_of(0xC0),
            schema_version: 1,
        }];
        let wal_reader: WalReplayReaderRef = Arc::new(MockWalReader { records });

        let result = recover(
            &RecoveryConfig {
                snapshot_path,
                shard_count: 4,
                sac_watermark: 0,
            },
            Arc::new(SnapshotSubscriptionManager::new()),
            Arc::new(TagMetadataStore::new(Arc::new(TestSchemaMirror))),
            SnapshotMetrics::noop(),
            None,
            Some(wal_reader),
            None,
        );

        assert!(result.is_ok());
        let rr = result.unwrap();
        assert_eq!(rr.source, RecoverySource::SnapshotBin);
        let entry = rr.store.get(1).unwrap();
        assert!(
            (entry.value() - 2.0).abs() < 1e-12,
            "相同 ts 回放后最终值应为 2.0（seq 2），实际 {}",
            entry.value()
        );
    }
}
